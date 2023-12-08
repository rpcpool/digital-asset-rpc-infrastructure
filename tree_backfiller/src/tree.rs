use anyhow::Result;
use borsh::BorshDeserialize;
use clap::Args;
use digital_asset_types::dao::tree_transactions;
use flatbuffers::FlatBufferBuilder;
use log::debug;
use plerkle_messenger::{Messenger, TRANSACTION_BACKFILL_STREAM};
use plerkle_serialization::serializer::seralize_encoded_transaction_with_status;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder};
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_client::GetConfirmedSignaturesForAddress2Config,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig, RpcTransactionConfig},
    rpc_filter::{Memcmp, RpcFilterType},
};
use solana_sdk::{account::Account, pubkey::Pubkey, signature::Signature};
use solana_transaction_status::UiTransactionEncoding;
use spl_account_compression::id;
use spl_account_compression::state::{
    merkle_tree_get_size, ConcurrentMerkleTreeHeader, ConcurrentMerkleTreeHeaderDataV1,
    CONCURRENT_MERKLE_TREE_HEADER_SIZE_V1,
};
use sqlx::{Pool, Postgres};
use std::str::FromStr;
use std::sync::Arc;
use thiserror::Error as ThisError;
use tokio::sync::mpsc::Sender;

const GET_SIGNATURES_FOR_ADDRESS_LIMIT: usize = 1000;

#[derive(Debug, Clone, Default, Args)]
pub struct ConfigBackfiller {
    /// Solana RPC URL
    #[arg(long, env)]
    pub solana_rpc_url: String,
}

#[derive(ThisError, Debug)]
pub enum TreeErrorKind {
    #[error("solana rpc")]
    Rpc(#[from] solana_client::client_error::ClientError),
    #[error("anchor")]
    Achor(#[from] anchor_client::anchor_lang::error::Error),
    #[error("perkle serialize")]
    PerkleSerialize(#[from] plerkle_serialization::error::PlerkleSerializationError),
}
#[derive(Debug, Clone)]
pub struct TreeHeaderResponse {
    pub max_depth: u32,
    pub max_buffer_size: u32,
    pub creation_slot: u64,
    pub size: usize,
}

impl TryFrom<ConcurrentMerkleTreeHeader> for TreeHeaderResponse {
    type Error = TreeErrorKind;

    fn try_from(payload: ConcurrentMerkleTreeHeader) -> Result<Self, Self::Error> {
        let size = merkle_tree_get_size(&payload)?;
        Ok(Self {
            max_depth: payload.get_max_depth(),
            max_buffer_size: payload.get_max_buffer_size(),
            creation_slot: payload.get_creation_slot(),
            size,
        })
    }
}

#[derive(Debug, Clone)]
pub struct TreeResponse {
    pub pubkey: Pubkey,
    pub tree_header: TreeHeaderResponse,
}

impl TreeResponse {
    pub fn from_rpc(pubkey: Pubkey, account: Account) -> Result<Self> {
        let (header_bytes, _rest) = account.data.split_at(CONCURRENT_MERKLE_TREE_HEADER_SIZE_V1);
        let header: ConcurrentMerkleTreeHeader =
            ConcurrentMerkleTreeHeader::try_from_slice(header_bytes)?;

        let (auth, _) = Pubkey::find_program_address(&[pubkey.as_ref()], &mpl_bubblegum::ID);

        header.assert_valid_authority(&auth)?;

        let tree_header = header.try_into()?;

        Ok(Self {
            pubkey,
            tree_header,
        })
    }
}

pub async fn all(client: &Arc<RpcClient>) -> Result<Vec<TreeResponse>, TreeErrorKind> {
    let config = RpcProgramAccountsConfig {
        filters: Some(vec![RpcFilterType::Memcmp(Memcmp::new_raw_bytes(
            0,
            vec![1u8],
        ))]),
        account_config: RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::Base64),
            ..RpcAccountInfoConfig::default()
        },
        ..RpcProgramAccountsConfig::default()
    };

    Ok(client
        .get_program_accounts_with_config(&id(), config)
        .await?
        .into_iter()
        .filter_map(|(pubkey, account)| TreeResponse::from_rpc(pubkey, account).ok())
        .collect())
}

pub async fn crawl(
    client: Arc<RpcClient>,
    sig_sender: Sender<Signature>,
    conn: &DatabaseConnection,
    tree: TreeResponse,
) -> Result<()> {
    let mut before = None;

    let until = tree_transactions::Entity::find()
        .filter(tree_transactions::Column::Tree.eq(tree.pubkey.as_ref()))
        .order_by_desc(tree_transactions::Column::Slot)
        .one(conn)
        .await?
        .map(|t| Signature::from_str(&t.signature).ok())
        .flatten();

    loop {
        let sigs = client
            .get_signatures_for_address_with_config(
                &tree.pubkey,
                GetConfirmedSignaturesForAddress2Config {
                    before,
                    until,
                    ..GetConfirmedSignaturesForAddress2Config::default()
                },
            )
            .await?;

        for sig in sigs.iter() {
            let sig = Signature::from_str(&sig.signature)?;
            println!("sig: {}", sig);

            sig_sender.send(sig.clone()).await?;

            before = Some(sig);
        }

        if sigs.len() < GET_SIGNATURES_FOR_ADDRESS_LIMIT {
            break;
        }
    }

    Ok(())
}

pub async fn transaction<'a>(
    client: Arc<RpcClient>,
    signature: Signature,
) -> Result<FlatBufferBuilder<'a>, TreeErrorKind> {
    let transaction = client
        .get_transaction_with_config(
            &signature,
            RpcTransactionConfig {
                encoding: Some(UiTransactionEncoding::Base58),
                max_supported_transaction_version: Some(0),
                ..RpcTransactionConfig::default()
            },
        )
        .await?;

    Ok(seralize_encoded_transaction_with_status(
        FlatBufferBuilder::new(),
        transaction,
    )?)
}
