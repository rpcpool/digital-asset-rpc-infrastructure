use anyhow::Result;
use borsh::BorshDeserialize;
use clap::Args;
use flatbuffers::FlatBufferBuilder;
use log::debug;
use plerkle_messenger::{Messenger, TRANSACTION_BACKFILL_STREAM};
use plerkle_serialization::serializer::seralize_encoded_transaction_with_status;
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_client::GetConfirmedSignaturesForAddress2Config,
    rpc_config::{RpcAccountInfoConfig, RpcBlockConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, RpcFilterType},
};
use solana_sdk::{account::Account, pubkey::Pubkey, signature::Signature};
use solana_transaction_status::UiTransactionEncoding;
use spl_account_compression::id;
use spl_account_compression::state::{
    merkle_tree_get_size, ConcurrentMerkleTreeHeader, ConcurrentMerkleTreeHeaderDataV1,
    CONCURRENT_MERKLE_TREE_HEADER_SIZE_V1,
};
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
    #[error("solana tree gpa")]
    FetchAll(#[from] solana_client::client_error::ClientError),
    #[error("anchor struct deserialize")]
    AchorDeserialize(#[from] anchor_client::anchor_lang::error::Error),
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
        .await
        .map_err(TreeErrorKind::FetchAll)?
        .into_iter()
        .filter_map(|(pubkey, account)| TreeResponse::from_rpc(pubkey, account).ok())
        .collect())
}

pub async fn crawl(
    client: Arc<RpcClient>,
    sig_sender: Sender<Signature>,
    tree: TreeResponse,
) -> Result<()> {
    println!("crawl tree: {:?}", tree.pubkey);

    // TODO: check db for tree_transactions picking the sig of the last processed transaction. `SELECT signature FROM tree_transactions WHERE tree = $1 ORDER BY position ASC LIMIT 1`
    let mut last_sig = None;
    loop {
        let before = last_sig;

        let sigs = client
            .get_signatures_for_address_with_config(
                &tree.pubkey,
                GetConfirmedSignaturesForAddress2Config {
                    before,
                    until: None,
                    ..GetConfirmedSignaturesForAddress2Config::default()
                },
            )
            .await?;

        for sig in sigs.iter() {
            let sig = Signature::from_str(&sig.signature)?;
            println!("send signature: {:?}", sig.clone());

            sig_sender.send(sig.clone()).await?;

            last_sig = Some(sig);
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
        .get_transaction(&signature, UiTransactionEncoding::Base58)
        .await?;

    println!("transaction: {:?}", signature);

    Ok(seralize_encoded_transaction_with_status(
        FlatBufferBuilder::new(),
        transaction,
    )?)
}
