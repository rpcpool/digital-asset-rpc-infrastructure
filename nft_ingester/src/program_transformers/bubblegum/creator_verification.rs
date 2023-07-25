use blockbuster::{
    instruction::InstructionBundle,
    programs::bubblegum::{BubblegumInstruction, LeafSchema, Payload},
};
use digital_asset_types::dao::{asset, asset_creators};
use log::{debug, info};
use mpl_bubblegum::{
    hash_creators, hash_metadata,
    state::metaplex_adapter::{Creator, MetadataArgs},
};
use sea_orm::{ConnectionTrait, Set, TransactionTrait, Unchanged};

use crate::{
    error::IngesterError,
    program_transformers::bubblegum::{
        update_creator, upsert_asset_with_leaf_info, upsert_asset_with_owner_and_delegate_info,
        upsert_asset_with_seq, upsert_asset_hashes_with_seq,
    }
};

use super::save_changelog_event;

pub async fn process<'c, T>(
    parsing_result: &BubblegumInstruction,
    bundle: &InstructionBundle<'c>,
    txn: &'c T,
    value: bool,
) -> Result<(), IngesterError>
where
    T: ConnectionTrait + TransactionTrait,
{
    if let (Some(le), Some(cl), Some(payload)) = (
        &parsing_result.leaf_update,
        &parsing_result.tree_update,
        &parsing_result.payload,
    ) {
        let (creator, verify, creator_hash, data_hash, metadata) = match payload {
            Payload::CreatorVerification {
                creator,
                verify,
                creator_hash,
                data_hash,
                args,
            } => (creator, verify, creator_hash, data_hash, args),
            _ => {
                return Err(IngesterError::ParsingError(
                    "Ix not parsed correctly".to_string(),
                ));
            }
        };
        debug!(
            "Handling creator verification event for creator {} (verify: {}): {}",
            creator, verify, bundle.txn_id
        );
        let seq = save_changelog_event(cl, bundle.slot, bundle.txn_id, txn).await?;

        let updated_creators = metadata
            .creators
            .iter()
            .map(|c| {
                let verified = if c.address == creator.clone() {
                    verify.clone()
                } else {
                    c.verified
                };
                Creator {
                    address: c.address,
                    verified,
                    share: c.share,
                }
            })
            .collect::<Vec<Creator>>();
        let mut updated_metadata = metadata.clone();
        updated_metadata.creators = updated_creators;
        let updated_data_hash = hash_metadata(&updated_metadata)
            .map(|e| bs58::encode(e).into_string())
            .unwrap_or("".to_string())
            .trim()
            .to_string();
        let updated_creator_hash = hash_creators(&updated_metadata.creators)
            .map(|e| bs58::encode(e).into_string())
            .unwrap_or("".to_string())
            .trim()
            .to_string();

        let asset_id_bytes = match le.schema {
            LeafSchema::V1 {
                id,
                owner,
                delegate,
                ..
            } => {
                let id_bytes = id.to_bytes().to_vec();
                let owner_bytes = owner.to_bytes().to_vec();
                let delegate = if owner == delegate {
                    None
                } else {
                    Some(delegate.to_bytes().to_vec())
                };

                // Partial update of asset table with just leaf.
                upsert_asset_with_leaf_info(
                    txn,
                    id_bytes.to_vec(),
                    Some(le.leaf_hash.to_vec()),
                    Some(seq as i64),
                    false,
                )
                .await?;

                // Partial update of asset table with just leaf owner and delegate.
                upsert_asset_with_owner_and_delegate_info(
                    txn,
                    id_bytes.to_vec(),
                    owner_bytes,
                    delegate,
                    seq as i64,
                )
                .await?;

                // Partial update of asset table with just seq.
                upsert_asset_with_seq(txn, id_bytes.to_vec(), seq as i64).await?;

                // Partial update of asset table with just creator hash.
                upsert_asset_hashes_with_seq(txn, id_bytes.to_vec(), seq as i64, updated_data_hash, updated_creator_hash).await?;

                id_bytes.to_vec()
            } //_ => return Err(IngesterError::NotImplemented),
        };

        // The primary key `id` is not required here since `update_creator` uses `update_many`
        // for the time being.
        let creator_to_update = asset_creators::ActiveModel {
            //id: Unchanged(14),
            verified: Set(value),
            seq: Set(seq as i64),
            ..Default::default()
        };

        update_creator(
            txn,
            asset_id_bytes,
            creator.to_bytes().to_vec(),
            seq,
            creator_to_update,
        )
        .await?;

        return Ok(());
    }
    Err(IngesterError::ParsingError(
        "Ix not parsed correctly".to_string(),
    ))
}
