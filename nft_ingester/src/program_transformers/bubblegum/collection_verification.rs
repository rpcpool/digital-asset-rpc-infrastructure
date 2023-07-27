use anchor_lang::prelude::Pubkey;
use blockbuster::{
    instruction::InstructionBundle,
    programs::bubblegum::{BubblegumInstruction, LeafSchema, Payload},
};
use digital_asset_types::dao::{asset, asset_grouping, cl_items};
use log::{debug, info, warn};
use mpl_bubblegum::{hash_metadata, state::metaplex_adapter::Collection};
use sea_orm::{entity::*, query::*, sea_query::OnConflict, DbBackend, Set, Unchanged};
use solana_sdk::keccak;

use super::{save_changelog_event, update_compressed_asset};
use crate::error::IngesterError;
pub async fn process<'c, T>(
    parsing_result: &BubblegumInstruction,
    bundle: &InstructionBundle<'c>,
    txn: &'c T,
    instruction: &str,
) -> Result<(), IngesterError>
where
    T: ConnectionTrait + TransactionTrait,
{
    if let (Some(le), Some(cl), Some(payload)) = (
        &parsing_result.leaf_update,
        &parsing_result.tree_update,
        &parsing_result.payload,
    ) {
        let (collection, verify, metadata) = match payload {
            Payload::CollectionVerification {
                collection,
                verify,
                args,
            } => (collection.clone(), verify.clone(), args.clone()),
            _ => {
                return Err(IngesterError::ParsingError(
                    "Ix not parsed correctly".to_string(),
                ));
            }
        };
        debug!(
            "Handling collection verification event for {} (verify: {}): {}",
            collection, verify, bundle.txn_id
        );
        let seq = save_changelog_event(cl, bundle.slot, bundle.txn_id, txn, instruction).await?;
        let id_bytes = match le.schema {
            LeafSchema::V1 { id, .. } => id.to_bytes().to_vec(),
        };

        let mut updated_metadata = metadata.clone();
        updated_metadata.collection = Some(Collection {
            key: collection.clone(),
            verified: verify,
        });

        let updated_data_hash = hash_metadata(&updated_metadata)
            .map(|e| bs58::encode(e).into_string())
            .unwrap_or("".to_string())
            .trim()
            .to_string();

        let asset_to_update = asset::ActiveModel {
            id: Unchanged(id_bytes.clone()),
            leaf: Set(Some(le.leaf_hash.to_vec())),
            seq: Set(seq as i64),
            data_hash: Set(Some(updated_data_hash.clone())), // todo remove clone
            ..Default::default()
        };
        update_compressed_asset(txn, id_bytes.clone(), Some(seq), asset_to_update).await?;

        if verify {
            let grouping = asset_grouping::ActiveModel {
                asset_id: Set(id_bytes.clone()),
                group_key: Set("collection".to_string()),
                group_value: Set(Some(collection.to_string())),
                seq: Set(seq as i64),
                slot_updated: Set(bundle.slot as i64),
                ..Default::default()
            };
            let mut query = asset_grouping::Entity::insert(grouping)
                .on_conflict(
                    OnConflict::columns([
                        asset_grouping::Column::AssetId,
                        asset_grouping::Column::GroupKey,
                    ])
                    .update_columns([
                        asset_grouping::Column::GroupKey,
                        asset_grouping::Column::GroupValue,
                        asset_grouping::Column::Seq,
                        asset_grouping::Column::SlotUpdated,
                    ])
                    .to_owned(),
                )
                .build(DbBackend::Postgres);
            query.sql = format!(
                    "{} WHERE excluded.slot_updated > asset_grouping.slot_updated AND excluded.seq >= asset_grouping.seq",
                    query.sql
                );
            txn.execute(query).await?;
        } else {
            // TODO: Support collection unverification.
            // We will likely need to nullify the collection field so we can maintain
            // the seq value and avoid out-of-order indexing bugs.
            warn!(
                "Collection unverification not processed for asset {} and collection {}",
                bs58::encode(id_bytes).into_string(),
                collection
            );
        }

        return Ok(());
    };
    Err(IngesterError::ParsingError(
        "Ix not parsed correctly".to_string(),
    ))
}
