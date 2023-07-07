use anchor_lang::prelude::Pubkey;
use log::{debug, error};

use crate::{error::IngesterError, program_transformers::bubblegum::update_asset};

use super::{save_changelog_event, u32_to_u8_array};
use blockbuster::{instruction::InstructionBundle, programs::bubblegum::BubblegumInstruction};
use digital_asset_types::dao::asset;
use sea_orm::{entity::*, ConnectionTrait, TransactionTrait};

pub async fn redeem<'c, T>(
    parsing_result: &BubblegumInstruction,
    bundle: &InstructionBundle<'c>,
    txn: &'c T,
) -> Result<(), IngesterError>
where
    T: ConnectionTrait + TransactionTrait,
{
    if let Some(cl) = &parsing_result.tree_update {
        let seq = save_changelog_event(cl, bundle.slot, bundle.txn_id, txn).await?;
        let leaf_index = cl.index;
        let (asset_id, _) = Pubkey::find_program_address(
            &[
                "asset".as_bytes(),
                cl.id.as_ref(),
                u32_to_u8_array(leaf_index).as_ref(),
            ],
            &mpl_bubblegum::ID,
        );
        debug!("Indexing redeem for asset id: {:?}", asset_id);
        let id_bytes = asset_id.to_bytes().to_vec();
        let asset_to_update = asset::ActiveModel {
            id: Unchanged(id_bytes.clone()),
            leaf: Set(Some(vec![0; 32])),
            seq: Set(seq as i64),
            ..Default::default()
        };
        update_asset(txn, id_bytes, Some(seq), asset_to_update).await?;
        return Ok(());
    }
    Err(IngesterError::ParsingError(
        "Ix not parsed correctly".to_string(),
    ))
}
