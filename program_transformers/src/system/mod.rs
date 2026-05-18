use crate::AccountInfo;

use digital_asset_types::dao::{token_accounts, tokens};

use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter};

use crate::error::{ProgramTransformerError, ProgramTransformerResult};

// Handles system-program account events. Currently only used to close mint
// and token-account rows when an account is reassigned to the system program
// (i.e. closed). Slot-gated so that an out-of-order older close cannot
// override a newer reopen — the row is only deleted if its `slot_updated`
// is at or below the close's slot.
pub async fn handle_system_program_account(
    account: &AccountInfo,
    db: &DatabaseConnection,
) -> ProgramTransformerResult<()> {
    if !account.data.is_empty() {
        return Err(ProgramTransformerError::NotImplemented);
    }

    let pubkey = account.pubkey.to_bytes().to_vec();
    let slot_i = account.slot as i64;

    // Try to delete the mint first; only delete if the existing row hasn't
    // been observed at a higher slot already.
    let mint_delete_res = tokens::Entity::delete_many()
        .filter(tokens::Column::Mint.eq(pubkey.clone()))
        .filter(tokens::Column::SlotUpdated.lte(slot_i))
        .exec(db)
        .await?;

    if mint_delete_res.rows_affected == 0 {
        token_accounts::Entity::delete_many()
            .filter(token_accounts::Column::Pubkey.eq(pubkey))
            .filter(token_accounts::Column::SlotUpdated.lte(slot_i))
            .exec(db)
            .await?;
    }

    Ok(())
}
