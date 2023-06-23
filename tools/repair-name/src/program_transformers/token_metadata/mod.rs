mod v1_asset;

use crate::{error::IngesterError, program_transformers::token_metadata::v1_asset::save_v1_asset};
use blockbuster::programs::token_metadata::{TokenMetadataAccountData, TokenMetadataAccountState};
use plerkle_serialization::AccountInfo;
use sea_orm::DatabaseConnection;

pub async fn handle_token_metadata_account<'a, 'b, 'c>(
    account_update: &'a AccountInfo<'a>,
    parsing_result: &'b TokenMetadataAccountState,
    db: &'c DatabaseConnection,
) -> Result<(), IngesterError> {
    match &parsing_result.data {
        TokenMetadataAccountData::MetadataV1(m) => {
            save_v1_asset(db, m.mint.as_ref().into(), account_update.slot(), m).await?;
            Ok(())
        }
        _ => Err(IngesterError::NotImplemented),
    }?;
    Ok(())
}
