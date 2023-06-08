use crate::dao::scopes;

use crate::rpc::response::TransactionSignatureList;
use sea_orm::DatabaseConnection;
use sea_orm::DbErr;

use super::common::{build_transaction_signatures_response, create_pagination};

pub async fn get_signatures_for_asset(
    db: &DatabaseConnection,
    asset_id: Vec<u8>,
    limit: u64,
    page: Option<u64>,
    before: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
) -> Result<TransactionSignatureList, DbErr> {
    let pagination = create_pagination(before, after, page)?;
    let transactions =
        scopes::asset::get_signatures_for_asset(db, asset_id, &pagination, limit).await?;
    Ok(build_transaction_signatures_response(
        transactions,
        limit,
        &pagination,
    ))
}
