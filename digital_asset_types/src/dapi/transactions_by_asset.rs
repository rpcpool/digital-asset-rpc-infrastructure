use crate::dao::scopes;

use crate::rpc::response::TransactionList;
use sea_orm::DatabaseConnection;
use sea_orm::DbErr;

use super::common::{build_transaction_response, create_pagination};

pub async fn get_transactions_by_asset(
    db: &DatabaseConnection,
    asset_id: Vec<u8>,
    limit: u64,
    page: Option<u64>,
    before: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
) -> Result<TransactionList, DbErr> {
    let pagination = create_pagination(before, after, page)?;
    let transactions =
        scopes::asset::get_transactions_by_asset(db, asset_id, &pagination, limit).await?;
    Ok(build_transaction_response(transactions, limit, &pagination))
}
