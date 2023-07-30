use super::common::{build_asset_response, create_pagination, create_sorting};
use crate::{
    dao::{scopes, SearchAssetsQuery},
    rpc::{filter::AssetSorting, response::AssetList, transform::AssetTransform},
};
use sea_orm::{DatabaseConnection, DbErr};

pub async fn search_assets(
    db: &DatabaseConnection,
    search_assets_query: SearchAssetsQuery,
    sorting: AssetSorting,
    limit: u64,
    page: Option<u64>,
    before: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
    transform: &AssetTransform,
    enable_grand_total_query: bool,
) -> Result<AssetList, DbErr> {
    let pagination = create_pagination(before, after, page)?;
    let (sort_direction, sort_column) = create_sorting(sorting);
    let (condition, joins) = search_assets_query.conditions()?;
    let (assets, grand_total) = scopes::asset::get_assets_by_condition(
        db,
        condition,
        joins,
        sort_column,
        sort_direction,
        &pagination,
        limit,
        enable_grand_total_query,
    )
    .await?;
    Ok(build_asset_response(
        assets,
        limit,
        grand_total,
        &pagination,
        &transform,
    ))
}
