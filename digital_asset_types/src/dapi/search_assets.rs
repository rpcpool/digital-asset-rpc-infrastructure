use super::common::build_asset_response;
use crate::{
    dao::{scopes, PageOptions, SearchAssetsQuery},
    rpc::{filter::AssetSorting, options::Options, response::AssetList},
};
use sea_orm::{DatabaseConnection, DbErr};

#[tracing::instrument(name = "db::searchAssets", skip_all)]
pub async fn search_assets(
    db: &DatabaseConnection,
    search_assets_query: SearchAssetsQuery,
    sorting: AssetSorting,
    page_options: &PageOptions,
    options: &Options,
) -> Result<AssetList, DbErr> {
    let pagination = page_options.try_into()?;
    let (column, order) = sorting.into_sorting();
    search_assets_query.validate()?;

    let mut assets = scopes::asset::search_assets(
        db,
        &search_assets_query,
        column,
        order,
        &pagination,
        page_options.limit,
        options,
    )
    .await?;

    // MIP-11 (#270): if the request filtered by grouping, drop assets whose
    // surviving grouping state (after `filter_out_stale_asset_groupings` runs
    // during hydration) no longer matches. The asset_grouping JOIN can match
    // a row that was later superseded by a higher-slot NULL sentinel.
    if let Some((ref gk, ref gv)) = search_assets_query.grouping {
        assets.retain(|asset| {
            asset
                .groups
                .iter()
                .any(|(g, _)| g.group_key == *gk && g.group_value.as_deref() == Some(gv.as_str()))
        });
    }

    Ok(build_asset_response(
        assets,
        page_options.limit,
        &pagination,
        options,
    ))
}
