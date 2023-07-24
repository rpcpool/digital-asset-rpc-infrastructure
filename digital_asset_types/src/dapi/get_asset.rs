use sea_orm::{DatabaseConnection, DbErr};

use crate::{
    dao::scopes,
    rpc::{transform::AssetTransform, Asset},
};

use super::common::asset_to_rpc;

pub async fn get_asset(
    db: &DatabaseConnection,
    id: Vec<u8>,
    transform: &AssetTransform,
    raw_data: Option<bool>,
) -> Result<Asset, DbErr> {
    let asset = scopes::asset::get_by_id(db, id, false).await?;
    asset_to_rpc(asset, transform, raw_data)
}
