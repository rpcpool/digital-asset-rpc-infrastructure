use crate::error::IngesterError;
use blockbuster::token_metadata::state::Metadata;
use digital_asset_types::dao::asset_data;

use log::warn;
use plerkle_serialization::Pubkey as FBPubkey;
use sea_orm::{entity::*, query::*, ActiveValue::Set, ConnectionTrait, DbBackend};

pub async fn save_v1_asset<T: ConnectionTrait + TransactionTrait>(
    conn: &T,
    id: FBPubkey,
    _slot: u64,
    metadata: &Metadata,
) -> Result<(), IngesterError> {
    let metadata = metadata.clone();
    let data = metadata.data;

    let name = data.name.clone().into_bytes();
    let symbol = data.symbol.clone().into_bytes();
    warn!(
        "Setting to name and symbol {:?}, {:?}, {:?}",
        id, data.name, data.symbol
    );
    let id = id.0;

    let asset_data_model = asset_data::ActiveModel {
        id: Set(id.to_vec()),
        raw_name: Set(Some(name.to_vec())),
        raw_symbol: Set(Some(symbol.to_vec())),
        ..Default::default()
    };
    let query = asset_data::Entity::update(asset_data_model)
        .filter(Condition::all().add(asset_data::Column::Id.eq(id.to_vec().clone())))
        .build(DbBackend::Postgres);
    conn.execute(query).await?;
    Ok(())
}
