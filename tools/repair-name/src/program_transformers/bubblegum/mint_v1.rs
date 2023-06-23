use crate::error::IngesterError;
use blockbuster::{
    instruction::InstructionBundle,
    programs::bubblegum::{BubblegumInstruction, LeafSchema, Payload},
};
use log::warn;

use digital_asset_types::dao::asset_data;

use sea_orm::{entity::*, query::*, ConnectionTrait, DbBackend, EntityTrait};

pub async fn update_name_symbol<'c, T>(
    parsing_result: &BubblegumInstruction,
    _bundle: &InstructionBundle<'c>,
    txn: &'c T,
) -> Result<(), IngesterError>
where
    T: ConnectionTrait + TransactionTrait,
{
    if let (Some(le), Some(_cl), Some(Payload::MintV1 { args })) = (
        &parsing_result.leaf_update,
        &parsing_result.tree_update,
        &parsing_result.payload,
    ) {
        let metadata = args;
        match le.schema {
            LeafSchema::V1 { id, .. } => {
                let id_bytes = id.to_bytes().to_vec();
                let name = metadata.name.clone().into_bytes();
                let symbol = metadata.symbol.clone().into_bytes();
                let data = asset_data::ActiveModel {
                    id: Set(id_bytes.clone()),
                    raw_name: Set(Some(name.to_vec())),
                    raw_symbol: Set(Some(symbol.to_vec())),
                    ..Default::default()
                };

                let query = asset_data::Entity::update(data)
                    .filter(Condition::all().add(asset_data::Column::Id.eq(id_bytes.clone())))
                    .build(DbBackend::Postgres);
                warn!("log: {:?}", query.to_string());
                txn.execute(query)
                    .await
                    .map(|_| ())
                    .map_err(|e| IngesterError::DatabaseError(e.to_string()))
            }
        }
    } else {
        Err(IngesterError::ParsingError(
            "Ix not parsed correctly".to_string(),
        ))
    }
}
