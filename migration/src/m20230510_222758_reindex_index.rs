use digital_asset_types::dao::asset_data;
use sea_orm_migration::{
    prelude::*,
    sea_orm::{ConnectionTrait, DatabaseBackend, Statement},
};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute(Statement::from_string(
              DatabaseBackend::Postgres,
              "CREATE INDEX IF NOT EXISTS asset_data_reindex ON asset_data (reindex);"
                .to_string(),
            ))
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(
              sea_query::Index::drop()
                .name("asset_data_reindex")
                .table(asset_data::Entity)
                .to_owned(),
            )
            .await?;

        Ok(())
    }
}
