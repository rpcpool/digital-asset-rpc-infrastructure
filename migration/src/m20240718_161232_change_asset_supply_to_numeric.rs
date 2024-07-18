use crate::model::table::Asset;
use sea_orm::{DatabaseBackend, Statement};
use sea_orm_migration::{prelude::*, sea_orm::ConnectionTrait};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute(Statement::from_string(
                DatabaseBackend::Postgres,
                "ALTER TABLE asset ALTER COLUMN supply TYPE NUMERIC;".to_string(),
            ))
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(Asset::Table)
                    .modify_column(ColumnDef::new(Asset::Supply).big_integer().not_null())
                    .to_owned(),
            )
            .await
    }
}
