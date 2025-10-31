use sea_orm_migration::prelude::*;

use crate::model::table::AccountSnapshot;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop the index on owner column (from `m20250605_114653_create_account_snapshots_table.rs`)
        manager
            .drop_index(
                Index::drop()
                    .name("account_snapshots_owner")
                    .table(AccountSnapshot::Table)
                    .to_owned(),
            )
            .await?;

        // Drop the owner column
        manager
            .alter_table(
                Table::alter()
                    .table(AccountSnapshot::Table)
                    .drop_column(AccountSnapshot::Owner)
                    .to_owned(),
            )
            .await?;

        // Drop the slot column
        manager
            .alter_table(
                Table::alter()
                    .table(AccountSnapshot::Table)
                    .drop_column(AccountSnapshot::Slot)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Add back the slot column
        manager
            .alter_table(
                Table::alter()
                    .table(AccountSnapshot::Table)
                    .add_column(
                        ColumnDef::new(AccountSnapshot::Slot)
                            .big_integer()
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        // Add back the owner column
        manager
            .alter_table(
                Table::alter()
                    .table(AccountSnapshot::Table)
                    .add_column(ColumnDef::new(AccountSnapshot::Owner).binary().not_null())
                    .to_owned(),
            )
            .await?;

        // Recreate the index on owner column
        manager
            .create_index(
                Index::create()
                    .name("account_snapshots_owner")
                    .index_type(sea_query::IndexType::Hash)
                    .col(AccountSnapshot::Owner)
                    .table(AccountSnapshot::Table)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}
