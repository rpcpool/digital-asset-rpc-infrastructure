use sea_orm_migration::{sea_orm::ConnectionTrait, prelude::*};
use sea_orm::Statement;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let db = manager.get_connection();
        manager
            .create_table(
                Table::create()
                    .table(TreeTransactions::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(TreeTransactions::Signature)
                            .string()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(TreeTransactions::Tree).string().not_null())
                    .col(ColumnDef::new(TreeTransactions::Slot).big_integer().not_null())
                    .col(ColumnDef::new(TreeTransactions::CreatedAt).timestamp_with_time_zone().default("now()"))
                    .col(ColumnDef::new(TreeTransactions::ProcessedAt).timestamp_with_time_zone())
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("tree_slot_index")
                    .table(TreeTransactions::Table)
                    .col(TreeTransactions::Tree)
                    .col(TreeTransactions::Slot)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_index(Index::drop().name("tree_slot_index").table(TreeTransactions::Table).to_owned())
            .await?;

        manager
            .drop_table(Table::drop().table(TreeTransactions::Table).to_owned())
            .await
    }
}

/// Learn more at https://docs.rs/sea-query#iden
#[derive(Iden)]
enum TreeTransactions {
    Table,
    Signature,
    Tree,
    CreatedAt,
    ProcessedAt,
    Slot,
}
