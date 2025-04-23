use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Slot::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(Slot::Index)
                            .big_integer()
                            .primary_key()
                            .auto_increment()
                            .not_null(),
                    )
                    .col(ColumnDef::new(Slot::Slot).big_integer().not_null())
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Slot::Table).to_owned())
            .await
    }
}

#[derive(Iden)]
#[allow(clippy::enum_variant_names)]
enum Slot {
    Table,
    Index,
    Slot,
}
