use enum_iterator::{all, Sequence};
use extension::postgres::Type;
use sea_orm_migration::prelude::*;

use crate::model::table::AssetData;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(AssetData::Table)
                    .add_column(
                        ColumnDef::new(AssetData::FetchDurationInMs)
                            .integer()
                            .null(),
                    )
                    .to_owned(),
            )
            .await?;

        manager
            .create_type(
                Type::create()
                    .as_enum(AssetData::LastRequestedStatusCode)
                    .values(vec![
                        LastRequestedStatusCode::Success,
                        LastRequestedStatusCode::Failure,
                    ])
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(AssetData::Table)
                    .add_column(
                        ColumnDef::new(AssetData::LastRequestedStatusCode)
                            .enumeration(
                                AssetData::LastRequestedStatusCode,
                                all::<LastRequestedStatusCode>().collect::<Vec<_>>(),
                            )
                            .not_null(),
                    )
                    .to_owned(),
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .alter_table(
                Table::alter()
                    .table(AssetData::Table)
                    .drop_column(AssetData::FetchDurationInMs)
                    .to_owned(),
            )
            .await?;

        manager
            .alter_table(
                Table::alter()
                    .table(AssetData::Table)
                    .drop_column(AssetData::LastRequestedStatusCode)
                    .to_owned(),
            )
            .await?;

        manager
            .drop_type(
                Type::drop()
                    .name(AssetData::LastRequestedStatusCode)
                    .to_owned(),
            )
            .await?;

        Ok(())
    }
}

#[derive(Iden, Sequence)]
pub enum LastRequestedStatusCode {
    Success,
    Failure,
}
