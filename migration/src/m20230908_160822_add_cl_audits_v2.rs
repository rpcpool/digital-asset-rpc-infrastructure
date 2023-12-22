use enum_iterator::{all, Sequence};
use sea_orm_migration::prelude::{sea_query::extension::postgres::Type, *};
use sea_orm_migration::sea_orm::{ConnectionTrait, DatabaseBackend, Statement};

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        let conn = manager.get_connection();

        manager
            .create_type(
                Type::create()
                    .as_enum(BubblegumInstruction::Table)
                    .values(all::<BubblegumInstruction>().map(|e| e).collect::<Vec<_>>())
                    .to_owned(),
            )
            .await?;

        manager
            .create_table(
                Table::create()
                    .table(ClAuditsV2::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ClAuditsV2::Id)
                            .big_integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(ClAuditsV2::Tree).binary().not_null())
                    .col(ColumnDef::new(ClAuditsV2::LeafIdx).big_integer().not_null())
                    .col(ColumnDef::new(ClAuditsV2::Seq).big_integer().not_null())
                    .col(
                        ColumnDef::new(ClAuditsV2::CreatedAt)
                            .date_time()
                            .default(SimpleExpr::Keyword(Keyword::CurrentTimestamp))
                            .not_null(),
                    )
                    .col(ColumnDef::new(ClAuditsV2::Tx).binary().not_null())
                    .col(
                        ColumnDef::new(ClAuditsV2::Instruction)
                            .custom(BubblegumInstruction::Table)
                            .not_null()
                    )
                    .to_owned(),
            )
            .await?;

        conn.execute(Statement::from_string(
            DatabaseBackend::Postgres,
            "ALTER TABLE cl_audits_v2 ADD CONSTRAINT unique_tree_leafidx_seq UNIQUE (tree, leaf_idx, seq);".to_string(),
        ))
        .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
            manager
            .drop_type(
                Type::drop()
                    .if_exists()
                    .name(BubblegumInstruction::Table)
                    .to_owned(),
            )
            .await?;

        manager
            .drop_table(Table::drop().table(ClAuditsV2::Table).to_owned())
            .await?;
        Ok(())
    }
}

/// Learn more at https://docs.rs/sea-query#iden
#[derive(Iden)]
enum ClAuditsV2 {
    Table,
    Id,
    Tree,
    LeafIdx,
    Seq,
    CreatedAt,
    Tx,
    Instruction,
}

#[derive(Iden, Debug, PartialEq, Sequence)]
enum BubblegumInstruction {
    Table,
    Unknown,
    MintV1,
    Redeem,
    CancelRedeem,
    Transfer,
    Delegate,
    DecompressV1,
    Compress,
    Burn,
    VerifyCreator,
    UnverifyCreator,
    VerifyCollection,
    UnverifyCollection,
    SetAndVerifyCollection,
    MintToCollectionV1,
    CreateTree,
}