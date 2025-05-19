use crate::{
    dao::{
        asset, asset_authority, asset_creators, asset_data, asset_grouping,
        asset_v1_account_attachments,
        extensions::{self, asset::AssetSelectStatementExt},
        generated::sea_orm_active_enums::OwnerType,
        sea_orm_active_enums::V1AccountAttachments,
        token_accounts, tokens, Cursor, FullAsset, Pagination, SearchAssetsQuery,
    },
    rpc::{filter::TokenTypeClass, options::Options},
};
use indexmap::IndexMap;
use sea_orm::{
    prelude::Decimal,
    sea_query::{Alias, Condition, Expr, PostgresQueryBuilder, Query, UnionType},
    ColumnTrait, ConnectionTrait, DbErr, EntityTrait, FromQueryResult, JoinType, Order,
    PaginatorTrait, QueryFilter, QueryOrder, QuerySelect, RelationDef, RelationTrait, Statement,
};

use std::{collections::HashMap, hash::RandomState};

pub fn paginate<T, C>(
    pagination: &Pagination,
    limit: u64,
    stmt: T,
    sort_direction: Order,
    column: C,
) -> T
where
    T: QueryFilter + QuerySelect,
    C: ColumnTrait,
{
    let mut stmt = stmt;
    match pagination {
        Pagination::Keyset { before, after } => {
            if let Some(b) = before {
                stmt = stmt.filter(column.lt(b.clone()));
            }
            if let Some(a) = after {
                stmt = stmt.filter(column.gt(a.clone()));
            }
        }
        Pagination::Page { page } => {
            if *page > 0 {
                stmt = stmt.offset((page - 1) * limit)
            }
        }
        Pagination::Cursor(cursor) => {
            if *cursor != Cursor::default() {
                if sort_direction == sea_orm::Order::Asc {
                    stmt = stmt.filter(column.gt(cursor.id.clone()));
                } else {
                    stmt = stmt.filter(column.lt(cursor.id.clone()));
                }
            }
        }
    }
    stmt.limit(limit)
}

#[allow(clippy::too_many_arguments)]
pub async fn get_by_creator<D>(
    conn: &D,
    creator: Vec<u8>,
    only_verified: bool,
    sort_by: extensions::asset::Column,
    sort_direction: Order,
    pagination: &Pagination,
    limit: u64,
    options: &Options,
) -> Result<Vec<FullAsset>, DbErr>
where
    D: ConnectionTrait + Send + Sync,
{
    let mut stmt = extensions::asset::Row::select()
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::Supply)),
            extensions::asset::Column::MintSupply,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::Decimals)),
            extensions::asset::Column::MintDecimals,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::TokenProgram)),
            extensions::asset::Column::MintTokenProgram,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::MintAuthority)),
            extensions::asset::Column::MintAuthority,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::FreezeAuthority)),
            extensions::asset::Column::MintFreezeAuthority,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::CloseAuthority)),
            extensions::asset::Column::MintCloseAuthority,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::ExtensionData)),
            extensions::asset::Column::MintExtensionData,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Pubkey)),
            extensions::asset::Column::TokenAccountPubkey,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Owner)),
            extensions::asset::Column::TokenOwner,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Delegate)),
            extensions::asset::Column::TokenAccountDelegate,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Amount)),
            extensions::asset::Column::TokenAccountAmount,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Frozen)),
            extensions::asset::Column::TokenAccountFrozen,
        )
        .expr_as(
            Expr::col((
                token_accounts::Entity,
                token_accounts::Column::CloseAuthority,
            )),
            extensions::asset::Column::TokenAccountCloseAuthority,
        )
        .expr_as(
            Expr::col((
                token_accounts::Entity,
                token_accounts::Column::DelegatedAmount,
            )),
            extensions::asset::Column::TokenAccountDelegatedAmount,
        )
        .join(
            JoinType::LeftJoin,
            tokens::Entity,
            Expr::tbl(extensions::asset::Entity, asset::Column::Id)
                .equals(tokens::Entity, tokens::Column::Mint),
        )
        .join(
            JoinType::LeftJoin,
            token_accounts::Entity,
            Expr::tbl(extensions::asset::Entity, asset::Column::Id)
                .equals(token_accounts::Entity, token_accounts::Column::Mint)
                .and(
                    Expr::tbl(extensions::asset::Entity, asset::Column::Owner)
                        .equals(token_accounts::Entity, token_accounts::Column::Owner),
                )
                .and(token_accounts::Column::Amount.gt(0)),
        )
        .join(
            JoinType::InnerJoin,
            asset_creators::Entity,
            Condition::all()
                .add(
                    Expr::tbl(extensions::asset::Entity, asset::Column::Id)
                        .equals(asset_creators::Entity, asset_creators::Column::AssetId)
                        .and(asset_creators::Column::Creator.eq(creator.clone())),
                )
                .add_option(only_verified.then(|| asset_creators::Column::Verified.eq(true))),
        )
        .from_as(asset::Entity, extensions::asset::Entity)
        .and_where(Expr::tbl(extensions::asset::Entity, asset::Column::Supply).gt(0))
        .to_owned();

    if !options.show_fungible {
        stmt = stmt
            .and_where(
                Expr::tbl(extensions::asset::Entity, asset::Column::OwnerType)
                    .eq(Expr::val(OwnerType::Single).as_enum(asset::Column::OwnerType)),
            )
            .to_owned();
    }

    stmt = stmt.sort_by(sort_by, &sort_direction).to_owned();

    stmt = stmt
        .page_by(
            pagination,
            limit,
            &sort_direction,
            extensions::asset::Column::Id,
        )
        .to_owned();

    let (sql, values) = stmt.build(PostgresQueryBuilder);

    let statment = Statement::from_sql_and_values(sea_orm::DatabaseBackend::Postgres, &sql, values);

    let assets = extensions::asset::Row::find_by_statement(statment)
        .all(conn)
        .await?;

    get_related_for_assets(conn, assets, options, Some(creator)).await
}

#[allow(clippy::too_many_arguments)]
pub async fn get_by_grouping<D>(
    conn: &D,
    group_key: String,
    group_value: String,
    sort_by: extensions::asset::Column,
    sort_direction: Order,
    pagination: &Pagination,
    limit: u64,
    options: &Options,
) -> Result<Vec<FullAsset>, DbErr>
where
    D: ConnectionTrait + Send + Sync,
{
    let mut stmt = extensions::asset::Row::select()
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::Supply)),
            extensions::asset::Column::MintSupply,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::Decimals)),
            extensions::asset::Column::MintDecimals,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::TokenProgram)),
            extensions::asset::Column::MintTokenProgram,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::MintAuthority)),
            extensions::asset::Column::MintAuthority,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::FreezeAuthority)),
            extensions::asset::Column::MintFreezeAuthority,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::CloseAuthority)),
            extensions::asset::Column::MintCloseAuthority,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::ExtensionData)),
            extensions::asset::Column::MintExtensionData,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Pubkey)),
            extensions::asset::Column::TokenAccountPubkey,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Owner)),
            extensions::asset::Column::TokenOwner,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Delegate)),
            extensions::asset::Column::TokenAccountDelegate,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Amount)),
            extensions::asset::Column::TokenAccountAmount,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Frozen)),
            extensions::asset::Column::TokenAccountFrozen,
        )
        .expr_as(
            Expr::col((
                token_accounts::Entity,
                token_accounts::Column::CloseAuthority,
            )),
            extensions::asset::Column::TokenAccountCloseAuthority,
        )
        .expr_as(
            Expr::col((
                token_accounts::Entity,
                token_accounts::Column::DelegatedAmount,
            )),
            extensions::asset::Column::TokenAccountDelegatedAmount,
        )
        .join(
            JoinType::LeftJoin,
            tokens::Entity,
            Expr::tbl(extensions::asset::Entity, asset::Column::Id)
                .equals(tokens::Entity, tokens::Column::Mint),
        )
        .join(
            JoinType::LeftJoin,
            token_accounts::Entity,
            Expr::tbl(extensions::asset::Entity, asset::Column::Id)
                .equals(token_accounts::Entity, token_accounts::Column::Mint)
                .and(
                    Expr::tbl(extensions::asset::Entity, asset::Column::Owner)
                        .equals(token_accounts::Entity, token_accounts::Column::Owner),
                )
                .and(token_accounts::Column::Amount.gt(0)),
        )
        .join(
            JoinType::InnerJoin,
            asset_grouping::Entity,
            Condition::all()
                .add(
                    Expr::tbl(extensions::asset::Entity, asset::Column::Id)
                        .equals(asset_grouping::Entity, asset_grouping::Column::AssetId)
                        .and(asset_grouping::Column::GroupKey.eq(group_key))
                        .and(asset_grouping::Column::GroupValue.eq(group_value)),
                )
                .add_option((!options.show_unverified_collections).then(|| {
                    asset_grouping::Column::Verified
                        .eq(true)
                        .or(asset_grouping::Column::Verified.is_null())
                })),
        )
        .from_as(asset::Entity, extensions::asset::Entity)
        .and_where(Expr::tbl(extensions::asset::Entity, asset::Column::Supply).gt(0))
        .to_owned();

    if !options.show_fungible {
        stmt = stmt
            .and_where(
                Expr::tbl(extensions::asset::Entity, asset::Column::OwnerType)
                    .eq(Expr::val(OwnerType::Single).as_enum(asset::Column::OwnerType)),
            )
            .to_owned();
    }

    stmt = stmt.sort_by(sort_by, &sort_direction).to_owned();

    stmt = stmt
        .page_by(
            pagination,
            limit,
            &sort_direction,
            extensions::asset::Column::Id,
        )
        .to_owned();

    let (sql, values) = stmt.build(PostgresQueryBuilder);

    let statment = Statement::from_sql_and_values(sea_orm::DatabaseBackend::Postgres, &sql, values);

    let assets = extensions::asset::Row::find_by_statement(statment)
        .all(conn)
        .await?;

    get_related_for_assets(conn, assets, options, None).await
}

pub async fn get_by_owner<D>(
    conn: &D,
    owner: Vec<u8>,
    sort_by: extensions::asset::Column,
    sort_direction: Order,
    pagination: &Pagination,
    limit: u64,
    options: &Options,
) -> Result<Vec<FullAsset>, DbErr>
where
    D: ConnectionTrait + Send + Sync,
{
    let subquery = if options.show_fungible {
        let token_asset_stmt = extensions::asset::Row::select()
            .expr_as(
                Expr::col((tokens::Entity, tokens::Column::Supply)),
                extensions::asset::Column::MintSupply,
            )
            .expr_as(
                Expr::col((tokens::Entity, tokens::Column::Decimals)),
                extensions::asset::Column::MintDecimals,
            )
            .expr_as(
                Expr::col((tokens::Entity, tokens::Column::TokenProgram)),
                extensions::asset::Column::MintTokenProgram,
            )
            .expr_as(
                Expr::col((tokens::Entity, tokens::Column::MintAuthority)),
                extensions::asset::Column::MintAuthority,
            )
            .expr_as(
                Expr::col((tokens::Entity, tokens::Column::FreezeAuthority)),
                extensions::asset::Column::MintFreezeAuthority,
            )
            .expr_as(
                Expr::col((tokens::Entity, tokens::Column::CloseAuthority)),
                extensions::asset::Column::MintCloseAuthority,
            )
            .expr_as(
                Expr::col((tokens::Entity, tokens::Column::ExtensionData)),
                extensions::asset::Column::MintExtensionData,
            )
            .expr_as(
                Expr::col((token_accounts::Entity, token_accounts::Column::Pubkey)),
                extensions::asset::Column::TokenAccountPubkey,
            )
            .expr_as(
                Expr::col((token_accounts::Entity, token_accounts::Column::Owner)),
                extensions::asset::Column::TokenOwner,
            )
            .expr_as(
                Expr::col((token_accounts::Entity, token_accounts::Column::Delegate)),
                extensions::asset::Column::TokenAccountDelegate,
            )
            .expr_as(
                Expr::col((token_accounts::Entity, token_accounts::Column::Amount)),
                extensions::asset::Column::TokenAccountAmount,
            )
            .expr_as(
                Expr::col((token_accounts::Entity, token_accounts::Column::Frozen)),
                extensions::asset::Column::TokenAccountFrozen,
            )
            .expr_as(
                Expr::col((
                    token_accounts::Entity,
                    token_accounts::Column::CloseAuthority,
                )),
                extensions::asset::Column::TokenAccountCloseAuthority,
            )
            .expr_as(
                Expr::col((
                    token_accounts::Entity,
                    token_accounts::Column::DelegatedAmount,
                )),
                extensions::asset::Column::TokenAccountDelegatedAmount,
            )
            .join(
                JoinType::LeftJoin,
                tokens::Entity,
                Expr::tbl(extensions::asset::Entity, asset::Column::Id)
                    .equals(tokens::Entity, tokens::Column::Mint),
            )
            .join(
                JoinType::InnerJoin,
                token_accounts::Entity,
                Expr::tbl(extensions::asset::Entity, asset::Column::Id)
                    .equals(token_accounts::Entity, token_accounts::Column::Mint)
                    .and(token_accounts::Column::Owner.eq(owner.to_vec()))
                    .and(token_accounts::Column::Amount.gt(0)),
            )
            .from_as(asset::Entity, extensions::asset::Entity)
            .to_owned();

    let mut stmt = Query::select()
        .column((asset::Entity, asset::Column::Id))
        .column((asset::Entity, asset::Column::AltId))
        .expr(
            Expr::col((asset::Entity, asset::Column::SpecificationVersion))
                .as_enum(Alias::new("TEXT")),
        )
        .expr(
            Expr::col((asset::Entity, asset::Column::SpecificationAssetClass))
                .as_enum(Alias::new("TEXT")),
        )
        .column((asset::Entity, asset::Column::Owner))
        .expr(Expr::col((asset::Entity, asset::Column::OwnerType)).as_enum(Alias::new("TEXT")))
        .column((asset::Entity, asset::Column::Delegate))
        .column((asset::Entity, asset::Column::Frozen))
        .column((asset::Entity, asset::Column::Supply))
        .column((asset::Entity, asset::Column::SupplyMint))
        .column((asset::Entity, asset::Column::Compressed))
        .column((asset::Entity, asset::Column::Compressible))
        .column((asset::Entity, asset::Column::Seq))
        .column((asset::Entity, asset::Column::TreeId))
        .column((asset::Entity, asset::Column::Leaf))
        .column((asset::Entity, asset::Column::Nonce))
        .expr(
            Expr::col((asset::Entity, asset::Column::RoyaltyTargetType))
                .as_enum(Alias::new("TEXT")),
        )
        .column((asset::Entity, asset::Column::RoyaltyTarget))
        .column((asset::Entity, asset::Column::RoyaltyAmount))
        .column((asset::Entity, asset::Column::AssetData))
        .column((asset::Entity, asset::Column::CreatedAt))
        .column((asset::Entity, asset::Column::Burnt))
        .column((asset::Entity, asset::Column::SlotUpdated))
        .column((asset::Entity, asset::Column::SlotUpdatedMetadataAccount))
        .column((asset::Entity, asset::Column::SlotUpdatedMintAccount))
        .column((asset::Entity, asset::Column::SlotUpdatedTokenAccount))
        .column((asset::Entity, asset::Column::SlotUpdatedCnftTransaction))
        .column((asset::Entity, asset::Column::DataHash))
        .column((asset::Entity, asset::Column::CreatorHash))
        .column((asset::Entity, asset::Column::OwnerDelegateSeq))
        .column((asset::Entity, asset::Column::LeafSeq))
        .column((asset::Entity, asset::Column::BaseInfoSeq))
        .column((asset::Entity, asset::Column::MintExtensions))
        .column((asset::Entity, asset::Column::MplCorePlugins))
        .column((asset::Entity, asset::Column::MplCoreUnknownPlugins))
        .column((asset::Entity, asset::Column::MplCoreCollectionNumMinted))
        .column((asset::Entity, asset::Column::MplCoreCollectionCurrentSize))
        .column((asset::Entity, asset::Column::MplCorePluginsJsonVersion))
        .column((asset::Entity, asset::Column::MplCoreExternalPlugins))
        .column((asset::Entity, asset::Column::MplCoreUnknownExternalPlugins))
        .column((asset::Entity, asset::Column::CollectionHash))
        .column((asset::Entity, asset::Column::AssetDataHash))
        .column((asset::Entity, asset::Column::BubblegumFlags))
        .column((asset::Entity, asset::Column::NonTransferable))
        .from(asset::Entity)
        .and_where(asset::Column::Owner.eq(owner.to_vec()))
        .and_where(asset::Column::Supply.gt(0))
        .to_owned();

    if options.show_fungible {
        stmt = stmt.union(UnionType::All, token_owner_query).to_owned()
    }

    if let Some(col) = sort_by {
        stmt = stmt
            .order_by(col, sort_direction.clone())
            .order_by(asset::Column::Id, sort_direction.clone())
            .to_owned();
    }

    match pagination {
        Pagination::Keyset { before, after } => {
            if let Some(b) = before {
                stmt = stmt.and_where(asset::Column::Id.lt(b.clone())).to_owned();
            }
            if let Some(a) = after {
                stmt = stmt.and_where(asset::Column::Id.gt(a.clone())).to_owned();
            }
        }
        Pagination::Page { page } => {
            if *page > 0 {
                stmt = stmt.offset((page - 1) * limit).to_owned();
            }
        }
        Pagination::Cursor(cursor) => {
            if *cursor != Cursor::default() {
                if sort_direction == sea_orm::Order::Asc {
                    stmt = stmt
                        .and_where(asset::Column::Id.gt(cursor.id.clone()))
                        .to_owned();
                } else {
                    stmt = stmt
                        .and_where(asset::Column::Id.lt(cursor.id.clone()))
                        .to_owned();
                }
            }
        }
    }
    stmt = stmt.limit(limit).to_owned();

    let (sql, values) = stmt.build(PostgresQueryBuilder);

    let statment = Statement::from_sql_and_values(sea_orm::DatabaseBackend::Postgres, &sql, values);

    let assets = asset::Model::find_by_statement(statment).all(conn).await?;

    get_related_for_assets(conn, assets, Some(owner), options, None).await
}

pub async fn get_assets(
    conn: &impl ConnectionTrait,
    asset_ids: Vec<Vec<u8>>,
    pagination: &Pagination,
    limit: u64,
    options: &Options,
) -> Result<Vec<FullAsset>, DbErr> {
    let cond = Condition::all()
        .add(asset::Column::Id.is_in(asset_ids))
        .add(asset::Column::Supply.gt(0));

    get_assets_by_condition(
        conn,
        cond,
        vec![],
        None,
        None,
        Order::Asc,
        pagination,
        limit,
        options,
    )
    .await
}

pub async fn get_by_authority(
    conn: &impl ConnectionTrait,
    authority: Vec<u8>,
    sort_by: Option<asset::Column>,
    sort_direction: Order,
    pagination: &Pagination,
    limit: u64,
    options: &Options,
) -> Result<Vec<FullAsset>, DbErr>
where
    D: ConnectionTrait + Send + Sync,
{
    let mut stmt = extensions::asset::Row::select()
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::Supply)),
            extensions::asset::Column::MintSupply,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::Decimals)),
            extensions::asset::Column::MintDecimals,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::TokenProgram)),
            extensions::asset::Column::MintTokenProgram,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::MintAuthority)),
            extensions::asset::Column::MintAuthority,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::FreezeAuthority)),
            extensions::asset::Column::MintFreezeAuthority,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::CloseAuthority)),
            extensions::asset::Column::MintCloseAuthority,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::ExtensionData)),
            extensions::asset::Column::MintExtensionData,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Pubkey)),
            extensions::asset::Column::TokenAccountPubkey,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Owner)),
            extensions::asset::Column::TokenOwner,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Delegate)),
            extensions::asset::Column::TokenAccountDelegate,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Amount)),
            extensions::asset::Column::TokenAccountAmount,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Frozen)),
            extensions::asset::Column::TokenAccountFrozen,
        )
        .expr_as(
            Expr::col((
                token_accounts::Entity,
                token_accounts::Column::CloseAuthority,
            )),
            extensions::asset::Column::TokenAccountCloseAuthority,
        )
        .expr_as(
            Expr::col((
                token_accounts::Entity,
                token_accounts::Column::DelegatedAmount,
            )),
            extensions::asset::Column::TokenAccountDelegatedAmount,
        )
        .join(
            JoinType::LeftJoin,
            tokens::Entity,
            Expr::tbl(extensions::asset::Entity, asset::Column::Id)
                .equals(tokens::Entity, tokens::Column::Mint),
        )
        .join(
            JoinType::LeftJoin,
            token_accounts::Entity,
            Expr::tbl(extensions::asset::Entity, asset::Column::Id)
                .equals(token_accounts::Entity, token_accounts::Column::Mint)
                .and(
                    Expr::tbl(extensions::asset::Entity, asset::Column::Owner)
                        .equals(token_accounts::Entity, token_accounts::Column::Owner),
                )
                .and(token_accounts::Column::Amount.gt(0)),
        )
        .from_as(asset::Entity, extensions::asset::Entity)
        .and_where(Expr::tbl(extensions::asset::Entity, asset::Column::Id).is_in(asset_ids))
        .and_where(Expr::tbl(extensions::asset::Entity, asset::Column::Supply).gt(0))
        .to_owned();

    if !options.show_fungible {
        stmt = stmt
            .and_where(
                Expr::tbl(
                    extensions::asset::Entity,
                    extensions::asset::Column::OwnerType,
                )
                .eq(Expr::val(OwnerType::Single).as_enum(asset::Column::OwnerType)),
            )
            .to_owned();
    }

    stmt = stmt
        .order_by(extensions::asset::Column::Id, Order::Desc)
        .to_owned();

    stmt = stmt
        .page_by(
            pagination,
            limit,
            &Order::Desc,
            extensions::asset::Column::Id,
        )
        .to_owned();

    let (sql, values) = stmt.build(PostgresQueryBuilder);

    let statment = Statement::from_sql_and_values(sea_orm::DatabaseBackend::Postgres, &sql, values);

    let assets = extensions::asset::Row::find_by_statement(statment)
        .all(conn)
        .await?;

    get_related_for_assets(conn, assets, options, None).await
}

pub async fn get_by_authority<D>(
    conn: &D,
    authority: Vec<u8>,
    sort_by: extensions::asset::Column,
    sort_direction: Order,
    pagination: &Pagination,
    limit: u64,
    options: &Options,
) -> Result<Vec<FullAsset>, DbErr>
where
    D: ConnectionTrait + Send + Sync,
{
    let mut stmt = extensions::asset::Row::select()
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::Supply)),
            extensions::asset::Column::MintSupply,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::Decimals)),
            extensions::asset::Column::MintDecimals,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::TokenProgram)),
            extensions::asset::Column::MintTokenProgram,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::MintAuthority)),
            extensions::asset::Column::MintAuthority,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::FreezeAuthority)),
            extensions::asset::Column::MintFreezeAuthority,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::CloseAuthority)),
            extensions::asset::Column::MintCloseAuthority,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::ExtensionData)),
            extensions::asset::Column::MintExtensionData,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Pubkey)),
            extensions::asset::Column::TokenAccountPubkey,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Owner)),
            extensions::asset::Column::TokenOwner,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Delegate)),
            extensions::asset::Column::TokenAccountDelegate,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Amount)),
            extensions::asset::Column::TokenAccountAmount,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Frozen)),
            extensions::asset::Column::TokenAccountFrozen,
        )
        .expr_as(
            Expr::col((
                token_accounts::Entity,
                token_accounts::Column::CloseAuthority,
            )),
            extensions::asset::Column::TokenAccountCloseAuthority,
        )
        .expr_as(
            Expr::col((
                token_accounts::Entity,
                token_accounts::Column::DelegatedAmount,
            )),
            extensions::asset::Column::TokenAccountDelegatedAmount,
        )
        .join(
            JoinType::LeftJoin,
            tokens::Entity,
            Expr::tbl(extensions::asset::Entity, asset::Column::Id)
                .equals(tokens::Entity, tokens::Column::Mint),
        )
        .join(
            JoinType::LeftJoin,
            token_accounts::Entity,
            Expr::tbl(extensions::asset::Entity, asset::Column::Id)
                .equals(token_accounts::Entity, token_accounts::Column::Mint)
                .and(
                    Expr::tbl(extensions::asset::Entity, asset::Column::Owner)
                        .equals(token_accounts::Entity, token_accounts::Column::Owner),
                )
                .and(token_accounts::Column::Amount.gt(0)),
        )
        .join(
            JoinType::InnerJoin,
            asset_authority::Entity,
            Expr::tbl(extensions::asset::Entity, asset::Column::Id)
                .equals(asset_authority::Entity, asset_authority::Column::AssetId)
                .and(asset_authority::Column::Authority.eq(authority.clone())),
        )
        .from_as(asset::Entity, extensions::asset::Entity)
        .and_where(Expr::tbl(extensions::asset::Entity, asset::Column::Supply).gt(0))
        .to_owned();

    if !options.show_fungible {
        stmt = stmt
            .and_where(
                Expr::tbl(extensions::asset::Entity, asset::Column::OwnerType)
                    .eq(OwnerType::Single.as_enum()),
            )
            .to_owned();
    }

    stmt = stmt.sort_by(sort_by, &sort_direction).to_owned();

    stmt = stmt
        .page_by(
            pagination,
            limit,
            &sort_direction,
            extensions::asset::Column::Id,
        )
        .to_owned();

    let (sql, values) = stmt.build(PostgresQueryBuilder);

    let statment = Statement::from_sql_and_values(sea_orm::DatabaseBackend::Postgres, &sql, values);

    let assets = extensions::asset::Row::find_by_statement(statment)
        .all(conn)
        .await?;

    get_related_for_assets(conn, assets, options, None).await
}

pub async fn get_related_for_assets<D>(
    conn: &D,
    assets: Vec<extensions::asset::Row>,
    options: &Options,
    required_creator: Option<Vec<u8>>,
) -> Result<Vec<FullAsset>, DbErr>
where
    D: ConnectionTrait + Send + Sync,
{
    let mut full_assets = IndexMap::new();
    for asset in assets {
        full_assets.insert(
            asset.id.clone(),
            FullAsset {
                asset,
                ..Default::default()
            },
        );
    }

    let ids = full_assets.keys().cloned().collect::<Vec<_>>();

    // Get all creators for all assets in `assets_map` using batch processing
    let creators = asset_creators::Entity::find()
        .filter(asset_creators::Column::AssetId.is_in(ids))
        .order_by_asc(asset_creators::Column::AssetId)
        .order_by_asc(asset_creators::Column::Position)
        .all(conn)
        .await?;

    // Add the creators to the assets in `full_assets`.
    for c in creators.into_iter() {
        if let Some(asset) = full_assets.get_mut(&c.asset_id) {
            asset.creators.push(c);
        }
    }

    // Filter out stale creators from each asset.
    for (_id, asset) in full_assets.iter_mut() {
        filter_out_stale_creators(&mut asset.creators);
    }

    // If we passed in a required creator, we make sure that creator is still in the creator array
    // of each asset after stale creators were filtered out above.  Only retain those assets that
    // have the required creator.  This corrects `getAssetByCreators` from returning assets for
    // which the required creator is no longer in the creator array.
    if let Some(required) = required_creator {
        full_assets.retain(|_id, asset| asset.creators.iter().any(|c| c.creator == required));
    }

    let ids = full_assets.keys().cloned().collect::<Vec<_>>();

    let authorities = asset_authority::Entity::find()
        .filter(asset_authority::Column::AssetId.is_in(ids.clone()))
        .all(conn)
        .await?;
    for a in authorities.into_iter() {
        if let Some(asset) = full_assets.get_mut(&a.asset_id) {
            asset.authorities.push(a);
        }
    }

    if options.show_inscription {
        let attachments = asset_v1_account_attachments::Entity::find()
            .filter(asset_v1_account_attachments::Column::AssetId.is_in(ids.clone()))
            .filter(
                asset_v1_account_attachments::Column::AttachmentType
                    .eq(V1AccountAttachments::TokenInscription),
            )
            .all(conn)
            .await?;

        for a in attachments.into_iter() {
            if let Some(asset_id) = a.asset_id.as_ref() {
                if let Some(asset) = full_assets.get_mut(asset_id) {
                    asset.inscription = Some(a);
                }
            }
        }
    }

    let cond = if options.show_unverified_collections {
        None
    } else {
        Some(
            Condition::any()
                .add(asset_grouping::Column::Verified.eq(true))
                // Older versions of the indexer did not have the verified flag. A group would be present if and only if it was verified.
                // Therefore if verified is null, we can assume that the group is verified.
                .add(asset_grouping::Column::Verified.is_null()),
        )
    };

    let grouping_base_query = asset_grouping::Entity::find().filter(
        Condition::all()
            .add(asset_grouping::Column::AssetId.is_in(ids.clone()))
            .add(asset_grouping::Column::GroupValue.is_not_null())
            .add_option(cond),
    );

    if options.show_collection_metadata {
        let groups = grouping_base_query.all(conn).await?;

        let group_values = groups
            .iter()
            .filter_map(|group| {
                group
                    .group_value
                    .as_ref()
                    .and_then(|g| bs58::decode(g).into_vec().ok())
            })
            .collect::<Vec<_>>();

        let asset_data = asset_data::Entity::find()
            .filter(asset_data::Column::Id.is_in(group_values))
            .all(conn)
            .await?;

        let asset_data_map: HashMap<_, _, RandomState> = HashMap::from_iter(
            asset_data
                .into_iter()
                .map(|ad| (ad.id.clone(), ad))
                .collect::<Vec<_>>(),
        );

        for g in groups.into_iter() {
            if let Some(asset) = full_assets.get_mut(&g.asset_id) {
                let a = g.group_value.as_ref().and_then(|g| {
                    bs58::decode(g)
                        .into_vec()
                        .ok()
                        .and_then(|v| asset_data_map.get(&v))
                        .cloned()
                });

                asset.groups.push((g, a));
            }
        }
    } else {
        let single_group_query = grouping_base_query.all(conn).await?;
        for g in single_group_query.into_iter() {
            if let Some(asset) = full_assets.get_mut(&g.asset_id) {
                asset.groups.push((g, None));
            }
        }
    };

    Ok(full_assets.into_values().collect())
}

pub async fn get_by_id<D>(
    conn: &D,
    asset_id: Vec<u8>,
    options: &Options,
) -> Result<FullAsset, DbErr>
where
    D: ConnectionTrait + Send + Sync,
{
    let stmt = extensions::asset::Row::select()
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::Supply)),
            extensions::asset::Column::MintSupply,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::Decimals)),
            extensions::asset::Column::MintDecimals,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::TokenProgram)),
            extensions::asset::Column::MintTokenProgram,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::MintAuthority)),
            extensions::asset::Column::MintAuthority,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::FreezeAuthority)),
            extensions::asset::Column::MintFreezeAuthority,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::CloseAuthority)),
            extensions::asset::Column::MintCloseAuthority,
        )
        .expr_as(
            Expr::col((tokens::Entity, tokens::Column::ExtensionData)),
            extensions::asset::Column::MintExtensionData,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Pubkey)),
            extensions::asset::Column::TokenAccountPubkey,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Owner)),
            extensions::asset::Column::TokenOwner,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Delegate)),
            extensions::asset::Column::TokenAccountDelegate,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Amount)),
            extensions::asset::Column::TokenAccountAmount,
        )
        .expr_as(
            Expr::col((token_accounts::Entity, token_accounts::Column::Frozen)),
            extensions::asset::Column::TokenAccountFrozen,
        )
        .expr_as(
            Expr::col((
                token_accounts::Entity,
                token_accounts::Column::CloseAuthority,
            )),
            extensions::asset::Column::TokenAccountCloseAuthority,
        )
        .expr_as(
            Expr::col((
                token_accounts::Entity,
                token_accounts::Column::DelegatedAmount,
            )),
            extensions::asset::Column::TokenAccountDelegatedAmount,
        )
        .join(
            JoinType::LeftJoin,
            tokens::Entity,
            Expr::tbl(extensions::asset::Entity, asset::Column::Id)
                .equals(tokens::Entity, tokens::Column::Mint),
        )
        .join(
            JoinType::LeftJoin,
            token_accounts::Entity,
            Expr::tbl(extensions::asset::Entity, asset::Column::Id)
                .equals(token_accounts::Entity, token_accounts::Column::Mint)
                .and(
                    Expr::tbl(extensions::asset::Entity, asset::Column::Owner)
                        .equals(token_accounts::Entity, token_accounts::Column::Owner),
                )
                .and(token_accounts::Column::Amount.gt(0)),
        )
        .from_as(asset::Entity, extensions::asset::Entity)
        .and_where(Expr::tbl(extensions::asset::Entity, asset::Column::Id).eq(asset_id.clone()))
        .and_where(Expr::tbl(extensions::asset::Entity, asset::Column::Supply).gt(0))
        .to_owned();

    let (sql, values) = stmt.build(PostgresQueryBuilder);

    let statment = Statement::from_sql_and_values(sea_orm::DatabaseBackend::Postgres, &sql, values);

    let asset = extensions::asset::Row::find_by_statement(statment)
        .one(conn)
        .await?
        .ok_or(DbErr::RecordNotFound("Asset not found".to_string()))?;

    get_related_for_assets(conn, vec![asset], options, None)
        .await?
        .pop()
        .ok_or(DbErr::RecordNotFound("Asset not found".to_string()))
}

fn filter_out_stale_creators(creators: &mut Vec<asset_creators::Model>) {
    // If the first creator is an empty Vec, it means the creator array is empty (which is allowed
    // for compressed assets in Bubblegum).
    if !creators.is_empty() && creators[0].creator.is_empty() {
        creators.clear();
    } else {
        // For both compressed and non-compressed assets, any creators that do not have the max
        // `slot_updated` value are stale and should be removed.
        let max_slot_updated = creators.iter().map(|creator| creator.slot_updated).max();
        if let Some(max_slot_updated) = max_slot_updated {
            creators.retain(|creator| creator.slot_updated == max_slot_updated);
        }

        // For compressed assets, any creators that do not have the max `seq` value are stale and
        // should be removed.  A `seq` value of 0 indicates a decompressed or never-compressed
        // asset.  So if a `seq` value of 0 is present, then all creators with nonzero `seq` values
        // are stale and should be removed.
        let seq = if creators
            .iter()
            .map(|creator| creator.seq)
            .any(|seq| seq == Some(0))
        {
            Some(Some(0))
        } else {
            creators.iter().map(|creator| creator.seq).max()
        };

        if let Some(seq) = seq {
            creators.retain(|creator| creator.seq == seq);
        }
    }
}
