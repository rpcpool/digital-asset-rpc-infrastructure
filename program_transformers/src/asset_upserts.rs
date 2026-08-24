use {
    digital_asset_types::dao::{
        asset,
        sea_orm_active_enums::{
            OwnerType, RoyaltyTargetType, SpecificationAssetClass, SpecificationVersions,
        },
    },
    sea_orm::{
        sea_query::{Alias, Condition, Expr, OnConflict},
        ConnectionTrait, DbErr, EntityTrait, Set, TransactionTrait,
    },
    serde_json::value::Value,
    sqlx::types::Decimal,
};

pub struct AssetTokenAccountColumns {
    pub mint: Vec<u8>,
    pub owner: Option<Vec<u8>>,
    pub frozen: bool,
    pub delegate: Option<Vec<u8>>,
    pub slot_updated_token_account: Option<i64>,
}

/// # Upsert Asset Token Account Columns
///
/// ```sql
/// INSERT INTO asset (
///     id,
///     owner,
///     frozen,
///     delegate,
///     slot_updated,
///     slot_updated_token_account
/// ) VALUES (
///     $1, $2, $3, $4, $5, $6
/// )
/// ON CONFLICT (id)
/// DO UPDATE SET
///     owner = EXCLUDED.owner,
///     frozen = EXCLUDED.frozen,
///     delegate = EXCLUDED.delegate,
///     slot_updated_token_account = EXCLUDED.slot_updated_token_account
/// WHERE (
///     -- First condition: Either field changes AND slot condition, OR slot is null
///     (
///         -- Any of these fields changed
///         (
///             EXCLUDED.owner IS DISTINCT FROM asset.owner OR
///             EXCLUDED.frozen IS DISTINCT FROM asset.frozen OR
///             EXCLUDED.delegate IS DISTINCT FROM asset.delegate
///         )
///         AND
///         -- Slot condition (only if $slot_updated_token_account is provided as value in the query)
///         (
///             CASE
///                 WHEN $slot_updated_token_account IS NOT NULL
///                 THEN asset.slot_updated_token_account <= $slot_updated_token_account
///                 ELSE TRUE
///             END
///         )
///     )
///     OR
///     -- Second condition: existing slot is null
///     asset.slot_updated_token_account IS NULL
/// )
/// AND
///     -- Final condition: only update Single owner type assets
///     asset.owner_type = 'Single'::owner_type;
/// ```
///
pub async fn upsert_assets_token_account_columns<T: ConnectionTrait + TransactionTrait>(
    columns: AssetTokenAccountColumns,
    txn_or_conn: &T,
) -> Result<(), DbErr> {
    let active_model = asset::ActiveModel {
        id: Set(columns.mint),
        owner: Set(columns.owner),
        frozen: Set(columns.frozen),
        delegate: Set(columns.delegate),
        slot_updated: Set(columns.slot_updated_token_account),
        slot_updated_token_account: Set(columns.slot_updated_token_account),
        ..Default::default()
    };

    asset::Entity::insert(active_model)
        .on_conflict(
            OnConflict::columns([asset::Column::Id])
                .update_columns([
                    asset::Column::Owner,
                    asset::Column::Frozen,
                    asset::Column::Delegate,
                    asset::Column::SlotUpdatedTokenAccount,
                ])
                .action_cond_where(
                    Condition::all()
                        .add(
                            Condition::any()
                                .add(
                                    Condition::all()
                                        .add(
                                            Condition::any()
                                                .add(
                                                    Expr::tbl(
                                                        Alias::new("excluded"),
                                                        asset::Column::Owner,
                                                    )
                                                    .ne(Expr::tbl(
                                                        asset::Entity,
                                                        asset::Column::Owner,
                                                    )),
                                                )
                                                .add(
                                                    Expr::tbl(
                                                        Alias::new("excluded"),
                                                        asset::Column::Frozen,
                                                    )
                                                    .ne(Expr::tbl(
                                                        asset::Entity,
                                                        asset::Column::Frozen,
                                                    )),
                                                )
                                                .add(
                                                    Expr::tbl(
                                                        Alias::new("excluded"),
                                                        asset::Column::Delegate,
                                                    )
                                                    .ne(Expr::tbl(
                                                        asset::Entity,
                                                        asset::Column::Delegate,
                                                    )),
                                                ),
                                        )
                                        .add_option(columns.slot_updated_token_account.map(
                                            |slot| {
                                                Expr::tbl(
                                                    asset::Entity,
                                                    asset::Column::SlotUpdatedTokenAccount,
                                                )
                                                .lte(slot)
                                            },
                                        )),
                                )
                                .add(
                                    Expr::tbl(
                                        asset::Entity,
                                        asset::Column::SlotUpdatedTokenAccount,
                                    )
                                    .is_null(),
                                ),
                        )
                        .add(
                            Expr::tbl(asset::Entity, asset::Column::OwnerType)
                                .eq(Expr::val(OwnerType::Single).as_enum(Alias::new("owner_type"))),
                        ),
                )
                .to_owned(),
        )
        .exec_without_returning(txn_or_conn)
        .await?;

    Ok(())
}

pub struct AssetMintAccountColumns {
    pub mint: Vec<u8>,
    pub supply: Decimal,
    pub decimals: u8,
    pub slot_updated_mint_account: i64,
    pub extensions: Option<Value>,
}

/// # Upsert Asset Mint Account Columns
///
/// ```sql
/// INSERT INTO asset (
///     id,
///     supply,
///     supply_mint,
///     slot_updated_mint_account,
///     slot_updated,
///     mint_extensions,
///     asset_data,
///     specification_asset_class,
///     specification_version,
///     owner_type
/// ) VALUES (
///     $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
/// )
/// ON CONFLICT (id)
/// DO UPDATE SET
///     supply = EXCLUDED.supply,
///     supply_mint = EXCLUDED.supply_mint,
///     slot_updated_mint_account = EXCLUDED.slot_updated_mint_account,
///     mint_extensions = EXCLUDED.mint_extensions,
///     asset_data = EXCLUDED.asset_data,
///     owner_type = EXCLUDED.owner_type,
///     specification_version = EXCLUDED.specification_version
/// WHERE (
///     -- First condition: Field changes AND slot condition
///     (
///         -- Any of these fields changed
///         (
///             EXCLUDED.supply IS DISTINCT FROM asset.supply OR
///             EXCLUDED.supply_mint IS DISTINCT FROM asset.supply_mint OR
///             EXCLUDED.slot_updated_mint_account IS DISTINCT FROM asset.slot_updated_mint_account OR
///             EXCLUDED.mint_extensions IS DISTINCT FROM asset.mint_extensions OR
///             EXCLUDED.asset_data IS DISTINCT FROM asset.asset_data OR
///             EXCLUDED.owner_type IS DISTINCT FROM asset.owner_type OR
///             EXCLUDED.specification_version IS DISTINCT FROM asset.specification_version
///         )
///         AND
///         -- Slot condition: existing slot <= new slot
///         asset.slot_updated_mint_account <= $slot_updated_mint_account
///     )
///     OR
///     -- Second condition: existing slot is null (allows update regardless)
///     asset.slot_updated_mint_account IS NULL
/// );
/// ```
pub async fn upsert_assets_mint_account_columns<T: ConnectionTrait + TransactionTrait>(
    columns: AssetMintAccountColumns,
    txn_or_conn: &T,
) -> Result<(), DbErr> {
    let specification_asset_class = SpecificationAssetClass::FungibleToken;
    let (specification_version, owner_type) =
        if columns.supply == Decimal::from(1) && columns.decimals == 0 {
            (Some(SpecificationVersions::V1), OwnerType::Single)
        } else {
            (None, OwnerType::Token)
        };

    let active_model = asset::ActiveModel {
        id: Set(columns.mint.clone()),
        supply: Set(columns.supply),
        supply_mint: Set(Some(columns.mint.clone())),
        slot_updated_mint_account: Set(Some(columns.slot_updated_mint_account)),
        slot_updated: Set(Some(columns.slot_updated_mint_account)),
        mint_extensions: Set(columns.extensions),
        asset_data: Set(Some(columns.mint.clone())),
        specification_asset_class: Set(Some(specification_asset_class)),
        specification_version: Set(specification_version),
        owner_type: Set(owner_type),
        ..Default::default()
    };
    asset::Entity::insert(active_model)
        .on_conflict(
            OnConflict::columns([asset::Column::Id])
                .update_columns([
                    asset::Column::Supply,
                    asset::Column::SupplyMint,
                    asset::Column::SlotUpdatedMintAccount,
                    asset::Column::MintExtensions,
                    asset::Column::AssetData,
                    asset::Column::OwnerType,
                    asset::Column::SpecificationVersion,
                ])
                .value(
                    asset::Column::SpecificationAssetClass,
                    Expr::cust(
                        r#"COALESCE("asset"."specification_asset_class", "excluded"."specification_asset_class")"#,
                    ),
                )
                .action_cond_where(
                    Condition::any()
                        .add(
                            Condition::all()
                                .add(
                                    Condition::any()
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::Supply,
                                            )
                                            .ne(Expr::tbl(asset::Entity, asset::Column::Supply)),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::SupplyMint,
                                            )
                                            .ne(
                                                Expr::tbl(asset::Entity, asset::Column::SupplyMint),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::SlotUpdatedMintAccount,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    asset::Entity,
                                                    asset::Column::SlotUpdatedMintAccount,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::MintExtensions,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    asset::Entity,
                                                    asset::Column::MintExtensions,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::AssetData,
                                            )
                                            .ne(Expr::tbl(asset::Entity, asset::Column::AssetData)),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::OwnerType,
                                            )
                                            .ne(Expr::tbl(asset::Entity, asset::Column::OwnerType)),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::SpecificationVersion,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    asset::Entity,
                                                    asset::Column::SpecificationVersion,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                asset::Entity,
                                                asset::Column::SpecificationAssetClass,
                                            )
                                            .is_null(),
                                        ),
                                )
                                .add(
                                    Expr::tbl(asset::Entity, asset::Column::SlotUpdatedMintAccount)
                                        .lte(columns.slot_updated_mint_account),
                                ),
                        )
                        .add(
                            Expr::tbl(asset::Entity, asset::Column::SlotUpdatedMintAccount)
                                .is_null(),
                        ),
                )
                .to_owned(),
        )
        .exec_without_returning(txn_or_conn)
        .await?;

    Ok(())
}

pub struct AssetMetadataAccountColumns {
    pub mint: Vec<u8>,
    pub specification_asset_class: Option<SpecificationAssetClass>,
    pub royalty_amount: i32,
    pub asset_data: Option<Vec<u8>>,
    pub slot_updated_metadata_account: u64,
    pub mpl_core_plugins: Option<Value>,
    pub mpl_core_unknown_plugins: Option<Value>,
    pub mpl_core_collection_num_minted: Option<i32>,
    pub mpl_core_collection_current_size: Option<i32>,
    pub mpl_core_plugins_json_version: Option<i32>,
    pub mpl_core_external_plugins: Option<Value>,
    pub mpl_core_unknown_external_plugins: Option<Value>,
    pub is_agent: bool,
    pub asset_signer: Option<Vec<u8>>,
}

pub async fn upsert_assets_metadata_account_columns<T: ConnectionTrait + TransactionTrait>(
    columns: AssetMetadataAccountColumns,
    txn_or_conn: &T,
) -> Result<(), DbErr> {
    let active_model = asset::ActiveModel {
        id: Set(columns.mint),
        specification_version: Set(Some(SpecificationVersions::V1)),
        specification_asset_class: Set(columns.specification_asset_class),
        tree_id: Set(None),
        nonce: Set(Some(0)),
        seq: Set(Some(0)),
        leaf: Set(None),
        data_hash: Set(None),
        creator_hash: Set(None),
        compressed: Set(false),
        compressible: Set(false),
        royalty_target_type: Set(RoyaltyTargetType::Creators),
        royalty_target: Set(None),
        royalty_amount: Set(columns.royalty_amount),
        asset_data: Set(columns.asset_data),
        slot_updated: Set(Some(columns.slot_updated_metadata_account as i64)),
        slot_updated_metadata_account: Set(Some(columns.slot_updated_metadata_account as i64)),
        burnt: Set(false),
        mpl_core_plugins: Set(columns.mpl_core_plugins),
        mpl_core_unknown_plugins: Set(columns.mpl_core_unknown_plugins),
        mpl_core_collection_num_minted: Set(columns.mpl_core_collection_num_minted),
        mpl_core_collection_current_size: Set(columns.mpl_core_collection_current_size),
        mpl_core_plugins_json_version: Set(columns.mpl_core_plugins_json_version),
        mpl_core_external_plugins: Set(columns.mpl_core_external_plugins),
        mpl_core_unknown_external_plugins: Set(columns.mpl_core_unknown_external_plugins),
        is_agent: Set(columns.is_agent),
        asset_signer: Set(columns.asset_signer),
        ..Default::default()
    };
    asset::Entity::insert(active_model)
        .on_conflict(
            OnConflict::columns([asset::Column::Id])
                .update_columns([
                    asset::Column::SpecificationVersion,
                    asset::Column::SpecificationAssetClass,
                    asset::Column::TreeId,
                    asset::Column::Nonce,
                    asset::Column::Seq,
                    asset::Column::Leaf,
                    asset::Column::DataHash,
                    asset::Column::CreatorHash,
                    asset::Column::Compressed,
                    asset::Column::Compressible,
                    asset::Column::RoyaltyTargetType,
                    asset::Column::RoyaltyTarget,
                    asset::Column::RoyaltyAmount,
                    asset::Column::AssetData,
                    asset::Column::SlotUpdatedMetadataAccount,
                    asset::Column::Burnt,
                    asset::Column::MplCorePlugins,
                    asset::Column::MplCoreUnknownPlugins,
                    asset::Column::MplCoreCollectionNumMinted,
                    asset::Column::MplCoreCollectionCurrentSize,
                    asset::Column::MplCorePluginsJsonVersion,
                    asset::Column::MplCoreExternalPlugins,
                    asset::Column::MplCoreUnknownExternalPlugins,
                    asset::Column::IsAgent,
                    asset::Column::AssetSigner,
                ])
                .action_cond_where(
                    Condition::any()
                        .add(
                            Condition::all()
                                .add(
                                    Condition::any()
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::SpecificationVersion,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    asset::Entity,
                                                    asset::Column::SpecificationVersion,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::SpecificationAssetClass,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    asset::Entity,
                                                    asset::Column::SpecificationAssetClass,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::TreeId,
                                            )
                                            .ne(Expr::tbl(asset::Entity, asset::Column::TreeId)),
                                        )
                                        .add(
                                            Expr::tbl(Alias::new("excluded"), asset::Column::Nonce)
                                                .ne(Expr::tbl(asset::Entity, asset::Column::Nonce)),
                                        )
                                        .add(
                                            Expr::tbl(Alias::new("excluded"), asset::Column::Seq)
                                                .ne(Expr::tbl(asset::Entity, asset::Column::Seq)),
                                        )
                                        .add(
                                            Expr::tbl(Alias::new("excluded"), asset::Column::Leaf)
                                                .ne(Expr::tbl(asset::Entity, asset::Column::Leaf)),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::DataHash,
                                            )
                                            .ne(Expr::tbl(asset::Entity, asset::Column::DataHash)),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::CreatorHash,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    asset::Entity,
                                                    asset::Column::CreatorHash,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::Compressed,
                                            )
                                            .ne(
                                                Expr::tbl(asset::Entity, asset::Column::Compressed),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::Compressible,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    asset::Entity,
                                                    asset::Column::Compressible,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::RoyaltyTargetType,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    asset::Entity,
                                                    asset::Column::RoyaltyTargetType,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::RoyaltyTarget,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    asset::Entity,
                                                    asset::Column::RoyaltyTarget,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::RoyaltyAmount,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    asset::Entity,
                                                    asset::Column::RoyaltyAmount,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::AssetData,
                                            )
                                            .ne(Expr::tbl(asset::Entity, asset::Column::AssetData)),
                                        )
                                        .add(
                                            Expr::tbl(Alias::new("excluded"), asset::Column::Burnt)
                                                .ne(Expr::tbl(asset::Entity, asset::Column::Burnt)),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::MplCorePlugins,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    asset::Entity,
                                                    asset::Column::MplCorePlugins,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::MplCoreUnknownPlugins,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    asset::Entity,
                                                    asset::Column::MplCoreUnknownPlugins,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::MplCoreCollectionNumMinted,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    asset::Entity,
                                                    asset::Column::MplCoreCollectionNumMinted,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::MplCoreCollectionCurrentSize,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    asset::Entity,
                                                    asset::Column::MplCoreCollectionCurrentSize,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::MplCorePluginsJsonVersion,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    asset::Entity,
                                                    asset::Column::MplCorePluginsJsonVersion,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::MplCoreExternalPlugins,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    asset::Entity,
                                                    asset::Column::MplCoreExternalPlugins,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::MplCoreUnknownExternalPlugins,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    asset::Entity,
                                                    asset::Column::MplCoreUnknownExternalPlugins,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::IsAgent,
                                            )
                                            .ne(Expr::tbl(asset::Entity, asset::Column::IsAgent)),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                asset::Column::AssetSigner,
                                            )
                                            .ne(Expr::tbl(
                                                asset::Entity,
                                                asset::Column::AssetSigner,
                                            )),
                                        ),
                                )
                                .add(
                                    Expr::tbl(
                                        asset::Entity,
                                        asset::Column::SlotUpdatedMetadataAccount,
                                    )
                                    .lte(columns.slot_updated_metadata_account as i64),
                                ),
                        )
                        .add(
                            Expr::tbl(asset::Entity, asset::Column::SlotUpdatedMetadataAccount)
                                .is_null(),
                        ),
                )
                .to_owned(),
        )
        .exec_without_returning(txn_or_conn)
        .await?;
    Ok(())
}
