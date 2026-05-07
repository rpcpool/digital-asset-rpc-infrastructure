use {
    crate::{
        error::{ProgramTransformerError, ProgramTransformerResult},
        find_model_with_retry,
    },
    blockbuster::programs::agent_registry::AgentRegistryAccount,
    digital_asset_types::dao::{asset, sea_orm_active_enums::SpecificationAssetClass},
    sea_orm::{
        entity::{ColumnTrait, EntityTrait},
        sea_query::Expr,
        ConnectionTrait, QueryFilter, Statement, TransactionTrait,
    },
    solana_sdk::pubkey::Pubkey,
};

const RETRY_INTERVALS: &[u64] = &[0, 5, 10];

/// Handle an Agent Registry program account update.
///
/// Writes `agent_token` (and its slot guard) directly on the `asset` row.
/// Uses a plain UPDATE rather than an INSERT ON CONFLICT upsert because the
/// Agent Registry is a secondary program that decorates existing Core assets
/// — it should never create an asset row. The `find_model_with_retry` above
/// validates the asset exists and isn't burnt before we reach the write.
pub async fn handle_agent_registry_account<T: ConnectionTrait + TransactionTrait>(
    conn: &T,
    _account_pubkey: Pubkey,
    parsed: &AgentRegistryAccount,
    slot: u64,
) -> ProgramTransformerResult<()> {
    let Some(inner) = parsed.inner.as_ref() else {
        return Ok(());
    };

    let asset_id = inner.asset.to_bytes().to_vec();

    let asset_model = find_model_with_retry(
        conn,
        "asset",
        &asset::Entity::find_by_id(asset_id.clone()),
        RETRY_INTERVALS,
    )
    .await?;

    let Some(asset_model) = asset_model else {
        return Ok(());
    };
    if asset_model.burnt {
        return Ok(());
    }

    let slot_i = slot as i64;
    let agent_token_bytes: Option<Vec<u8>> =
        inner.agent_token_mint.map(|mint| mint.to_bytes().to_vec());

    // Wrap the UPDATE in a transaction with a short lock_timeout so a stuck
    // concurrent writer (e.g. a Core asset upsert holding the row lock) can't
    // block agent-registry ingestion indefinitely. Mirrors the pattern used in
    // mpl_core_program/v1_asset.rs and token/mod.rs.
    let txn = conn.begin().await?;
    let backend = txn.get_database_backend();
    txn.execute(Statement::from_string(
        backend,
        "SET LOCAL lock_timeout = '1s';".to_string(),
    ))
    .await?;
    txn.execute(Statement::from_string(
        backend,
        "SET LOCAL application_name = 'das::program_transformers::agent_registry';".to_string(),
    ))
    .await?;

    asset::Entity::update_many()
        .col_expr(asset::Column::AgentToken, Expr::value(agent_token_bytes))
        .col_expr(asset::Column::SlotUpdatedAgentRegistry, Expr::value(slot_i))
        .filter(asset::Column::Id.eq(asset_id))
        .filter(asset::Column::Burnt.eq(false))
        .filter(asset::Column::SpecificationAssetClass.eq(SpecificationAssetClass::MplCoreAsset))
        .filter(
            asset::Column::SlotUpdatedAgentRegistry
                .is_null()
                .or(asset::Column::SlotUpdatedAgentRegistry.lte(slot_i)),
        )
        .exec(&txn)
        .await
        .map_err(|db_err| ProgramTransformerError::AssetIndexError(db_err.to_string()))?;

    txn.commit().await?;

    Ok(())
}
