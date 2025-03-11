use super::BubblegumContext;
use crate::tree::TreeResponse;
use anyhow::Result;
use das_core::Rpc;
use digital_asset_types::dao::{
    asset, asset_authority, asset_creators, asset_data, asset_grouping, cl_audits_v2,
};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, FromQueryResult, Order, QueryFilter, QueryOrder,
    QuerySelect, SqlxPostgresConnector, TransactionTrait,
};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use tracing::error;

const CHUNK_SIZE: u64 = 10_000;

#[derive(FromQueryResult)]
struct TxQueryResult {
    tx: Vec<u8>,
    leaf_idx: i64,
    id: i64,
}

/// Query chunks from cl_audits_v2 for the tree and purge assets with txs containing a `TransactionError`.
pub async fn start(
    context: BubblegumContext,
    tree: TreeResponse,
    tx_concurrency: usize,
) -> Result<()> {
    let db = SqlxPostgresConnector::from_sqlx_postgres_pool(context.database_pool.clone());
    let tree_bytes = tree.pubkey.as_ref().to_vec();

    let mut last_id = 0;

    loop {
        let txs_batch: Vec<TxQueryResult> = cl_audits_v2::Entity::find()
            .filter(cl_audits_v2::Column::Tree.eq(tree_bytes.clone()))
            .filter(cl_audits_v2::Column::Id.gt(last_id))
            .select_only()
            .columns([
                cl_audits_v2::Column::Tx,
                cl_audits_v2::Column::LeafIdx,
                cl_audits_v2::Column::Id,
            ])
            .order_by(cl_audits_v2::Column::Id, Order::Asc)
            .limit(CHUNK_SIZE)
            .into_model::<TxQueryResult>()
            .all(&db)
            .await?;

        if txs_batch.is_empty() {
            break;
        }

        let mut handlers = FuturesUnordered::new();

        for tx in txs_batch {
            last_id = tx.id;

            if handlers.len() >= tx_concurrency {
                handlers.next().await;
            }

            let rpc = context.solana_rpc.clone();
            let context = context.clone();
            let tree_bytes = tree_bytes.clone();

            let handle = tokio::spawn(async move {
                if let Err(e) = process_signature(tx, rpc, context, tree_bytes).await {
                    error!("Error processing signature: {:?}", e);
                }
            });

            handlers.push(handle);
        }

        futures::future::join_all(handlers).await;
    }

    Ok(())
}

/// Get tx from rpc and in case of error, purge the asset from the db.
async fn process_signature(
    cl_audit: TxQueryResult,
    rpc: Rpc,
    context: BubblegumContext,
    tree: Vec<u8>,
) -> Result<()> {
    let sig = Signature::try_from(cl_audit.tx.as_ref()).unwrap();
    let tx = rpc.get_transaction(&sig).await?;

    if tx.transaction.meta.clone().unwrap().err.is_some() {
        let db = SqlxPostgresConnector::from_sqlx_postgres_pool(context.database_pool);
        purge_asset_from_db(&db, cl_audit.leaf_idx, tree).await?;
    }

    Ok(())
}

/// Remove asset from all tables in the db
async fn purge_asset_from_db(db: &DatabaseConnection, leaf_idx: i64, tree: Vec<u8>) -> Result<()> {
    let (asset, _) = Pubkey::find_program_address(
        &[b"asset", &tree, &leaf_idx.to_le_bytes()],
        &mpl_bubblegum::ID,
    );
    let asset_bytes = asset.to_bytes().to_vec();
    let multi_txn = db.begin().await?;

    asset_data::Entity::delete_by_id(asset_bytes.clone())
        .exec(&multi_txn)
        .await?;

    asset::Entity::delete_by_id(asset_bytes.clone())
        .exec(&multi_txn)
        .await?;

    asset_creators::Entity::delete_many()
        .filter(asset_creators::Column::AssetId.eq(asset_bytes.clone()))
        .exec(&multi_txn)
        .await?;

    asset_authority::Entity::delete_many()
        .filter(asset_authority::Column::AssetId.eq(asset_bytes.clone()))
        .exec(&multi_txn)
        .await?;

    asset_grouping::Entity::delete_many()
        .filter(asset_grouping::Column::AssetId.eq(asset_bytes))
        .exec(&multi_txn)
        .await?;

    // Remove all txs for the asset from cl_audits_v2 so that the asset can be re-indexed by the backfiller
    cl_audits_v2::Entity::delete_many()
        .filter(cl_audits_v2::Column::LeafIdx.eq(leaf_idx))
        .filter(cl_audits_v2::Column::Tree.eq(tree))
        .exec(db)
        .await?;

    multi_txn.commit().await?;

    Ok(())
}
