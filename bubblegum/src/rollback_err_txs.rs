use super::BubblegumContext;
use crate::backfill::worker::FetchedEncodedTransactionWithStatusMeta;
use crate::tree::TreeResponse;
use anyhow::Result;
use das_core::{
    create_download_metadata_notifier, DownloadMetadataJsonRetryConfig,
    MetadataJsonDownloadWorkerArgs, Rpc,
};
use digital_asset_types::dao::{
    asset, asset_authority, asset_creators, asset_data, asset_grouping, cl_audits_v2, cl_items,
};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use program_transformers::{ProgramTransformer, TransactionInfo};
use sea_orm::{
    ColumnTrait, DatabaseConnection, DbErr, EntityTrait, FromQueryResult, QueryFilter, QuerySelect,
    QueryTrait, SqlxPostgresConnector, TransactionTrait,
};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use tracing::{debug, error};

pub async fn start(
    context: BubblegumContext,
    tree: TreeResponse,
    tx_concurrency: usize,
    metadata_json_download_worker: MetadataJsonDownloadWorkerArgs,
) -> Result<()> {
    let download_config = Arc::new(DownloadMetadataJsonRetryConfig::default());

    let (metadata_json_download_worker, metadata_json_download_sender) =
        metadata_json_download_worker.start(context.database_pool.clone(), download_config)?;

    let download_metadata_notifier =
        create_download_metadata_notifier(metadata_json_download_sender).await;

    let program_transformer = Arc::new(ProgramTransformer::new(
        context.database_pool.clone(),
        download_metadata_notifier,
    ));

    let db = SqlxPostgresConnector::from_sqlx_postgres_pool(context.database_pool.clone());
    let rpc = context.solana_rpc.clone();

    debug!("Rolling back err txs for tree: {:?}", tree.pubkey);

    let signatures_set = create_signatures_set(&db, tree.pubkey.as_ref().to_vec()).await?;
    let mut errors_in_rpc = 0;
    let mut errors_in_db = 0;
    let mut batch_iteration = 0;
    let mut before = None;

    let mut sigs_with_error = HashSet::new();

    loop {
        let sigs = rpc
            .get_signatures_for_address(&tree.pubkey, before, None, None)
            .await?;
        let sig_count = sigs.len();

        for sig in sigs {
            let signature = Signature::from_str(&sig.signature)?;
            if sig.err.is_some() {
                errors_in_rpc += 1;
                if signatures_set.contains(&signature) {
                    debug!(
                        "signature: {:?} - batch_iteration: {:?}",
                        signature, batch_iteration
                    );
                    errors_in_db += 1;

                    sigs_with_error.insert(signature);
                }
            }

            before = Some(signature);
        }

        batch_iteration += 1;

        if sig_count < 1000 {
            break;
        }
    }

    debug!("errors_in_rpc: {:?}", errors_in_rpc);
    debug!("errors_in_db: {:?}", errors_in_db);

    process_signatures(
        context,
        tree.pubkey.as_ref().to_vec(),
        sigs_with_error,
        program_transformer,
        tx_concurrency,
    )
    .await?;

    metadata_json_download_worker.await?;

    Ok(())
}

/// Process a Set of signatures with `TransactionError` and all txs in the db with the same seq.
async fn process_signatures(
    context: BubblegumContext,
    tree: Vec<u8>,
    sigs_with_error: HashSet<Signature>,
    program_transformer: Arc<ProgramTransformer>,
    tx_concurrency: usize,
) -> Result<()> {
    let db = SqlxPostgresConnector::from_sqlx_postgres_pool(context.database_pool.clone());

    let initial_txs = sigs_with_error
        .iter()
        .map(|sig| sig.as_ref().to_vec())
        .collect();

    let txs = get_txs_for_seqs(&db, tree.clone(), initial_txs).await?;

    // Delete all cl_items with the same seq/seqs
    delete_cl_items(&db, tree.clone(), &txs).await?;

    let sigs_with_error = Arc::new(sigs_with_error);

    let mut handlers = FuturesUnordered::new();

    for cl_audit in txs {
        if handlers.len() >= tx_concurrency {
            handlers.next().await;
        }

        let sigs_with_error = Arc::clone(&sigs_with_error);
        let rpc = context.solana_rpc.clone();
        let context = context.clone();
        let program_transformer = Arc::clone(&program_transformer);

        let handle = tokio::spawn(async move {
            let sig = Signature::try_from(cl_audit.tx.as_ref()).unwrap();

            if sigs_with_error.contains(&sig) {
                debug!("Rolling back errored tx: {:?}", sig);
                let res =
                    rollback_errored_tx(cl_audit, sigs_with_error, context, program_transformer)
                        .await;
                if let Err(e) = res {
                    error!("Failed to rollback errored tx: {:?}", e);
                }
            } else {
                debug!("Rolling back successful tx: {:?}", sig);
                let res = rollback_successful_tx(&sig, rpc.clone(), program_transformer).await;
                if let Err(e) = res {
                    error!("Failed to rollback successful tx: {:?}", e);
                }
            }
        });

        handlers.push(handle);
    }

    futures::future::join_all(handlers).await;

    Ok(())
}

async fn delete_cl_items(
    db: &DatabaseConnection,
    tree: Vec<u8>,
    cl_audits: &[cl_audits_v2::Model],
) -> Result<()> {
    let seqs = cl_audits
        .iter()
        .map(|cl_audit| cl_audit.seq)
        .collect::<HashSet<i64>>();

    debug!("seqs: {:?}", seqs);

    let rows_affected = cl_items::Entity::delete_many()
        .filter(cl_items::Column::Tree.eq(tree))
        .filter(cl_items::Column::Seq.is_in(seqs))
        .exec(db)
        .await?;

    debug!("rows_affected: {:?}", rows_affected);

    Ok(())
}

/// Get assets affected by the tx, remove them from the db and re-process all txs in `cl_audits_v2` for the asset.
///  Remove tx from cl_audits_v2.
async fn rollback_errored_tx(
    tx_to_rollback: cl_audits_v2::Model,
    sigs_with_error: Arc<HashSet<Signature>>,
    context: BubblegumContext,
    program_transformer: Arc<ProgramTransformer>,
) -> Result<()> {
    let db = SqlxPostgresConnector::from_sqlx_postgres_pool(context.database_pool.clone());

    let assets_set =
        get_assets_for_tx(&db, tx_to_rollback.tree.clone(), tx_to_rollback.tx.clone()).await?;

    let sig = Signature::try_from(tx_to_rollback.tx.as_ref()).unwrap();

    for (asset, leaf_idx) in assets_set {
        remove_asset_from_db(&db, asset).await?;

        debug!(
            "rolling back asset: {:?} - leaf_idx: {:?} (tx: {:?})",
            asset, leaf_idx, sig
        );

        replay_all_txs_for_asset(
            &db,
            tx_to_rollback.tree.clone(),
            leaf_idx,
            Arc::clone(&sigs_with_error),
            context.solana_rpc.clone(),
            Arc::clone(&program_transformer),
        )
        .await?;
    }

    remove_tx_from_cl_audits_v2(&db, tx_to_rollback.tx, tx_to_rollback.tree).await?;

    Ok(())
}

/// Only reprocess current tx without updating assets related tables in db
async fn rollback_successful_tx(
    sig: &Signature,
    rpc: Rpc,
    program_transformer: Arc<ProgramTransformer>,
) -> Result<()> {
    let tx = rpc.get_transaction(sig).await?;
    let tx: TransactionInfo = FetchedEncodedTransactionWithStatusMeta(tx).try_into()?;

    program_transformer.handle_transaction(&tx).await?;

    Ok(())
}

/// Reads all txs from cl_audits_v2 and creates a Set of signatures.
async fn create_signatures_set(
    db: &DatabaseConnection,
    tree: Vec<u8>,
) -> Result<HashSet<Signature>> {
    #[derive(FromQueryResult)]
    struct TxColumn {
        tx: Vec<u8>,
    }

    let txs: Vec<TxColumn> = cl_audits_v2::Entity::find()
        .filter(cl_audits_v2::Column::Tree.eq(tree))
        .select_only()
        .column(cl_audits_v2::Column::Tx)
        .into_model::<TxColumn>()
        .all(db)
        .await?;

    debug!("txs in cl_audits_v2: {:?}", txs.len());

    let mut signatures_set = HashSet::new();
    for tx in txs {
        signatures_set.insert(Signature::try_from(tx.tx.as_ref())?);
    }

    debug!("signatures_set len: {:?}", signatures_set.len());

    Ok(signatures_set)
}

/// Get all txs in cl_audits_v2 with the same `seq`s as the received txs
async fn get_txs_for_seqs(
    db: &DatabaseConnection,
    tree: Vec<u8>,
    txs: Vec<Vec<u8>>,
) -> Result<Vec<cl_audits_v2::Model>, DbErr> {
    cl_audits_v2::Entity::find()
        .filter(cl_audits_v2::Column::Tree.eq(tree.clone()))
        .filter(
            cl_audits_v2::Column::Seq.in_subquery(
                cl_audits_v2::Entity::find()
                    .select_only()
                    .column(cl_audits_v2::Column::Seq)
                    .filter(cl_audits_v2::Column::Tree.eq(tree))
                    .filter(cl_audits_v2::Column::Tx.is_in(txs))
                    .into_query(),
            ),
        )
        .all(db)
        .await
}

/// Get all assets affected by the tx (in case of multiple ixs)
async fn get_assets_for_tx(
    db: &DatabaseConnection,
    tree: Vec<u8>,
    tx: Vec<u8>,
) -> Result<HashMap<Pubkey, i64>> {
    let txs = cl_audits_v2::Entity::find()
        .filter(cl_audits_v2::Column::Tree.eq(tree.clone()))
        .filter(cl_audits_v2::Column::Tx.eq(tx))
        .all(db)
        .await?;

    let mut assets_set = HashMap::new();
    for tx in txs {
        let (asset, _) = Pubkey::find_program_address(
            &[b"asset", &tree, &tx.leaf_idx.to_le_bytes()],
            &mpl_bubblegum::ID,
        );
        assets_set.insert(asset, tx.leaf_idx);
    }

    Ok(assets_set)
}

/// Fetch all txs for the asset and replay them. Filtering out the ones with errors.
async fn replay_all_txs_for_asset(
    db: &DatabaseConnection,
    tree: Vec<u8>,
    leaf_idx: i64,
    sigs_with_error: Arc<HashSet<Signature>>,
    rpc: Rpc,
    program_transformer: Arc<ProgramTransformer>,
) -> Result<()> {
    let txs = cl_audits_v2::Entity::find()
        .filter(cl_audits_v2::Column::Tree.eq(tree))
        .filter(cl_audits_v2::Column::LeafIdx.eq(leaf_idx))
        .all(db)
        .await?;

    for tx in txs {
        let sig = Signature::try_from(tx.tx.as_ref()).unwrap();

        if sigs_with_error.contains(&sig) {
            debug!("Trying to replay tx with error: {:?}", sig);
            continue;
        }

        let tx = rpc.get_transaction(&sig).await?;
        // Double check tx is not errored
        if tx.transaction.meta.clone().unwrap().err.is_some() {
            return Err(anyhow::anyhow!(
                "Tx with error not contained in sigs_with_error"
            ));
        }

        let tx: TransactionInfo = FetchedEncodedTransactionWithStatusMeta(tx).try_into()?;

        program_transformer.handle_transaction(&tx).await?;
    }

    Ok(())
}

/// Remove asset from all tables in the db
async fn remove_asset_from_db(db: &DatabaseConnection, asset: Pubkey) -> Result<()> {
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

    multi_txn.commit().await?;

    Ok(())
}

/// Remove tx from cl_audits_v2
async fn remove_tx_from_cl_audits_v2(
    db: &DatabaseConnection,
    tx: Vec<u8>,
    tree: Vec<u8>,
) -> Result<()> {
    cl_audits_v2::Entity::delete_many()
        .filter(cl_audits_v2::Column::Tx.eq(tx))
        .filter(cl_audits_v2::Column::Tree.eq(tree))
        .exec(db)
        .await?;

    Ok(())
}
