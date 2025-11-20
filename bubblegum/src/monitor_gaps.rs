use crate::backfill::gap::TreeGapModel;
use crate::metrics;
use crate::tree::TreeResponse;
use crate::BubblegumContext;
use digital_asset_types::dao::cl_audits_v2;
use sea_orm::{
    ColumnTrait, ConnectionTrait, DatabaseBackend, DatabaseConnection, EntityTrait, QueryFilter,
    QueryOrder, SqlxPostgresConnector, Statement,
};
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use std::io::Write;
use std::{fs::File, str::FromStr};
use tokio::time::Instant;

const GET_SIGNATURES_FOR_ADDRESS_LIMIT_DEFAULT: usize = 100;

/// Monitor gaps in the bubblegum index
pub async fn run(context: BubblegumContext) -> anyhow::Result<()> {
    tracing::info!(target: "monitor_gaps", "Starting monitor gaps");

    // RPC trees
    let trees = TreeResponse::all(&context.solana_rpc).await?;
    metrics::TOTAL_TREES_COUNT
        .with_label_values(&["rpc"])
        .set(trees.len() as i64);

    tracing::info!(target: "monitor_gaps", "Trees in RPC: {}", trees.len());

    let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(context.database_pool.clone());

    get_trees_in_db(&conn).await?;
    get_total_cl_audits_v2_in_db(&conn).await?;

    let mut trees_with_no_sigs_in_db = vec![];

    for tree in trees {
        let start_time = Instant::now();

        let gaps = TreeGapModel::find(&conn, tree.pubkey, 0).await?;

        let last_potential_gap = check_last_potential_gap(context.clone(), tree.pubkey).await?;

        if last_potential_gap.is_none() {
            tracing::error!(
                target: "no_sigs_for_tree",
                "No last sig in db for tree {} - seq: {}",
                tree.pubkey,
                tree.seq
            );

            if tree.seq > 0 {
                // Save to file
                trees_with_no_sigs_in_db.push(tree.pubkey);

                metrics::BUBBLEGUM_GAPS_MONITOR_NON_EXISTENT_TREES_IN_DB
                    .with_label_values(&["gt_0"])
                    .inc();
            } else {
                metrics::BUBBLEGUM_GAPS_MONITOR_NON_EXISTENT_TREES_IN_DB
                    .with_label_values(&["eq_0"])
                    .inc();
            }
        }

        if !gaps.is_empty() || last_potential_gap.is_some() {
            let mut total_gaps = gaps.len();
            if last_potential_gap.is_some() {
                total_gaps += 1;
            }
            metrics::BUBBLEGUM_GAPS_MONITOR_GAPS_PER_TREE.observe(total_gaps as f64);
            metrics::BUBBLEGUM_GAPS_MONITOR_TOTAL_GAPS_COUNT
                .with_label_values(&["trees_with_gaps"])
                .inc();
            metrics::BUBBLEGUM_GAPS_MONITOR_TOTAL_GAPS_COUNT
                .with_label_values(&["gaps"])
                .add(total_gaps as i64);
        }

        for gap in gaps {
            let gap_len = gap.gap_end_seq - gap.gap_start_seq;
            metrics::BUBBLEGUM_GAPS_MONITOR_LENGTH_PER_GAP.observe(gap_len as f64);
            metrics::BUBBLEGUM_GAPS_MONITOR_TOTAL_GAPS_COUNT
                .with_label_values(&["gaps_length"])
                .add(gap_len);
        }

        if let Some(last_gap_length) = last_potential_gap {
            metrics::BUBBLEGUM_GAPS_MONITOR_LENGTH_PER_GAP.observe(last_gap_length as f64);
            metrics::BUBBLEGUM_GAPS_MONITOR_TOTAL_GAPS_COUNT
                .with_label_values(&["gaps_length"])
                .add(last_gap_length as i64);
            metrics::BUBBLEGUM_GAPS_MONITOR_TOTAL_GAPS_COUNT
                .with_label_values(&["last_gaps"])
                .inc();

            // Record discriminated last gap length
            metrics::BUBBLEGUM_GAPS_MONITOR_LAST_GAPS_LENGTH.observe(last_gap_length as f64);
        }

        metrics::BUBBLEGUM_GAPS_MONITOR_TIME_PER_TREE.observe(start_time.elapsed().as_secs_f64());
        metrics::SCANNED_TREES_COUNT.inc();
    }

    // Write trees with no sigs in DB to file (overwrite if exists)
    if !trees_with_no_sigs_in_db.is_empty() {
        let mut file = File::create("bubblegum_trees_with_no_sigs_in_db_errors.log")?;

        for tree in &trees_with_no_sigs_in_db {
            writeln!(file, "{}", tree)?;
        }
    }

    Ok(())
}

/// Get the most recent tx for the tree from the DB, and compares it against the
///  las RPC get_signatures_for_address call, to check if there is not any new txs
/// missing in the DB.
async fn check_last_potential_gap(
    context: BubblegumContext,
    tree: Pubkey,
) -> anyhow::Result<Option<usize>> {
    let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(context.database_pool);

    let last_tx_sigs_in_rpc = context
        .solana_rpc
        .get_signatures_for_address(
            &tree,
            None,
            None,
            Some(GET_SIGNATURES_FOR_ADDRESS_LIMIT_DEFAULT),
        )
        .await?;

    let last_sig_in_db = cl_audits_v2::Entity::find()
        .filter(cl_audits_v2::Column::Tree.eq(tree.as_ref().to_vec()))
        .order_by_desc(cl_audits_v2::Column::Seq)
        .one(&conn)
        .await?;

    // Check final last potential gap (rpc vs db)
    let mut last_potential_gap = None;

    if let Some(model) = last_sig_in_db {
        let tx_sig = Signature::try_from(model.tx.as_ref())?;
        let mut gap_length = 0;

        for sig in last_tx_sigs_in_rpc {
            if sig.err.is_some() {
                continue;
            }

            let rpc_sig = Signature::from_str(&sig.signature)?;
            if rpc_sig == tx_sig {
                if gap_length > 0 {
                    last_potential_gap = Some(gap_length);
                }

                break;
            }

            gap_length += 1;
        }
    };

    Ok(last_potential_gap)
}

/// Reads the trees in the DB (cl_audits_v2 and cl_items) and updates the metrics for it.
async fn get_trees_in_db(conn: &DatabaseConnection) -> anyhow::Result<i64> {
    // Trees in DB cl_audits_v2
    let trees_in_cl_audits_v2 = conn
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT COUNT(DISTINCT tree) AS count FROM cl_audits_v2".to_string(),
        ))
        .await?
        .map(|row| row.try_get::<i64>("", "count"))
        .transpose()?
        .unwrap_or(0);

    tracing::info!(target: "monitor_gaps", "Trees in DB cl_audits_v2: {}", trees_in_cl_audits_v2);

    metrics::TOTAL_TREES_COUNT
        .with_label_values(&["db_cl_audits_v2"])
        .set(trees_in_cl_audits_v2);

    // Trees in DB cl_items
    let trees_in_cl_items = conn
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT COUNT(DISTINCT tree) AS count FROM cl_items".to_string(),
        ))
        .await?
        .map(|row| row.try_get::<i64>("", "count"))
        .transpose()?
        .unwrap_or(0);

    tracing::info!(target: "monitor_gaps", "Trees in DB cl_items: {}", trees_in_cl_items);

    metrics::TOTAL_TREES_COUNT
        .with_label_values(&["db_cl_items"])
        .set(trees_in_cl_items);

    Ok(trees_in_cl_audits_v2)
}

/// Get the total number of cl_audits_v2 in the DB and update the metric.
async fn get_total_cl_audits_v2_in_db(conn: &DatabaseConnection) -> anyhow::Result<i64> {
    let total_cl_audits_v2 = conn
        .query_one(Statement::from_string(
            DatabaseBackend::Postgres,
            "SELECT COUNT(*) FROM cl_audits_v2".to_string(),
        ))
        .await?
        .map(|row| row.try_get::<i64>("", "count"))
        .transpose()?
        .unwrap_or(0);

    tracing::info!(target: "monitor_gaps", "Total cl_audits_v2 in DB: {}", total_cl_audits_v2);

    metrics::BUBBLEGUM_GAPS_MONITOR_TOTAL_CL_AUDITS_V2_COUNT.set(total_cl_audits_v2);

    Ok(total_cl_audits_v2)
}
