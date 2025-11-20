use std::str::FromStr;

use crate::backfill::gap::TreeGapModel;
use crate::metrics;
use crate::tree::TreeResponse;
use crate::BubblegumContext;
use digital_asset_types::dao::cl_audits_v2;
use sea_orm::{ColumnTrait, EntityTrait, QueryFilter, QueryOrder, SqlxPostgresConnector};
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use tokio::time::Instant;

const GET_SIGNATURES_FOR_ADDRESS_LIMIT_DEFAULT: usize = 100;

/// Monitor gaps in the bubblegum index
pub async fn run(context: BubblegumContext) -> anyhow::Result<()> {
    let trees = TreeResponse::all(&context.solana_rpc).await?;

    let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(context.database_pool.clone());

    for tree in trees {
        let start_time = Instant::now();

        let gaps = TreeGapModel::find(&conn, tree.pubkey, 0).await?;

        let last_potential_gap = check_last_potential_gap(context.clone(), tree.pubkey).await?;

        if !gaps.is_empty() || last_potential_gap.is_some() {
            let mut total_gaps = gaps.len();
            if last_potential_gap.is_some() {
                total_gaps += 1;
            }
            metrics::BUBBLEGUM_GAPS_MONITOR_GAPS_PER_TREE.observe(total_gaps as f64);
        }

        for gap in gaps {
            let gap_len = gap.gap_end_seq - gap.gap_start_seq;
            metrics::BUBBLEGUM_GAPS_MONITOR_LENGTH_PER_GAP.observe(gap_len as f64);
        }

        if let Some(last_gap_length) = last_potential_gap {
            metrics::BUBBLEGUM_GAPS_MONITOR_LENGTH_PER_GAP.observe(last_gap_length as f64);
        }

        metrics::BUBBLEGUM_GAPS_MONITOR_TIME_PER_TREE.observe(start_time.elapsed().as_secs_f64());
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

    match last_sig_in_db {
        Some(model) => {
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
        }
        None => tracing::error!("No last sig in db for tree {}", tree),
    };

    Ok(last_potential_gap)
}
