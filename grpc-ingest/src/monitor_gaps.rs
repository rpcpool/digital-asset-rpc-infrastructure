use std::time::Duration;

use crate::config::ConfigMonitorGaps;
use crate::postgres::create_pool;
use das_bubblegum::{metrics, monitor_gaps, BubblegumContext};
use das_core::{Rpc, SolanaRpcArgs};
use tokio::time::Instant;

pub async fn run(config: ConfigMonitorGaps) -> anyhow::Result<()> {
    let database_pool = create_pool(config.postgres).await?;
    let rpc = Rpc::from_config(&SolanaRpcArgs {
        solana_rpc_url: config.rpc,
    });
    let context = BubblegumContext::new(database_pool, rpc);

    loop {
        let start_time = Instant::now();

        monitor_gaps::run(context.clone()).await?;

        // Reset total counts metrics
        metrics::SCANNED_TREES_COUNT.set(0);
        metrics::BUBBLEGUM_GAPS_MONITOR_TOTAL_GAPS_COUNT
            .with_label_values(&["trees_with_gaps"])
            .set(0);
        metrics::BUBBLEGUM_GAPS_MONITOR_TOTAL_GAPS_COUNT
            .with_label_values(&["gaps"])
            .set(0);
        metrics::BUBBLEGUM_GAPS_MONITOR_TOTAL_GAPS_COUNT
            .with_label_values(&["last_gaps"])
            .set(0);
        metrics::BUBBLEGUM_GAPS_MONITOR_TOTAL_GAPS_COUNT
            .with_label_values(&["gaps_length"])
            .set(0);
        metrics::BUBBLEGUM_GAPS_MONITOR_TOTAL_GAPS_COUNT
            .with_label_values(&["last_gap_with_seq_gt_0"])
            .set(0);
        metrics::BUBBLEGUM_GAPS_MONITOR_TOTAL_GAPS_COUNT
            .with_label_values(&["last_gap_with_seq_eq_0"])
            .set(0);
        metrics::BUBBLEGUM_GAPS_MONITOR_TOTAL_CL_AUDITS_V2_COUNT.set(0);

        tracing::info!(target: "monitor_gaps", "Monitor gaps iteration took {} seconds", start_time.elapsed().as_secs());
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}
