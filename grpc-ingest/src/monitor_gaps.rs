use std::time::Duration;

use crate::config::ConfigMonitorGaps;
use crate::postgres::create_pool;
use das_bubblegum::{monitor_gaps, BubblegumContext};
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

        tracing::info!(target: "monitor_gaps", "Monitor gaps iteration took {} seconds", start_time.elapsed().as_secs());
        tokio::time::sleep(Duration::from_secs(600)).await;
    }
}
