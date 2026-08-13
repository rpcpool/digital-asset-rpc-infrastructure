use anyhow::{Context, Result};
use clap::Parser;
use das_core::{
    build_download_client, connect_db, perform_metadata_json_task, DownloadMetadataInfo,
    DownloadMetadataJsonRetryConfig, MetadataJsonDownloadWorkerArgs, PoolArgs,
};

use indicatif::HumanDuration;
use log::{debug, error};
use std::sync::Arc;
use tokio::{sync::mpsc::channel, task::JoinSet, time::Instant};

const SCAN_WINDOW: i64 = 50_000;

#[derive(Parser, Clone, Debug)]
pub struct ConfigArgs {
    /// The number of db entries to process in a single batch
    #[arg(long, env, default_value = "10")]
    pub batch_size: u64,
}

#[derive(Debug, Parser, Clone)]
pub struct Args {
    /// Metadata JSON download worker configuration
    #[clap(flatten)]
    pub metadata_json_download_worker: MetadataJsonDownloadWorkerArgs,
    // Configuration arguments
    #[clap(flatten)]
    pub config: ConfigArgs,
    /// Database configuration
    #[clap(flatten)]
    pub database: PoolArgs,
}

#[derive(Debug, Clone)]
pub struct MetadataJsonBackfillerContext {
    pub database_pool: sqlx::PgPool,
    pub batch_size: u64,
    pub metadata_json_download_worker: MetadataJsonDownloadWorkerArgs,
}

pub async fn start_backfill(context: MetadataJsonBackfillerContext) -> Result<()> {
    let MetadataJsonBackfillerContext {
        database_pool,
        batch_size,
        metadata_json_download_worker:
            MetadataJsonDownloadWorkerArgs {
                metadata_json_download_worker_count,
                metadata_json_download_worker_request_timeout,
            },
    } = context;

    let worker_count = metadata_json_download_worker_count;
    let batch_size = batch_size.max(1) as usize;

    let control_pool = database_pool.clone();

    let (batch_sender, mut batch_receiver) =
        channel::<Vec<DownloadMetadataInfo>>(worker_count.max(1));

    let control = tokio::spawn(async move {
        let mut last_id: Vec<u8> = Vec::new();

        loop {
            let rows: Vec<(Vec<u8>, String, bool)> = sqlx::query_as(
                r#"
                SELECT
                    id,
                    metadata_url,
                    COALESCE(
                        metadata = '"processing"'::jsonb
                        OR (
                            metadata ->> '_das_status' = 'unreachable'
                            AND metadata ->> '_das_url' IS DISTINCT FROM metadata_url
                        ),
                        false
                    ) AS is_match
                FROM asset_data
                WHERE id > $1
                ORDER BY id ASC
                LIMIT $2
                "#,
            )
            .bind(&last_id)
            .bind(SCAN_WINDOW)
            .fetch_all(&control_pool)
            .await
            .context("fetching asset_data primary-key window")?;

            if rows.is_empty() {
                break;
            }

            last_id = rows
                .last()
                .map(|(id, _, _)| id.clone())
                .expect("non-empty window has a last row");

            let matches: Vec<DownloadMetadataInfo> = rows
                .into_iter()
                .filter(|(_, _, is_match)| *is_match)
                .map(|(id, metadata_url, _)| DownloadMetadataInfo::new(id, metadata_url))
                .collect();

            for chunk in matches.chunks(batch_size) {
                if batch_sender.send(chunk.to_vec()).await.is_err() {
                    return Ok(());
                }
            }
        }

        Ok::<(), anyhow::Error>(())
    });

    let mut tasks = JoinSet::new();

    let client = build_download_client(metadata_json_download_worker_request_timeout)?;

    let retry_config = Arc::new(DownloadMetadataJsonRetryConfig::default());

    while let Some(dm_vec) = batch_receiver.recv().await {
        let pool = database_pool.clone();
        if tasks.len() >= worker_count {
            tasks.join_next().await;
        }

        tasks.spawn(fetch_metadata_and_process(
            client.clone(),
            pool.clone(),
            dm_vec,
            Arc::clone(&retry_config),
        ));
    }

    control.await??;

    while tasks.join_next().await.is_some() {}

    Ok(())
}

async fn fetch_metadata_and_process(
    client: reqwest::Client,
    pool: sqlx::PgPool,
    download_metadata_info: Vec<DownloadMetadataInfo>,
    config: Arc<DownloadMetadataJsonRetryConfig>,
) {
    debug!(
        "Spawning metadata fetch task for {} assets",
        download_metadata_info.len()
    );

    for d in download_metadata_info.iter() {
        let timing = Instant::now();
        let asset_data_id = bs58::encode(d.asset_data_id.clone()).into_string();

        if let Err(e) =
            perform_metadata_json_task(client.clone(), pool.clone(), d, Arc::clone(&config)).await
        {
            error!("Asset {} failed: {}", asset_data_id, e);
        }

        debug!(
            "Asset {} finished in {}",
            asset_data_id,
            HumanDuration(timing.elapsed())
        );
    }
}

pub async fn run(args: Args) -> Result<()> {
    let database_pool = connect_db(&args.database).await?;

    let context = MetadataJsonBackfillerContext {
        database_pool,
        batch_size: args.config.batch_size,
        metadata_json_download_worker: args.metadata_json_download_worker,
    };

    start_backfill(context).await?;

    Ok(())
}
