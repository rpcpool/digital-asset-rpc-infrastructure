use {
    crate::{
        config::{ConfigIngester, REDIS_STREAM_DATA_KEY},
        postgres::{create_pool as pg_create_pool, report_pgpool},
        prom::redis_xadd_status_inc,
        redis::{AccountHandle, DownloadMetadataJsonHandle, IngestStream, TransactionHandle},
        util::create_shutdown,
    },
    das_core::{
        create_download_metadata_notifier, DownloadMetadata, DownloadMetadataInfo,
        DownloadMetadataJsonRetryConfig, DownloadMetadataNotifier,
    },
    futures::{
        future::BoxFuture,
        stream::{FuturesUnordered, StreamExt},
    },
    program_transformers::ProgramTransformer,
    redis::aio::MultiplexedConnection,
    std::sync::Arc,
    tokio::{
        sync::mpsc::{unbounded_channel, UnboundedSender},
        task::JoinHandle,
        time::{sleep, Duration},
    },
    tracing::warn,
};

#[allow(unused)]
fn download_metadata_notifier_v2(
    connection: MultiplexedConnection,
    stream: String,
    stream_maxlen: usize,
) -> anyhow::Result<DownloadMetadataNotifier> {
    Ok(
        Box::new(
            move |info: DownloadMetadataInfo| -> BoxFuture<
                'static,
                Result<(), Box<dyn std::error::Error + Send + Sync>>,
            > {
                let mut connection = connection.clone();
                let stream = stream.clone();
                Box::pin(async move {

                    let info_bytes = serde_json::to_vec(&info)?;

                    let xadd = redis::cmd("XADD")
                        .arg(&stream)
                        .arg("MAXLEN")
                        .arg("~")
                        .arg(stream_maxlen)
                        .arg("*")
                        .arg(REDIS_STREAM_DATA_KEY)
                        .arg(info_bytes)
                        .query_async::<_, redis::Value>(&mut connection)
                        .await;

                    let status = xadd.map(|_| ()).map_err(|_| ());

                    redis_xadd_status_inc(&stream, "metadata_notifier",status, 1);

                    Ok(())
                })
            },
        ),
    )
}

/// Concurrent version for executing redis_xadd cmd.
///
/// It creates an Tokio task that spawns a new concurrent task per msg received over the channel
pub fn download_metadata_notifier_v3(
    connection: MultiplexedConnection,
    stream: String,
    stream_maxlen: usize,
    worker_count: usize,
) -> (JoinHandle<()>, UnboundedSender<DownloadMetadataInfo>) {
    let (sender, mut rx) = unbounded_channel::<DownloadMetadataInfo>();

    let handle = tokio::spawn(async move {
        let mut handlers = FuturesUnordered::new();

        while let Some(download_metadata_info) = rx.recv().await {
            if handlers.len() >= worker_count {
                handlers.next().await;
            }

            let connection = connection.clone();
            let stream = stream.clone();

            handlers.push(tokio::spawn(async move {
                redis_xadd(connection, stream, stream_maxlen, download_metadata_info).await;
            }));
        }

        while handlers.next().await.is_some() {}
    });

    (handle, sender)
}

/// Wrapper around redis xadd cmd
pub async fn redis_xadd(
    mut connection: MultiplexedConnection,
    stream: String,
    stream_maxlen: usize,
    download_metadata_info: DownloadMetadataInfo,
) {
    match serde_json::to_vec(&download_metadata_info) {
        Ok(info_bytes) => {
            let xadd = redis::cmd("XADD")
                .arg(&stream)
                .arg("MAXLEN")
                .arg("~")
                .arg(stream_maxlen)
                .arg("*")
                .arg(REDIS_STREAM_DATA_KEY)
                .arg(info_bytes)
                .query_async::<_, redis::Value>(&mut connection)
                .await;

            let status = xadd.map(|_| ()).map_err(|_| ());

            redis_xadd_status_inc(&stream, "metadata_notifier", status, 1);
        }
        Err(_) => {
            tracing::error!("download_metadata_info failed to bytes")
        }
    }
}

pub async fn run(config: ConfigIngester) -> anyhow::Result<()> {
    let redis_client = redis::Client::open(config.redis)?;
    let connection = redis_client.get_multiplexed_tokio_connection().await?;
    let pool = pg_create_pool(config.postgres).await?;

    let download_metadata_stream = config.download_metadata.stream.clone();
    let download_metadata_stream_maxlen = config.download_metadata.stream_maxlen;

    let (metadata_notifier_rec_handle, metadata_notifier_sender) = download_metadata_notifier_v3(
        connection.clone(),
        download_metadata_stream.name.clone(),
        download_metadata_stream_maxlen,
        config.download_metadata.max_concurrency,
    );

    let download_metadata_notifier =
        create_download_metadata_notifier(metadata_notifier_sender).await;

    let program_transformer = Arc::new(ProgramTransformer::new(
        pool.clone(),
        download_metadata_notifier,
    ));
    let http_client = reqwest::Client::builder()
        .timeout(config.download_metadata.request_timeout)
        .build()?;

    let download_metadata = Arc::new(DownloadMetadata::new(http_client, pool.clone()));
    let download_metadatas = IngestStream::build()
        .config(config.download_metadata.stream.clone())
        .connection(connection.clone())
        .handler(DownloadMetadataJsonHandle::new(
            Arc::clone(&download_metadata),
            Arc::new(DownloadMetadataJsonRetryConfig::new(
                config.download_metadata.max_attempts,
                config.download_metadata.retry_max_delay_ms,
                config.download_metadata.retry_min_delay_ms,
            )),
        ))
        .start()
        .await?;

    let accounts = IngestStream::build()
        .config(config.accounts)
        .connection(connection.clone())
        .handler(AccountHandle::new(Arc::clone(&program_transformer)))
        .start()
        .await?;

    let transactions = IngestStream::build()
        .config(config.transactions)
        .connection(connection.clone())
        .handler(TransactionHandle::new(Arc::clone(&program_transformer)))
        .start()
        .await?;

    let snapshots = IngestStream::build()
        .config(config.snapshots)
        .connection(connection.clone())
        .handler(AccountHandle::new(program_transformer))
        .start()
        .await?;

    let mut shutdown = create_shutdown()?;

    let report_pool = pool.clone();
    let report = tokio::spawn(async move {
        let pool = report_pool.clone();
        loop {
            sleep(Duration::from_millis(100)).await;
            report_pgpool(pool.clone());
        }
    });

    if let Some(signal) = shutdown.next().await {
        warn!(
            target: "ingester",
            action = "shutdown_signal_received",
            message = "Shutdown signal received, waiting for spawned tasks to complete",
            signal = ?signal
        );
    }

    futures::future::join_all(vec![
        accounts.stop(),
        transactions.stop(),
        snapshots.stop(),
        download_metadatas.stop(),
    ])
    .await
    .into_iter()
    .collect::<anyhow::Result<()>>()?;

    metadata_notifier_rec_handle.await?;

    report.abort();

    pool.close().await;

    Ok::<(), anyhow::Error>(())
}
