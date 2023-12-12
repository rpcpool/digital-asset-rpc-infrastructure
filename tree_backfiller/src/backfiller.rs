use crate::db;
use crate::{queue, tree};
use anyhow::Result;
use clap::Parser;
use log::{debug, error, info};
use sea_orm::SqlxPostgresConnector;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Signature;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::Duration;

#[derive(Debug, Parser, Clone)]
pub struct Args {
    /// Solana RPC URL
    #[arg(long, env)]
    pub solana_rpc_url: String,

    /// Number of tree crawler workers
    #[arg(long, env, default_value = "1")]
    pub tree_crawler_count: usize,

    /// The size of the signature channel. This is the number of signatures that can be queued up. If the channel is full, the crawler will block until there is space in the channel.
    #[arg(long, env, default_value = "1")]
    pub signature_channel_size: usize,

    #[arg(long, env, default_value = "1")]
    pub queue_channel_size: usize,

    #[arg(long, env, default_value = "3000")]
    pub transaction_check_timeout: u64,

    /// Database configuration
    #[clap(flatten)]
    pub database: db::PoolArgs,

    /// Redis configuration
    #[clap(flatten)]
    pub queue: queue::QueueArgs,
}

/// A thread-safe counter.
pub struct Counter(Arc<AtomicUsize>);

impl Counter {
    /// Creates a new counter initialized to zero.
    pub fn new() -> Self {
        Self(Arc::new(AtomicUsize::new(0)))
    }

    /// Increments the counter by one.
    pub fn increment(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    /// Decrements the counter by one.
    pub fn decrement(&self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }

    /// Returns the current value of the counter.
    pub fn get(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }

    /// Returns a future that resolves when the counter reaches zero.
    /// The future periodically checks the counter value and sleeps for a short duration.
    pub fn zero(&self) -> impl std::future::Future<Output = ()> {
        let counter = self.clone();
        async move {
            while counter.get() > 0 {
                println!("Counter value: {}", counter.get());
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

impl Clone for Counter {
    /// Returns a clone of the counter.
    /// The returned counter shares the same underlying atomic integer.
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

/// Runs the tree backfiller.
///
/// This function takes a `Config` as input and returns a `Result<()>`.
/// It creates an `RpcClient` and retrieves all trees.
/// It then spawns a thread for each tree and a separate thread to handle transaction workers.
/// The function waits for all threads to finish before returning.
pub async fn run(config: Args) -> Result<()> {
    let solana_rpc = Arc::new(RpcClient::new(config.solana_rpc_url));
    let sig_solana_rpc = Arc::clone(&solana_rpc);

    let pool = db::connect(config.database).await?;

    let (queue_sender, mut queue_receiver) = mpsc::channel::<Vec<u8>>(config.queue_channel_size);
    let (sig_sender, mut sig_receiver) = mpsc::channel::<Signature>(config.signature_channel_size);

    let transaction_worker_count = Counter::new();
    let transaction_worker_count_check = transaction_worker_count.clone();

    tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(signature) = sig_receiver.recv() => {
                    let solana_rpc = Arc::clone(&sig_solana_rpc);
                    let transaction_worker_count_sig = transaction_worker_count.clone();
                    let queue_sender = queue_sender.clone();

                    transaction_worker_count_sig.increment();

                    let transaction_task = async move {
                        if let Err(e) = tree::transaction(solana_rpc, queue_sender,  signature).await {
                            error!("retrieving transaction: {:?}", e);
                        }

                        transaction_worker_count_sig.decrement();
                    };

                    tokio::spawn(transaction_task);
                },
                else => break,
            }
        }

        Ok::<(), anyhow::Error>(())
    });

    let queue_handler = tokio::spawn(async move {
        let mut queue = queue::Queue::setup(config.queue).await?;

        while let Some(data) = queue_receiver.recv().await {
            if let Err(e) = queue.push(&data).await {
                error!("pushing to queue: {:?}", e);
            }
        }

        Ok::<(), anyhow::Error>(())
    });

    let trees = tree::all(&solana_rpc).await?;

    let semaphore = Arc::new(Semaphore::new(config.tree_crawler_count));
    let mut crawl_handlers = Vec::with_capacity(trees.len());

    for tree in trees {
        let solana_rpc = Arc::clone(&solana_rpc);
        let semaphore = Arc::clone(&semaphore);
        let sig_sender = sig_sender.clone();
        let pool = pool.clone();
        let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(pool);

        let crawl_handler = tokio::spawn(async move {
            let _permit = semaphore.acquire().await?;

            if let Err(e) = tree.crawl(solana_rpc, sig_sender, conn).await {
                error!("crawling tree: {:?}", e);
            }

            Ok::<(), anyhow::Error>(())
        });

        crawl_handlers.push(crawl_handler);
    }

    futures::future::try_join_all(crawl_handlers).await?;
    transaction_worker_count_check.zero().await;
    let _ = queue_handler.await?;

    Ok(())
}
