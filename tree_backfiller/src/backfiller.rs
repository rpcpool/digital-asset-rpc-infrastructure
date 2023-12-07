use crate::tree;
use anyhow::Result;
use clap::Parser;
use log::{debug, error, info};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Signature;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Semaphore};

#[derive(Debug, Parser, Clone)]
pub struct Config {
    /// Solana RPC URL
    #[arg(long, env)]
    pub solana_rpc_url: String,

    /// Number of tree crawler workers
    #[arg(long, env, default_value = "100")]
    pub tree_crawler_count: usize,

    /// The size of the signature channel. This is the number of signatures that can be queued up. If the channel is full, the crawler will block until there is space in the channel.
    #[arg(long, env, default_value = "1000")]
    pub signature_channel_size: usize,
}

pub async fn run(config: Config) -> Result<()> {
    let solana_rpc = Arc::new(RpcClient::new(config.solana_rpc_url));

    let trees = tree::all(&solana_rpc).await?;

    let semaphore = Arc::new(Semaphore::new(config.tree_crawler_count));
    let mut crawl_handlers = Vec::with_capacity(trees.len());

    let (sig_sender, mut sig_receiver) = mpsc::channel::<Signature>(config.signature_channel_size);
    let sig_solana_rpc = Arc::clone(&solana_rpc);

    let transaction_worker_count = Arc::new(AtomicUsize::new(0));
    let transaction_worker_count_check = Arc::clone(&transaction_worker_count);

    tokio::spawn(async move {
        loop {
            tokio::select! {
                Some(signature) = sig_receiver.recv() => {
                    // TODO: possibly limit spawn rate. currently limited by the sign channel size
                    let solana_rpc = Arc::clone(&sig_solana_rpc);
                    let transaction_worker_count = Arc::clone(&transaction_worker_count);

                    transaction_worker_count.fetch_add(1, Ordering::SeqCst);

                    tokio::spawn(async move {
                        match tree::transaction(solana_rpc, signature).await {
                            Ok(builder) => {}
                            Err(e) => error!("retrieving transaction: {:?}", e),
                        }

                        transaction_worker_count.fetch_sub(1, Ordering::SeqCst);
                    });
                }
                else => break,
            }
        }

        Ok::<(), anyhow::Error>(())
    });

    for tree in trees {
        let solana_rpc = Arc::clone(&solana_rpc);
        let semaphore = Arc::clone(&semaphore);
        let sig_sender = sig_sender.clone();

        let crawl_handler = tokio::spawn(async move {
            let _permit = semaphore.acquire().await?;

            if let Err(e) = tree::crawl(solana_rpc, sig_sender, tree).await {
                error!("crawling tree: {:?}", e);
            }

            Ok::<(), anyhow::Error>(())
        });

        crawl_handlers.push(crawl_handler);
    }

    futures::future::try_join_all(crawl_handlers).await?;

    while transaction_worker_count_check.load(Ordering::SeqCst) > 0 {
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    Ok(())
}
