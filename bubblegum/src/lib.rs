mod backfill;
mod error;
mod tree;
mod verify;

use anyhow::Result;
use backfill::worker::TreeWorkerArgs;
use clap::Parser;
use das_core::Rpc;
use futures::{stream::FuturesUnordered, StreamExt};
use tree::TreeResponse;

#[derive(Clone)]
pub struct BubblegumContext {
    pub database_pool: sqlx::PgPool,
    pub solana_rpc: Rpc,
}

impl BubblegumContext {
    pub const fn new(database_pool: sqlx::PgPool, solana_rpc: Rpc) -> Self {
        Self {
            database_pool,
            solana_rpc,
        }
    }
}

#[derive(Debug, Parser, Clone)]
pub struct BackfillArgs {
    /// Number of tree crawler workers
    #[arg(long, env, default_value = "20")]
    pub tree_crawler_count: usize,

    /// The list of trees to crawl. If not specified, all trees will be crawled.
    #[arg(long, env, use_value_delimiter = true)]
    pub only_trees: Option<Vec<String>>,

    #[clap(flatten)]
    pub tree_worker: TreeWorkerArgs,
}

#[derive(Debug, Parser, Clone)]
pub struct VerifyArgs {
    /// The list of trees to verify. If not specified, all trees will be crawled.
    #[arg(long, env, use_value_delimiter = true)]
    pub only_trees: Option<Vec<String>>,
}

pub async fn start_backfill(context: BubblegumContext, args: BackfillArgs) -> Result<()> {
    let trees = if let Some(ref only_trees) = args.only_trees {
        TreeResponse::find(&context.solana_rpc, only_trees.clone()).await?
    } else {
        TreeResponse::all(&context.solana_rpc).await?
    };

    let mut crawl_handles = FuturesUnordered::new();

    for tree in trees {
        if crawl_handles.len() >= args.tree_crawler_count {
            crawl_handles.next().await;
        }
        let context = context.clone();
        let handle = args.tree_worker.start(context, tree);

        crawl_handles.push(handle);
    }

    futures::future::try_join_all(crawl_handles).await?;

    Ok(())
}

pub async fn verify_bubblegum(context: BubblegumContext, args: VerifyArgs) -> Result<()> {
    let trees = if let Some(ref only_trees) = args.only_trees {
        TreeResponse::find(&context.solana_rpc, only_trees.clone()).await?
    } else {
        TreeResponse::all(&context.solana_rpc).await?
    };

    for tree in trees {
        verify::check(context.clone(), tree).await?;
    }

    Ok(())
}
