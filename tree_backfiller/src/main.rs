mod backfiller;
mod tree;

use anyhow::Result;
use clap::{Parser, Subcommand};
use log::{debug, error, info};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Signature;
use std::sync::Arc;
use tokio::sync::Semaphore;

#[derive(Debug, Parser)]
#[clap(author, version)]
struct Args {
    #[command(subcommand)]
    action: ArgsAction,
}

#[derive(Debug, Clone, Subcommand)]
enum ArgsAction {
    /// The 'run' command is used to cross-reference the index against on-chain accounts.
    /// It crawls through trees and backfills any missed tree transactions.
    /// This is particularly useful for ensuring data consistency and completeness.
    #[command(name = "run")]
    Run(backfiller::Config),
}
#[tokio::main]
async fn main() -> Result<()> {
    // lookup all trees
    // fetch transaction for tree
    // tx already exist in db and processed then next else write transaction to database
    // fetch and parse block for transaction

    // tree::lookup_all(rpc, tree_channel)
    // tree::crawl_tree(rpc, conn, tree_channel)
    // forward::send(rpc, conn, tree_channel)

    let args = Args::parse();

    match args.action {
        ArgsAction::Run(config) => backfiller::run(config).await,
    }
}
