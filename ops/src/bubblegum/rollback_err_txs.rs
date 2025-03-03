use anyhow::Result;
use clap::Parser;
use das_bubblegum::{rollback_err_txs, BubblegumContext, RollbackErrTxsArgs};
use das_core::{connect_db, PoolArgs, Rpc, SolanaRpcArgs};

#[derive(Debug, Parser, Clone)]
pub struct Args {
    /// Backfill Bubblegum Args
    #[clap(flatten)]
    pub rollback_err_txs_args: RollbackErrTxsArgs,

    /// Database configuration
    #[clap(flatten)]
    pub database: PoolArgs,

    /// Solana configuration
    #[clap(flatten)]
    pub solana: SolanaRpcArgs,
}

pub async fn run(config: Args) -> Result<()> {
    let database_pool = connect_db(&config.database).await?;

    let solana_rpc = Rpc::from_config(&config.solana);
    let context = BubblegumContext::new(database_pool, solana_rpc);

    rollback_err_txs(context, config.rollback_err_txs_args).await?;

    Ok(())
}
