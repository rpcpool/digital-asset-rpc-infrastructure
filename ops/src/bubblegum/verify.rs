use anyhow::Result;
use clap::Parser;
use das_bubblegum::{verify_bubblegum, BubblegumContext, VerifyArgs};
use das_core::{connect_db, PoolArgs, Rpc, SolanaRpcArgs};

#[derive(Debug, Parser, Clone)]
pub struct Args {
    /// Verify Bubblegum Args
    #[clap(flatten)]
    pub verify_bubblegum: VerifyArgs,

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

    verify_bubblegum(context, config.verify_bubblegum).await?;

    Ok(())
}
