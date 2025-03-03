use super::{backfiller, replay, rollback_err_txs, verify};
use anyhow::Result;
use clap::{Args, Subcommand};

#[derive(Debug, Clone, Subcommand)]
pub enum Commands {
    /// The 'backfill' command is used to cross-reference the index against on-chain accounts.
    /// It crawls through trees and backfills any missed tree transactions.
    #[clap(name = "backfill")]
    Backfill(backfiller::Args),
    #[clap(name = "replay")]
    Replay(replay::Args),
    /// The 'verify' command is used to verify the integrity of the bubblegum index.
    Verify(verify::Args),
    /// The 'rollback-errored-transactions' command is used to rollback errored transactions.
    #[clap(name = "rollback-errored-transactions")]
    RollbackErroredTransactions(rollback_err_txs::Args),
}

#[derive(Debug, Clone, Args)]
pub struct BubblegumCommand {
    #[clap(subcommand)]
    pub action: Commands,
}

pub async fn subcommand(subcommand: BubblegumCommand) -> Result<()> {
    match subcommand.action {
        Commands::Backfill(args) => {
            backfiller::run(args).await?;
        }
        Commands::Replay(args) => {
            replay::run(args).await?;
        }
        Commands::Verify(args) => {
            verify::run(args).await?;
        }
        Commands::RollbackErroredTransactions(args) => {
            rollback_err_txs::run(args).await?;
        }
    }

    Ok(())
}
