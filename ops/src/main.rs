mod account;
mod bubblegum;
mod metadata;
mod purge;

use std::env;

use account::{subcommand as account_subcommand, AccountCommand};
use anyhow::Result;
use bubblegum::{subcommand as bubblegum_subcommand, BubblegumCommand};
use clap::{Parser, Subcommand};
use das_ops::metrics;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[derive(Debug, Parser)]
#[clap(author, version)]
struct Args {
    #[arg(long, env = "PROMETHEUS_ADDR", default_value = "0.0.0.0:9464")]
    prometheus_addr: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    #[clap(name = "bubblegum")]
    Bubblegum(BubblegumCommand),
    #[clap(name = "account")]
    Account(AccountCommand),
    #[clap(name = "metadata_json")]
    MetadataJson(metadata::MetadataJsonCommand),
    #[clap(name = "purge")]
    Purge(purge::PurgeCommand),
}

#[tokio::main]
async fn main() -> Result<()> {
    let env_filter = EnvFilter::builder().parse(
        env::var(EnvFilter::DEFAULT_ENV)
            .unwrap_or_else(|_| "failed to parse env filter".to_owned()),
    )?;

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt::layer())
        .init();

    let args = Args::parse();

    metrics::run_metrics_server(args.prometheus_addr)?;

    match args.command {
        Command::Bubblegum(subcommand) => bubblegum_subcommand(subcommand).await?,
        Command::Account(subcommand) => account_subcommand(subcommand).await?,
        Command::MetadataJson(subcommand) => metadata::subcommand(subcommand).await?,
        Command::Purge(subcommand) => purge::subcommand(subcommand).await?,
    }

    Ok(())
}
