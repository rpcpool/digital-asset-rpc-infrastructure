use {
    crate::{
        config::{load as config_load, ConfigGrpc, ConfigIngest, ConfigPrometheus, ConfigSnapshot},
        prom::run_metrics_server,
    },
    anyhow::Context,
    clap::{Parser, Subcommand},
    config::ConfigMonitor,
    std::{env, net::SocketAddr},
    tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter},
};

mod accountsdb_helpers;
mod config;
mod grpc;
mod ingest;
mod monitor;
mod postgres;
mod prom;
mod redis;
mod snapshot;
mod util;
mod version;

#[derive(Debug, Parser)]
#[clap(author, version)]
struct Args {
    /// Path to config file
    #[clap(short, long)]
    config: String,

    /// Prometheus listen address
    #[clap(long)]
    prometheus: Option<SocketAddr>,

    #[command(subcommand)]
    action: ArgsAction,
}

#[derive(Debug, Clone, Subcommand)]
enum ArgsAction {
    /// Subscribe on Geyser events using gRPC and send them to Redis
    #[command(name = "grpc")]
    Grpc,
    /// Run ingester process (process events from Redis)
    #[command(name = "ingest")]
    /// Ingest live updates from Redis
    Ingest,
    #[command(name = "monitor")]
    /// Monitor correctness of Bubblegum proofs
    Monitor,
    /// Continual snapshot repair
    #[command(name = "snapshot")]
    Snapshot,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env_filter = EnvFilter::builder()
        .parse(env::var(EnvFilter::DEFAULT_ENV).unwrap_or_else(|_| "error".to_owned()))
        .unwrap();

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt::layer())
        .init();

    let args = Args::parse();

    // Run prometheus server
    let config = config_load::<ConfigPrometheus>(&args.config)
        .await
        .with_context(|| format!("failed to parse prometheus config from: {}", args.config))?;
    if let Some(address) = args.prometheus.or(config.prometheus) {
        run_metrics_server(address)?;
    }

    match args.action {
        ArgsAction::Grpc => {
            let config = config_load::<ConfigGrpc>(&args.config)
                .await
                .with_context(|| format!("failed to parse config from: {}", args.config))?;
            grpc::run(config).await
        }
        ArgsAction::Ingest => {
            let config = config_load::<ConfigIngest>(&args.config)
                .await
                .with_context(|| format!("failed to parse config from: {}", args.config))?;
            ingest::run(config).await
        }
        ArgsAction::Monitor => {
            let config = config_load::<ConfigMonitor>(&args.config)
                .await
                .with_context(|| format!("failed to parse config from: {}", args.config))?;

            monitor::run(config).await
        }
        ArgsAction::Snapshot => {
            let config = config_load::<ConfigSnapshot>(&args.config)
                .await
                .with_context(|| format!("failed to parse config from: {}", args.config))?;

            snapshot::run(config).await
        }
    }
}
