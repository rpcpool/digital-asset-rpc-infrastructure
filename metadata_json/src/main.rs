use clap::{Parser, Subcommand};

mod cmds;
mod stream;
mod worker;

use cmds::{backfill, ingest, report, single};

#[derive(Parser)]
#[command(author, about, next_line_help = true)]
struct Args {
    #[command(subcommand)]
    action: Action,
}

#[derive(Subcommand, Clone)]
enum Action {
    Ingest(ingest::IngestArgs),
    Backfill(backfill::BackfillArgs),
    Single(single::SingleArgs),
    Report(report::ReportArgs),
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    env_logger::init();

    let args = Args::parse();

    match args.action {
        Action::Ingest(args) => ingest::run(args).await,
        Action::Backfill(args) => backfill::run(args).await,
        Action::Single(args) => single::run(args).await,
        Action::Report(args) => report::run(args).await,
    }
}
