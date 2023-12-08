use anyhow::Result;
use clap::Parser;
use sea_orm::{ConnectOptions, Database, DatabaseConnection, DbErr};

#[derive(Debug, Parser, Clone)]
pub struct PoolArgs {
    #[arg(long, env)]
    pub database_url: String,
    #[arg(long, env, default_value = "125")]
    pub database_max_connections: u32,
    #[arg(long, env, default_value = "5")]
    pub database_min_connections: u32,
}

pub async fn connect(config: PoolArgs) -> Result<DatabaseConnection, DbErr> {
    let mut options = ConnectOptions::new(config.database_url);

    options
        .min_connections(config.database_min_connections)
        .max_connections(config.database_max_connections);

    Database::connect(options).await
}
