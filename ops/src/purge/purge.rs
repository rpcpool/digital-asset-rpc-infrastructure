use anyhow::Result;
use borsh::BorshDeserialize;
use clap::Parser;
use das_core::{connect_db, PoolArgs};
use das_core::{Rpc, SolanaRpcArgs};
use digital_asset_types::dao::{asset, token_accounts};
use log::{debug, error};
use sea_orm::{entity::*, sea_query::Expr, SqlxPostgresConnector};
use sea_orm::{ConnectionTrait, DeleteMany, EntityTrait, QueryFilter};
use solana_sdk::pubkey;
use solana_sdk::pubkey::Pubkey;
use tokio::task::JoinHandle;

pub const TOKEN_PROGRAM_ID: Pubkey = pubkey!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");

pub const TOKEN_2022_PROGRAM_ID: Pubkey = pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");

pub const MAX_GET_MULTIPLE_ACCOUNTS: usize = 100;

#[derive(Parser, Clone, Debug)]
pub struct ConfigArgs {
    /// The number of worker threads
    #[arg(long, env, default_value = "25")]
    pub workers: u64,
}

#[derive(Debug, Parser, Clone)]
pub struct Args {
    // The configuration for the purge command
    #[clap(flatten)]
    pub config: ConfigArgs,
    /// Database configuration
    #[clap(flatten)]
    pub database: PoolArgs,
    /// Solana configuration
    #[clap(flatten)]
    pub solana: SolanaRpcArgs,
}

pub const DEFAULT_WORKER_COUNT: usize = 25;

pub async fn start_ta_purge(args: Args) -> Result<()> {
    let db_pool = connect_db(&args.database).await?;
    let mut worker_count = if args.config.workers > 0 {
        args.config.workers as usize
    } else {
        DEFAULT_WORKER_COUNT
    };
    let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(db_pool.clone());

    let token_accounts = token_accounts::Entity::find().all(&conn).await?;

    let token_accounts_len = token_accounts.len();

    if token_accounts_len == 0 {
        debug!("No token accounts to purge");
        return Ok(());
    }

    if worker_count > token_accounts_len {
        if token_accounts_len == 1 {
            worker_count = 1;
        } else {
            // If the number of entries is less than the number of workers, we assume each worker will handle 2 assets
            worker_count = token_accounts_len / 2;
        }
    }

    let excess_tasks = token_accounts_len % worker_count;
    let tasks_per_worker = token_accounts_len / worker_count;

    let mut tasks: Vec<JoinHandle<()>> = Vec::with_capacity(worker_count);

    let mut curr_start = 0;

    for worker_index in 0..worker_count {
        let start = curr_start;

        let additional_task = if worker_index < excess_tasks { 1 } else { 0 };

        let end = start + tasks_per_worker + additional_task;

        let rpc = Rpc::from_config(&args.solana);

        let handle = fetch_and_purge_ta(db_pool.clone(), &token_accounts[start..end], &rpc);

        tasks.push(handle);

        curr_start = end;
    }

    futures::future::join_all(tasks).await;

    Ok(())
}

fn fetch_and_purge_ta(
    pool: sqlx::PgPool,
    token_accounts: &[token_accounts::Model],
    rpc: &Rpc,
) -> JoinHandle<()> {
    let acc_keys = token_accounts
        .iter()
        .filter_map(|ta| Pubkey::try_from_slice(&ta.pubkey.as_slice()).ok())
        .collect::<Vec<Pubkey>>();

    let rpc = rpc.clone();

    let handle = tokio::spawn(async move {
        let acc_keys_chuncks = acc_keys.chunks(MAX_GET_MULTIPLE_ACCOUNTS);

        let mut tasks = Vec::with_capacity(acc_keys_chuncks.len());

        for chunk in acc_keys_chuncks {
            let keys = chunk.to_vec();
            let pool = pool.clone();
            let rpc = rpc.clone();
            let handle = tokio::spawn(async move {
                if let Ok(accounts) = rpc.get_multiple_accounts(&keys).await {
                    let mut accounts_to_purge = Vec::new();
                    for (key, acc) in keys.iter().zip(accounts.iter()) {
                        match acc {
                            Some(acc) => {
                                if acc.owner.ne(&TOKEN_PROGRAM_ID)
                                    && acc.owner.ne(&TOKEN_2022_PROGRAM_ID)
                                {
                                    accounts_to_purge.push(key);
                                }
                            }
                            None => {
                                accounts_to_purge.push(key);
                            }
                        }
                    }

                    let accounts_to_purge = accounts_to_purge
                        .iter()
                        .map(|a| a.to_bytes().to_vec())
                        .collect::<Vec<Vec<u8>>>();

                    if !accounts_to_purge.is_empty() {
                        let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone());
                        let delete_res = token_accounts::Entity::delete_many()
                            .filter(
                                token_accounts::Column::Pubkey
                                    .is_in(accounts_to_purge.iter().map(|a| a.as_slice())),
                            )
                            .exec(&conn)
                            .await;

                        if let Ok(res) = delete_res {
                            debug!(
                                "Successfully deleted token accounts: {:?}",
                                res.rows_affected
                            );
                        } else {
                            error!("Failed to delete token accounts: {:?}", accounts_to_purge);
                        }
                    }
                }
            });

            tasks.push(handle);
        }

        futures::future::join_all(tasks).await;
    });

    handle
}

pub async fn start_mint_purge(args: Args) -> Result<()> {
    let db_pool = connect_db(&args.database).await?;
    let mut worker_count = if args.config.workers > 0 {
        args.config.workers as usize
    } else {
        DEFAULT_WORKER_COUNT
    };
    let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(db_pool.clone());

    let assets = asset::Entity::find().all(&conn).await?;

    let assets_len = assets.len();

    if assets_len == 0 {
        debug!("No assets to purge");
        return Ok(());
    }

    if worker_count > assets_len {
        if assets_len == 1 {
            worker_count = 1;
        } else {
            // If the number of entries is less than the number of workers, we assume each worker will handle 2 assets
            worker_count = assets_len / 2;
        }
    }

    let excess_tasks = assets_len % worker_count;
    let tasks_per_worker = assets_len / worker_count;

    let mut tasks: Vec<JoinHandle<()>> = Vec::with_capacity(worker_count);

    let mut curr_start = 0;

    for worker_index in 0..worker_count {
        let start = curr_start;

        let additional_task = if worker_index < excess_tasks { 1 } else { 0 };

        let end = start + tasks_per_worker + additional_task;

        let rpc = Rpc::from_config(&args.solana);

        let handle = fetch_and_purge_assets(db_pool.clone(), &assets[start..end], &rpc);

        tasks.push(handle);

        curr_start = end;
    }

    futures::future::join_all(tasks).await;

    Ok(())
}

fn fetch_and_purge_assets(
    pool: sqlx::PgPool,
    assets: &[asset::Model],
    rpc: &Rpc,
) -> JoinHandle<()> {
    let mint_keys = assets
        .iter()
        .filter_map(|a| Pubkey::try_from_slice(&a.id.as_slice()).ok())
        .collect::<Vec<Pubkey>>();

    let rpc = rpc.clone();

    let handle = tokio::spawn(async move {
        let mint_keys_chuncks = mint_keys.chunks(MAX_GET_MULTIPLE_ACCOUNTS);

        let mut tasks = Vec::with_capacity(mint_keys_chuncks.len());

        for chunk in mint_keys_chuncks {
            let keys = chunk.to_vec();
            let pool = pool.clone();
            let rpc = rpc.clone();
            let handle = tokio::spawn(async move {
                if let Ok(accounts) = rpc.get_multiple_accounts(&keys).await {
                    let mut accounts_to_update = Vec::new();
                    for (key, acc) in keys.iter().zip(accounts.iter()) {
                        match acc {
                            Some(acc) => {
                                if acc.owner.ne(&TOKEN_PROGRAM_ID)
                                    && acc.owner.ne(&TOKEN_2022_PROGRAM_ID)
                                {
                                    accounts_to_update.push(key);
                                }
                            }
                            None => {
                                accounts_to_update.push(key);
                            }
                        }
                    }

                    let accounts_to_update = accounts_to_update
                        .iter()
                        .map(|a| a.to_bytes().to_vec())
                        .collect::<Vec<Vec<u8>>>();

                    if !accounts_to_update.is_empty() {
                        let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone());
                        let update_res = asset::Entity::update_many()
                            .filter(
                                asset::Column::Id
                                    .is_in(accounts_to_update.iter().map(|a| a.as_slice())),
                            )
                            .col_expr(asset::Column::Burnt, Expr::value(true))
                            .exec(&conn)
                            .await;

                        if let Ok(res) = update_res {
                            debug!(
                                "Successfully marked assets as burnt: {:?}",
                                res.rows_affected
                            );
                        } else {
                            error!("Failed to update assets: {:?}", accounts_to_update);
                        }
                    }
                }
            });

            tasks.push(handle);
        }

        futures::future::join_all(tasks).await;
    });

    handle
}
