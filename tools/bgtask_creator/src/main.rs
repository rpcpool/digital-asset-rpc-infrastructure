use digital_asset_types::dao::{
    asset, asset_authority, asset_creators, asset_data, asset_grouping,
    sea_orm_active_enums::TaskStatus, tasks, tokens,
};

use log::{debug, error, info};

use nft_ingester::{
    config::rand_string,
    config::{init_logger, setup_config},
    database::setup_database,
    error::IngesterError,
    metrics::setup_metrics,
    tasks::{
        BgTask, BgTaskConfig, DownloadMetadata, DownloadMetadataTask, IntoTaskData, TaskManager,
    },
};

use std::{path::PathBuf, time};

use futures::TryStreamExt;

use sea_orm::{
    entity::*, query::*, DbBackend, DeleteResult, EntityTrait, JsonValue, SqlxPostgresConnector,
};

use clap::{value_parser, Arg, ArgAction, Command};

use sqlx::types::chrono::Utc;

use solana_sdk::{bs58, pubkey::Pubkey};
use std::{collections::HashMap, str::FromStr, sync::Arc};

/**
 * The bgtask creator is intended to be use as a tool to handle assets that have not been indexed.
 * It will delete all the current bgtasks and create new ones for assets where the metadata is missing.
 *
 * Currently it will try every missing asset every run.
 */

#[tokio::main(flavor = "multi_thread")]
pub async fn main() {
    init_logger();
    info!("Starting bgtask creator");

    let matches = Command::new("bgtaskcreator")
        .arg(
            Arg::new("config")
                .long("config")
                .short('c')
                .help("Sets a custom config file")
                .required(false)
                .action(ArgAction::Set)
                .value_parser(value_parser!(PathBuf)),
        )
        .arg(
            Arg::new("batch_size")
                .long("batch-size")
                .short('b')
                .help("Sets the batch size for the assets to be processed.")
                .required(false)
                .action(ArgAction::Set)
                .value_parser(value_parser!(u64))
                .default_value("1000"),
        )
        .arg(
            Arg::new("ignore-url")
                .long("ignore-url")
                .short('i')
                .help("ignore matching url when creating tasks")
                .required(false)
                .action(ArgAction::Set),
        )
        .arg(
            Arg::new("include-url")
                .long("include-url")
                .short('u')
                .help("include only the matching url when creating tasks")
                .required(false)
                .action(ArgAction::Set),
        )
        .arg(
            Arg::new("limit")
                .long("limit")
                .short('l')
                .help("maximum number of tasks to create")
                .required(false)
                .action(ArgAction::Set)
                .value_parser(value_parser!(u64))
                .default_value("0"),
        )
        .arg(
            Arg::new("authority")
                .long("authority")
                .short('a')
                .help("Create background tasks for the given authority")
                .required(false)
                .action(ArgAction::Set),
        )
        .arg(
            Arg::new("collection")
                .long("collection")
                .short('o')
                .help("Create background tasks for the given collection")
                .required(false)
                .action(ArgAction::Set),
        )
        .arg(
            Arg::new("mint")
                .long("mint")
                .short('m')
                .help("Create background tasks for the given mint")
                .required(false)
                .action(ArgAction::Set),
        )
        .arg(
            Arg::new("creator")
                .long("creator")
                .short('r')
                .help("Create background tasks for the given creator")
                .required(false)
                .action(ArgAction::Set),
        )
        .arg(
            Arg::new("force-reindex")
                .long("force-reindex")
                .help("Re-index even if off-chain is already indexed")
                .required(false)
                .action(ArgAction::SetTrue),
        )
        .subcommand(
            Command::new("show").about("Show tasks").arg(
                Arg::new("print")
                    .long("print")
                    .short('p')
                    .help("Print the tasks to stdout")
                    .required(false)
                    .action(clap::ArgAction::SetTrue),
            ),
        )
        .subcommand(Command::new("create").about("Create new background tasks"))
        .subcommand(Command::new("delete").about("Delete ALL pending background tasks"))
        .subcommand(Command::new("find").about("Find and describe a task by asset id"))
        .get_matches();

    let config = setup_config();

    // Optionally setup metrics if config demands it
    setup_metrics(&config);

    // One pool many clones, this thing is thread safe and send sync
    let database_pool = setup_database(config.clone()).await;

    //Setup definitions for background tasks
    let task_runner_config = BgTaskConfig::default();
    let bg_task_definitions: Vec<Box<dyn BgTask>> = vec![Box::new(DownloadMetadataTask {
        lock_duration: task_runner_config.lock_duration,
        max_attempts: task_runner_config.max_attempts,
        timeout: Some(time::Duration::from_secs(
            task_runner_config.timeout.unwrap_or(3),
        )),
    })];
    let mut bg_tasks = HashMap::new();
    for task in bg_task_definitions {
        bg_tasks.insert(task.name().to_string(), task);
    }
    let task_map = Arc::new(bg_tasks);

    let instance_name = rand_string();

    // Get a postgres connection from the pool
    let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(database_pool.clone());

    let batch_size = matches.get_one::<u64>("batch_size").unwrap();
    let authority = matches.get_one::<String>("authority");
    let collection = matches.get_one::<String>("collection");
    let mint = matches.get_one::<String>("mint");
    let creator = matches.get_one::<String>("creator");
    let ignore_url = matches.get_one::<String>("ignore-url");
    let include_url = matches.get_one::<String>("include-url");
    let limit = matches.get_one::<u64>("limit").unwrap();
    let force_reindex = matches.get_one::<bool>("force-reindex");

    match matches.subcommand_name() {
        Some("find") => {
            // Re-using this field for simplicity.
            let asset_id = Pubkey::from_str(mint.unwrap()).unwrap();
            let asset_id_bytes = asset_id.clone().to_bytes().to_vec();
            let asset_data = asset_data::Entity::find_by_id(asset_id_bytes.clone())
                .one(&conn)
                .await
                .unwrap()
                .unwrap();

            println!("off-chain data for asset: {:?}", asset_data.metadata);

            let mut task = DownloadMetadata {
                asset_data_id: asset_id_bytes.clone(),
                uri: asset_data.metadata_url,
                created_at: Some(Utc::now().naive_utc()),
            };
            task.sanitize();
            let task_data = task.clone().into_task_data().unwrap();
            let hash = task_data.hash().unwrap().clone();
            let task_entry = tasks::Entity::find_by_id(hash)
                .filter(tasks::Column::Status.ne(TaskStatus::Pending))
                .one(&conn)
                .await;
            println!("task: {:?}", task_entry)
        }
        Some("show") => {

            // TODO: Needs to be refactored after re-adding "force-reindex".

            // // Check the total number of assets in the DB
            // let condition_found =
            //     asset_data::Column::Metadata.ne(JsonValue::String("processing".to_string()));
            // let condition_missing =
            //     asset_data::Column::Metadata.eq(JsonValue::String("processing".to_string()));

            // let asset_data_finished = find_by_type(
            //     authority,
            //     collection,
            //     creator,
            //     mint,
            //     condition_found,
            //     include_url,
            //     ignore_url,
            // );
            // let asset_data_processing = find_by_type(
            //     authority,
            //     collection,
            //     creator,
            //     mint,
            //     condition_missing,
            //     include_url,
            //     ignore_url,
            // );
            // let asset_data_reindex = find_by_type(
            //     authority,
            //     collection,
            //     creator,
            //     mint,
            //     condition_reindex,
            //     include_url,
            //     ignore_url,
            // );

            // let mut asset_data_missing = asset_data_processing
            //     .0
            //     .order_by(asset_data::Column::Id, Order::Asc)
            //     .paginate(&conn, *batch_size)
            //     .into_stream();

            // let asset_data_count = asset_data_finished.0.count(&conn).await;
            // let asset_reindex_count = asset_data_reindex.0.count(&conn).await;

            // let mut i = 0;
            // while let Some(assets) = asset_data_missing.try_next().await.unwrap() {
            //     info!("Found {} assets", assets.len());
            //     i += assets.len();
            //     if let Some(matches) = matches.subcommand_matches("show") {
            //         if matches.get_flag("print") {
            //             for asset in assets {
            //                 info!(
            //                     "{}, missing asset, {:?}",
            //                     asset_data_processing.1,
            //                     Pubkey::try_from(asset.id)
            //                 );
            //             }
            //         }
            //     }
            // }

            // let total_finished = asset_data_count.unwrap_or(0);
            // let total_assets = i + total_finished as usize;
            // info!(
            //     "{}, reindexing assets: {:?}, total finished assets: {}, missing assets: {}, total assets: {}",
            //     asset_data_processing.1,
            //     asset_reindex_count,
            //     total_finished,
            //     i,
            //     total_assets
            // );
        }
        Some("delete") => {
            info!("Deleting all existing tasks");

            // Delete all existing tasks
            let deleted_tasks: Result<DeleteResult, IngesterError> = tasks::Entity::delete_many()
                .exec(&conn)
                .await
                .map_err(|e| e.into());

            match deleted_tasks {
                Ok(result) => {
                    info!("Deleted a number of tasks {}", result.rows_affected);
                }
                Err(e) => {
                    info!("Error deleting tasks: {}", e);
                }
            }
        }
        Some("create") => {
            let asset_data = find_by_type(
                authority,
                collection,
                creator,
                mint,
                include_url,
                ignore_url,
                force_reindex,
            );

            let mut asset_data_missing = asset_data
                .0
                .order_by(asset_data::Column::Id, Order::Asc)
                .paginate(&conn, *batch_size)
                .into_stream();

            let mut count = 0;
            // Find all the assets with missing metadata
            let mut tasks = Vec::new();
            while let Some(assets) = asset_data_missing.try_next().await.unwrap() {
                info!("Total {} assets matched conditions", assets.len());
                for asset in assets {
                    let asset_id = bs58::encode(asset.id.clone()).into_string();
                    let mut task = DownloadMetadata {
                        asset_data_id: asset.id,
                        uri: asset.metadata_url,
                        created_at: Some(Utc::now().naive_utc()),
                    };

                    task.sanitize();
                    let task_data = task.clone().into_task_data().unwrap();

                    debug!(
                        "Print task {} hash {:?}, uri: {:?}, asset_id: {:?}",
                        task_data.data,
                        task_data.hash(),
                        task.uri,
                        asset_id
                    );
                    let name = instance_name.clone();
                    if let Ok(hash) = task_data.hash() {
                        let database_pool = database_pool.clone();
                        let task_map = task_map.clone();
                        let name = name.clone();
                        let new_task = tokio::task::spawn(async move {
                            let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(
                                database_pool.clone(),
                            );

                            // Check if the task being added is already stored in the DB and is not pending
                            let task_entry = tasks::Entity::find_by_id(hash.clone())
                                .filter(tasks::Column::Status.ne(TaskStatus::Pending))
                                .one(&conn)
                                .await;
                            if let Ok(Some(e)) = task_entry {
                                info!("Found duplicate task: {:?} {:?}", e, hash.clone());
                                return;
                            }

                            let task_hash = task_data.hash().unwrap();
                            let res = TaskManager::new_task_handler(
                                database_pool.clone(),
                                name.clone(),
                                name,
                                task_data,
                                task_map.clone(),
                                false,
                            )
                            .await;

                            match res {
                                Ok(_) => {
                                    info!("Task created: {:?} asset: {:?}", task_hash, asset_id);
                                }
                                Err(e) => {
                                    error!("Task failed: {:?} asset: {:?}", e, asset_id);
                                }
                            }
                        });
                        tasks.push(new_task);
                    }
                    count += 1;
                }
                if *limit > 0 && count >= *limit {
                    break;
                }
            }

            if tasks.is_empty() {
                info!("No assets with missing metadata found");
            } else {
                info!("Found {} tasks to process", tasks.len());
                let mut succeeded = 0;
                let mut failed = 0;
                for task in tasks {
                    match task.await {
                        Ok(_) => succeeded += 1,
                        Err(e) => {
                            info!("Task failed: {}", e);
                            failed += 1;
                        }
                    }
                }
                info!("Tasks succeeded={}, failed={}", succeeded, failed);
            }
        }
        _ => {
            info!("Please provide an action")
        }
    }
}

fn find_by_type<'a>(
    authority: Option<&'a String>,
    collection: Option<&'a String>,
    creator: Option<&'a String>,
    mint: Option<&'a String>,
    include_url: Option<&'a String>,
    ignore_url: Option<&'a String>,
    force_reindex: Option<&'a bool>,
) -> (
    sea_orm::Select<digital_asset_types::dao::asset_data::Entity>,
    String,
) {
    let mut conditions = Condition::all();

    if !force_reindex.unwrap_or(&false).clone() {
        conditions = conditions.add(asset_data::Column::Reindex.eq(true));
    }

    if let Some(url) = include_url {
        conditions = conditions.add(asset_data::Column::MetadataUrl.like(url));
    }
    if let Some(url) = ignore_url {
        conditions = conditions.add(asset_data::Column::MetadataUrl.not_like(url));
    }

    if let Some(authority) = authority {
        info!(
            "Find asset data for authority {} conditions {:?}",
            authority, conditions
        );

        let pubkey = Pubkey::from_str(authority.as_str()).unwrap();
        let pubkey_bytes = pubkey.to_bytes().to_vec();

        (
            asset_data::Entity::find()
                .join_rev(
                    JoinType::InnerJoin,
                    asset_authority::Entity::belongs_to(asset_data::Entity)
                        .from(asset_authority::Column::AssetId)
                        .to(asset_data::Column::Id)
                        .into(),
                )
                .filter(conditions.add(asset_authority::Column::Authority.eq(pubkey_bytes))),
            authority.to_string(),
        )
    } else if let Some(collection) = collection {
        info!(
            "Finding asset_data for collection {}, conditions {:?}",
            collection, conditions
        );

        (
            asset_data::Entity::find()
                .join_rev(
                    JoinType::InnerJoin,
                    asset_grouping::Entity::belongs_to(asset_data::Entity)
                        .from(asset_grouping::Column::AssetId)
                        .to(asset_data::Column::Id)
                        .into(),
                )
                .filter(conditions.add(asset_grouping::Column::GroupValue.eq(collection.as_str()))),
            collection.to_string(),
        )
    } else if let Some(mint) = mint {
        info!(
            "Finding assets for mint {}, conditions {:?}",
            mint, conditions
        );

        let pubkey = Pubkey::from_str(mint.as_str()).unwrap();
        let pubkey_bytes = pubkey.to_bytes().to_vec();

        (
            asset_data::Entity::find_by_id(pubkey_bytes),
            mint.to_string(),
        )
    } else if let Some(creator) = creator {
        info!(
            "Finding assets for creator {} with conditions {:?}",
            creator, conditions
        );

        let pubkey = Pubkey::from_str(creator.as_str()).unwrap();
        let pubkey_bytes = pubkey.to_bytes().to_vec();

        (
            asset_data::Entity::find()
                .join_rev(
                    JoinType::InnerJoin,
                    asset_creators::Entity::belongs_to(asset_data::Entity)
                        .from(asset_creators::Column::AssetId)
                        .to(asset_data::Column::Id)
                        .into(),
                )
                .filter(conditions.add(asset_creators::Column::Creator.eq(pubkey_bytes))),
            creator.to_string(),
        )
    } else {
        info!("Finding all assets with condition {:?}", conditions);
        (
            asset_data::Entity::find().filter(Condition::all().add(conditions)),
            "all".to_string(),
        )
    }
}
