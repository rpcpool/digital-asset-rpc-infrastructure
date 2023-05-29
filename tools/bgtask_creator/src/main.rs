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
    tasks::{BgTask, DownloadMetadata, DownloadMetadataTask, IntoTaskData, TaskManager},
};

use std::path::PathBuf;

use futures::TryStreamExt;

use sea_orm::{entity::*, query::*, DeleteResult, EntityTrait, JsonValue, SqlxPostgresConnector};

use clap::{value_parser, Arg, ArgAction, Command};

use sqlx::types::chrono::Utc;

use solana_sdk::pubkey::Pubkey;
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
            Arg::new("delete")
                .long("delete")
                .short('d')
                .help("Delete all existing tasks before creating new ones.")
                .required(false)
                .action(clap::ArgAction::SetTrue),
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
            Arg::new("ignore-ipfs")
                .long("ignore-ipfs")
                .short('i')
                .help("ignore ipfs URLs when creating tasks")
                .required(false)
                .action(clap::ArgAction::SetTrue),
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
        .get_matches();

    let config = setup_config();

    // Optionally setup metrics if config demands it
    setup_metrics(&config);

    // One pool many clones, this thing is thread safe and send sync
    let database_pool = setup_database(config.clone()).await;

    //Setup definitions for background tasks
    let bg_task_definitions: Vec<Box<dyn BgTask>> = vec![Box::new(DownloadMetadataTask {})];
    let mut bg_tasks = HashMap::new();
    for task in bg_task_definitions {
        bg_tasks.insert(task.name().to_string(), task);
    }
    let task_map = Arc::new(bg_tasks);

    let instance_name = rand_string();

    // Get a postgres connection from the pool
    let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(database_pool.clone());

    if matches.get_flag("delete") {
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

    let batch_size = matches.get_one::<u64>("batch_size").unwrap();
    let authority = matches.get_one::<String>("authority");
    let collection = matches.get_one::<String>("collection");
    let mint = matches.get_one::<String>("mint");
    let creator = matches.get_one::<String>("creator");
    let ignore_ipfs = matches.get_flag("ignore-ipfs");

    let all = "all".to_string();
    let mut asset_data_missing = if let Some(authority) = authority {
        info!(
            "Creating new tasks for assets with missing metadata for authority {}, batch size={}",
            authority, batch_size
        );

        let pubkey = Pubkey::from_str(&authority.as_str()).unwrap();
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
                .filter(
                    Condition::all()
                        .add(asset_authority::Column::Authority.eq(pubkey_bytes))
                        .add(asset_data::Column::Reindex.eq(false)),
                )
                .order_by(asset_data::Column::Id, Order::Asc)
                .paginate(&conn, *batch_size)
                .into_stream(),
            authority,
        )
    } else if let Some(collection) = collection {
        info!(
            "Creating new tasks for assets with missing metadata for collection {}, batch size={}",
            collection, batch_size
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
                .filter(
                    Condition::all()
                        .add(asset_grouping::Column::GroupValue.eq(collection.as_str()))
                        .add(asset_data::Column::Reindex.eq(false)),
                )
                .order_by(asset_data::Column::Id, Order::Asc)
                .paginate(&conn, *batch_size)
                .into_stream(),
            collection,
        )
    } else if let Some(mint) = mint {
        info!(
            "Creating new tasks for assets with missing metadata for mint {}, batch size={}",
            mint, batch_size
        );

        let pubkey = Pubkey::from_str(&mint.as_str()).unwrap();
        let pubkey_bytes = pubkey.to_bytes().to_vec();

        (
            asset_data::Entity::find()
                .join(JoinType::InnerJoin, asset::Relation::AssetData.def())
                .join_rev(
                    JoinType::InnerJoin,
                    tokens::Entity::belongs_to(asset::Entity)
                        .from(tokens::Column::Mint)
                        .to(asset::Column::SupplyMint)
                        .into(),
                )
                .filter(
                    Condition::all()
                        .add(tokens::Column::MintAuthority.eq(pubkey_bytes))
                        .add(asset_data::Column::Reindex.eq(false)),
                )
                .order_by(asset_data::Column::Id, Order::Asc)
                .paginate(&conn, *batch_size)
                .into_stream(),
            mint,
        )
    } else if let Some(creator) = creator {
        info!(
            "Creating new tasks for assets with missing metadata for creator {}, batch size={}",
            creator, batch_size
        );

        let pubkey = Pubkey::from_str(&creator.as_str()).unwrap();
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
                .filter(
                    Condition::all()
                        .add(asset_creators::Column::Creator.eq(pubkey_bytes))
                        .add(asset_data::Column::Reindex.eq(false)),
                )
                .order_by(asset_data::Column::Id, Order::Asc)
                .paginate(&conn, *batch_size)
                .into_stream(),
            creator,
        )
    } else {
        info!(
            "Creating new tasks for all assets with missing metadata, batch size={}",
            batch_size
        );

        let mut query =
            asset_data::Entity::find()
                .filter(Condition::all().add(
                    asset_data::Column::Metadata.eq(JsonValue::String("processing".to_string())),
                ))
                .order_by(asset_data::Column::Id, Order::Asc);

        if ignore_ipfs {
            query = query.filter(
                Condition::all()
                    .add(asset_data::Column::MetadataUrl.not_like("https://nftstorage.link/ipfs%"))
                    .add(asset_data::Column::MetadataUrl.not_like("https://api.stepn.com%")),
            );
        }

        let stream = query.paginate(&conn, *batch_size).into_stream();

        (stream, &all)
    };

    let mut tasks = Vec::new();
    match matches.subcommand_name() {
        Some("show") => {
            // Check the assets found
            let asset_data_found = if let Some(authority) = authority {
                let pubkey = Pubkey::from_str(&authority.as_str()).unwrap();
                let pubkey_bytes = pubkey.to_bytes().to_vec();

                asset_data::Entity::find()
                    .join_rev(
                        JoinType::InnerJoin,
                        asset_authority::Entity::belongs_to(asset_data::Entity)
                            .from(asset_authority::Column::AssetId)
                            .to(asset_data::Column::Id)
                            .into(),
                    )
                    .filter(
                        Condition::all()
                            .add(asset_authority::Column::Authority.eq(pubkey_bytes))
                            .add(
                                asset_data::Column::Metadata
                                    .ne(JsonValue::String("processing".to_string())),
                            ),
                    )
                    .count(&conn)
                    .await
            } else if let Some(collection) = collection {
                asset_data::Entity::find()
                    .join_rev(
                        JoinType::InnerJoin,
                        asset_grouping::Entity::belongs_to(asset_data::Entity)
                            .from(asset_grouping::Column::AssetId)
                            .to(asset_data::Column::Id)
                            .into(),
                    )
                    .filter(
                        Condition::all()
                            .add(asset_grouping::Column::GroupValue.eq(collection.as_str()))
                            .add(
                                asset_data::Column::Metadata
                                    .ne(JsonValue::String("processing".to_string())),
                            ),
                    )
                    .count(&conn)
                    .await
            } else if let Some(mint) = mint {
                let pubkey = Pubkey::from_str(&mint.as_str()).unwrap();
                let pubkey_bytes = pubkey.to_bytes().to_vec();

                asset_data::Entity::find()
                    .join(JoinType::InnerJoin, asset::Relation::AssetData.def())
                    .join_rev(
                        JoinType::InnerJoin,
                        tokens::Entity::belongs_to(asset::Entity)
                            .from(tokens::Column::Mint)
                            .to(asset::Column::SupplyMint)
                            .into(),
                    )
                    .filter(
                        Condition::all()
                            .add(tokens::Column::MintAuthority.eq(pubkey_bytes))
                            .add(
                                asset_data::Column::Metadata
                                    .ne(JsonValue::String("processing".to_string())),
                            ),
                    )
                    .count(&conn)
                    .await
            } else if let Some(creator) = creator {
                let pubkey = Pubkey::from_str(&creator.as_str()).unwrap();
                let pubkey_bytes = pubkey.to_bytes().to_vec();

                asset_data::Entity::find()
                    .join_rev(
                        JoinType::InnerJoin,
                        asset_creators::Entity::belongs_to(asset_data::Entity)
                            .from(asset_creators::Column::AssetId)
                            .to(asset_data::Column::Id)
                            .into(),
                    )
                    .filter(
                        Condition::all()
                            .add(asset_creators::Column::Creator.eq(pubkey_bytes))
                            .add(
                                asset_data::Column::Metadata
                                    .ne(JsonValue::String("processing".to_string())),
                            ),
                    )
                    .count(&conn)
                    .await
            } else {
                asset_data::Entity::find()
                    .filter(
                        Condition::all().add(
                            asset_data::Column::Metadata
                                .ne(JsonValue::String("processing".to_string())),
                        ),
                    )
                    .count(&conn)
                    .await
            };

            let mut i = 0;
            while let Some(assets) = asset_data_missing.0.try_next().await.unwrap() {
                info!("Found {} assets", assets.len());
                i += assets.len();
                if let Some(matches) = matches.subcommand_matches("show") {
                    if matches.get_flag("print") {
                        for asset in assets {
                            println!(
                                "{}, missing asset, {:?}",
                                asset_data_missing.1,
                                Pubkey::try_from(asset.id)
                            );
                        }
                    }
                }
            }
            if let Ok(total) = asset_data_found {
                println!("{}, total assets, {}", asset_data_missing.1, total);
            }
            println!("{}, total missing assets, {}", asset_data_missing.1, i)
        }
        _ => {
            // Find all the assets with missing metadata
            while let Some(assets) = asset_data_missing.0.try_next().await.unwrap() {
                info!("Found {} assets", assets.len());
                for asset in assets {
                    let mut task = DownloadMetadata {
                        asset_data_id: asset.id,
                        uri: asset.metadata_url,
                        created_at: Some(Utc::now().naive_utc()),
                    };

                    task.sanitize();
                    let task_data = task.clone().into_task_data().unwrap();

                    debug!("Print task {} hash {:?}", task_data.data, task_data.hash());
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
                                debug!("Found duplicate task: {:?} {:?}", e, hash.clone());
                                return;
                            }

                            let task_hash = task_data.hash();

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
                                    info!("Task Created: {:?} {:?}", task_hash, task.asset_data_id);
                                }
                                Err(e) => {
                                    error!("Task failed: {}", e);
                                }
                            }
                        });
                        tasks.push(new_task);
                    }
                }
            }

            if tasks.is_empty() {
                info!("No assets with missing metadata found");
            } else {
                info!("Found {} tasks to process", tasks.len());
                for task in tasks {
                    let res = task.await;
                    match res {
                        Ok(_) => {}
                        Err(e) => {
                            error!("Task failed: {}", e);
                        }
                    }
                }
            }
        }
    }
}