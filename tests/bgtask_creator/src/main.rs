use tokio::{
    task::JoinSet,
    signal,
};
use digital_asset_types::dao::{asset_data, asset_authority, asset_grouping, tasks};

use log::{info, debug, error};

use nft_ingester::{
    tasks::{BgTask, DownloadMetadata, IntoTaskData, DownloadMetadataTask, TaskManager},
    config::{init_logger, setup_config},
    database::setup_database,
    metrics::setup_metrics,
    config::rand_string,
    error::IngesterError,
};

use std::{
    path::PathBuf,
    time
};

use futures::TryStreamExt;

use sea_orm::{
    entity::*, query::*, EntityTrait, JsonValue, SqlxPostgresConnector, DeleteResult
};

use clap::{Arg, ArgAction, Command, value_parser};

use sqlx::types::chrono::Utc;

use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;


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
                .value_parser(value_parser!(PathBuf))
        )
        .arg(
            Arg::new("delete")
                .long("delete")
                .short('d')
                .help("Delete all existing tasks before creating new ones.")
                .required(false)
        )
        .arg(
            Arg::new("batch_size")
                .long("batch-size")
                .short('b')
                .help("Sets the batch size for the assets to be processed.")
                .required(false)
                .action(ArgAction::Set)
                .value_parser(value_parser!(u64))
                .default_value("1000")
        )
        .arg(
            Arg::new("authority")
                .long("authority")
                .short('a')
                .help("Create background tasks for the given authority")
                .required(false)
                .action(ArgAction::Set)
        )
        .arg(
            Arg::new("collection")
                .long("collection")
                .short('o')
                .help("Create background tasks for the given collection")
                .required(false)
                .action(ArgAction::Set)
        )
        .subcommand(
            Command::new("show")
                .about("Show tasks")
        )
        .get_matches();

    let config_path = matches.get_one::<PathBuf>("config");
    if let Some(config_path) = config_path {
        info!("Loading config from: {}", config_path.display());
    }

    // Pull Env variables into config struct
    let config = setup_config(config_path);

    // Optionally setup metrics if config demands it
    setup_metrics(&config);

    // One pool many clones, this thing is thread safe and send sync
    let database_pool = setup_database(config.clone()).await;

    // Set up a task pool
    let mut tasks = JoinSet::new();

    //Setup definitions for background tasks
    let task_runner_config = config.background_task_runner_config.clone().unwrap_or_default();
    let bg_task_definitions: Vec<Box<dyn BgTask>> = vec![Box::new(DownloadMetadataTask {
        lock_duration: task_runner_config.lock_duration,
        max_attempts: task_runner_config.max_attempts,
        timeout: Some(time::Duration::from_secs(task_runner_config.timeout.unwrap_or(3))),
    })];

    let mut background_task_manager =
        TaskManager::new(rand_string(), database_pool.clone(), bg_task_definitions);
        
    // This is how we send new bg tasks
    let bg_task_listener = background_task_manager.start_listener(false);
    tasks.spawn(bg_task_listener);

    let bg_task_sender = background_task_manager.get_sender().unwrap();

    // Create new postgres connection
    let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(database_pool.clone());

    if matches.contains_id("delete") {
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

    /*
            select ad.id from asset_data ad 
     inner join asset_authority aa  on aa.asset_id = ad.id 
     where  
      aa.authority='\x0b6eeb8809df3468cbe2ee7b224e7b3291d99770811728fcdefbc180c6933157' and 
      ad.metadata=to_jsonb('processing'::text); 
       */

    let all = "all".to_string();
    let mut asset_data_missing =  if let Some(authority) = authority {
            info!("Creating new tasks for assets with missing metadata for authority {}, batch size={}", authority, batch_size);

            let pubkey = Pubkey::from_str(&authority.as_str()).unwrap();
            let pubkey_bytes = pubkey.to_bytes().to_vec();
            
            (asset_data::Entity::find()
                 .join_rev(
                    JoinType::InnerJoin,
                    asset_authority::Entity::belongs_to(asset_data::Entity)
                        .from(asset_authority::Column::AssetId)
                        .to(asset_data::Column::Id)
                        .into()
                 )
                 .filter(
                    Condition::all()
                        .add(asset_authority::Column::Authority.eq(pubkey_bytes))
                        .add(asset_data::Column::Metadata.eq(JsonValue::String("processing".to_string())))
                 )
                 .order_by(asset_data::Column::Id, Order::Asc)
                .paginate(&conn, *batch_size)
                .into_stream(), authority)

        } else if let Some(collection) = collection {
            info!("Creating new tasks for assets with missing metadata for collection {}, batch size={}", collection, batch_size);

            (asset_data::Entity::find()
                 .join_rev(
                    JoinType::InnerJoin,
                    asset_grouping::Entity::belongs_to(asset_data::Entity)
                        .from(asset_grouping::Column::AssetId)
                        .to(asset_data::Column::Id)
                        .into()
                 )
                 .filter(
                    Condition::all()
                        .add(asset_grouping::Column::GroupValue.eq(collection.as_str()))
                        .add(asset_data::Column::Metadata.eq(JsonValue::String("processing".to_string())))
                 )
                 .order_by(asset_data::Column::Id, Order::Asc)
                .paginate(&conn, *batch_size)
                .into_stream(), collection)
        } else {
            info!("Creating new tasks for all assets with missing metadata, batch size={}", batch_size);
            (asset_data::Entity::find()
                .filter(
                    Condition::all()
                        .add(asset_data::Column::Metadata.eq(JsonValue::String("processing".to_string())))
                )
                .order_by(asset_data::Column::Id, Order::Asc)
                .paginate(&conn, *batch_size)
                .into_stream(), &all)
        };
    
    match matches.subcommand_name() {
        Some("show") => {
            // Check the assets found
            let asset_data_found = if let Some(collection) = collection {
                asset_data::Entity::find()
                    .join_rev(
                        JoinType::InnerJoin,
                        asset_grouping::Entity::belongs_to(asset_data::Entity)
                            .from(asset_grouping::Column::AssetId)
                            .to(asset_data::Column::Id)
                            .into()
                    )
                    .filter(
                        Condition::all()
                            .add(asset_grouping::Column::GroupValue.eq(collection.as_str()))
                            .add(asset_data::Column::Metadata.ne(JsonValue::String("processing".to_string())))
                    )
                    .count(&conn)
                    .await
            } else {
                asset_data::Entity::find()
                    .filter(
                        Condition::all()
                            .add(asset_data::Column::Metadata.ne(JsonValue::String("processing".to_string())))
                    )
                    .count(&conn)
                    .await
            };

            let mut i = 0;
            while let Some(assets) = asset_data_missing.0.try_next().await.unwrap() {
                info!("Found {} assets", assets.len());
                i += assets.len()
            }
            if let Ok(total) = asset_data_found {
                println!("{}, total assets, {}", asset_data_missing.1, total);
            }
            println!("{}, total missing assets, {}", asset_data_missing.1, i)
        }
        _ => {

            let mut i = 0;
            // Find all the assets with missing metadata
            while let Some(assets) = asset_data_missing.0.try_next().await.unwrap() {
                    info!("Found {} assets", assets.len());
                    for asset in assets {
                        let asset_clone = asset.clone();

                        let mut task = DownloadMetadata {
                            asset_data_id: asset.id,
                            uri: asset.metadata_url,
                            created_at: Some(Utc::now().naive_utc()),
                        };


                        task.sanitize();
                        let task_data = task.into_task_data().unwrap();
                        debug!("Print task {} hash {:?}", task_data.data, task_data.hash());
                        let res = bg_task_sender.send(task_data);
                        

                        match res {
                            Ok(_) => {
                                info!("Created new task for asset {:?}", asset_clone.id);
                            }
                            Err(e) => {
                                error!("Error creating new task for asset {:?}: {}", asset_clone.id, e);
                            }
                        }
                        i += 1;
                    }
            }

            if i  == 0 {
                info!("No assets with missing metadata found");
                return
            }

            info!("Queue length: {}", bg_task_listener.
            match signal::ctrl_c().await {
                Ok(()) => {}
                Err(err) => {
                    error!("Unable to listen for shutdown signal: {}", err);
                    // we also shut down in case of error
                }
            }

            tasks.shutdown().await;
        }
    }
}
