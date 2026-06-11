use crate::{
    accountsdb_helpers::{self, AccountsDbFields},
    config::ConfigSnapshot,
};
use anyhow::anyhow;
use bincode::Options;
use das_core::{DownloadMetadataJsonRetryConfig, MetadataJsonDownloadWorker};
use digital_asset_types::dao::account_snapshots;
use futures::stream::StreamExt;
use program_transformers::AccountInfo;
use sea_orm::{sea_query::OnConflict, ActiveValue, EntityTrait, SqlxPostgresConnector, Value};
use sea_orm::{ConnectionTrait, Statement};
use solana_accounts_db::accounts_file::{AccountsFile, StorageAccess};
use solana_sdk::pubkey::Pubkey;
use sqlx::PgPool;
use std::{
    path::{Path, PathBuf},
    str::FromStr,
};
use tokio::{
    fs::File,
    io::AsyncWriteExt,
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
};
use tracing::{error, info, warn};
use {
    crate::{postgres::create_pool as pg_create_pool, util::create_shutdown},
    das_core::create_download_metadata_notifier,
    program_transformers::ProgramTransformer,
    std::sync::Arc,
};

const DEFAULT_PROGRAM_TRANSFORMER_MAX_WORKERS: usize = 20;
const DEFAULT_PROGRAM_TRANSFORMER_BUFFER_CAPACITY: usize = 10_000;

/// Runs snapshot repair using the latest full and incremental snapshot tar files.
/// After ingestion, cleans up "closed" accounts: any `token_accounts`/`tokens` rows
/// with `slot_updated <= snapshot_slot` (to don't delete accounts newer than the snapshot) that
/// are missing from `account_snapshots` are deleted. The cleanup only affects Token (Tokenkeg)
/// and Token-2022; other programs won’t delete anything.
pub async fn run(config: ConfigSnapshot) -> anyhow::Result<()> {
    let pool = pg_create_pool(config.postgres.clone()).await?;

    let (download_metadata_sender, download_metadata_worker) = MetadataJsonDownloadWorker::build()
        .pool(pool.clone())
        .request_timeout(
            config
                .download_metadata
                .metadata_json_download_worker_request_timeout,
        )
        .worker_count(config.download_metadata.metadata_json_download_worker_count)
        .retry(Arc::new(DownloadMetadataJsonRetryConfig::default()))
        .build()?
        .run();
    let download_metadata_notifier =
        create_download_metadata_notifier(download_metadata_sender).await;

    let program_transformer = Arc::new(ProgramTransformer::new(
        pool.clone(),
        download_metadata_notifier,
    ));
    let program_transformer_runner = ProgramTransformerRunner::builder()
        .max_workers(config.program_transform.max_workers)
        .buffer_capacity(config.program_transform.buffer_capacity)
        .program_transformer(program_transformer)
        .build()?;
    let program_transformer_runner_sender = program_transformer_runner.sender();

    let mut account_snapshot_writer = AccountSnapshotWriter::builder()
        .pool(pool.clone())
        .max_workers(config.snapshot_write.max_workers)
        .batch_size(config.snapshot_write.batch_size)
        .channel_capacity(config.snapshot_write.channel_capacity)
        .program_transformer_runner_sender(program_transformer_runner_sender)
        .build();
    let account_snapshot_writer_sender = account_snapshot_writer.sender();
    let mut account_snapshot_writer_error_receiver = account_snapshot_writer
        .take_error_receiver()
        .expect("Error receiver already taken");

    let (slot, incremental_snapshot_join_handle, full_snapshot_join_handle) =
        download_and_process_snapshot(config.clone(), account_snapshot_writer_sender).await?;

    let mut shutdown = create_shutdown()?;

    tokio::select! {
        _ = shutdown.next() => {
            warn!(
                action = "shutdown_signal_received",
                message = "Shutdown signal received, stopping ingest streams",
            );

            return Ok(());
        }

        result = async {
            tokio::try_join!(
                incremental_snapshot_join_handle,
                full_snapshot_join_handle
            )
        } => {
            match result {
                Ok(_) => (),
                Err(e) => {
                    return Err(e.into());
                }
            }
        }

        _ = account_snapshot_writer_error_receiver.recv() => {
            return Err(anyhow!("Failed to write a snapshot batch"))
        }
    }

    account_snapshot_writer.shutdown().await;

    program_transformer_runner.shutdown().await;

    download_metadata_worker.stop().await?;

    let db_connection = SqlxPostgresConnector::from_sqlx_postgres_pool(pool);

    // Delete token accounts that are not in the snapshot (but not newer) for the selected subset of programs
    let start_time = tokio::time::Instant::now();

    let sql = r#"
DELETE FROM token_accounts
WHERE pubkey IN (
    SELECT token_accounts.pubkey
    FROM token_accounts
    LEFT JOIN account_snapshots
        ON account_snapshots.pubkey = token_accounts.pubkey
    WHERE account_snapshots.pubkey IS NULL
        AND token_accounts.slot_updated <= $1
);
        "#;

    let token_accounts_deleted = db_connection
        .execute(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Postgres,
            sql,
            vec![Value::BigInt(Some(slot as i64))],
        ))
        .await?
        .rows_affected();

    info!(
        target: "snapshot_cleanup_closed_accounts",
        "action=delete_token_accounts count={} - elapsed={}s",
        token_accounts_deleted,
        start_time.elapsed().as_secs_f64()
    );

    // Delete mints that are not in the snapshot (but not newer)
    let sql = r#"
DELETE FROM tokens
WHERE mint IN (
    SELECT tokens.mint
    FROM tokens
    LEFT JOIN account_snapshots
        ON account_snapshots.pubkey = tokens.mint
    WHERE account_snapshots.pubkey IS NULL
        AND tokens.slot_updated <= $1
);
        "#;

    let tokens_deleted = db_connection
        .execute(Statement::from_sql_and_values(
            sea_orm::DatabaseBackend::Postgres,
            sql,
            vec![Value::BigInt(Some(slot as i64))],
        ))
        .await?
        .rows_affected();

    info!(
        target: "snapshot_cleanup_closed_accounts",
        "action=delete_tokens count={} - elapsed={}s",
        tokens_deleted,
        start_time.elapsed().as_secs_f64()
    );

    // Delete all account snapshots
    account_snapshots::Entity::delete_many()
        .exec(&db_connection)
        .await?;

    info!(
        target: "snapshot_cleanup_closed_accounts",
        "action=delete_account_snapshots - elapsed={}s",
        start_time.elapsed().as_secs_f64()
    );

    Ok(())
}

pub struct AccountSnapshotWriter {
    update_sender: mpsc::Sender<AccountInfo>,
    stop_sender: Option<oneshot::Sender<()>>,
    error_receiver: Option<mpsc::Receiver<()>>,
    handle: tokio::task::JoinHandle<()>,
}

#[derive(Debug, Clone, Default)]
pub struct AccountSnapshotWriterBuilder {
    channel_capacity: Option<usize>,
    batch_size: Option<usize>,
    pool: Option<PgPool>,
    max_workers: Option<usize>,
    program_transformer_runner_sender: Option<mpsc::Sender<AccountInfo>>,
}

impl AccountSnapshotWriterBuilder {
    pub const fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    pub const fn channel_capacity(mut self, channel_capacity: usize) -> Self {
        self.channel_capacity = Some(channel_capacity);
        self
    }

    pub fn pool(mut self, pool: PgPool) -> Self {
        self.pool = Some(pool);
        self
    }

    pub const fn max_workers(mut self, max_workers: usize) -> Self {
        self.max_workers = Some(max_workers);
        self
    }

    pub fn program_transformer_runner_sender(
        mut self,
        program_transformer_runner_sender: mpsc::Sender<AccountInfo>,
    ) -> Self {
        self.program_transformer_runner_sender = Some(program_transformer_runner_sender);
        self
    }

    pub fn build(self) -> AccountSnapshotWriter {
        let channel_capacity = self.channel_capacity.expect("Channel capacity is required");
        let batch_size = self.batch_size.expect("Batch size is required");
        let pool = self.pool.expect("PgPool is required");
        let max_workers = self.max_workers.expect("Max workers is required");
        let program_transformer_runner_sender = self
            .program_transformer_runner_sender
            .expect("Program transformer runner sender is required");

        let (update_sender, mut update_receiver) = mpsc::channel::<AccountInfo>(channel_capacity);
        let (stop_sender, stop_receiver) = oneshot::channel::<()>();
        let (error_sender, error_receiver) = mpsc::channel::<()>(max_workers);

        let handle = tokio::spawn(async move {
            let mut updates = Vec::new();
            tokio::pin!(stop_receiver);
            let error_sender = error_sender.clone();
            let mut join_set = JoinSet::new();

            // We only save to account_snapshots table tokenkeg and token-2022 accounts
            let token_programs_ids = [
                Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap(),
                Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").unwrap(),
            ];

            loop {
                tokio::select! {
                    Some(update) = update_receiver.recv() => {
                        // We only batch and save to account_snapshots table tokenkeg and token2022 accounts
                        if token_programs_ids.contains(&update.owner) {
                            updates.push(update.clone());
                        }

                        if let Err(e) = program_transformer_runner_sender.send(update).await {
                            error!("Failed program transformer sender: {}", e)
                        }

                        if updates.len() >= batch_size {
                            let batch = std::mem::take(&mut updates);

                            let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone());
                            let accounts: Vec<account_snapshots::ActiveModel> = batch
                                .clone()
                                .iter()
                                .map(|info|
                                    account_snapshots::ActiveModel {
                                        pubkey: ActiveValue::Set(info.pubkey.to_bytes().to_vec()),
                                })
                                .collect();

                            while join_set.len() >= max_workers {
                                join_set.join_next().await;
                            }

                            let error_sender = error_sender.clone();

                            join_set.spawn(async move {
                                if let Err(db_err) = account_snapshots::Entity::insert_many(accounts)
                                    .on_conflict(
                                        OnConflict::columns([account_snapshots::Column::Pubkey])
                                            .do_nothing()
                                            .to_owned(),
                                    )
                                    .exec(&conn)
                                    .await
                                {
                                    if db_err.to_string().contains("None of the records are being inserted") {
                                        // Expected behavior - all records already exist (this is not a Postgres error but SeaORM does return it as an error)
                                        tracing::debug!("All account snapshots already exist (expected during snapshot processing)");
                                        return;
                                    }

                                    error!("Failed to insert accounts: db_err: {}", db_err);
                                    if let Err(send_err) = error_sender.send(()).await {
                                        error!("Failed to send batch write send_err: {} - db_err: {}", send_err, db_err);
                                    }
                                }
                            });
                        }
                    }
                    _ = &mut stop_receiver => {
                        if !updates.is_empty() {
                            let batch = std::mem::take(&mut updates);

                            let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone());
                            let accounts: Vec<account_snapshots::ActiveModel> = batch
                                .clone()
                                .iter()
                                .map(|info|
                                    account_snapshots::ActiveModel {
                                        pubkey: ActiveValue::Set(info.pubkey.to_bytes().to_vec()),
                                })
                                .collect();

                            while join_set.len() >= max_workers {
                                join_set.join_next().await;
                            }


                            if let Err(db_err) = account_snapshots::Entity::insert_many(accounts)
                                    .on_conflict(
                                        OnConflict::columns([account_snapshots::Column::Pubkey])
                                            .do_nothing()
                                            .to_owned(),
                                    )
                                    .exec(&conn)
                                    .await
                                {
                                    error!("Failed to insert accounts: db_err: {}", db_err);
                                    if let Err(send_err) = error_sender.send(()).await {
                                        error!("Failed to send batch write send_err(shutdown): {} - db_err: {}", send_err, db_err);
                                    }
                                }
                        }
                        break;
                    }
                }
            }

            while (join_set.join_next().await).is_some() {}
        });

        AccountSnapshotWriter {
            update_sender,
            stop_sender: Some(stop_sender),
            error_receiver: Some(error_receiver),
            handle,
        }
    }
}

impl AccountSnapshotWriter {
    pub fn builder() -> AccountSnapshotWriterBuilder {
        AccountSnapshotWriterBuilder::default()
    }

    pub fn sender(&self) -> mpsc::Sender<AccountInfo> {
        self.update_sender.clone()
    }

    pub const fn take_error_receiver(&mut self) -> Option<mpsc::Receiver<()>> {
        self.error_receiver.take()
    }

    pub async fn shutdown(mut self) {
        if let Some(stop_sender) = self.stop_sender.take() {
            let _ = stop_sender.send(());
        }

        let _ = self.handle.await;
    }
}

pub struct ProgramTransformerRunner {
    handle: JoinHandle<()>,
    worker_sender: mpsc::Sender<AccountInfo>,
    shutdown_sender: Option<oneshot::Sender<()>>,
}

impl ProgramTransformerRunner {
    pub fn builder() -> ProgramTransformerRunnerBuilder {
        ProgramTransformerRunnerBuilder::default()
    }

    pub fn sender(&self) -> mpsc::Sender<AccountInfo> {
        self.worker_sender.clone()
    }

    pub async fn shutdown(mut self) {
        if let Some(shutdown_sender) = self.shutdown_sender.take() {
            let _ = shutdown_sender.send(());
        }

        let _ = self.handle.await;
    }
}

#[derive(Default)]
pub struct ProgramTransformerRunnerBuilder {
    max_workers: Option<usize>,
    buffer_capacity: Option<usize>,
    program_transformer: Option<Arc<ProgramTransformer>>,
}

impl ProgramTransformerRunnerBuilder {
    pub const fn max_workers(mut self, max_workers: usize) -> Self {
        self.max_workers = Some(max_workers);
        self
    }

    pub const fn buffer_capacity(mut self, buffer_capacity: usize) -> Self {
        self.buffer_capacity = Some(buffer_capacity);
        self
    }

    pub fn program_transformer(mut self, program_transformer: Arc<ProgramTransformer>) -> Self {
        self.program_transformer = Some(program_transformer);
        self
    }

    pub fn build(self) -> Result<ProgramTransformerRunner, anyhow::Error> {
        let buffer_capacity = self
            .buffer_capacity
            .unwrap_or(DEFAULT_PROGRAM_TRANSFORMER_BUFFER_CAPACITY);
        let (worker_sender, mut worker_receiver) = mpsc::channel::<AccountInfo>(buffer_capacity);
        let (shutdown_sender, mut shutdown_receiver) = oneshot::channel();
        let program_transformer = self
            .program_transformer
            .expect("Program transform to be set");

        let max_workers = self
            .max_workers
            .unwrap_or(DEFAULT_PROGRAM_TRANSFORMER_MAX_WORKERS);

        let handle = tokio::spawn(async move {
            let mut join_set = JoinSet::new();

            loop {
                tokio::select! {
                    _ = &mut shutdown_receiver => {
                        break;
                    }
                    Some(account_info) = worker_receiver.recv() => {
                        let program_transformer = Arc::clone(&program_transformer);

                        while join_set.len() >= max_workers {
                            join_set.join_next().await;
                        }

                        crate::prom::PROGRAM_TRANSFORMER_ACCOUNT_INFO_COUNT.inc();

                        let program_transformer = Arc::clone(&program_transformer);

                        join_set.spawn(async move {
                            let result = program_transformer.handle_account_update(&account_info).await;
                            if let Err(e) = result {
                                eprintln!("Failed program_transformer.handle_account_update: {:?}", e);
                                crate::prom::PROGRAM_TRANSFORMER_ACCOUNT_ERROR_COUNT.inc();
                            }
                        });
                    }
                }
            }

            while (join_set.join_next().await).is_some() {}
        });

        Ok(ProgramTransformerRunner {
            handle,
            worker_sender,
            shutdown_sender: Some(shutdown_sender),
        })
    }
}

/// Downloads, uncompresses and sends the last snapshot data (full + incremental)
///  through the received channel
/// Returns the slot of the (incremental) snapshot
pub async fn download_and_process_snapshot(
    config: ConfigSnapshot,
    account_snapshot_writer_sender: mpsc::Sender<AccountInfo>,
) -> anyhow::Result<(u64, JoinHandle<()>, JoinHandle<()>)> {
    // # Get the list of snapshots
    let client = reqwest::Client::new();
    let response = client
        .get(format!("{}/v1/snapshots", config.sidecar_endpoint))
        .send()
        .await
        .expect("Failed to get snapshots");

    let json_value: serde_json::Value = response.json().await?;

    let last_snapshot_data = json_value
        .as_array()
        .expect("snapshots not array")
        .first()
        .expect("no snapshots found");
    let slot = last_snapshot_data
        .get("slot")
        .expect("slot not found")
        .as_u64()
        .unwrap();
    let base_slot = last_snapshot_data
        .get("base_slot")
        .expect("base_slot not found")
        .as_u64()
        .unwrap();

    let mut incremental_snapshot_file_name = None;
    let mut full_snapshot_file_name = None;

    // 0. Incremental snapshot (check filename contains incremental)
    // 1. Full snapshot
    for file in last_snapshot_data.get("files").unwrap().as_array().unwrap() {
        let file_name = file
            .get("file_name")
            .expect("file_name not found")
            .as_str()
            .unwrap()
            .to_string();
        let file_slot = file.get("slot").expect("slot not found").as_u64().unwrap();

        if file_slot == slot && file_name.contains("incremental") {
            incremental_snapshot_file_name = Some(file_name.clone());
        } else if file_slot == base_slot {
            full_snapshot_file_name = Some(file_name);
        }
    }

    let incremental_snapshot_file_name =
        incremental_snapshot_file_name.expect("Incremental snapshot file name not found");
    let full_snapshot_file_name =
        full_snapshot_file_name.expect("Full snapshot file name not found");

    // Process the incremental snapshot file in a separate task
    let account_snapshot_writer_sender_clone = account_snapshot_writer_sender.clone();
    let config_clone = config.clone();
    let incremental_snapshot_join_handle = tokio::task::spawn(async move {
        download_and_process_snapshot_file(
            config_clone,
            incremental_snapshot_file_name,
            slot,
            account_snapshot_writer_sender_clone.clone(),
        )
        .await;
    });

    // Process the full snapshot file in a separate task
    let full_snapshot_join_handle = tokio::task::spawn(async move {
        download_and_process_snapshot_file(
            config,
            full_snapshot_file_name,
            base_slot,
            account_snapshot_writer_sender,
        )
        .await;
    });

    Ok((
        slot,
        incremental_snapshot_join_handle,
        full_snapshot_join_handle,
    ))
}

/// Downloads the snapshot file from the sidecar
pub async fn download_snapshot_file(
    sidecar_endpoint: &str,
    snapshot_file_name: String,
    snapshot_slot: u64,
) -> anyhow::Result<()> {
    let url = format!("{}/v1/snapshot/{}", sidecar_endpoint, snapshot_file_name);

    let client = reqwest::Client::new();
    let start_time = tokio::time::Instant::now();
    let response = client.get(url).send().await?;

    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "Failed to download file: HTTP {}",
            response.status()
        ));
    }

    let total_size = response.content_length().expect("Content length not found");
    tracing::info!(
        target: "snapshot_download_progress",
        "Downloading file {} of size: {} MB",
        snapshot_file_name,
        total_size / 1024 / 1024
    );

    let file_path = format!("/tmp/snapshot_{}/{}", snapshot_slot, snapshot_file_name);

    // Create the directory if it doesn't exist
    if let Some(parent) = Path::new(&file_path).parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let mut file = File::create(&file_path).await?;
    let mut stream = response.bytes_stream();
    let mut downloaded = 0u64;
    let mut last_log_time = tokio::time::Instant::now();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;

        // Log progress every 30 seconds
        if total_size > 0 && last_log_time.elapsed().as_secs() > 30 {
            let progress = (downloaded as f64 / total_size as f64) * 100.0;
            tracing::debug!(
                target: "snapshot_download_progress",
                "Progress: {:.1}% ({}/{}) - {} seconds - {:.1} MB/s",
                progress,
                downloaded / 1024 / 1024,
                total_size / 1024 / 1024,
                start_time.elapsed().as_secs_f64(),
                downloaded as f64 / 1024.0 / 1024.0 / start_time.elapsed().as_secs_f64()
            );
            last_log_time = tokio::time::Instant::now();
        }
    }

    file.flush().await?;

    tracing::info!(
        target: "snapshot_download_progress",
        "File {} downloaded successfully in {} secs",
        snapshot_file_name,
        start_time.elapsed().as_secs_f64()
    );

    Ok(())
}

pub fn unpack_compressed_snapshot<P: Into<PathBuf>>(path: P, slot: u64) -> Vec<AccountFileData> {
    let start_time = tokio::time::Instant::now();
    tracing::debug!(
        target: "snapshot_download_progress",
        "Starting Unpacking compressed snapshot slot: {:?}",
        slot
    );

    let path_buf: PathBuf = path.into();

    let temp_dir = PathBuf::from(format!("/tmp/snapshot_{}/uncompressed_snapshot", slot));

    let file = std::fs::File::open(path_buf).expect("Failed to open file");

    let decoder = zstd::stream::Decoder::new(file).expect("Failed to create decoder");

    let mut archive = tar::Archive::new(decoder);
    archive
        .unpack(temp_dir.clone())
        .expect("Failed to unpack archive");

    tracing::debug!(
        target: "snapshot_download_progress",
        "Uncompressed finished snapshot slot: {:?} - elapsed={}s",
        slot,
        start_time.elapsed().as_secs_f64()
    );

    let version_path = temp_dir.join("version");
    let _version = std::fs::read_to_string(version_path)
        .expect("Failed to read version file")
        .trim()
        .to_string();

    // Deserializing the snapshot metadata file
    let snapshots_dir = temp_dir.join("snapshots");
    let snapshot_file_name = format!("{}/{}", slot, slot);
    let snapshot_file = std::fs::File::open(snapshots_dir.join(snapshot_file_name))
        .expect("Snapshot metadatafile not found");

    let mut snapshot_stream = std::io::BufReader::new(snapshot_file);

    pub const MAX_STREAM_SIZE: u64 = 32 * 1024 * 1024 * 1024;

    let bank_fields: accountsdb_helpers::DeserializableVersionedBank = bincode::options()
        .with_limit(MAX_STREAM_SIZE)
        .with_fixint_encoding()
        .allow_trailing_bytes()
        .deserialize_from(&mut snapshot_stream)
        .unwrap();

    let accounts_db_fields: AccountsDbFields<accountsdb_helpers::SerializableAccountStorageEntry> =
        bincode::options()
            .with_limit(MAX_STREAM_SIZE)
            .with_fixint_encoding()
            .allow_trailing_bytes()
            .deserialize_from(&mut snapshot_stream)
            .unwrap();

    let AccountsDbFields(accounts_metadata, _, accountsdb_fields_slot, ..) = accounts_db_fields;

    assert_eq!(slot, accountsdb_fields_slot);
    assert_eq!(slot, bank_fields.slot);

    // Deserializing the accounts directory files
    let accounts_dir = temp_dir.join("accounts");

    let mut account_file_data = Vec::new();

    for entry in std::fs::read_dir(accounts_dir)
        .expect("Failed to read accounts directory")
        .filter_map(|entry| entry.ok())
    {
        let path = entry.path();
        let file_size = std::fs::metadata(&path)
            .expect("Failed to get metadata")
            .len() as usize;
        let file_name = entry.file_name().to_string_lossy().to_string();

        let (slot_str, id_str) = file_name
            .split_once('.')
            .unwrap_or_else(|| panic!("Invalid file name: {}", file_name));
        let slot = slot_str
            .parse::<u64>()
            .unwrap_or_else(|_| panic!("Invalid slot: {}", slot_str));
        let id = id_str
            .parse::<u64>()
            .unwrap_or_else(|_| panic!("Invalid id: {}", id_str));

        let accounts_metadata = match accounts_metadata.get(&slot) {
            Some(accounts_metadata) => accounts_metadata,
            None => {
                tracing::error!(
                    "accounts_metadata not found for slot: {} - file_size: {} - write_version: {}",
                    slot,
                    file_size,
                    id
                );
                account_file_data.push(return_default_account_file_data(path, slot, file_size, id));
                continue;
            }
        };

        let mut size = None;
        for account in accounts_metadata {
            if account.id as u64 == id {
                size = Some(account.accounts_current_len);
                break;
            }
        }
        let size = match size {
            Some(size) => size,
            None => {
                tracing::error!(
                    "size not found for write version: {} and slot: {} - file_size: {} - accounts_metadata: {:?}",
                    id,
                    slot,
                    file_size,
                    accounts_metadata
                );
                account_file_data.push(return_default_account_file_data(path, slot, file_size, id));
                continue;
            }
        };

        if size != file_size {
            tracing::error!("size mismatch for id: {} and slot: {}", id, slot);

            // In case of mismatch also use the file size for deserialization
            account_file_data.push(return_default_account_file_data(path, slot, file_size, id));
            continue;
        }

        account_file_data.push(AccountFileData {
            path,
            size,
            slot,
            write_version: id,
        });
    }

    tracing::debug!(
        target: "snapshot_download_progress",
        "Deserialized and unpacked snapshot slot: {:?} - elapsed={}s",
        slot,
        start_time.elapsed().as_secs_f64()
    );

    account_file_data
}

pub struct AccountFileData {
    pub path: PathBuf,
    pub size: usize,
    pub slot: u64,
    pub write_version: u64,
}

/// If for some reason we don't file the account file we are looking for (or the write version doesn't match)
///  we use the file name for getting the write version and the file size for the default account file data
const fn return_default_account_file_data(
    path: PathBuf,
    slot: u64,
    file_size: usize,
    write_version: u64,
) -> AccountFileData {
    AccountFileData {
        path,
        size: file_size,
        slot,
        write_version,
    }
}

/// Downloads and then uncompresses the received snapshot file and sends the account info through the channel
/// Note: We read sequentially for now and just store results in the channel
async fn download_and_process_snapshot_file(
    config: ConfigSnapshot,
    snapshot_file_name: String,
    snapshot_slot: u64,
    account_snapshot_writer_sender: mpsc::Sender<AccountInfo>,
) {
    let path = PathBuf::from(format!(
        "/tmp/snapshot_{}/{}",
        snapshot_slot, snapshot_file_name
    ));

    download_snapshot_file(&config.sidecar_endpoint, snapshot_file_name, snapshot_slot)
        .await
        .expect("Failed to download incremental snapshot file");

    let solana_snapshot = unpack_compressed_snapshot(path, snapshot_slot);

    // We only try to process all programs related to DAS standard
    let programs_to_process = [
        Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap(),
        Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").unwrap(),
        Pubkey::from_str("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s").unwrap(),
        Pubkey::from_str("inscokhJarcjaEs59QbQ7hYjrKz25LEPRfCbP8EmdUp").unwrap(),
        Pubkey::from_str("CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d").unwrap(),
        Pubkey::from_str("1DREGFgysWYxLnRnKQnwrxnJQeSMk2HmGaC6whw2B2p").unwrap(),
    ];

    // The accounts-file iteration helpers in solana-accounts-db 3.x carry
    // their own `Pubkey` (from solana-pubkey 4.x = `solana_address::Address`)
    // which is *not* the same Rust type as the `solana_sdk::pubkey::Pubkey`
    // used by `program_transformers::AccountInfo`. Bridge via the 32-byte
    // representation, which is stable across both crates.
    let programs_to_process_bytes: std::collections::HashSet<[u8; 32]> =
        programs_to_process.iter().map(|p| p.to_bytes()).collect();

    let mut total_accounts = 0;

    for AccountFileData {
        path,
        size: current_len,
        slot: account_slot,
        // write_version is no longer tracked per-account in solana-accounts-db
        // 3.x (StoredAccountInfo lost the field), so we can't sanity-check
        // it against the snapshot's metadata anymore.
        write_version: _write_version,
    } in solana_snapshot
    {
        let accounts = AccountsFile::new_for_startup(path, current_len, StorageAccess::Mmap)
            .expect("Unpack account file");

        // First pass: enumerate offsets and tally totals without loading
        // account data — `scan_accounts_without_data` is the only `pub` API
        // in 3.x to walk the storage. Filter by owner here so we don't reload
        // every account in pass 2.
        let mut offsets_to_load: Vec<usize> = Vec::new();
        accounts
            .scan_accounts_without_data(|offset, info| {
                total_accounts += 1;
                if programs_to_process_bytes.contains(&info.owner.to_bytes()) {
                    offsets_to_load.push(offset);
                }
            })
            .expect("scan account file");

        // Second pass: re-read each surviving account *with* data, using the
        // public single-account callback API.
        for offset in offsets_to_load {
            let account_info_opt =
                accounts.get_stored_account_callback(offset, |full| AccountInfo {
                    pubkey: Pubkey::from(full.pubkey.to_bytes()),
                    owner: Pubkey::from(full.owner.to_bytes()),
                    slot: account_slot,
                    data: full.data.to_vec(),
                });

            if let Some(account_info) = account_info_opt {
                if let Err(e) = account_snapshot_writer_sender.send(account_info).await {
                    tracing::error!("Failed to send account info: {}", e);
                }
                crate::prom::PROCESSED_SNAPSHOT_UPDATES_COUNT.inc();
            }
        }
    }

    tracing::debug!(target: "snapshot_download_progress", "Total accounts: {}", total_accounts);
}
