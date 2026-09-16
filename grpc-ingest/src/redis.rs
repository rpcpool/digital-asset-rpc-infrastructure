use {
    crate::{
        config::{ConfigIngestStream, REDIS_STREAM_DATA_KEY},
        prom::{
            ack_tasks_total_dec, ack_tasks_total_inc, current_ingester_slot_set,
            download_metadata_json_task_status_count_inc, ingest_job_time_set,
            ingest_tasks_total_dec, ingest_tasks_total_inc, program_transformer_task_status_inc,
            redis_xack_inc, redis_xlen_set, redis_xread_inc, ProgramTransformerTaskStatusKind,
        },
    },
    das_core::{
        DownloadMetadata, DownloadMetadataInfo, DownloadMetadataJsonRetryConfig,
        FetchMetadataJsonError, MetadataJsonTaskError, StatusCode,
    },
    futures::future::BoxFuture,
    program_transformers::{AccountInfo, ProgramTransformer, SlotInfo, TransactionInfo},
    redis::{
        aio::MultiplexedConnection,
        streams::{StreamId, StreamKey, StreamMaxlen, StreamReadOptions, StreamReadReply},
        AsyncCommands, ErrorKind as RedisErrorKind, RedisResult, Value as RedisValue,
    },
    solana_sdk::{pubkey::Pubkey, signature::Signature},
    std::{collections::HashMap, marker::PhantomData, sync::Arc},
    tokio::{
        sync::mpsc::error::SendError,
        task::JoinSet,
        time::{sleep, Duration},
    },
    tracing::{debug, error, warn},
    yellowstone_grpc_proto::{
        geyser::SubscribeUpdateBlockMeta,
        prelude::{
            CompiledInstruction as ProtoCompiledInstruction,
            InnerInstructions as ProtoInnerInstructions, SubscribeUpdateAccount,
            SubscribeUpdateTransaction,
        },
        prost::Message,
    },
};

use solana_message::compiled_instruction::CompiledInstruction as MessageCompiledInstruction;
use solana_transaction_status::{InnerInstruction, InnerInstructions};

// Convert proto pubkey bytes (yellowstone-grpc-proto v9.1 ships with
// solana-pubkey 2.x via its `convert_from` helpers, which clashes with our
// solana-sdk 3.x types). Going through the 32-byte representation is
// version-agnostic.
fn proto_pubkey(bytes: &[u8]) -> Result<Pubkey, RedisStreamMessageError> {
    Pubkey::try_from(bytes).map_err(RedisStreamMessageError::PubkeyConversion)
}

fn proto_pubkey_vec(raw: Vec<Vec<u8>>) -> Result<Vec<Pubkey>, RedisStreamMessageError> {
    raw.iter().map(|bytes| proto_pubkey(bytes)).collect()
}

fn proto_to_compiled_instruction(
    ix: ProtoCompiledInstruction,
) -> Result<MessageCompiledInstruction, RedisStreamMessageError> {
    let program_id_index = u8::try_from(ix.program_id_index).map_err(|_| {
        RedisStreamMessageError::InvalidData("CompiledInstruction.program_id_index".to_string())
    })?;
    Ok(MessageCompiledInstruction {
        program_id_index,
        accounts: ix.accounts,
        data: ix.data,
    })
}

fn proto_to_inner_instructions(
    ix: ProtoInnerInstructions,
) -> Result<InnerInstructions, RedisStreamMessageError> {
    let index = u8::try_from(ix.index)
        .map_err(|_| RedisStreamMessageError::InvalidData("InnerInstructions.index".to_string()))?;
    let mut instructions = Vec::with_capacity(ix.instructions.len());
    for inner in ix.instructions {
        let program_id_index = u8::try_from(inner.program_id_index).map_err(|_| {
            RedisStreamMessageError::InvalidData("InnerInstruction.program_id_index".to_string())
        })?;
        instructions.push(InnerInstruction {
            instruction: MessageCompiledInstruction {
                program_id_index,
                accounts: inner.accounts,
                data: inner.data,
            },
            stack_height: inner.stack_height,
        });
    }
    Ok(InnerInstructions {
        index,
        instructions,
    })
}

#[derive(thiserror::Error, Debug)]
pub enum RedisStreamMessageError {
    #[error("failed to get data (key: {0}) from stream")]
    MissingData(String),
    #[error("invalid data (key: {0}) from stream")]
    InvalidData(String),
    #[error("failed to decode message")]
    Decode(#[from] yellowstone_grpc_proto::prost::DecodeError),
    #[error("received invalid SubscribeUpdateAccount")]
    InvalidSubscribeUpdateAccount,
    #[error("failed to convert pubkey")]
    PubkeyConversion(#[from] std::array::TryFromSliceError),
    #[error("JSON deserialization error: {0}")]
    JsonDeserialization(#[from] serde_json::Error),
}

pub trait RedisStreamMessage<M> {
    fn try_parse_msg(msg: HashMap<String, RedisValue>) -> Result<M, RedisStreamMessageError>;

    fn get_data_as_vec(
        msg: &HashMap<String, RedisValue>,
    ) -> Result<&Vec<u8>, RedisStreamMessageError> {
        let data = msg.get(REDIS_STREAM_DATA_KEY).ok_or_else(|| {
            RedisStreamMessageError::MissingData(REDIS_STREAM_DATA_KEY.to_string())
        })?;

        match data {
            RedisValue::Data(data) => Ok(data),
            _ => Err(RedisStreamMessageError::InvalidData(
                REDIS_STREAM_DATA_KEY.to_string(),
            )),
        }
    }
}

impl RedisStreamMessage<Self> for AccountInfo {
    fn try_parse_msg(msg: HashMap<String, RedisValue>) -> Result<Self, RedisStreamMessageError> {
        let account_data = Self::get_data_as_vec(&msg)?;

        let SubscribeUpdateAccount { account, slot, .. } = Message::decode(account_data.as_ref())?;

        let account = account.ok_or(RedisStreamMessageError::InvalidSubscribeUpdateAccount)?;

        Ok(Self {
            slot,
            pubkey: Pubkey::try_from(account.pubkey.as_slice())?,
            owner: Pubkey::try_from(account.owner.as_slice())?,
            data: account.data,
        })
    }
}

impl RedisStreamMessage<Self> for TransactionInfo {
    fn try_parse_msg(msg: HashMap<String, RedisValue>) -> Result<Self, RedisStreamMessageError> {
        let transaction_data = Self::get_data_as_vec(&msg)?;

        let SubscribeUpdateTransaction { transaction, slot } =
            Message::decode(transaction_data.as_ref())?;

        let transaction = transaction.ok_or_else(|| {
            RedisStreamMessageError::InvalidData(
                "received invalid SubscribeUpdateTransaction".to_string(),
            )
        })?;
        let tx = transaction.transaction.ok_or_else(|| {
            RedisStreamMessageError::InvalidData(
                "received invalid transaction in SubscribeUpdateTransaction".to_string(),
            )
        })?;
        let message = tx.message.ok_or_else(|| {
            RedisStreamMessageError::InvalidData(
                "received invalid message in SubscribeUpdateTransaction".to_string(),
            )
        })?;
        let meta = transaction.meta.ok_or_else(|| {
            RedisStreamMessageError::InvalidData(
                "received invalid meta in SubscribeUpdateTransaction".to_string(),
            )
        })?;

        let mut account_keys = proto_pubkey_vec(message.account_keys)?;
        for pubkey in proto_pubkey_vec(meta.loaded_writable_addresses)? {
            account_keys.push(pubkey);
        }
        for pubkey in proto_pubkey_vec(meta.loaded_readonly_addresses)? {
            account_keys.push(pubkey);
        }

        let message_instructions = message
            .instructions
            .into_iter()
            .map(proto_to_compiled_instruction)
            .collect::<Result<Vec<_>, _>>()?;
        let meta_inner_instructions = meta
            .inner_instructions
            .into_iter()
            .map(proto_to_inner_instructions)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            slot,
            signature: Signature::try_from(transaction.signature.as_slice())?,
            account_keys,
            message_instructions,
            meta_inner_instructions,
        })
    }
}

impl RedisStreamMessage<Self> for DownloadMetadataInfo {
    fn try_parse_msg(msg: HashMap<String, RedisValue>) -> Result<Self, RedisStreamMessageError> {
        let metadata_data = Self::get_data_as_vec(&msg)?;

        let info: DownloadMetadataInfo = serde_json::from_slice(metadata_data.as_ref())?;

        Ok(info)
    }
}

impl RedisStreamMessage<Self> for SlotInfo {
    fn try_parse_msg(msg: HashMap<String, RedisValue>) -> Result<Self, RedisStreamMessageError> {
        let block_meta_data = Self::get_data_as_vec(&msg)?;

        let SubscribeUpdateBlockMeta { slot, .. } = Message::decode(block_meta_data.as_ref())?;

        Ok(SlotInfo { slot: slot as i64 })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum IngestMessageError {
    #[error("Redis stream message parse error: {0}")]
    RedisStreamMessage(#[from] RedisStreamMessageError),
    #[error("Program transformer error: {0}")]
    ProgramTransformer(#[from] program_transformers::error::ProgramTransformerError),
    #[error("Download metadata JSON task error: {0}")]
    DownloadMetadataJson(#[from] das_core::MetadataJsonTaskError),
    #[error("Snapshot send error: {0}")]
    SnapshotSend(#[from] SendError<AccountInfo>),
}

pub struct IngestStreamStop {
    shutdown_tx: tokio::sync::oneshot::Sender<()>,
    control: tokio::task::JoinHandle<()>,
}

impl IngestStreamStop {
    pub async fn stop(self) -> anyhow::Result<()> {
        self.shutdown_tx
            .send(())
            .map_err(|_| anyhow::anyhow!("Failed to send shutdown signal"))?;

        self.control.await?;

        Ok(())
    }
}

pub trait MessageHandler: Send + Sync + Clone + 'static {
    fn handle(
        &self,
        input: HashMap<String, RedisValue>,
    ) -> BoxFuture<'static, Result<(), IngestMessageError>>;
}

pub struct DownloadMetadataJsonHandle(Arc<DownloadMetadata>, Arc<DownloadMetadataJsonRetryConfig>);

impl MessageHandler for DownloadMetadataJsonHandle {
    fn handle(
        &self,
        input: HashMap<String, RedisValue>,
    ) -> BoxFuture<'static, Result<(), IngestMessageError>> {
        let download_metadata = Arc::clone(&self.0);
        let download_config = Arc::clone(&self.1);

        Box::pin(async move {
            let info = DownloadMetadataInfo::try_parse_msg(input)?;
            let response = download_metadata
                .handle_download(&info, download_config)
                .await;
            let status =
                if let Err(MetadataJsonTaskError::Fetch(FetchMetadataJsonError::Response {
                    status: StatusCode::Code(code),
                    ..
                })) = response
                {
                    code.as_u16()
                } else {
                    200
                };

            download_metadata_json_task_status_count_inc(status);

            response.map_err(IngestMessageError::DownloadMetadataJson)
        })
    }
}

impl DownloadMetadataJsonHandle {
    pub const fn new(
        download_metadata: Arc<DownloadMetadata>,
        config: Arc<DownloadMetadataJsonRetryConfig>,
    ) -> Self {
        Self(download_metadata, config)
    }
}

impl Clone for DownloadMetadataJsonHandle {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0), Arc::clone(&self.1))
    }
}

pub struct AccountHandle(Arc<ProgramTransformer>);

impl AccountHandle {
    pub const fn new(program_transformer: Arc<ProgramTransformer>) -> Self {
        Self(program_transformer)
    }
}

impl MessageHandler for AccountHandle {
    fn handle(
        &self,
        input: HashMap<String, RedisValue>,
    ) -> BoxFuture<'static, Result<(), IngestMessageError>> {
        let program_transformer = Arc::clone(&self.0);
        Box::pin(async move {
            let account = AccountInfo::try_parse_msg(input)?;
            program_transformer
                .handle_account_update(&account)
                .await
                .map_err(IngestMessageError::ProgramTransformer)
        })
    }
}

impl Clone for AccountHandle {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

pub struct TransactionHandle(Arc<ProgramTransformer>);

impl TransactionHandle {
    pub const fn new(program_transformer: Arc<ProgramTransformer>) -> Self {
        Self(program_transformer)
    }
}

impl MessageHandler for TransactionHandle {
    fn handle(
        &self,
        input: HashMap<String, RedisValue>,
    ) -> BoxFuture<'static, Result<(), IngestMessageError>> {
        let program_transformer = Arc::clone(&self.0);

        Box::pin(async move {
            let transaction = TransactionInfo::try_parse_msg(input)?;
            program_transformer
                .handle_transaction(&transaction)
                .await
                .map_err(IngestMessageError::ProgramTransformer)
        })
    }
}

impl Clone for TransactionHandle {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

pub struct SlotHandle(Arc<ProgramTransformer>);

impl SlotHandle {
    pub const fn new(program_transformer: Arc<ProgramTransformer>) -> Self {
        Self(program_transformer)
    }
}

impl Clone for SlotHandle {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl MessageHandler for SlotHandle {
    fn handle(
        &self,
        input: HashMap<String, RedisValue>,
    ) -> BoxFuture<'static, Result<(), IngestMessageError>> {
        let program_transformer = Arc::clone(&self.0);

        Box::pin(async move {
            let SlotInfo { slot } = SlotInfo::try_parse_msg(input)?;
            current_ingester_slot_set(slot);
            program_transformer
                .handle_slot_update(slot)
                .await
                .map_err(IngestMessageError::ProgramTransformer)
        })
    }
}

#[derive(Clone)]
pub struct Acknowledge {
    config: Arc<ConfigIngestStream>,
    connection: MultiplexedConnection,
}

impl Acknowledge {
    pub const fn new(config: Arc<ConfigIngestStream>, connection: MultiplexedConnection) -> Self {
        Self { config, connection }
    }
}

impl Acknowledge {
    async fn handle(&self, ids: Vec<String>) {
        let mut connection = self.connection.clone();
        let config = &self.config;

        let count = ids.len();

        match redis::pipe()
            .atomic()
            .xack(&config.name, &config.group, &ids)
            .xdel(&config.name, &ids)
            .query_async::<_, redis::Value>(&mut connection)
            .await
        {
            Ok(response) => {
                debug!(
                    "action=acknowledge_and_delete stream={} response={:?} expected={:?}",
                    config.name, response, count
                );

                redis_xack_inc(&config.name, &config.consumer, count);
            }
            Err(e) => {
                error!(
                    "action=acknowledge_and_delete_failed stream={} error={:?}",
                    config.name, e
                );
            }
        }

        ack_tasks_total_dec(&config.name, &config.consumer);
    }
}

pub struct IngestStream<H: MessageHandler> {
    config: Arc<ConfigIngestStream>,
    connection: Option<MultiplexedConnection>,
    handler: Option<H>,
    _handler: PhantomData<H>,
}

impl<H: MessageHandler> IngestStream<H> {
    pub fn build() -> Self {
        Self {
            config: Arc::new(ConfigIngestStream::default()),
            connection: None,
            handler: None,
            _handler: PhantomData,
        }
    }

    pub fn handler(mut self, handler: H) -> Self {
        self.handler = Some(handler);
        self
    }

    pub fn config(mut self, config: ConfigIngestStream) -> Self {
        self.config = Arc::new(config);
        self
    }

    pub fn connection(mut self, connection: MultiplexedConnection) -> Self {
        self.connection = Some(connection);
        self
    }

    async fn read(&self, connection: &mut MultiplexedConnection) -> RedisResult<StreamReadReply> {
        let config = &self.config;

        let opts = StreamReadOptions::default()
            .group(&config.group, &config.consumer)
            .count(config.batch_size)
            .block(250);

        connection
            .xread_options(&[&config.name], &[">"], &opts)
            .await
    }

    pub async fn start(mut self) -> anyhow::Result<IngestStreamStop> {
        let config = Arc::clone(&self.config);
        let (internal_shutdown_tx, mut internal_shutdown_rx) = tokio::sync::oneshot::channel();

        let mut connection = self.connection.take().expect("Connection is required");
        let handler = self.handler.take().expect("Handler is required");

        debug!(
            "action=setup_consumer_group stream={} group={} consumer={}",
            config.name, config.group, config.consumer
        );

        xgroup_create(&mut connection, &config.name, &config.group).await?;

        let group_info: redis::RedisResult<Vec<redis::Value>> = redis::cmd("XINFO")
            .arg("GROUPS")
            .arg(&config.name)
            .query_async(&mut connection)
            .await;
        debug!(
            "action=consumer_group_info stream={} info={:?}",
            config.name, group_info
        );

        xgroup_delete_consumer(
            &mut connection,
            &config.name,
            &config.group,
            &config.consumer,
        )
        .await?;

        xgroup_create_consumer(
            &mut connection,
            &config.name,
            &config.group,
            &config.consumer,
        )
        .await?;

        let consumer_info: redis::RedisResult<Vec<redis::Value>> = redis::cmd("XINFO")
            .arg("CONSUMERS")
            .arg(&config.name)
            .arg(&config.group)
            .query_async(&mut connection)
            .await;
        debug!(
            "action=consumer_info stream={} group={} info={:?}",
            config.name, config.group, consumer_info
        );

        let (ack_tx, mut ack_rx) = tokio::sync::mpsc::channel::<String>(config.xack_buffer_size);
        let (ack_shutdown_tx, mut ack_shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let (msg_tx, mut msg_rx) =
            tokio::sync::mpsc::channel::<Vec<StreamId>>(config.message_buffer_size);
        let (msg_shutdown_tx, mut msg_shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let config_messages = Arc::clone(&config);

        let messages = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            let config = Arc::clone(&config_messages);
            let handler = handler.clone();

            loop {
                tokio::select! {
                    Some(ids) = msg_rx.recv() => {
                        for StreamId { id, map } in ids {
                            if tasks.len() >= config.max_concurrency {
                                tasks.join_next().await;
                            }

                            let handler = handler.clone();
                            let ack_tx = ack_tx.clone();
                            let config = Arc::clone(&config);

                            // each `StreamId` represents an `AccountInfo`
                            ingest_tasks_total_inc(&config.name, &config.consumer);

                            tasks.spawn(async move {
                                let start_time = tokio::time::Instant::now();
                                let result = handler.handle(map).await;
                                let elapsed_time = start_time.elapsed().as_secs_f64();

                                ingest_job_time_set(&config.name, &config.consumer, elapsed_time);

                                match result {
                                    Ok(()) => {
                                        program_transformer_task_status_inc(&config.name, &config.consumer, ProgramTransformerTaskStatusKind::Success);
                                    }
                                    Err(IngestMessageError::SnapshotSend(e)) => {
                                        program_transformer_task_status_inc(&config.name, &config.consumer, e.into());
                                    }
                                    Err(IngestMessageError::RedisStreamMessage(e)) => {
                                        program_transformer_task_status_inc(&config.name, &config.consumer, e.into());
                                    }
                                    Err(IngestMessageError::DownloadMetadataJson(e)) => {
                                        program_transformer_task_status_inc(&config.name, &config.consumer, e.into());
                                    }
                                    Err(IngestMessageError::ProgramTransformer(e)) => {
                                        error!("Failed to process message: {:?}", e);
                                        program_transformer_task_status_inc(&config.name, &config.consumer, e.into());
                                    }
                                }

                                if let Err(e) = ack_tx.send(id).await {
                                    error!(target: "ingest_stream", "action=send_ack stream={} error={:?}", &config.name, e);
                                }

                                ingest_tasks_total_dec(&config.name, &config.consumer);
                            });
                        }
                    }
                    _ = &mut msg_shutdown_rx => {
                        break;
                    }
                }
            }

            while (tasks.join_next().await).is_some() {}
        });

        let ack = tokio::spawn({
            let config = Arc::clone(&config);
            let mut pending = Vec::new();
            let mut tasks = JoinSet::new();
            let handler = Arc::new(Acknowledge::new(Arc::clone(&config), connection.clone()));

            async move {
                let deadline = tokio::time::sleep(config.xack_batch_max_idle);
                tokio::pin!(deadline);

                loop {
                    tokio::select! {
                        Some(id) = ack_rx.recv() => {
                            pending.push(id);

                            if pending.len() >= config.xack_batch_max_size {
                                if tasks.len() >= config.ack_concurrency {
                                    tasks.join_next().await;
                                }

                                let ids = std::mem::take(&mut pending);
                                let handler = Arc::clone(&handler);


                                ack_tasks_total_inc(&config.name, &config.consumer);

                                tasks.spawn(async move {
                                    handler.handle(ids).await;
                                });

                                deadline.as_mut().reset(tokio::time::Instant::now() + config.xack_batch_max_idle);
                            }
                        }
                        _ = &mut deadline, if !pending.is_empty() => {
                            if tasks.len() >= config.ack_concurrency {
                                tasks.join_next().await;
                            }
                            let ids = std::mem::take(&mut pending);
                            let handler = Arc::clone(&handler);

                            ack_tasks_total_inc(&config.name, &config.consumer);

                            tasks.spawn(async move {
                                handler.handle(ids).await;
                            });

                            deadline.as_mut().reset(tokio::time::Instant::now() + config.xack_batch_max_idle);
                        }
                        _ = &mut ack_shutdown_rx => {
                            break;
                        }
                    }
                }

                if !pending.is_empty() {
                    let handler = Arc::clone(&handler);
                    handler.handle(std::mem::take(&mut pending)).await;
                }

                while (tasks.join_next().await).is_some() {}
            }
        });

        let labels = vec![config.name.clone()];
        tokio::spawn({
            let connection = connection.clone();
            let config = Arc::clone(&config);

            async move {
                let config = Arc::clone(&config);

                loop {
                    let connection = connection.clone();
                    let labels = labels.clone();

                    if let Err(e) = report_xlen(connection, labels).await {
                        error!("action=report_xlen stream={} error={:?}", &config.name, e);
                    }

                    sleep(Duration::from_millis(100)).await;
                }
            }
        });

        let control = tokio::spawn({
            let mut connection = connection.clone();

            async move {
                let config = Arc::clone(&config);

                debug!("action=read_stream_start stream={}", config.name);

                loop {
                    let config = Arc::clone(&config);

                    tokio::select! {
                        biased;
                        _ = &mut internal_shutdown_rx => {
                            if let Err(e) = msg_shutdown_tx.send(()) {
                                error!("action=msg_shutdown stream={} error={:?}", &config.name, e);
                            }

                            if let Err(e) = messages.await {
                                error!("action=await_messages stream={} error={:?}", &config.name, e);
                            }

                            if let Err(e) = ack_shutdown_tx.send(()) {
                                error!("action=ack_shutdown stream={} error={:?}", &config.name, e);
                            }

                            if let Err(e) = ack.await {
                                error!("action=ack_shutdown stream={} error={:?}", &config.name, e);
                            }

                            break;
                        },
                        result = self.read(&mut connection) => {
                            match result {
                                Ok(reply) => {
                                    debug!(
                                        "action=xread_response stream={} keys_len={} reply={:?}",
                                        config.name,
                                        reply.keys.len(),
                                        reply
                                    );

                                    for StreamKey { key: _, ids } in reply.keys {
                                        let config = Arc::clone(&config);
                                        let count = ids.len();
                                        debug!(
                                            "action=xread_messages stream={} count={:?} first_id={:?} last_id={:?}",
                                            &config.name,
                                            count,
                                            ids.first().map(|id| &id.id),
                                            ids.last().map(|id| &id.id)
                                        );

                                        redis_xread_inc(&config.name, &config.consumer, count);

                                        if let Err(e) = msg_tx.send(ids).await {
                                            error!(target: "ingest_stream", "action=send_ids stream={} error={:?}", &config.name, e);
                                        }
                                    }
                                }
                                Err(err) => {
                                    error!(
                                        "action=xread_error stream={} error={:?}",
                                        &config.name,
                                        err
                                    );

                                    if err.code() == Some("NOGROUP") {
                                        warn!(
                                            "action=xread_recreate_group stream={} group={} consumer={} message=re-creating consumer group after NOGROUP",
                                            &config.name, &config.group, &config.consumer
                                        );

                                        if let Err(e) =
                                            xgroup_create(&mut connection, &config.name, &config.group)
                                                .await
                                        {
                                            error!(
                                                "action=xread_recreate_group_failed stream={} error={:?}",
                                                &config.name, e
                                            );
                                        } else if let Err(e) = xgroup_create_consumer(
                                            &mut connection,
                                            &config.name,
                                            &config.group,
                                            &config.consumer,
                                        )
                                        .await
                                        {
                                            error!(
                                                "action=xread_recreate_consumer_failed stream={} error={:?}",
                                                &config.name, e
                                            );
                                        }
                                    }

                                    sleep(Duration::from_millis(500)).await;
                                }
                            }
                        }
                    }
                }

                warn!(
                    "action=stream_shutdown stream={} stream shutdown",
                    config.name
                );
            }
        });

        Ok(IngestStreamStop {
            control,
            shutdown_tx: internal_shutdown_tx,
        })
    }
}

#[derive(Clone)]
pub struct TrackedPipeline {
    pipeline: redis::Pipeline,
    count: usize,
}

impl Default for TrackedPipeline {
    fn default() -> Self {
        Self {
            pipeline: redis::pipe(),
            count: 0,
        }
    }
}

impl TrackedPipeline {
    pub fn xadd_maxlen<F, V>(&mut self, key: &str, maxlen: StreamMaxlen, id: F, field: V)
    where
        F: redis::ToRedisArgs,
        V: redis::ToRedisArgs,
    {
        self.pipeline
            .xadd_maxlen(key, maxlen, id, &[(REDIS_STREAM_DATA_KEY, field)]);
        self.count += 1;
    }

    pub async fn flush(&mut self, connection: &mut MultiplexedConnection) -> Result<usize, usize> {
        let result: RedisResult<RedisValue> = self.pipeline.atomic().query_async(connection).await;
        let count = self.count;
        self.count = 0;
        self.pipeline.clear();

        match result {
            Ok(_) => Ok(count),
            Err(_) => Err(count),
        }
    }
}

pub async fn report_xlen<C: AsyncCommands>(
    mut connection: C,
    streams: Vec<String>,
) -> anyhow::Result<()> {
    let mut pipe = redis::pipe();
    for stream in &streams {
        pipe.xlen(stream);
    }
    let xlens: Vec<usize> = pipe.query_async(&mut connection).await?;

    for (stream, xlen) in streams.iter().zip(xlens) {
        redis_xlen_set(stream, xlen);
    }

    Ok(())
}

pub async fn xgroup_create<C: AsyncCommands>(
    connection: &mut C,
    name: &str,
    group: &str,
) -> anyhow::Result<()> {
    let result: RedisResult<RedisValue> = connection.xgroup_create_mkstream(name, group, "0").await;
    if let Err(error) = result {
        if !(error.kind() == RedisErrorKind::ExtensionError
            && error.detail() == Some("Consumer Group name already exists")
            && error.code() == Some("BUSYGROUP"))
        {
            return Err(error.into());
        }
    }

    Ok(())
}

pub async fn xgroup_create_consumer<C: AsyncCommands>(
    connection: &mut C,
    name: &str,
    group: &str,
    consumer: &str,
) -> anyhow::Result<()> {
    let result: RedisResult<RedisValue> = redis::cmd("XGROUP")
        .arg("CREATECONSUMER")
        .arg(name)
        .arg(group)
        .arg(consumer)
        .query_async(connection)
        .await;

    match result {
        Ok(_) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub async fn xgroup_delete_consumer<C: AsyncCommands>(
    connection: &mut C,
    name: &str,
    group: &str,
    consumer: &str,
) -> anyhow::Result<()> {
    let result: RedisResult<RedisValue> = redis::cmd("XGROUP")
        .arg("DELCONSUMER")
        .arg(name)
        .arg(group)
        .arg(consumer)
        .query_async(connection)
        .await;

    match result {
        Ok(_) => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::{collections::HashMap, time::Duration};
    use yellowstone_grpc_client::{ClientTlsConfig, GeyserGrpcClient};
    use yellowstone_grpc_proto::geyser::{
        subscribe_update::UpdateOneof, SubscribeRequest, SubscribeRequestFilterTransactions,
    };

    /// The same real devnet v1 transaction used by the das-core tests.
    const V1_FIXTURE: &str = include_str!("../../core/tests/fixtures/transaction_v1.json");

    /// Builds the `SubscribeUpdateTransaction` a v1-capable Yellowstone plugin
    /// emits, then runs it through the real redis-stream parser. Yellowstone
    /// proto v9 has no `config` field, so a v1 message reaches us as an
    /// ordinary versioned message; only the keys and instructions matter.
    #[test]
    fn parses_a_v1_transaction_from_the_grpc_proto() {
        use yellowstone_grpc_proto::geyser::SubscribeUpdateTransactionInfo;
        use yellowstone_grpc_proto::solana::storage::confirmed_block::{
            CompiledInstruction as ProtoCompiledInstruction, Message as ProtoMessage,
            MessageHeader as ProtoMessageHeader, Transaction as ProtoTransaction,
            TransactionStatusMeta as ProtoMeta,
        };

        let fetched: solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta =
            serde_json::from_str(V1_FIXTURE).expect("fixture parses");
        let slot = fetched.slot;

        let decoded = fetched
            .transaction
            .transaction
            .decode()
            .expect("v1 transaction decodes");
        let header = decoded.message.header();

        let proto = SubscribeUpdateTransaction {
            slot,
            transaction: Some(SubscribeUpdateTransactionInfo {
                signature: decoded.signatures[0].as_ref().to_vec(),
                is_vote: false,
                index: 0,
                transaction: Some(ProtoTransaction {
                    signatures: decoded
                        .signatures
                        .iter()
                        .map(|s| s.as_ref().to_vec())
                        .collect(),
                    message: Some(ProtoMessage {
                        header: Some(ProtoMessageHeader {
                            num_required_signatures: header.num_required_signatures as u32,
                            num_readonly_signed_accounts: header.num_readonly_signed_accounts
                                as u32,
                            num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts
                                as u32,
                        }),
                        account_keys: decoded
                            .message
                            .static_account_keys()
                            .iter()
                            .map(|k| k.to_bytes().to_vec())
                            .collect(),
                        recent_blockhash: decoded.message.recent_blockhash().to_bytes().to_vec(),
                        instructions: decoded
                            .message
                            .instructions()
                            .iter()
                            .map(|ix| ProtoCompiledInstruction {
                                program_id_index: ix.program_id_index as u32,
                                accounts: ix.accounts.clone(),
                                data: ix.data.clone(),
                            })
                            .collect(),
                        // v1 is a versioned message with no lookup tables.
                        versioned: true,
                        address_table_lookups: vec![],
                    }),
                }),
                meta: Some(ProtoMeta::default()),
            }),
        };

        let mut msg = HashMap::new();
        msg.insert(
            REDIS_STREAM_DATA_KEY.to_string(),
            RedisValue::Data(proto.encode_to_vec()),
        );

        let info = TransactionInfo::try_parse_msg(msg).expect("v1 proto message parses");

        assert_eq!(info.slot, slot);
        assert_eq!(info.signature, decoded.signatures[0]);
        // v1 has no loaded addresses, so the key list is exactly the static keys.
        assert_eq!(info.account_keys.len(), 2);
        assert_eq!(info.message_instructions.len(), 1);
        assert_eq!(
            info.account_keys,
            decoded.message.static_account_keys().to_vec()
        );
    }

    const BUBBLEGUM: &str = "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY";

    /// Yellowstone proto v9 has no per-message version: `Message.versioned` is
    /// true for both v0 and v1. Ask the RPC what version a signature really was.
    async fn transaction_version(rpc_url: &str, signature: &str) -> Option<i64> {
        let body = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "getTransaction",
            "params": [signature, {
                "encoding": "base64",
                "maxSupportedTransactionVersion": 1,
                "commitment": "confirmed"
            }]
        });
        let res: serde_json::Value = reqwest::Client::new()
            .post(rpc_url)
            .json(&body)
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        match res.get("result")?.get("version")? {
            serde_json::Value::Number(n) => n.as_i64(),
            // "legacy"
            _ => Some(-1),
        }
    }

    /// Streams live transactions and runs them through the real redis-stream
    /// parser. Run with:
    /// `GRPC_ENDPOINT=... GRPC_X_TOKEN=... cargo test -p das-grpc-ingest --bin das-grpc-ingest -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "requires a live Dragon's Mouth endpoint"]
    async fn parses_live_transactions_from_dragon_mouth() {
        let endpoint = std::env::var("GRPC_ENDPOINT").expect("GRPC_ENDPOINT");
        let x_token = std::env::var("GRPC_X_TOKEN").ok();

        let mut client = GeyserGrpcClient::build_from_shared(endpoint)
            .expect("endpoint")
            .x_token(x_token)
            .expect("token")
            .tls_config(ClientTlsConfig::new().with_native_roots())
            .expect("tls")
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .connect()
            .await
            .expect("connects to Dragon's Mouth");

        // Override to widen the filter, e.g. the memo program on devnet where
        // the v1 test traffic lives.
        let account_include =
            std::env::var("GRPC_ACCOUNT_INCLUDE").unwrap_or_else(|_| BUBBLEGUM.to_string());

        let mut transactions = HashMap::new();
        transactions.insert(
            "test".to_string(),
            SubscribeRequestFilterTransactions {
                account_include: account_include
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .map(str::to_string)
                    .collect(),
                ..Default::default()
            },
        );

        let (_tx, stream) = client
            .subscribe_with_request(Some(SubscribeRequest {
                transactions,
                ..Default::default()
            }))
            .await
            .expect("subscribes");
        tokio::pin!(stream);

        let target: usize = std::env::var("GRPC_SAMPLE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5);
        let verify_rpc = std::env::var("VERIFY_RPC_URL").ok();

        let (mut parsed, mut versioned) = (0usize, 0usize);
        let mut signatures: Vec<String> = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);

        while parsed < target && tokio::time::Instant::now() < deadline {
            let Ok(Some(Ok(update))) =
                tokio::time::timeout(Duration::from_secs(20), stream.next()).await
            else {
                break;
            };

            let Some(UpdateOneof::Transaction(update)) = update.update_oneof else {
                continue;
            };

            if update
                .transaction
                .as_ref()
                .and_then(|t| t.transaction.as_ref())
                .and_then(|t| t.message.as_ref())
                .map(|m| m.versioned)
                .unwrap_or(false)
            {
                versioned += 1;
            }

            // Exactly what grpc2redis writes into the stream.
            let mut msg = HashMap::new();
            msg.insert(
                REDIS_STREAM_DATA_KEY.to_string(),
                RedisValue::Data(update.encode_to_vec()),
            );

            let info = TransactionInfo::try_parse_msg(msg).expect("live transaction parses");
            assert!(!info.account_keys.is_empty());
            assert!(!info.message_instructions.is_empty());
            signatures.push(info.signature.to_string());
            parsed += 1;
        }

        println!("parsed {parsed} live transactions ({versioned} versioned)");
        assert!(parsed > 0, "expected at least one live transaction");

        if let Some(rpc_url) = verify_rpc {
            // The stream is ahead of what the RPC will serve; let it catch up.
            tokio::time::sleep(Duration::from_secs(30)).await;
            let (mut legacy, mut v0, mut v1, mut unknown) = (0, 0, 0, 0);
            for signature in &signatures {
                match transaction_version(&rpc_url, signature).await {
                    Some(-1) => legacy += 1,
                    Some(0) => v0 += 1,
                    Some(1) => v1 += 1,
                    _ => unknown += 1,
                }
            }
            println!("versions via RPC: legacy={legacy} v0={v0} v1={v1} unknown={unknown}");
            if v1 > 0 {
                println!("PROVEN: {v1} transaction v1 message(s) parsed from the live stream");
            }
        }
    }
}
