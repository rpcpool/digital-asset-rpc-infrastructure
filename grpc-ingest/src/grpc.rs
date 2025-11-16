use {
    crate::{
        config::{ConfigGrpc, ConfigGrpcRequestFilter, ConfigStream, ConfigSubscription}, fumarole::FumaroleCheckerSource, prom::{self, grpc_tasks_total_dec, grpc_tasks_total_inc, redis_xadd_status_inc}, redis::TrackedPipeline
    },
    futures::stream::{FuturesUnordered, StreamExt},
    redis::streams::StreamMaxlen,
    solana_sdk::system_program::ID as system_program_id,
    std::{collections::HashMap, sync::Arc, time::Duration},
    tokio::{
        sync::{
            Mutex, mpsc::{self, Sender}
        },
        task::JoinHandle,
        time::sleep,
    },
    tracing::{debug, warn},
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::{
        geyser::{
            CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterTransactions, SubscribeRequestPing, SubscribeUpdate
        },
        prelude::subscribe_update::UpdateOneof,
        prost::Message,
    },
    yellowstone_grpc_tools::config::GrpcRequestToProto,
};

const PING_ID: i32 = 0;

pub async fn run(config: ConfigGrpc) -> anyhow::Result<()> {
    let redis_client = redis::Client::open(config.redis.url.clone())?;
    let connection = redis_client.get_multiplexed_tokio_connection().await?;
    let config = Arc::new(config);

    let subscriptions = config.subscriptions.clone();

    //TODO: This is only serving to test fumarole against common gRPC stream
    let fumarole_checker_tx = crate::fumarole::fumarole_checker();

    // for (label, subscription_config) in subscriptions.clone() {   
    //     let subscription = Subscription {
    //         label,
    //         config: subscription_config,
    //     };
    //     SubscriptionTask::build()
    //         .config(Arc::clone(&config))
    //         .subscription(subscription)
    //         .start(fumarole_checker_tx.clone())
    //         .await;
    // }

    start_individual_grpc_subscription(Arc::clone(&config), fumarole_checker_tx.clone()).await;

    let handle = start(config, connection, subscriptions.values().next().unwrap().clone(), fumarole_checker_tx.clone()).await?;
    match handle.await {
        Ok(_) => {
            warn!(target: "grpc2redis", message = "Subscription task ended OK");
        }
        Err(err) => {
            tracing::error!(target: "grpc2redis", message = "Subscription task ended with error", ?err);
        }
    }

    Ok(())
}

pub struct Subscription {
    pub label: String,
    pub config: ConfigSubscription,
}

#[derive(Default)]
pub struct SubscriptionTask {
    pub config: Arc<ConfigGrpc>,
    pub subscription: Option<Subscription>,
}

impl SubscriptionTask {
    pub fn build() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: Arc<ConfigGrpc>) -> Self {
        self.config = config;
        self
    }

    pub fn subscription(mut self, subscription: Subscription) -> Self {
        self.subscription = Some(subscription);
        self
    }

    pub async fn start(
        mut self,
        fumarole_checker_tx: Sender<(FumaroleCheckerSource, SubscribeUpdate)>,
    ) {
        let config = Arc::clone(&self.config);

        let subscription = self.subscription.take().expect("Subscription is required");
        let label = subscription.label.clone();
        let subscription_config = Arc::new(subscription.config);

        let ConfigSubscription { stream: _, filter } = subscription_config.as_ref().clone();

        let mut req_accounts = HashMap::with_capacity(1);
        let mut req_transactions = HashMap::with_capacity(1);
        let mut req_slot = HashMap::with_capacity(1);

        let ConfigGrpcRequestFilter {
            accounts,
            transactions,
            slot,
        } = filter;

        if let Some(accounts) = accounts {
            req_accounts.insert(label.clone(), accounts.to_proto());
        }

        if let Some(transactions) = transactions {
            req_transactions.insert(label.clone(), transactions.to_proto());
        }

        if let Some(true) = slot {
            req_slot.insert(label.clone(), SubscribeRequestFilterBlocksMeta {});
        }

        let request = SubscribeRequest {
            accounts: req_accounts,
            transactions: req_transactions,
            commitment: Some(config.geyser.commitment.to_proto().into()),
            blocks_meta: req_slot,
            ..Default::default()
        };

        let mut dragon_mouth_client =
            GeyserGrpcClient::build_from_shared(config.geyser.endpoint.clone())
                .expect("failed to build gRPC client")
                .x_token(config.geyser.x_token.clone())
                .expect("failed to set x-token")
                .connect_timeout(Duration::from_secs(config.geyser.connect_timeout))
                .timeout(Duration::from_secs(config.geyser.timeout))
                .connect()
                .await
                .expect("failed to connect to gRPC");

        let (mut _subscribe_tx, stream) = dragon_mouth_client
            .subscribe_with_request(Some(request))
            .await.expect("failed to subscribe to gRPC");

        // Send GRPC updates to fumarole checker
        tokio::spawn(async move {
            tokio::pin!(stream);
            while let Some(Ok(update)) = stream.next().await {

                prom::GRPC_UPDATES_COUNT.inc();

                match fumarole_checker_tx.send((FumaroleCheckerSource::Grpc, update.clone())).await {
                    Ok(_) => (),
                    Err(err) => {
                        tracing::error!(target: "grpc2redis", message = "Failed to send GRPC update to fumarole checker", ?err);
                    }
                }
            }
        });
    }
}

pub async fn start(
    config: Arc<ConfigGrpc>,
    connection: redis::aio::MultiplexedConnection,
    subscription_config: ConfigSubscription,
    fumarole_checker_tx: Sender<(FumaroleCheckerSource, SubscribeUpdate)>,
) -> anyhow::Result<JoinHandle<()>> {
    let connection = connection.clone();

    let ConfigSubscription { stream, filter } = subscription_config.clone();

    let stream_config = Arc::new(stream.clone());

    let pipes: Vec<_> = (0..stream_config.pipeline_count)
        .map(|_| Arc::new(Mutex::new(TrackedPipeline::default())))
        .collect();

    let (fumarole_tx, mut fumarole_rx) = mpsc::channel(10_000);

    let (mut fumarole_handle, mut fumarole_sink) =
        crate::fumarole::connect(config.fumarole.clone(), fumarole_tx.clone()).await?;

    let control = tokio::spawn({
        async move {

            for pipe in &pipes {
                let pipe = Arc::clone(pipe);
                let stream_config = Arc::clone(&stream_config);
                let mut connection = connection.clone();

                tokio::spawn(async move {
                    loop {
                        sleep(stream_config.pipeline_max_idle).await;

                        let mut pipe = pipe.lock().await;
                        let flush = pipe.flush(&mut connection).await;

                        let status = flush.as_ref().map(|_| ()).map_err(|_| ());
                        let count = flush.as_ref().unwrap_or_else(|count| count);

                        debug!(target: "grpc2redis", action = "flush_redis_pip_deadline", status = ?status, count = ?count);
                        redis_xadd_status_inc("fumarole", "TOTAL", status, *count);
                    }
                });
            }

            let mut current_pipe_index = 0;
            let mut tasks = FuturesUnordered::new();

            loop {
                tokio::select! {
                    event = fumarole_rx.recv() => {
                        match event {
                            Some(msg) => {

                                prom::FUMAROLE_UPDATES_COUNT.inc();

                                match fumarole_checker_tx.send((FumaroleCheckerSource::Fumarole, msg.clone())).await {
                                    Ok(_) => (),
                                    Err(err) => {
                                        tracing::error!(target: "grpc2redis", message = "Failed to send message to fumarole checker", ?err);
                                    }
                                }


                                let pipe = Arc::clone(&pipes[current_pipe_index]);

                                save_update_to_redis(pipe, msg, Arc::clone(&stream_config), fumarole_sink.clone(), &mut tasks).await;

                                current_pipe_index = (current_pipe_index + 1) % pipes.len();
                            }
                            None => {
                                tracing::error!("Receiving None on the Fumarole receiver. Reconnection went wrong");
                            }
                        }
                    }
                    // If the JoinHandle ends, means we lost the fumarole connection, reconnect
                    result = &mut fumarole_handle => {
                        match result {
                            Ok(_) => {
                                warn!(target: "grpc2redis", message = "Fumarole connection ended OK");
                            }
                            Err(err) => {
                                warn!(target: "grpc2redis", message = "Fumarole connection ended with error", ?err);
                            }
                        }

                        // Wait before reconnecting to avoid overwhelming the fumarole server
                        sleep(Duration::from_secs(10)).await;

                        // This will replace the previous fumarole handle with a new connection, to keep awaiting on it for future reconnections
                        let (fumarole_new_handle, fumarole_new_sink) =
                            crate::fumarole::connect(config.fumarole.clone(), fumarole_tx.clone()).await.expect("Failed to reconnect to fumarole");

                        fumarole_handle = fumarole_new_handle;
                        fumarole_sink = fumarole_new_sink;
                    }
                }
            }
        }
    });

    Ok(control)
}

pub async fn save_update_to_redis(
    pipe: Arc<Mutex<TrackedPipeline>>,
    msg: SubscribeUpdate,
    stream_config: Arc<ConfigStream>,
    sink: Sender<SubscribeRequest>,
    tasks: &mut FuturesUnordered<JoinHandle<()>>,
) {
    if tasks.len() >= stream_config.max_concurrency {
        tasks.next().await;
    }

    grpc_tasks_total_inc("fumarole", "TOTAL");

    tasks.push(tokio::spawn( async move {
        match msg.update_oneof {
            Some(UpdateOneof::Account(account)) => {
                if let Some(ref acc) = account.account {
                    if acc.owner == system_program_id.to_bytes().to_vec()
                        && (acc.lamports != 0 || !acc.data.is_empty())
                    {
                        return;
                    }
                }
    
                let mut pipe = pipe.lock().await;
                pipe.xadd_maxlen(
                    "ACCOUNTS",
                    StreamMaxlen::Approx(stream_config.max_len),
                    "*",
                    account.encode_to_vec(),
                );
            }
            Some(UpdateOneof::Transaction(transaction)) => {
                if let Some(transaction) = &transaction.transaction {
                    if let Some(meta) = &transaction.meta {
                        if meta.err.is_some() {
                            return;
                        }
                    }
                }
    
                let mut pipe = pipe.lock().await;
                pipe.xadd_maxlen(
                    "TRANSACTIONS",
                    StreamMaxlen::Approx(stream_config.max_len),
                    "*",
                    transaction.encode_to_vec(),
                );
            }
            Some(UpdateOneof::BlockMeta(block_meta)) => {
                let mut pipe = pipe.lock().await;
    
                pipe.xadd_maxlen(
                    "SLOT",
                    StreamMaxlen::Approx(stream_config.max_len),
                    "*",
                    block_meta.encode_to_vec(),
                );
            }
            Some(UpdateOneof::Ping(_)) => {
                let ping = sink
                    .send(SubscribeRequest {
                        ping: Some(SubscribeRequestPing { id: PING_ID }),
                        ..Default::default()
                    })
                    .await;
    
                match ping {
                    Ok(_) => {
                        debug!(target: "grpc2redis", action = "send_ping", message = "Ping sent successfully", id = PING_ID);
                    }
                    Err(err) => {
                        warn!(target: "grpc2redis", action = "send_ping_failed", message = "Failed to send ping", ?err, id = PING_ID);
                    }
                }
            }
            Some(UpdateOneof::Pong(pong)) => {
                if pong.id == PING_ID {
                    debug!(target: "grpc2redis", action = "receive_pong", message = "Pong received", id = PING_ID);
                } else {
                    warn!(target: "grpc2redis", action = "receive_unknown_pong", message = "Unknown pong id received", id = pong.id);
                }
            }
            _ => warn!(target: "grpc2redis", action = "unknown_update_variant", message = "Unknown update variant")
            
        }

        grpc_tasks_total_dec("fumarole", "TOTAL");
    }));
}

pub async fn start_individual_grpc_subscription(
    config: Arc<ConfigGrpc>,
    fumarole_checker_tx: Sender<(FumaroleCheckerSource, SubscribeUpdate)>,
) {

    let subscribe_request = SubscribeRequest {
        accounts: [(
            "accounts".to_string(),
            SubscribeRequestFilterAccounts {
                account: vec![],
                owner: vec![
                    "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s".to_string(),
                    "inscokhJarcjaEs59QbQ7hYjrKz25LEPRfCbP8EmdUp".to_string(),
                    "CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d".to_string(),
                    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
                    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb".to_string(),
                    "11111111111111111111111111111111".to_string(),
                ],
                filters: vec![],
            },
        )]
        .into(),
        transactions: [(
            "transactions".to_string(),
            SubscribeRequestFilterTransactions {
                account_include: vec![
                    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb".to_string(),
                    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".to_string(),
                    "BGUMAp9Gq7iTEuizy4pqaxsTyUCBK68MDfK752saRPUY".to_string(),
                ],
                account_exclude: vec![],
                account_required: vec![],
                vote: Some(false),
                failed: Some(false),
                signature: None,
            },
        )]
        .into(),
        commitment: Some(CommitmentLevel::Finalized as i32),
        blocks_meta: [(
            "blocks_meta".to_string(),
            SubscribeRequestFilterBlocksMeta {},
        )]
        .into(),
        ..Default::default()
    };

    let mut dragon_mouth_client =
        GeyserGrpcClient::build_from_shared(config.geyser.endpoint.clone())
            .expect("failed to build gRPC client")
            .x_token(config.geyser.x_token.clone())
            .expect("failed to set x-token")
            .connect_timeout(Duration::from_secs(config.geyser.connect_timeout))
            .timeout(Duration::from_secs(config.geyser.timeout))
            .connect()
            .await
            .expect("failed to connect to gRPC");

    let (mut _subscribe_tx, stream) = dragon_mouth_client
        .subscribe_with_request(Some(subscribe_request))
        .await.expect("failed to subscribe to gRPC");

    // Send GRPC updates to fumarole checker
    tokio::spawn(async move {
        tokio::pin!(stream);
        while let Some(Ok(update)) = stream.next().await {

            prom::GRPC_UPDATES_COUNT.inc();

            match fumarole_checker_tx.send((FumaroleCheckerSource::Grpc, update.clone())).await {
                Ok(_) => (),
                Err(err) => {
                    tracing::error!(target: "grpc2redis", message = "Failed to send GRPC update to fumarole checker", ?err);
                }
            }
        }
    });
}