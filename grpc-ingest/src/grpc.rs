use {
    crate::{
        config::{ConfigGrpc, ConfigGrpcRequestFilter, ConfigStream, ConfigSubscription},
        prom::{grpc_tasks_total_dec, grpc_tasks_total_inc, redis_xadd_status_inc},
        redis::TrackedPipeline,
        util::create_shutdown,
    },
    anyhow::Context,
    futures::{stream::StreamExt, SinkExt},
    redis::streams::StreamMaxlen,
    std::{collections::HashMap, sync::Arc, time::Duration},
    tokio::{sync::Mutex, time::sleep},
    topograph::{
        executor::{Executor, Nonblock, Tokio},
        prelude::*,
        AsyncHandler,
    },
    tracing::{debug, error, warn},
    yellowstone_grpc_client::GeyserGrpcClient,
    yellowstone_grpc_proto::{
        geyser::{SubscribeRequest, SubscribeRequestPing, SubscribeUpdate},
        prelude::subscribe_update::UpdateOneof,
        prost::Message,
    },
    yellowstone_grpc_tools::config::GrpcRequestToProto,
};

const PING_ID: i32 = 0;

enum GrpcJob {
    FlushRedisPipe,
    ProcessSubscribeUpdate(Box<SubscribeUpdate>),
}

#[derive(Clone)]
pub struct GrpcJobHandler {
    connection: redis::aio::MultiplexedConnection,
    stream_config: Arc<ConfigStream>,
    pipe: Arc<Mutex<TrackedPipeline>>,
    label: String,
}

impl<'a> AsyncHandler<GrpcJob, topograph::executor::Handle<'a, GrpcJob, Nonblock<Tokio>>>
    for GrpcJobHandler
{
    type Output = ();

    fn handle(
        &self,
        job: GrpcJob,
        _handle: topograph::executor::Handle<'a, GrpcJob, Nonblock<Tokio>>,
    ) -> impl futures::Future<Output = Self::Output> + Send + 'a {
        let stream_config = Arc::clone(&self.stream_config);
        let connection = self.connection.clone();
        let pipe = Arc::clone(&self.pipe);
        let label = self.label.clone();

        grpc_tasks_total_inc(&label, &stream_config.name.to_string());

        async move {
            match job {
                GrpcJob::FlushRedisPipe => {
                    let mut pipe = pipe.lock().await;
                    let mut connection = connection;

                    let flush = pipe.flush(&mut connection).await;

                    let status = flush.as_ref().map(|_| ()).map_err(|_| ());
                    let counts = flush.as_ref().unwrap_or_else(|counts| counts);

                    for (stream, count) in counts.iter() {
                        debug!(target: "grpc2redis", action = "flush_redis_pipe", stream = ?stream, status = ?status, count = ?count);
                        redis_xadd_status_inc(stream, status, *count);
                    }
                }
                GrpcJob::ProcessSubscribeUpdate(update) => {
                    let stream = stream_config.name.clone();
                    let stream_maxlen = stream_config.max_len;

                    let SubscribeUpdate { update_oneof, .. } = *update;

                    let mut pipe = pipe.lock().await;

                    if let Some(update) = update_oneof {
                        match update {
                            UpdateOneof::Account(account) => {
                                pipe.xadd_maxlen(
                                    &stream.to_string(),
                                    StreamMaxlen::Approx(stream_maxlen),
                                    "*",
                                    account.encode_to_vec(),
                                );
                                debug!(target: "grpc2redis", action = "process_account_update",label = ?label, stream = ?stream, maxlen = ?stream_maxlen);
                            }

                            UpdateOneof::Transaction(transaction) => {
                                pipe.xadd_maxlen(
                                    &stream.to_string(),
                                    StreamMaxlen::Approx(stream_maxlen),
                                    "*",
                                    transaction.encode_to_vec(),
                                );
                                debug!(target: "grpc2redis", action = "process_transaction_update",label = ?label, stream = ?stream, maxlen = ?stream_maxlen);
                            }
                            _ => {
                                warn!(target: "grpc2redis", action = "unknown_update_variant",label = ?label, message = "Unknown update variant")
                            }
                        }
                    }
                }
            }

            grpc_tasks_total_dec(&label, &stream_config.name.to_string());
        }
    }
}

pub async fn run(config: ConfigGrpc) -> anyhow::Result<()> {
    let redis_client = redis::Client::open(config.redis.url.clone())?;
    let connection = redis_client.get_multiplexed_tokio_connection().await?;

    let mut shutdown = create_shutdown()?;

    let config = Arc::new(config);

    let subscriptions = config.subscriptions.clone();

    let mut subscription_tasks = Vec::new();
    for (label, subscription_config) in subscriptions {
        let subscription = Subscription {
            label,
            config: subscription_config,
        };
        let task = SubscriptionTask::build()
            .config(Arc::clone(&config))
            .pipeline(TrackedPipeline::default())
            .connection(connection.clone())
            .subscription(subscription)
            .start()
            .await?;

        subscription_tasks.push(task);
    }

    if let Some(signal) = shutdown.next().await {
        warn!(
            target: "grpc2redis",
            action = "shutdown_signal_received",
            message = "Shutdown signal received, waiting for spawned tasks to complete",
            signal = ?signal
        );
    }

    let res = futures::future::join_all(
        subscription_tasks
            .into_iter()
            .map(|task| task.stop())
            .collect::<Vec<_>>(),
    )
    .await;

    res.into_iter().collect::<Result<(), anyhow::Error>>()?;

    Ok(())
}

pub struct Subscription {
    pub label: String,
    pub config: ConfigSubscription,
}

#[derive(Default)]
pub struct SubscriptionTask {
    pub config: Arc<ConfigGrpc>,
    pub pipe: Option<Arc<Mutex<TrackedPipeline>>>,
    pub connection: Option<redis::aio::MultiplexedConnection>,
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

    pub fn pipeline(mut self, pipe: TrackedPipeline) -> Self {
        self.pipe = Some(Arc::new(Mutex::new(pipe)));
        self
    }

    pub fn subscription(mut self, subscription: Subscription) -> Self {
        self.subscription = Some(subscription);
        self
    }

    pub fn connection(mut self, connection: redis::aio::MultiplexedConnection) -> Self {
        self.connection = Some(connection);
        self
    }

    pub async fn start(mut self) -> anyhow::Result<SubscriptionTaskStop> {
        let config = Arc::clone(&self.config);
        let connection = self
            .connection
            .take()
            .expect("Redis Connection is required");
        let pipe = self.pipe.take().expect("Pipeline is required");

        let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();
        let subscription = self.subscription.take().expect("Subscription is required");
        let label = subscription.label.clone();
        let subscription_config = Arc::new(subscription.config);
        let connection = connection.clone();
        let pipe = Arc::clone(&pipe);

        let ConfigSubscription { stream, filter } = subscription_config.as_ref().clone();

        let stream_config = Arc::new(stream.clone());
        let mut req_accounts = HashMap::with_capacity(1);
        let mut req_transactions = HashMap::with_capacity(1);

        let ConfigGrpcRequestFilter {
            accounts,
            transactions,
        } = filter;

        if let Some(accounts) = accounts {
            req_accounts.insert(label.clone(), accounts.to_proto());
        }

        if let Some(transactions) = transactions {
            req_transactions.insert(label.clone(), transactions.to_proto());
        }

        let request = SubscribeRequest {
            accounts: req_accounts,
            transactions: req_transactions,
            ..Default::default()
        };

        let mut dragon_mouth_client =
                    GeyserGrpcClient::build_from_shared(config.geyser.endpoint.clone())?
                        .x_token(config.geyser.x_token.clone())?
                        .connect_timeout(Duration::from_secs(config.geyser.connect_timeout))
                        .timeout(Duration::from_secs(config.geyser.timeout ))
                        .connect()
                        .await
                        .context("failed to connect to gRPC").map_err(|err| {
                            error!(target: "grpc2redis", action = "grpc_connection_failed", message = "Failed to connect to gRPC", ?err);
                            err
                        })?;

        let (mut subscribe_tx, stream) = dragon_mouth_client
                    .subscribe_with_request(Some(request))
                    .await.map_err(|err| {
                        error!(target: "grpc2redis", action = "subscribe_failed", message = "Failed to subscribe", ?err);
                        err
                    })?;

        let exec = Executor::builder(Nonblock(Tokio))
                    .max_concurrency(Some(stream_config.max_concurrency))
                    .build_async(GrpcJobHandler {
                        stream_config: Arc::clone(&stream_config),
                        connection: connection.clone(),
                        pipe: Arc::clone(&pipe),
                        label: label.clone(),
                    }).map_err(|err| {
                        warn!(target: "grpc2redis", action = "executor_failed", message = "Failed to create executor", ?err);
                        err
                    })?;

        let deadline_config = Arc::clone(&config);

        let control = tokio::spawn(async move {
            tokio::pin!(stream);
            loop {
                tokio::select! {
                    _ = sleep(deadline_config.redis.pipeline_max_idle) => {
                        exec.push(GrpcJob::FlushRedisPipe);
                    }
                    Some(Ok(msg)) = stream.next() => {
                        match msg.update_oneof {
                            Some(UpdateOneof::Account(_)) | Some(UpdateOneof::Transaction(_)) => {
                                exec.push(GrpcJob::ProcessSubscribeUpdate(Box::new(msg)));
                            }
                            Some(UpdateOneof::Ping(_)) => {
                                let ping = subscribe_tx
                                    .send(SubscribeRequest {
                                        ping: Some(SubscribeRequestPing { id: PING_ID }),
                                        ..Default::default()
                                    })
                                    .await;

                                match ping {
                                    Ok(_) => {
                                        debug!(target: "grpc2redis", action = "send_ping", message = "Ping sent successfully", id = PING_ID)
                                    }
                                    Err(err) => {
                                        warn!(target: "grpc2redis", action = "send_ping_failed", message = "Failed to send ping", ?err, id = PING_ID)
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
                            _ => {
                                warn!(target: "grpc2redis", action = "unknown_update_variant", message = "Unknown update variant", ?msg.update_oneof)
                            }
                        }
                    }
                    _ = &mut shutdown_rx => {
                        debug!(target: "grpc2redis", action = "shutdown_signal_received", message = "Shutdown signal received, stopping subscription task", ?label);
                        exec.push(GrpcJob::FlushRedisPipe);
                        break;
                    }
                }
            }

            exec.join_async().await;
        });

        Ok(SubscriptionTaskStop {
            shutdown_tx,
            control,
        })
    }
}

#[derive(Debug)]
pub struct SubscriptionTaskStop {
    pub shutdown_tx: tokio::sync::oneshot::Sender<()>,
    pub control: tokio::task::JoinHandle<()>,
}

impl SubscriptionTaskStop {
    pub async fn stop(self) -> anyhow::Result<()> {
        self.shutdown_tx
            .send(())
            .map_err(|_| anyhow::anyhow!("Failed to send shutdown signal"))?;

        self.control.await?;

        Ok(())
    }
}
