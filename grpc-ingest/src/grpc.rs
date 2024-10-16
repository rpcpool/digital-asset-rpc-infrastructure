use {
    crate::{
        config::{ConfigGrpc, ConfigGrpcRequestFilter, StreamConfig, SubscriptionConfig},
        prom::{
            grpc_subscription_task_inc, grpc_tasks_total_dec, grpc_tasks_total_inc,
            redis_xadd_status_inc,
        },
        redis::TrackedPipeline,
        util::create_shutdown,
    },
    anyhow::Context,
    futures::{stream::StreamExt, SinkExt},
    redis::streams::StreamMaxlen,
    std::{collections::HashMap, sync::Arc, time::Duration},
    tokio::{sync::Mutex, task::JoinHandle, time::sleep},
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
    stream_config: Arc<StreamConfig>,
    pipe: Arc<Mutex<TrackedPipeline>>,
    subscription_label: String,
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
        let subscription_label = self.subscription_label.clone();

        grpc_tasks_total_inc();

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
                                debug!(target: "grpc2redis", action = "process_account_update", stream = ?stream, maxlen = ?stream_maxlen);
                                grpc_subscription_task_inc(
                                    &subscription_label,
                                    &stream.to_string(),
                                );
                            }

                            UpdateOneof::Transaction(transaction) => {
                                pipe.xadd_maxlen(
                                    &stream.to_string(),
                                    StreamMaxlen::Approx(stream_maxlen),
                                    "*",
                                    transaction.encode_to_vec(),
                                );
                                debug!(target: "grpc2redis", action = "process_transaction_update", stream = ?stream, maxlen = ?stream_maxlen);
                                grpc_subscription_task_inc(
                                    &subscription_label,
                                    &stream.to_string(),
                                );
                            }
                            _ => {
                                warn!(target: "grpc2redis", action = "unknown_update_variant", message = "Unknown update variant")
                            }
                        }
                    }
                }
            }

            grpc_tasks_total_dec();
        }
    }
}

pub async fn run(config: ConfigGrpc) -> anyhow::Result<()> {
    let redis_client = redis::Client::open(config.redis.url.clone())?;
    let config = Arc::new(config);
    let connection = redis_client.get_multiplexed_tokio_connection().await?;

    let pipe = Arc::new(Mutex::new(TrackedPipeline::default()));

    let subscriptions = config.subscriptions.clone();

    let mut subscription_tasks: Vec<JoinHandle<Result<(), anyhow::Error>>> =
        Vec::with_capacity(subscriptions.len());

    for subscription in subscriptions {
        let subscription_label = subscription.0.clone();
        let subscription_config = Arc::new(subscription.1);
        let config = Arc::clone(&config);
        let connection = connection.clone();
        let pipe = Arc::clone(&pipe);
        let subscription_task_handle: JoinHandle<Result<(), anyhow::Error>> = tokio::spawn(
            async move {
                let mut shutdown = create_shutdown()?;
                let SubscriptionConfig { stream, filter } = subscription_config.as_ref();

                let stream_config = Arc::new(stream.clone());
                let mut accounts = HashMap::with_capacity(1);
                let mut transactions = HashMap::with_capacity(1);

                match filter {
                    ConfigGrpcRequestFilter::Accounts(accounts_filter) => {
                        accounts.insert(
                            subscription_label.clone(),
                            accounts_filter.clone().to_proto(),
                        );
                    }
                    ConfigGrpcRequestFilter::Transactions(transactions_filter) => {
                        transactions.insert(
                            subscription_label.clone(),
                            transactions_filter.clone().to_proto(),
                        );
                    }
                }

                let request = SubscribeRequest {
                    accounts,
                    transactions,
                    ..Default::default()
                };

                let mut dragon_mouth_client =
                    GeyserGrpcClient::build_from_shared(config.geyser_endpoint.clone())?
                        .x_token(config.x_token.clone())?
                        .connect_timeout(Duration::from_secs(10))
                        .timeout(Duration::from_secs(10))
                        .connect()
                        .await
                        .context("failed to connect to gRPC")?;

                let (mut subscribe_tx, stream) = dragon_mouth_client
                    .subscribe_with_request(Some(request))
                    .await?;

                tokio::pin!(stream);

                let exec = Executor::builder(Nonblock(Tokio))
                    .max_concurrency(Some(config.max_concurrency))
                    .build_async(GrpcJobHandler {
                        stream_config: Arc::clone(&stream_config),
                        connection: connection.clone(),
                        pipe: Arc::clone(&pipe),
                        subscription_label: subscription_label.clone(),
                    })?;

                let deadline_config = Arc::clone(&config);

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
                        _ = shutdown.next() => {
                            exec.push(GrpcJob::FlushRedisPipe);
                            break;
                        }
                    }
                }

                exec.join_async().await;

                Ok(())
            },
        );

        subscription_tasks.push(subscription_task_handle);
    }

    // Wait for all subscription tasks to finish
    let task_results = futures::future::join_all(subscription_tasks).await;

    for task_result in task_results {
        if let Err(e) = task_result {
            error!(target: "grpc2redis", action = "subscription_task_error", message = "Subscription task failed", ?e);
        } else if let Ok(Err(e)) = task_result {
            error!(target: "grpc2redis", action = "subscription_task_error", message = "Subscription task failed", ?e);
        }
    }

    Ok(())
}
