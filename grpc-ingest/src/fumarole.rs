use crate::{config::DasFumaroleConfig, prom};
use bytesize::ByteSize;
use std::{collections::HashMap, num::NonZero};
use tokio::{sync::mpsc::Sender, task::JoinHandle};
use yellowstone_fumarole_client::{
    proto::{
        CreateConsumerGroupRequest, DeleteConsumerGroupRequest, InitialOffsetPolicy,
        ListConsumerGroupsRequest,
    },
    DragonsmouthAdapterSession, FumaroleClient, FumaroleSubscribeConfig,
};
use yellowstone_grpc_proto::geyser::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterTransactions, SubscribeUpdate,
};
use yellowstone_grpc_proto::tonic::codec::CompressionEncoding;

/// The returned JoinHandle will return an error if the fumarole connection is lost or
///  if the update is not sent to the buffer.
pub async fn connect(
    das_fumarole_config: DasFumaroleConfig,
    tx: Sender<SubscribeUpdate>,
) -> anyhow::Result<(JoinHandle<anyhow::Result<()>>, Sender<SubscribeRequest>)> {
    prom::FUMAROLE_CONNECT_COUNT.inc();

    let config = yellowstone_fumarole_client::config::FumaroleConfig {
        endpoint: das_fumarole_config.fumarole_endpoint,
        x_token: das_fumarole_config.fumarole_x_token,
        max_decoding_message_size_bytes: usize::MAX,
        x_metadata: [(
            "x-subscription-id".to_string(),
            das_fumarole_config.x_subscription_id,
        )]
        .into(),
        response_compression: Some(CompressionEncoding::Gzip),
        request_compression: Some(CompressionEncoding::Gzip),
        initial_connection_window_size: ByteSize::mb(100),
        initial_stream_window_size: ByteSize::mib(9),
        enable_http2_adaptive_window: true,
    };

    let mut fumarole_client = FumaroleClient::connect(config)
        .await
        .expect("failing to connect to fumarole");

    let consumer_group_list = fumarole_client
        .list_consumer_groups(ListConsumerGroupsRequest {})
        .await
        .expect("failing to list consumer groups");

    let mut consumer_group_exists = false;

    for consumer_group in consumer_group_list.into_inner().consumer_groups {
        if consumer_group.consumer_group_name == das_fumarole_config.consumer_group_name {
            // If the consumer group is stale, we need to delete it and create it again
            if consumer_group.is_stale {
                let delete_result = fumarole_client
                    .delete_consumer_group(DeleteConsumerGroupRequest {
                        consumer_group_name: das_fumarole_config.consumer_group_name.clone(),
                    })
                    .await;

                match delete_result {
                    Ok(_) => (),
                    Err(status) => {
                        tracing::error!("Failed to delete consumer group: {status:?}");
                        return Err(anyhow::anyhow!(
                            "Failed to delete consumer group: {status:?}"
                        ));
                    }
                }

                prom::FUMAROLE_GROUP_STALE_COUNT.inc();
            } else {
                consumer_group_exists = true;
                break;
            }
        }
    }

    if !consumer_group_exists {
        prom::FUMAROLE_GROUP_NOT_FOUND_COUNT.inc();

        let group_result = fumarole_client
            .create_consumer_group(CreateConsumerGroupRequest {
                consumer_group_name: das_fumarole_config.consumer_group_name.clone(),
                initial_offset_policy: InitialOffsetPolicy::Latest as i32, // InitialOffsetPolicy::FromSlot
                // If the initial offset policy is "from-slot", this is the slot to start from.
                // If not specified, the subscriber will start from the latest slot.
                from_slot: None,
            })
            .await;

        match group_result {
            Ok(_) => (),
            Err(status) => {
                tracing::error!("Failed to create consumer group: {status:?}");
                return Err(anyhow::anyhow!(
                    "Failed to create consumer group: {status:?}"
                ));
            }
        }
    }

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
        commitment: Some(CommitmentLevel::Confirmed as i32),
        blocks_meta: [(
            "blocks_meta".to_string(),
            SubscribeRequestFilterBlocksMeta {},
        )]
        .into(),
        ..Default::default()
    };

    const MAX_PARA_DATA_STREAMS: u8 = 4; //Fumarole const

    let dragonsmouth_session = fumarole_client
        .dragonsmouth_subscribe_with_config(
            das_fumarole_config.consumer_group_name,
            subscribe_request,
            FumaroleSubscribeConfig {
                num_data_plane_tcp_connections: NonZero::new(MAX_PARA_DATA_STREAMS).unwrap(),
                ..Default::default()
            },
        )
        .await
        .expect("failing to subscribe to fumarole");

    let DragonsmouthAdapterSession {
        sink,
        mut source,
        fumarole_handle: _,
    } = dragonsmouth_session;

    let handle: JoinHandle<Result<(), anyhow::Error>> = tokio::spawn(async move {
        while let Some(Ok(update)) = source.recv().await {
            let res = tx.send(update).await;
            if res.is_err() {
                tracing::error!("Failed to send update to buffer");
                break;
            }
        }

        Err(anyhow::anyhow!(
            "Failed to receive update from fumarole (None or Err)"
        ))
    });

    Ok((handle, sink))
}

pub enum FumaroleCheckerSource {
    Fumarole,
    Grpc,
}

pub fn fumarole_checker() -> Sender<(FumaroleCheckerSource, SubscribeUpdate)> {
    let (tx, mut rx) =
        tokio::sync::mpsc::channel::<(FumaroleCheckerSource, SubscribeUpdate)>(10_000);

    tokio::spawn(async move {
        let mut accounts_map = HashMap::new();
        let mut txs_map = HashMap::new();
        let mut last_clean_slot = 0;

        while let Some((source, update)) = rx.recv().await {
            let mut slot = None;

            // If fumarole, check if update is in hashmap (if not increment error)(if I see there are too many errors, I can try to make a buffer also for fumarole,
            //  in case they are arriving earlier for some reason)
            match update.update_oneof {
                Some(UpdateOneof::Account(account)) => {
                    match account.account {
                        Some(account_data) => {
                            match source {
                                FumaroleCheckerSource::Grpc => {
                                    slot = Some(account.slot);

                                    // If GRPC, save into hashmap
                                    accounts_map.insert(
                                        (account_data.pubkey, account_data.txn_signature),
                                        account.slot,
                                    );
                                }
                                FumaroleCheckerSource::Fumarole => {
                                    let grpc_data = accounts_map
                                        .remove(&(account_data.pubkey, account_data.txn_signature));
                                    if let Some(grpc_slot) = grpc_data {
                                        if grpc_slot != account.slot {
                                            // increment counter error (account not found in grpc)
                                            prom::ACCOUNT_NOT_FOUND_IN_GRPC_COUNT.inc();
                                        }
                                    } else {
                                        // increment counter error (account not found in grpc)
                                        prom::ACCOUNT_NOT_FOUND_IN_GRPC_COUNT.inc();
                                    }
                                }
                            }
                        }
                        None => tracing::error!("Received invalid account"),
                    }
                }
                Some(UpdateOneof::Transaction(transaction)) => {
                    match transaction.transaction {
                        Some(txn) => {
                            match source {
                                FumaroleCheckerSource::Grpc => {
                                    slot = Some(transaction.slot);

                                    // If GRPC, save into hashmap
                                    txs_map.insert(txn.signature, transaction.slot);
                                }
                                FumaroleCheckerSource::Fumarole => {
                                    let grpc_data = txs_map.remove(&txn.signature);
                                    if let Some(grpc_slot) = grpc_data {
                                        if grpc_slot != transaction.slot {
                                            // increment counter error (tx not found in grpc)
                                            prom::TX_NOT_FOUND_IN_GRPC_COUNT.inc();
                                        }
                                    } else {
                                        // increment counter error (tx not found in grpc)
                                        prom::TX_NOT_FOUND_IN_GRPC_COUNT.inc();
                                    }
                                }
                            }
                        }
                        None => tracing::error!("Received invalid transaction"),
                    }
                }
                _ => (),
            }

            //   - clean hashmap (older than 30 slots)
            //      - if new_slot - last_clean_slot > 30, clean hashmaps
            //      - increment errors for each deleted update
            if let Some(slot) = slot {
                if slot > last_clean_slot + 30 {
                    last_clean_slot = slot;
                    let mut deleted_accounts = 0; // accounts not found in fumarole
                    let mut deleted_txs = 0; // txs not found in fumarole

                    accounts_map.retain(|(_pubkey, _tx_sig), account_slot| {
                        if *account_slot < slot - 25 {
                            deleted_accounts += 1;
                            false
                        } else {
                            true
                        }
                    });

                    txs_map.retain(|_sig, tx_slot| {
                        if *tx_slot < slot - 25 {
                            deleted_txs += 1;
                            false
                        } else {
                            true
                        }
                    });

                    if deleted_accounts > 0 {
                        prom::ACCOUNT_NOT_FOUND_IN_FUMAROLE_COUNT.inc_by(deleted_accounts as f64);
                    }
                    if deleted_txs > 0 {
                        prom::TX_NOT_FOUND_IN_FUMAROLE_COUNT.inc_by(deleted_txs as f64);
                    }
                }
            }
        }
    });

    tx
}
