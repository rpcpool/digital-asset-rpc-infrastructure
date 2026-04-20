use crate::{config::DasFumaroleConfig, prom};
use bytesize::ByteSize;
use solana_sdk::{bs58, pubkey::Pubkey};
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
        commitment: Some(CommitmentLevel::Finalized as i32),
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

        let mut fumarole_accounts_map: HashMap<(Vec<u8>, Option<Vec<u8>>), FetcherAccount> =
            HashMap::new();

        while let Some((source, update)) = rx.recv().await {
            let mut slot = None;

            match update.update_oneof {
                Some(UpdateOneof::Account(account)) => {
                    match account.account {
                        Some(account_data) => {
                            let program_owner = match Pubkey::try_from(
                                account_data.owner.as_slice(),
                            ) {
                                Ok(owner) => owner.to_string(),
                                Err(_) => {
                                    tracing::error!(target: "account_not_found_in_grpc_owner_conversion_failed", "Owner conversion failed: {:?}", account_data.owner);
                                    "unknown".to_string()
                                }
                            };

                            match source {
                                FumaroleCheckerSource::Grpc => {
                                    prom::DISCRIMINATED_UPDATES_COUNT
                                        .with_label_values(&["grpc", "account", &program_owner])
                                        .inc();

                                    slot = Some(account.slot);

                                    // If GRPC, save into hashmap
                                    accounts_map.insert(
                                        (
                                            account_data.pubkey.clone(),
                                            account_data.txn_signature.clone(),
                                        ),
                                        FetcherAccount {
                                            slot: account.slot,
                                            times_visited: 0,
                                        },
                                    );

                                    // If the data is already in the fumarole buffer, means that fumarole arrived earlier than grpc
                                    let fumarole_data = fumarole_accounts_map.get_mut(&(
                                        account_data.pubkey,
                                        account_data.txn_signature,
                                    ));

                                    if let Some(fumarole_account) = fumarole_data {
                                        fumarole_account.times_visited += 1;

                                        // increment counter
                                        prom::ACCOUNT_NOT_FOUND_IN_GRPC_COUNT
                                            .with_label_values(&["fumarole_arrived_earlier", "all"])
                                            .inc();
                                    }
                                }
                                FumaroleCheckerSource::Fumarole => {
                                    prom::DISCRIMINATED_UPDATES_COUNT
                                        .with_label_values(&["fumarole", "account", &program_owner])
                                        .inc();

                                    let grpc_data = accounts_map.get_mut(&(
                                        account_data.pubkey.clone(),
                                        account_data.txn_signature.clone(),
                                    ));

                                    if let Some(fetcher_account) = grpc_data {
                                        // Check for potential duplicated updates from fumarole
                                        if fetcher_account.times_visited > 0 {
                                            prom::DUPLICATED_FUMAROLE_ACCOUNT_COUNT
                                                .with_label_values(&[&program_owner])
                                                .inc();
                                        }

                                        fetcher_account.times_visited += 1;

                                        if fetcher_account.slot != account.slot {
                                            // increment counter error (account not found in grpc)
                                            prom::ACCOUNT_NOT_FOUND_IN_GRPC_COUNT
                                                .with_label_values(&["slot_mismatch", "all"])
                                                .inc();

                                            let tx_sig = match &account_data.txn_signature {
                                                Some(sig) => bs58::encode(sig).into_string(),
                                                None => {
                                                    tracing::error!(target: "account_not_found_in_grpc_slot_mismatch-unknown", "Account slot mismatch: {:?}", bs58::encode(&account_data.pubkey).into_string());
                                                    "".to_string()
                                                }
                                            };
                                            let msg = format!(
                                                "Account slot mismatch: {:?} - tx_sig: {:?} - slot: {:?} - program_owner: {:?}",
                                                bs58::encode(&account_data.pubkey).into_string(),
                                                tx_sig,
                                                account.slot,
                                                program_owner
                                            );

                                            tracing::error!(target: "account_not_found_in_grpc_slot_mismatch", msg)
                                        }
                                    } else {
                                        // Discriminate logs by program owner
                                        let program_owner = match Pubkey::try_from(
                                            account_data.owner.as_slice(),
                                        ) {
                                            Ok(owner) => owner.to_string(),
                                            Err(_) => {
                                                tracing::error!(target: "account_not_found_in_grpc_owner_conversion_failed", "Owner conversion failed: {:?}", account_data.owner);
                                                "unknown".to_string()
                                            }
                                        };

                                        // increment counter error (account not found in grpc)
                                        prom::ACCOUNT_NOT_FOUND_IN_GRPC_COUNT
                                            .with_label_values(&["not_found", &program_owner])
                                            .inc();

                                        let tx_sig = match &account_data.txn_signature {
                                            Some(sig) => bs58::encode(sig).into_string(),
                                            None => {
                                                tracing::error!(target: "account_not_found_in_grpc_not_found-unknown", "Account not found in grpc: {:?}", bs58::encode(&account_data.pubkey).into_string());
                                                "".to_string()
                                            }
                                        };
                                        let msg = format!(
                                            "Account not found in grpc: {:?} - tx_sig: {:?} - slot: {:?} - program_owner: {:?}",
                                            bs58::encode(&account_data.pubkey).into_string(),
                                            tx_sig,
                                            account.slot,
                                            program_owner
                                        );

                                        match program_owner.as_str() {
                                            "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s" => {
                                                tracing::error!(target: "account_not_found_in_grpc_not_found-metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s", msg)
                                            }
                                            "inscokhJarcjaEs59QbQ7hYjrKz25LEPRfCbP8EmdUp" => {
                                                tracing::error!(target: "account_not_found_in_grpc_not_found-inscokhJarcjaEs59QbQ7hYjrKz25LEPRfCbP8EmdUp", msg)
                                            }
                                            "CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d" => {
                                                tracing::error!(target: "account_not_found_in_grpc_not_found-CoREENxT6tW1HoK8ypY1SxRMZTcVPm7R94rH4PZNhX7d", msg)
                                            }
                                            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" => {
                                                tracing::error!(target: "account_not_found_in_grpc_not_found-TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", msg)
                                            }
                                            "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb" => {
                                                tracing::error!(target: "account_not_found_in_grpc_not_found-TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb", msg)
                                            }
                                            "11111111111111111111111111111111" => {
                                                tracing::error!(target: "account_not_found_in_grpc_not_found-11111111111111111111111111111111", msg)
                                            }
                                            _ => {
                                                tracing::error!(target: "account_not_found_in_grpc_not_found-unknown", msg)
                                            }
                                        }

                                        // We push it also to the fumarole buffer (to cover the case where fumarole arrives earlier than grpc)
                                        fumarole_accounts_map.insert(
                                            (
                                                account_data.pubkey.clone(),
                                                account_data.txn_signature.clone(),
                                            ),
                                            FetcherAccount {
                                                slot: account.slot,
                                                times_visited: 0,
                                            },
                                        );
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
                                    prom::DISCRIMINATED_UPDATES_COUNT
                                        .with_label_values(&["grpc", "transaction", "all"])
                                        .inc();

                                    slot = Some(transaction.slot);

                                    // If GRPC, save into hashmap
                                    txs_map.insert(txn.signature, transaction.slot);
                                }
                                FumaroleCheckerSource::Fumarole => {
                                    prom::DISCRIMINATED_UPDATES_COUNT
                                        .with_label_values(&["fumarole", "transaction", "all"])
                                        .inc();

                                    let grpc_data = txs_map.remove(&txn.signature);
                                    if let Some(grpc_slot) = grpc_data {
                                        if grpc_slot != transaction.slot {
                                            // increment counter error (tx not found in grpc)
                                            prom::TX_NOT_FOUND_IN_GRPC_COUNT
                                                .with_label_values(&["slot_mismatch"])
                                                .inc();

                                            tracing::error!(target: "tx_not_found_in_grpc_slot_mismatch", "Transaction slot mismatch: {:?}", bs58::encode(&txn.signature).into_string());
                                        }
                                    } else {
                                        // increment counter error (tx not found in grpc)
                                        prom::TX_NOT_FOUND_IN_GRPC_COUNT
                                            .with_label_values(&["not_found"])
                                            .inc();

                                        tracing::error!(target: "tx_not_found_in_grpc_not_found", "Transaction not found in grpc: {:?}", bs58::encode(&txn.signature).into_string());
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
                if slot > last_clean_slot + 200 {
                    last_clean_slot = slot;
                    let mut deleted_accounts = 0; // accounts not found in fumarole
                    let mut deleted_txs = 0; // txs not found in fumarole

                    accounts_map.retain(|(pubkey, tx_sig), account_update| {
                        if account_update.slot < slot - 200 {
                            let pubkey = bs58::encode(pubkey).into_string();
                            let tx_sig = bs58::encode(tx_sig.clone().unwrap_or_default()).into_string();
                            tracing::error!(target: "account_not_found_in_fumarole", "Account not found in fumarole: {:?} - tx_sig: {:?}", pubkey, tx_sig);

                            deleted_accounts += 1;
                            false
                        } else {
                            true
                        }
                    });

                    txs_map.retain(|tx_sig, tx_slot| {
                        if *tx_slot < slot - 200 {
                            let tx_sig = bs58::encode(tx_sig).into_string();
                            tracing::error!(target: "tx_not_found_in_fumarole", "Transaction not found in fumarole: {:?}", tx_sig);

                            deleted_txs += 1;
                            false
                        } else {
                            true
                        }
                    });

                    // No checks, just to prevent the hashmap from growing indefinitely
                    fumarole_accounts_map
                        .retain(|_, account_update| account_update.slot >= slot - 200);

                    if deleted_accounts > 0 {
                        // TODO: With the new check for duplicated accounts updates, this is now incorrect (we are not removing accounts
                        //  from the hashmap any more)
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

struct FetcherAccount {
    pub slot: u64,
    pub times_visited: u8,
}
