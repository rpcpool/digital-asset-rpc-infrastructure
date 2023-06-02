use {
    anyhow::Context,
    futures::stream::{BoxStream, StreamExt},
    log::{debug, error, info},
    plerkle_messenger::TRANSACTION_STREAM,
    plerkle_serialization::serializer::seralize_encoded_transaction_with_status,
    solana_client::{
        client_error::ClientError, nonblocking::rpc_client::RpcClient,
        rpc_client::GetConfirmedSignaturesForAddress2Config,
        rpc_request::RpcError::RpcRequestError, rpc_request::RpcRequest,
    },
    solana_sdk::{
        pubkey::Pubkey,
        signature::{ParseSignatureError, Signature},
    },
    solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta,
    std::sync::Arc,
    std::{fmt, io::Result as IoResult, str::FromStr},
    tokio::sync::Mutex,
    tokio::{
        fs::File,
        io::{stdin, AsyncBufReadExt, BufReader},
        sync::mpsc,
        time::{sleep, Duration},
    },
    tokio_stream::wrappers::LinesStream,
};

#[derive(Debug, thiserror::Error)]
pub enum FindSignaturesError {
    #[error("Failed to fetch signatures: {0}")]
    Fetch(#[from] ClientError),
    #[error("Failed to parse signature: {0}")]
    Parse(#[from] ParseSignatureError),
}

pub fn find_signatures(
    address: Pubkey,
    client: RpcClient,
    before: Option<Signature>,
    after: Option<Signature>,
    buffer: usize,
    replay_forward: bool,
) -> mpsc::Receiver<Result<Signature, FindSignaturesError>> {
    let (chan, rx) = mpsc::channel(buffer);
    tokio::spawn(async move {
        let mut last_signature = before;
        let mut all_signatures: Vec<Signature> = Vec::new();

        loop {
            debug!(
                "fetching signatures for {} before {:?}",
                address, last_signature
            );
            let config = GetConfirmedSignaturesForAddress2Config {
                before: last_signature,
                until: after,
                ..Default::default()
            };

            let batch = match client
                .get_signatures_for_address_with_config(&address, config)
                .await
            {
                Ok(vec) => {
                    info!(
                        "fetched {} signatures for address {:?} before {:?}",
                        vec.len(),
                        address,
                        last_signature
                    );
                    vec
                }
                Err(error) => {
                    chan.send(Err(error.into())).await.map_err(|_| ())?;
                    break;
                }
            };

            // Collect all the signatures in the batch
            let signatures: Vec<Signature> = batch
                .into_iter()
                .filter_map(|tx| Signature::from_str(&tx.signature).ok())
                .collect();

            if signatures.is_empty() {
                break;
            }

            last_signature = signatures.last().cloned();
            if replay_forward {
                all_signatures.extend(signatures);
            } else {
                for signature in signatures.into_iter() {
                    chan.send(Ok(signature)).await.map_err(|_| ())?;
                }
            }
        }
        info!(
            "sending {} signatures for address {:?}",
            all_signatures.len(),
            address
        );

        if replay_forward {
            for signature in all_signatures.into_iter().rev() {
                chan.send(Ok(signature)).await.map_err(|_| ())?;
            }
        }

        Ok::<(), ()>(())
    });

    rx
}

pub async fn rpc_send_with_retries(
    client: &RpcClient,
    request: RpcRequest,
    value: serde_json::Value,
    max_retries: u8,
    messenger: Arc<Mutex<Box<dyn plerkle_messenger::Messenger>>>,
    signature: Signature,
) -> Result<(), ClientError> {
    let mut retries = 0;
    let mut delay = Duration::from_millis(500);

    loop {
        let response = client.send(request.clone(), value.clone()).await;

        if let Err(error) = response {
            if retries < max_retries {
                error!("retrying {:?} {:?}: {:?}", request, signature, error);
                sleep(delay).await;
                delay *= 2;
                retries += 1;
                continue;
            } else {
                return Err(error);
            }
        }
        let value = response.unwrap();
        let tx: EncodedConfirmedTransactionWithStatusMeta = value;
        match send(signature, tx, Arc::clone(&messenger)).await {
            Ok(_) => return Ok(()),
            Err(e) => {
                if retries < max_retries {
                    error!(
                        "retrying {:?} {:?}: Transaction could not be sent: {:?}",
                        request, signature, e
                    );
                    sleep(delay).await;
                    delay *= 2;
                    retries += 1;
                } else {
                    return Err(ClientError::from(RpcRequestError(format!(
                        "Transaction could not be decoded: {}",
                        e
                    ))));
                }
                continue;
            }
        }
    }
}

async fn send(
    signature: Signature,
    tx: EncodedConfirmedTransactionWithStatusMeta,
    messenger: Arc<Mutex<Box<dyn plerkle_messenger::Messenger>>>,
) -> anyhow::Result<()> {
    // Ignore if tx failed or meta is missed
    let meta = tx.transaction.meta.as_ref();
    if meta.map(|meta| meta.status.is_err()).unwrap_or(true) {
        return Ok(());
    }

    let fbb = flatbuffers::FlatBufferBuilder::new();
    let fbb = seralize_encoded_transaction_with_status(fbb, tx)
        .with_context(|| format!("failed to serialize transaction with {}", signature))?;
    let bytes = fbb.finished_data();

    let mut locked = messenger.lock().await;
    locked.send(TRANSACTION_STREAM, bytes).await?;
    info!("Sent transaction to stream {}", signature);

    Ok(())
}

pub async fn read_lines(path: &str) -> anyhow::Result<BoxStream<'static, IoResult<String>>> {
    Ok(if path == "-" {
        LinesStream::new(BufReader::new(stdin()).lines()).boxed()
    } else {
        let file = File::open(path)
            .await
            .with_context(|| format!("failed to read file: {:?}", path))?;
        LinesStream::new(BufReader::new(file).lines()).boxed()
    }
    .filter_map(|line| async move {
        match line {
            Ok(line) => {
                let line = line.trim();
                (!line.is_empty()).then(|| Ok(line.to_string()))
            }
            Err(error) => Some(Err(error)),
        }
    })
    .boxed())
}
