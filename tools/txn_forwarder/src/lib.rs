use solana_client::client_error::ClientErrorKind;

use {
    anyhow::Context,
    futures::stream::{BoxStream, StreamExt},
    log::{debug, error, info},
    serde::de::DeserializeOwned,
    solana_client::{
        client_error::ClientError, client_error::Result as RpcClientResult,
        nonblocking::rpc_client::RpcClient, rpc_client::GetConfirmedSignaturesForAddress2Config,
        rpc_request::RpcError::RpcRequestError, rpc_request::RpcRequest,
    },
    solana_sdk::{
        pubkey::Pubkey,
        signature::{ParseSignatureError, Signature},
    },
    solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding},
    std::{fmt, io::Result as IoResult, str::FromStr},
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

pub async fn rpc_send_with_retries<E>(
    client: &RpcClient,
    request: RpcRequest,
    value: serde_json::Value,
    max_retries: u8,
    error_key: E,
) -> Result<EncodedConfirmedTransactionWithStatusMeta, ClientError>
where
    E: fmt::Debug,
{
    let mut retries = 0;
    let mut delay = Duration::from_millis(500);

    loop {
        let response = client.send(request.clone(), value.clone()).await;

        if let Err(error) = response {
            if retries < max_retries {
                error!("retrying {:?} {:?}: {:?}", request, error_key, error);
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

        if let Some(_) = tx.transaction.transaction.decode() {
            return Ok(tx);
        } else {
            if retries < max_retries {
                error!(
                    "retrying {:?} {:?}: Transaction could not be decoded",
                    request, error_key
                );
                sleep(delay).await;
                delay *= 2;
                retries += 1;
            } else {
                return Err(ClientError::from(RpcRequestError(
                    "Transaction could not be decoded".to_string(),
                )));
            }
        }
    }
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
