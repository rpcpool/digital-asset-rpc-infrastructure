use {
    solana_client::{
        client_error::ClientError, nonblocking::rpc_client::RpcClient,
        rpc_client::GetConfirmedSignaturesForAddress2Config,
    },
    solana_sdk::{
        pubkey::Pubkey,
        signature::{ParseSignatureError, Signature},
    },
    std::str::FromStr,
    tokio::sync::mpsc,
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
) -> mpsc::UnboundedReceiver<Result<Signature, FindSignaturesError>> {
    let (chan, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let mut last_signature = None;
        loop {
            let config = GetConfirmedSignaturesForAddress2Config {
                before: last_signature,
                until: None,
                ..Default::default()
            };
            match client
                .get_signatures_for_address_with_config(&address, config)
                .await
            {
                Ok(vec) => {
                    for tx in vec {
                        match Signature::from_str(&tx.signature) {
                            Ok(signature) => {
                                last_signature = Some(signature);
                                if tx.confirmation_status.is_some() && tx.err.is_none() {
                                    let _ = chan.send(Ok(signature));
                                }
                            }
                            Err(error) => {
                                let _ = chan.send(Err(error.into()));
                                break;
                            }
                        }
                    }
                }
                Err(error) => {
                    let _ = chan.send(Err(error.into()));
                    break;
                }
            }
        }
    });
    rx
}
