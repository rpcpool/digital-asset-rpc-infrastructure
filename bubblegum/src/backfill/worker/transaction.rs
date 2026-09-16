use crate::{error::ErrorKind, BubblegumContext};
use anyhow::Result;
use clap::Parser;
use das_core::Rpc;
use futures::{stream::FuturesUnordered, StreamExt};
use log::error;
use program_transformers::TransactionInfo;
use solana_message::compiled_instruction::CompiledInstruction;
use solana_program::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::VersionedTransaction;
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedConfirmedTransactionWithStatusMeta,
    InnerInstruction, InnerInstructions, UiInstruction,
};
use tokio::{
    sync::mpsc::{channel, Sender},
    task::JoinHandle,
};

pub struct PubkeyString(pub String);

impl TryFrom<PubkeyString> for Pubkey {
    type Error = ErrorKind;

    fn try_from(value: PubkeyString) -> Result<Self, Self::Error> {
        let decoded_bytes = bs58::decode(value.0)
            .into_vec()
            .map_err(|e| ErrorKind::Generic(e.to_string()))?;

        Pubkey::try_from(decoded_bytes)
            .map_err(|_| ErrorKind::Generic("unable to convert pubkey".to_string()))
    }
}

#[derive(Debug)]
pub struct FetchedEncodedTransactionWithStatusMeta(pub EncodedConfirmedTransactionWithStatusMeta);

impl TryFrom<FetchedEncodedTransactionWithStatusMeta> for TransactionInfo {
    type Error = ErrorKind;

    fn try_from(
        fetched_transaction: FetchedEncodedTransactionWithStatusMeta,
    ) -> Result<Self, Self::Error> {
        let mut account_keys = Vec::new();
        let encoded_transaction_with_status_meta = fetched_transaction.0;

        let ui_transaction: VersionedTransaction = encoded_transaction_with_status_meta
            .transaction
            .transaction
            .decode()
            .ok_or(ErrorKind::Generic(
                "unable to decode transaction".to_string(),
            ))?;

        let signature = ui_transaction.signatures[0];

        let msg = ui_transaction.message;

        let meta = encoded_transaction_with_status_meta
            .transaction
            .meta
            .ok_or(ErrorKind::Generic(
                "transaction metadata is missing".to_string(),
            ))?;

        for address in msg.static_account_keys().iter().copied() {
            account_keys.push(address);
        }

        // Only a message that declares lookup tables has loaded addresses. v1
        // has none, so requiring them unconditionally would reject valid input.
        if msg.address_table_lookups().is_some() {
            let ui_loaded_addresses = match meta.loaded_addresses {
                OptionSerializer::Some(addresses) => addresses,
                OptionSerializer::None => {
                    return Err(ErrorKind::Generic(
                        "loaded addresses data is missing".to_string(),
                    ))
                }
                OptionSerializer::Skip => {
                    return Err(ErrorKind::Generic(
                        "loaded addresses are skipped".to_string(),
                    ));
                }
            };

            for address in ui_loaded_addresses.writable {
                account_keys.push(PubkeyString(address).try_into()?);
            }

            for address in ui_loaded_addresses.readonly {
                account_keys.push(PubkeyString(address).try_into()?);
            }
        }

        let mut meta_inner_instructions = Vec::new();

        if let OptionSerializer::Some(inner_instructions) = meta.inner_instructions {
            for ix in inner_instructions {
                let mut instructions = Vec::new();

                for inner in ix.instructions {
                    if let UiInstruction::Compiled(compiled) = inner {
                        instructions.push(InnerInstruction {
                            stack_height: compiled.stack_height,
                            instruction: CompiledInstruction {
                                program_id_index: compiled.program_id_index,
                                accounts: compiled.accounts,
                                data: bs58::decode(compiled.data).into_vec().map_err(|e| {
                                    ErrorKind::Generic(format!("Error decoding data: {}", e))
                                })?,
                            },
                        });
                    }
                }

                meta_inner_instructions.push(InnerInstructions {
                    index: ix.index,
                    instructions,
                });
            }
        }

        Ok(Self {
            slot: encoded_transaction_with_status_meta.slot,
            account_keys,
            signature,
            message_instructions: msg.instructions().to_vec(),
            meta_inner_instructions,
        })
    }
}

#[derive(Parser, Clone, Debug)]
pub struct SignatureWorkerArgs {
    /// The size of the signature channel.
    #[arg(long, env, default_value = "100000")]
    pub signature_channel_size: usize,
    /// The number of transaction workers.
    #[arg(long, env, default_value = "50")]
    pub signature_worker_count: usize,
}

type TransactionSender = Sender<TransactionInfo>;

impl SignatureWorkerArgs {
    pub fn start(
        &self,
        context: BubblegumContext,
        forwarder: TransactionSender,
    ) -> (JoinHandle<()>, Sender<Signature>) {
        let (sig_sender, mut sig_receiver) = channel::<Signature>(self.signature_channel_size);
        let worker_count = self.signature_worker_count;

        let handle = tokio::spawn(async move {
            let mut handlers = FuturesUnordered::new();

            while let Some(signature) = sig_receiver.recv().await {
                if handlers.len() >= worker_count {
                    handlers.next().await;
                }

                let solana_rpc = context.solana_rpc.clone();
                let transaction_sender = forwarder.clone();

                let handle = spawn_transaction_worker(solana_rpc, transaction_sender, signature);

                handlers.push(handle);
            }

            futures::future::join_all(handlers).await;
        });

        (handle, sig_sender)
    }
}

async fn queue_transaction(
    client: Rpc,
    sender: Sender<TransactionInfo>,
    signature: Signature,
) -> Result<(), ErrorKind> {
    let transaction = client.get_transaction(&signature).await?;

    crate::metrics::BUBBLEGUM_RPC_GET_TRANSACTION_COUNT.inc();

    sender
        .send(FetchedEncodedTransactionWithStatusMeta(transaction).try_into()?)
        .await
        .map_err(|e| ErrorKind::Generic(e.to_string()))?;

    Ok(())
}

fn spawn_transaction_worker(
    client: Rpc,
    sender: Sender<TransactionInfo>,
    signature: Signature,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(e) = queue_transaction(client, sender, signature).await {
            error!("queue transaction: {:?}", e);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::message::VersionedMessage;

    // Shared with the das-core v1 test: a real devnet v1 transaction.
    const V1_FIXTURE: &str = include_str!("../../../../core/tests/fixtures/transaction_v1.json");

    /// Hits devnet, where transaction v1 is active. Run with:
    /// `cargo test -p das-bubblegum --lib -- --ignored --nocapture`
    #[tokio::test]
    #[ignore = "requires network access to devnet"]
    async fn fetches_and_converts_a_live_v1_transaction() {
        let rpc = Rpc::from_config(&das_core::SolanaRpcArgs {
            solana_rpc_url: "https://api.devnet.solana.com".to_string(),
        });

        let signature: Signature = "5fw6J243j6796Jf7KryyRuw4jkvbioX39gjc4dWW98zNQRTDAcAESMsusX41qtozNZCmkWm2GGmxbKLqJfo6njcR"
            .parse()
            .expect("signature parses");

        let fetched = rpc
            .get_transaction(&signature)
            .await
            .expect("devnet returns the v1 transaction");

        let decoded = fetched
            .transaction
            .transaction
            .decode()
            .expect("live v1 transaction decodes");
        assert!(
            matches!(decoded.message, VersionedMessage::V1(_)),
            "devnet returned a v1 message"
        );

        let info: TransactionInfo = FetchedEncodedTransactionWithStatusMeta(fetched)
            .try_into()
            .expect("live v1 transaction converts");

        println!(
            "devnet v1 tx: slot={} account_keys={} instructions={}",
            info.slot,
            info.account_keys.len(),
            info.message_instructions.len()
        );
        assert_eq!(info.signature, signature);
        assert_eq!(info.account_keys.len(), 2);
        assert_eq!(info.message_instructions.len(), 1);
    }

    #[test]
    fn builds_transaction_info_from_a_v1_transaction() {
        let fetched: EncodedConfirmedTransactionWithStatusMeta =
            serde_json::from_str(V1_FIXTURE).expect("fixture parses");
        let slot = fetched.slot;

        let info: TransactionInfo = FetchedEncodedTransactionWithStatusMeta(fetched)
            .try_into()
            .expect("v1 transaction converts");

        assert_eq!(info.slot, slot);
        // v1 has no lookup tables, so the key list is exactly the static keys.
        assert_eq!(info.account_keys.len(), 2);
        assert_eq!(info.message_instructions.len(), 1);
    }
}
