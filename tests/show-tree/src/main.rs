use {
    anchor_client::anchor_lang::AnchorDeserialize,
    anyhow::Context,
    clap::Parser,
    futures::future::try_join_all,
    solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcTransactionConfig},
    solana_sdk::{
        commitment_config::CommitmentConfig,
        pubkey::{ParsePubkeyError, Pubkey},
        signature::Signature,
        transaction::VersionedTransaction,
    },
    solana_transaction_status::{
        option_serializer::OptionSerializer, EncodedConfirmedTransactionWithStatusMeta,
        UiTransactionEncoding, UiTransactionStatusMeta,
    },
    spl_account_compression::{AccountCompressionEvent, ChangeLogEvent},
    std::{num::NonZeroUsize, str::FromStr, sync::Arc},
    tokio::{
        sync::Mutex,
        time::{sleep, Duration},
    },
    txn_forwarder::find_signatures,
};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    #[error("failed to load Transaction Meta")]
    TransactionMeta,
    #[error("failed to decode Transaction")]
    Transaction,
    #[error("failed to decode instruction data: {0}")]
    Instruction(#[from] bs58::decode::Error),
    #[error("failed to parse pubkey: {0}")]
    Pubkey(#[from] ParsePubkeyError),
}

#[derive(Parser)]
#[command(next_line_help = true)]
struct Args {
    /// Tree address.
    #[arg(long)]
    address: String,

    /// Solana RPC endpoint.
    #[arg(long, alias = "rpc-url")]
    rpc: String,

    /// Number of concurrent requests for fetching transactions.
    #[arg(long, short, default_value_t = 25)]
    concurrency: usize,

    /// Maximum number of retries for transaction fetching.
    #[arg(long, short, default_value_t = 3)]
    max_retries: u8,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let address = Pubkey::from_str(&args.address)
        .with_context(|| format!("failed to parse address: {}", args.address))?;
    let concurrency = NonZeroUsize::new(args.concurrency)
        .ok_or_else(|| anyhow::anyhow!("invalid concurrency: {}", args.concurrency))?;

    read_tree(address, args.rpc, concurrency, args.max_retries).await
}

// Fetches all the transactions referencing a specific trees
async fn read_tree(
    address: Pubkey,
    client_url: String,
    concurrency: NonZeroUsize,
    max_retries: u8,
) -> anyhow::Result<()> {
    let rx_sig = Arc::new(Mutex::new(find_signatures(
        address,
        RpcClient::new(client_url.clone()),
        2_000,
    )));

    try_join_all((0..concurrency.get()).map(|_| {
        let rx_sig = Arc::clone(&rx_sig);
        let client = RpcClient::new(client_url.clone());
        async move {
            loop {
                let mut lock = rx_sig.lock().await;
                let maybe_msg = lock.recv().await;
                drop(lock);
                match maybe_msg {
                    Some(maybe_sig) => {
                        if let Err(error) = process_txn(maybe_sig?, &client, max_retries).await {
                            eprintln!("{}", error);
                        }
                    }
                    None => return Ok(()),
                }
            }
        }
    }))
    .await
    .map(|_| ())
}

// Process and individual transaction, fetching it and reading out the sequence numbers
async fn process_txn(sig: Signature, client: &RpcClient, mut retries: u8) -> anyhow::Result<()> {
    let mut delay = Duration::from_millis(100);
    loop {
        let config = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };
        match client.get_transaction_with_config(&sig, config).await {
            Ok(tx) => {
                for seq in parse_txn_sequence(tx).await? {
                    println!("{} {}", seq, sig);
                }
                return Ok(());
            }
            Err(error) => {
                eprintln!(
                    "Retrying transaction {} retry no {}: {}",
                    sig, retries, error
                );
                anyhow::ensure!(retries > 0, "Failed to load transaction {}: {}", sig, error);
                sleep(delay).await;
                retries -= 1;
                delay *= 2;
            }
        }
    }
}

// Parse the trasnaction data
async fn parse_txn_sequence(
    tx: EncodedConfirmedTransactionWithStatusMeta,
) -> Result<Vec<u64>, ParseError> {
    let mut seq_updates = vec![];

    // Get `UiTransaction` out of `EncodedTransactionWithStatusMeta`.
    let meta: UiTransactionStatusMeta = tx.transaction.meta.ok_or(ParseError::TransactionMeta)?;

    // See https://github.com/ngundotra/spl-ac-seq-parse/blob/main/src/main.rs
    if let OptionSerializer::Some(inner_instructions_vec) = meta.inner_instructions.as_ref() {
        let transaction: VersionedTransaction = tx
            .transaction
            .transaction
            .decode()
            .ok_or(ParseError::Transaction)?;

        // Add the account lookup stuff
        let mut account_keys = transaction.message.static_account_keys().to_vec();
        if let OptionSerializer::Some(loaded_addresses) = meta.loaded_addresses {
            for pubkey in loaded_addresses.writable.iter() {
                account_keys.push(Pubkey::from_str(pubkey)?);
            }
            for pubkey in loaded_addresses.readonly.iter() {
                account_keys.push(Pubkey::from_str(pubkey)?);
            }
        }

        for inner_ixs in inner_instructions_vec.iter() {
            for inner_ix in inner_ixs.instructions.iter() {
                if let solana_transaction_status::UiInstruction::Compiled(instr) = inner_ix {
                    if let Some(program) = account_keys.get(instr.program_id_index as usize) {
                        if *program == spl_noop::id() {
                            let data = bs58::decode(&instr.data)
                                .into_vec()
                                .map_err(ParseError::Instruction)?;

                            if let Ok(AccountCompressionEvent::ChangeLog(cl_data)) =
                                AccountCompressionEvent::try_from_slice(&data)
                            {
                                let ChangeLogEvent::V1(cl_data) = cl_data;
                                seq_updates.push(cl_data.seq);
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(seq_updates)
}
