use {
    anchor_client::anchor_lang::AnchorDeserialize,
    anyhow::Context,
    clap::{arg, Parser, Subcommand},
    digital_asset_types::dao::cl_items,
    futures::future::try_join_all,
    sea_orm::{
        sea_query::Expr, ColumnTrait, ConnectionTrait, DatabaseConnection, DbBackend, DbErr,
        EntityTrait, FromQueryResult, QueryFilter, QuerySelect, QueryTrait, SqlxPostgresConnector,
        Statement,
    },
    // plerkle_serialization::serializer::seralize_encoded_transaction_with_status,
    // solana_client::rpc_client::GetConfirmedSignaturesForAddress2Config,
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
    // solana_sdk::signature::Signature,
    // solana_transaction_status::UiTransactionEncoding,
    spl_account_compression::state::{
        merkle_tree_get_size, ConcurrentMerkleTreeHeader, CONCURRENT_MERKLE_TREE_HEADER_SIZE_V1,
    },
    spl_account_compression::{AccountCompressionEvent, ChangeLogEvent},
    sqlx::postgres::{PgConnectOptions, PgPoolOptions},
    std::{
        cmp,
        num::NonZeroUsize,
        str::FromStr,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    },
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

#[derive(Debug, FromQueryResult, Clone)]
struct MaxSeqItem {
    max_seq: i64,
    cnt_seq: i64,
}

#[allow(dead_code)]
#[derive(Debug, FromQueryResult, Clone)]
struct MissingSeq {
    missing_seq: i64,
}

#[derive(Parser)]
#[command(next_line_help = true, author, version, about)]
struct Args {
    /// Solana RPC endpoint.
    #[arg(long, short, alias = "rpc-url")]
    rpc: String,

    /// Number of concurrent requests for fetching transactions.
    #[arg(long, short, default_value_t = 25)]
    concurrency: usize,

    /// Maximum number of retries for transaction fetching.
    #[arg(long, short, default_value_t = 3)]
    max_retries: u8,

    #[command(subcommand)]
    action: Action,
}

#[derive(Subcommand, Clone)]
enum Action {
    /// Checks a single merkle tree to check if it;s fully indexed
    CheckTree {
        #[arg(short, long)]
        pg_url: String,

        #[arg(short, long, help = "Takes a single pubkey as a parameter to check")]
        tree: String,
    },
    /// Checks a list of merkle trees to check if they're fully indexed
    CheckTrees {
        #[arg(short, long)]
        pg_url: String,

        #[arg(
            short,
            long,
            help = "Takes a path to a file with pubkeys as a parameter to check"
        )]
        file: String,
    },
    /// Show a tree
    ShowTree {
        #[arg(short, long, help = "Takes a single tree as a parameter to check")]
        tree: String,
    },
    /// Shows a list of trees
    ShowTrees {
        #[arg(short, long, help = "Takes a single tree as a parameter to check")]
        file: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let concurrency = NonZeroUsize::new(args.concurrency)
        .ok_or_else(|| anyhow::anyhow!("invalid concurrency: {}", args.concurrency))?;

    // Set up RPC interface
    let pubkeys = match &args.action {
        Action::CheckTree { tree, .. } | Action::ShowTree { tree } => vec![tree.to_string()],
        Action::CheckTrees { file, .. } | Action::ShowTrees { file } => {
            tokio::fs::read_to_string(&file)
                .await
                .with_context(|| format!("failed to read file with keys: {:?}", file))?
                .split('\n')
                .filter_map(|x| {
                    let x = x.trim();
                    (!x.is_empty()).then(|| x.to_string())
                })
                .collect()
        }
    };

    match args.action {
        Action::CheckTree { pg_url, .. } | Action::CheckTrees { pg_url, .. } => {
            // Set up db connection
            let url = pg_url;
            let options: PgConnectOptions = url.parse().unwrap();

            // Create postgres pool
            let pool = PgPoolOptions::new()
                .min_connections(2)
                .max_connections(10)
                .connect_with(options)
                .await
                .unwrap();

            // Create new postgres connection
            let conn = SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone());

            let client = RpcClient::new(args.rpc);
            check_trees(pubkeys, &conn, &client).await?;
        }
        Action::ShowTree { .. } | Action::ShowTrees { .. } => {
            for pubkey in pubkeys {
                let address = Pubkey::from_str(&pubkey)
                    .with_context(|| format!("failed to parse address: {}", &pubkey))?;

                println!("showing tree {:?}", address);
                read_tree(address, args.rpc.clone(), concurrency, args.max_retries).await?;
            }
        }
    }

    Ok(())
}

async fn check_trees(
    pubkeys: Vec<String>,
    conn: &DatabaseConnection,
    client: &RpcClient,
) -> anyhow::Result<()> {
    for pubkey in pubkeys {
        match pubkey.parse() {
            Ok(pubkey) => {
                let seq = get_tree_latest_seq(pubkey, client).await;
                //println!("seq for pubkey {:?}: {:?}", pubkey, seq);
                if seq.is_err() {
                    eprintln!(
                        "[{:?}] tree is missing from chain or error occurred: {:?}",
                        pubkey, seq
                    );
                    continue;
                }

                let seq = seq.unwrap();

                let fetch_seq = get_tree_max_seq(&pubkey.to_bytes(), conn).await;
                if fetch_seq.is_err() {
                    eprintln!(
                        "[{:?}] couldn't query tree from index: {:?}",
                        pubkey, fetch_seq
                    );
                    continue;
                }
                match fetch_seq.unwrap() {
                    Some(indexed_seq) => {
                        let mut indexing_successful = false;
                        // Check tip
                        match indexed_seq.max_seq.cmp(&seq.try_into().unwrap()) {
                            cmp::Ordering::Less => {
                                eprintln!(
                                    "[{:?}] tree not fully indexed: {:?} < {:?}",
                                    pubkey, indexed_seq.max_seq, seq
                                );
                            }
                            cmp::Ordering::Equal => {
                                indexing_successful = true;
                            }
                            cmp::Ordering::Greater => {
                                eprintln!(
                                    "[{:?}] indexer error: {:?} > {:?}",
                                    pubkey, indexed_seq.max_seq, seq
                                );
                            }
                        }

                        // Check completeness
                        if indexed_seq.max_seq != indexed_seq.cnt_seq {
                            eprintln!(
                                "[{:?}] tree has gaps {:?} != {:?}",
                                pubkey, indexed_seq.max_seq, indexed_seq.cnt_seq
                            );
                            indexing_successful = false;
                        }

                        if indexing_successful {
                            println!("[{:?}] indexing is complete, seq={:?}", pubkey, seq)
                        } else {
                            eprintln!(
                                "[{:?}] indexing is failed, seq={:?} max_seq={:?}",
                                pubkey, seq, indexed_seq
                            );
                            let ret =
                                get_missing_seq(&pubkey.to_bytes(), seq.try_into().unwrap(), conn)
                                    .await;
                            if ret.is_err() {
                                eprintln!("[{:?}] failed to query missing seq: {:?}", pubkey, ret);
                            } else {
                                let ret = ret.unwrap();
                                eprintln!("[{:?}] missing seq: {:?}", pubkey, ret);
                            }
                        }
                    }
                    None => {
                        eprintln!("[{:?}] tree  missing from index", pubkey)
                    }
                }
            }
            Err(error) => {
                eprintln!("failed to parse pubkey {:?}, reason: {:?}", pubkey, error);
            }
        }
    }

    Ok(())
}

async fn get_missing_seq(
    tree: &[u8],
    max_seq: i64,
    conn: &DatabaseConnection,
) -> Result<Vec<MissingSeq>, DbErr> {
    let query = Statement::from_string(
            DbBackend::Postgres,
            format!("SELECT s.seq AS missing_seq FROM generate_series(1::bigint,{}::bigint) s(seq) WHERE NOT EXISTS (SELECT 1 FROM cl_items WHERE seq = s.seq AND tree='\\x{}')", max_seq, hex::encode(tree))
        );

    let missing_sequence_numbers: Vec<MissingSeq> = conn.query_all(query).await.map(|qr| {
        qr.iter()
            .map(|q| MissingSeq::from_query_result(q, "").unwrap())
            .collect()
    })?;

    Ok(missing_sequence_numbers)
}

async fn get_tree_max_seq(
    tree: &[u8],
    conn: &DatabaseConnection,
) -> Result<Option<MaxSeqItem>, DbErr> {
    let query = cl_items::Entity::find()
        .select_only()
        .filter(cl_items::Column::Tree.eq(tree))
        .column_as(Expr::col(cl_items::Column::Seq).max(), "max_seq")
        .column_as(Expr::cust("count(distinct seq)"), "cnt_seq")
        .build(DbBackend::Postgres);

    let res = MaxSeqItem::find_by_statement(query).one(conn).await?;
    Ok(res)
}

async fn get_tree_latest_seq(address: Pubkey, client: &RpcClient) -> anyhow::Result<u64> {
    // get account info
    let account_info = client
        .get_account_with_commitment(&address, CommitmentConfig::confirmed())
        .await?;

    let mut account = account_info
        .value
        .ok_or_else(|| anyhow::anyhow!("No account found"))?;

    let (header_bytes, rest) = account
        .data
        .split_at_mut(CONCURRENT_MERKLE_TREE_HEADER_SIZE_V1);
    let header: ConcurrentMerkleTreeHeader =
        ConcurrentMerkleTreeHeader::try_from_slice(header_bytes)?;

    // let auth = Pubkey::find_program_address(&[address.as_ref()], &mpl_bubblegum::id()).0;

    let merkle_tree_size = merkle_tree_get_size(&header)?;
    let (tree_bytes, _canopy_bytes) = rest.split_at_mut(merkle_tree_size);

    let seq_bytes = tree_bytes[0..8].try_into().context("Error parsing bytes")?;
    Ok(u64::from_le_bytes(seq_bytes))
}

// Fetches all the transactions referencing a specific trees
async fn read_tree(
    address: Pubkey,
    client_url: String,
    concurrency: NonZeroUsize,
    max_retries: u8,
) -> anyhow::Result<()> {
    let sig_id = Arc::new(AtomicUsize::new(0));
    let rx_sig = Arc::new(Mutex::new(find_signatures(
        address,
        RpcClient::new(client_url.clone()),
        2_000,
    )));

    try_join_all((0..concurrency.get()).map(|_| {
        let sig_id = Arc::clone(&sig_id);
        let rx_sig = Arc::clone(&rx_sig);
        let client = RpcClient::new(client_url.clone());
        async move {
            loop {
                let mut lock = rx_sig.lock().await;
                let maybe_msg = lock.recv().await;
                let id = sig_id.fetch_add(1, Ordering::SeqCst);
                drop(lock);
                match maybe_msg {
                    Some(maybe_sig) => {
                        if let Err(error) = process_txn(maybe_sig?, id, &client, max_retries).await
                        {
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
async fn process_txn(
    sig: Signature,
    id: usize,
    client: &RpcClient,
    mut retries: u8,
) -> anyhow::Result<()> {
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
                    println!("{} {} {}", seq, sig, id);
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
