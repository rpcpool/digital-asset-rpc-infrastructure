use anyhow::Result;
use clap::Parser;
use das_bubblegum::{BubblegumContext, TreeResponse};
use das_core::{DatabasePool, Rpc};
use digital_asset_types::dao::{
    asset, asset_authority, asset_creators, asset_data, asset_grouping, cl_audits_v2,
};
use log::{debug, error};
use sea_orm::TransactionTrait;
use sea_orm::{
    ColumnTrait, EntityTrait, FromQueryResult, PaginatorTrait, QueryFilter, QuerySelect,
};

use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use sqlx::PgPool;
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::task::{JoinHandle, JoinSet};

use super::purger::Args as PurgeArgs;

#[derive(FromQueryResult)]
struct TxQueryResult {
    tx: Vec<u8>,
    leaf_idx: i64,
}

struct Paginate<P: DatabasePool> {
    pool: Option<P>,
    batch_size: Option<u64>,
    sender: Option<UnboundedSender<Vec<TxQueryResult>>>,
    tree: Option<Vec<u8>>,
}

impl<P: DatabasePool> Paginate<P> {
    pub const DEFAULT_DB_BATCH_SIZE: u64 = 100;

    fn build() -> Self {
        Self {
            pool: None,
            batch_size: None,
            sender: None,
            tree: None,
        }
    }

    fn pool(mut self, pool: P) -> Self {
        self.pool = Some(pool);
        self
    }

    fn batch_size(mut self, batch_size: u64) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    fn sender(mut self, sender: UnboundedSender<Vec<TxQueryResult>>) -> Self {
        self.sender = Some(sender);
        self
    }

    fn tree(mut self, tree: Vec<u8>) -> Self {
        self.tree = Some(tree);
        self
    }

    fn start(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let pool = self.pool.expect("Pool not set");
            let sender = self.sender.expect("Sender not set");
            let batch_size = self.batch_size.unwrap_or(Self::DEFAULT_DB_BATCH_SIZE);
            let conn = pool.connection();
            let tree = self.tree.expect("Tree not set");

            let mut paginator = cl_audits_v2::Entity::find()
                .filter(cl_audits_v2::Column::Tree.eq(tree))
                .select_only()
                .columns([cl_audits_v2::Column::Tx, cl_audits_v2::Column::LeafIdx])
                .into_model::<TxQueryResult>()
                .paginate(&conn, batch_size);

            while let Ok(Some(records)) = paginator.fetch_and_next().await {
                if sender.send(records).is_err() {
                    error!("Failed to send keys");
                }
            }
        })
    }
}

struct MarkDeletion {
    rpc: Option<Rpc>,
    receiver: Option<UnboundedReceiver<Vec<TxQueryResult>>>,
    sender: Option<UnboundedSender<i64>>,
    concurrency: Option<usize>,
}

impl MarkDeletion {
    pub const DEFAULT_CONCURRENCY: usize = 10;

    fn build() -> Self {
        Self {
            rpc: None,
            receiver: None,
            sender: None,
            concurrency: None,
        }
    }

    fn rpc(mut self, rpc: Rpc) -> Self {
        self.rpc = Some(rpc);
        self
    }

    fn receiver(mut self, receiver: UnboundedReceiver<Vec<TxQueryResult>>) -> Self {
        self.receiver = Some(receiver);
        self
    }

    fn sender(mut self, sender: UnboundedSender<i64>) -> Self {
        self.sender = Some(sender);
        self
    }

    fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = Some(concurrency);
        self
    }

    fn start(self) -> JoinHandle<()> {
        let rpc = self.rpc.expect("Rpc not set");
        let mut receiver = self.receiver.expect("Receiver not set");
        let sender = self.sender.expect("Sender not set");
        let concurrency = self.concurrency.unwrap_or(Self::DEFAULT_CONCURRENCY);

        tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            while let Some(chunk) = receiver.recv().await {
                let rpc = rpc.clone();
                let sender = sender.clone();
                if tasks.len() >= concurrency {
                    tasks.join_next().await;
                }

                tasks.spawn(async move {
                    for tx_query_result in chunk {
                        let sig = Signature::try_from(tx_query_result.tx.as_ref()).unwrap();
                        if let Ok(tx) = rpc.get_transaction(&sig).await {
                            if tx.transaction.meta.clone().unwrap().err.is_some() {
                                if let Err(e) = sender.send(tx_query_result.leaf_idx) {
                                    error!("Failed to send marked leaves {:?}", e);
                                }
                            }
                        }
                    }
                });
            }
            while tasks.join_next().await.is_some() {}
        })
    }
}

trait Purge {
    async fn purge(&self, leaf_idx: i64) -> Result<()>;
}

#[derive(Clone)]
struct TreeErrTxsPurge<P: DatabasePool> {
    pool: P,
    tree: Vec<u8>,
}

impl<P: DatabasePool> TreeErrTxsPurge<P> {
    pub fn new(pool: P, tree: Vec<u8>) -> Self {
        Self { pool, tree }
    }
}

impl<P: DatabasePool> Purge for TreeErrTxsPurge<P> {
    async fn purge(&self, leaf_idx: i64) -> Result<()> {
        let conn = self.pool.connection();

        let (asset, _) = Pubkey::find_program_address(
            &[b"asset", &self.tree, &leaf_idx.to_le_bytes()],
            &mpl_bubblegum::ID,
        );
        let asset_bytes = asset.to_bytes().to_vec();
        let multi_txn = conn.begin().await?;

        asset_data::Entity::delete_by_id(asset_bytes.clone())
            .exec(&multi_txn)
            .await?;

        asset::Entity::delete_by_id(asset_bytes.clone())
            .exec(&multi_txn)
            .await?;

        asset_creators::Entity::delete_many()
            .filter(asset_creators::Column::AssetId.eq(asset_bytes.clone()))
            .exec(&multi_txn)
            .await?;

        asset_authority::Entity::delete_many()
            .filter(asset_authority::Column::AssetId.eq(asset_bytes.clone()))
            .exec(&multi_txn)
            .await?;

        asset_grouping::Entity::delete_many()
            .filter(asset_grouping::Column::AssetId.eq(asset_bytes))
            .exec(&multi_txn)
            .await?;

        // Remove all txs for the asset from cl_audits_v2 so that the asset can be re-indexed by the backfiller
        cl_audits_v2::Entity::delete_many()
            .filter(cl_audits_v2::Column::LeafIdx.eq(leaf_idx))
            .filter(cl_audits_v2::Column::Tree.eq(self.tree.clone()))
            .exec(&multi_txn)
            .await?;

        multi_txn.commit().await?;

        Ok(())
    }
}

#[derive(Debug, Parser, Clone)]
pub struct PurgeErrTxsArgs {
    /// The list of trees to verify. If not specified, all trees will be crawled.
    #[arg(long, env, use_value_delimiter = true)]
    pub only_trees: Option<Vec<String>>,
    #[clap(flatten)]
    pub purge_args: PurgeArgs,
}

pub async fn start_errored_txs_purge(args: PurgeErrTxsArgs, db: PgPool, rpc: Rpc) -> Result<()> {
    let start = tokio::time::Instant::now();

    let context = BubblegumContext::new(db.clone(), rpc.clone());
    let trees = if let Some(ref only_trees) = args.only_trees {
        TreeResponse::find(&context.solana_rpc, only_trees.clone()).await?
    } else {
        TreeResponse::all(&context.solana_rpc).await?
    };

    let purge_worker_count = args.purge_args.purge_worker_count as usize;

    for tree in trees {
        let tree_bytes = tree.pubkey.as_ref().to_vec();
        let (paginate_sender, paginate_receiver) = unbounded_channel::<Vec<TxQueryResult>>();
        let (mark_sender, mut mark_receiver) = unbounded_channel::<i64>();

        let paginate_handle = Paginate::<PgPool>::build()
            .pool(db.clone())
            .batch_size(args.purge_args.batch_size)
            .sender(paginate_sender)
            .tree(tree_bytes.clone())
            .start();

        let mark_handle = MarkDeletion::build()
            .rpc(rpc.clone())
            .receiver(paginate_receiver)
            .sender(mark_sender)
            .concurrency(args.purge_args.mark_deletion_worker_count as usize)
            .start();

        let db = db.clone();

        let purge_handle = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            let purge = TreeErrTxsPurge::new(db, tree_bytes);

            while let Some(addresses) = mark_receiver.recv().await {
                if tasks.len() >= purge_worker_count {
                    tasks.join_next().await;
                }

                let purge = purge.clone();

                tasks.spawn(async move {
                    if let Err(e) = purge.purge(addresses).await {
                        error!("Failed to purge asset {:?}", e);
                    }
                });
            }

            while tasks.join_next().await.is_some() {}
        });

        let _ = futures::future::join3(paginate_handle, mark_handle, purge_handle).await;
    }

    debug!("Purge took: {:?}", start.elapsed());

    Ok(())
}
