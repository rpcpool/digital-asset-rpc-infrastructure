use super::BubblegumContext;
use crate::error::ErrorKind;
use crate::tree::TreeResponse;
use anyhow::{anyhow, Result};
use borsh::BorshDeserialize;
use digital_asset_types::dapi::get_proof_for_asset;
use digital_asset_types::rpc::AssetProof;
use futures::stream::{FuturesUnordered, StreamExt};
use mpl_bubblegum::accounts::TreeConfig;
use sea_orm::SqlxPostgresConnector;
use sha3::{Digest, Keccak256};
use solana_sdk::{pubkey::Pubkey, syscalls::MAX_CPI_INSTRUCTION_ACCOUNTS};
use spl_account_compression::{
    canopy::fill_in_proof_from_canopy,
    concurrent_tree_wrapper::ProveLeafArgs,
    state::{
        merkle_tree_get_size, ConcurrentMerkleTreeHeader, CONCURRENT_MERKLE_TREE_HEADER_SIZE_V1,
    },
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::error;

trait TryFromAssetProof {
    fn try_from_asset_proof(proof: AssetProof) -> Result<Self, anyhow::Error>
    where
        Self: Sized;
}

impl TryFromAssetProof for ProveLeafArgs {
    fn try_from_asset_proof(proof: AssetProof) -> Result<Self, anyhow::Error> {
        Ok(ProveLeafArgs {
            current_root: bs58::decode(&proof.root)
                .into_vec()
                .map_err(|e| anyhow!(e))?
                .try_into()
                .map_err(|_| anyhow!("Invalid root length"))?,
            leaf: bs58::decode(&proof.leaf)
                .into_vec()
                .map_err(|e| anyhow!(e))?
                .try_into()
                .map_err(|_| anyhow!("Invalid leaf length"))?,
            proof_vec: proof
                .proof
                .iter()
                .map(|p| {
                    bs58::decode(p)
                        .into_vec()
                        .map_err(|e| anyhow!(e))
                        .and_then(|v| v.try_into().map_err(|_| anyhow!("Invalid proof length")))
                })
                .collect::<Result<Vec<[u8; 32]>>>()?,
            index: proof.node_index as u32,
        })
    }
}

fn hash(left: &[u8], right: &[u8]) -> [u8; 32] {
    let mut hasher = Keccak256::new();
    hasher.update(left);
    hasher.update(right);
    let result = hasher.finalize();
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&result);
    hash
}

fn verify_merkle_proof(root: [u8; 32], proof: &ProveLeafArgs) -> bool {
    let mut node = proof.leaf;
    for (i, sibling) in proof.proof_vec.iter().enumerate() {
        if (proof.index >> i) & 1 == 0 {
            node = hash(&node, sibling);
        } else {
            node = hash(sibling, &node);
        }
    }
    node == root
}

#[derive(Debug)]
pub struct ProofReport {
    pub tree_pubkey: Pubkey,
    pub total_leaves: usize,
    pub incorrect_proofs: usize,
    pub not_found_proofs: usize,
    pub correct_proofs: usize,
}

enum ProofResult {
    Correct,
    Incorrect,
    NotFound,
}

pub async fn check(
    context: BubblegumContext,
    tree: TreeResponse,
    max_concurrency: usize,
) -> Result<ProofReport> {
    let (tree_config_pubkey, _) = TreeConfig::find_pda(&tree.pubkey);

    let pool = context.database_pool.clone();

    let account = context.solana_rpc.get_account(&tree_config_pubkey).await?;
    let account = account
        .value
        .ok_or_else(|| ErrorKind::Generic("Account not found".to_string()))?;

    let tree_config = TreeConfig::from_bytes(account.data.as_slice())?;

    let report = Arc::new(Mutex::new(ProofReport {
        tree_pubkey: tree.pubkey,
        total_leaves: tree_config.num_minted as usize,
        incorrect_proofs: 0,
        not_found_proofs: 0,
        correct_proofs: 0,
    }));

    let mut tasks = FuturesUnordered::new();

    for i in 0..tree_config.num_minted {
        if tasks.len() >= max_concurrency {
            tasks.next().await;
        }

        let db = SqlxPostgresConnector::from_sqlx_postgres_pool(pool.clone());
        let tree_pubkey = tree.pubkey.clone();
        let report = Arc::clone(&report);

        tasks.push(tokio::spawn(async move {
            let (asset, _) = Pubkey::find_program_address(
                &[b"asset", &tree_pubkey.to_bytes(), &i.to_le_bytes()],
                &mpl_bubblegum::ID,
            );
            let result: Result<ProofResult, anyhow::Error> =
                match get_proof_for_asset(&db, asset.to_bytes().to_vec()).await {
                    Ok(proof) => match ProveLeafArgs::try_from_asset_proof(proof) {
                        Ok(prove_leaf_args) => {
                            if verify_merkle_proof(prove_leaf_args.current_root, &prove_leaf_args) {
                                Ok(ProofResult::Correct)
                            } else {
                                Ok(ProofResult::Incorrect)
                            }
                        }
                        Err(_) => Ok(ProofResult::Incorrect),
                    },
                    Err(_) => Ok(ProofResult::NotFound),
                };

            if let Ok(proof_result) = result {
                let mut report = report.lock().await;
                match proof_result {
                    ProofResult::Correct => report.correct_proofs += 1,
                    ProofResult::Incorrect => {
                        report.incorrect_proofs += 1;
                        error!(tree = %tree_pubkey, leaf_index = i, asset = %asset, "Incorrect proof found");
                    }
                    ProofResult::NotFound => {
                        report.not_found_proofs += 1;
                        error!(tree = %tree_pubkey, leaf_index = i, asset = %asset, "Proof not found");
                    }
                }
            }
        }));
    }

    while tasks.next().await.is_some() {}

    let final_report = Arc::try_unwrap(report)
        .expect("Failed to unwrap Arc")
        .into_inner();

    Ok(final_report)
}
