use super::BubblegumContext;
use crate::error::ErrorKind;
use crate::tree::TreeResponse;
use anyhow::{anyhow, Result};
use digital_asset_types::dapi::get_proof_for_asset;
use digital_asset_types::rpc::AssetProof;
use mpl_bubblegum::accounts::TreeConfig;
use sea_orm::SqlxPostgresConnector;
use solana_sdk::pubkey::Pubkey;
use spl_account_compression::concurrent_tree_wrapper::{merkle_tree_prove_leaf, ProveLeafArgs};

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
pub async fn check(context: BubblegumContext, tree: TreeResponse) -> Result<()> {
    let TreeResponse {
        pubkey,
        concurrent_merkle_tree_header,
        bytes,
        ..
    } = tree;

    let db = SqlxPostgresConnector::from_sqlx_postgres_pool(context.database_pool);

    let (tree_config_pubkey, _) = TreeConfig::find_pda(&tree.pubkey);

    let account = context.solana_rpc.get_account(&tree_config_pubkey).await?;
    let account = account
        .value
        .ok_or_else(|| ErrorKind::Generic("Account not found".to_string()))?;

    let tree_config = TreeConfig::from_bytes(account.data.as_slice())?;

    for i in 0..tree_config.num_minted {
        let (asset, _) = Pubkey::find_program_address(
            &[b"asset", &pubkey.to_bytes(), &i.to_le_bytes()],
            &mpl_bubblegum::ID,
        );
        let proof = get_proof_for_asset(&db, asset.to_bytes().to_vec()).await?;

        let prove_leaf_args = ProveLeafArgs::try_from_asset_proof(proof)?;

        let result = merkle_tree_prove_leaf(
            &concurrent_merkle_tree_header,
            pubkey,
            &bytes,
            &prove_leaf_args,
        );

        if let Err(e) = &result {
            log::error!("Proof not valid for asset pubkey {}: {:?}", asset, e);
        }
    }

    Ok(())
}
