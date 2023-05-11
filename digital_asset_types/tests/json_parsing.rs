#[cfg(test)]
use blockbuster::token_metadata::state::TokenStandard as TSBlockbuster;
use digital_asset_types::dao::asset_data;
use digital_asset_types::dao::sea_orm_active_enums::{ChainMutability, Mutability};
use digital_asset_types::dapi::common::v1_content_from_json;
use digital_asset_types::json::ChainDataV1;
use digital_asset_types::rpc::Content;
use digital_asset_types::rpc::File;
use solana_sdk::signature::Keypair;
use solana_sdk::signer::Signer;
use tokio;

pub async fn load_test_json(file_name: &str) -> serde_json::Value {
    let json = tokio::fs::read_to_string(format!("tests/data/{}", file_name))
        .await
        .unwrap();
    serde_json::from_str(&json).unwrap()
}

pub async fn parse_offchain_json(json: serde_json::Value, cdn_prefix: Option<String>) -> Content {
    let asset_data = asset_data::Model {
        id: Keypair::new().pubkey().to_bytes().to_vec(),
        chain_data_mutability: ChainMutability::Mutable,
        chain_data: serde_json::to_value(ChainDataV1 {
            name: String::from("Handalf"),
            symbol: String::from(""),
            edition_nonce: None,
            primary_sale_happened: true,
            token_standard: Some(TSBlockbuster::NonFungible),
            uses: None,
        })
        .unwrap(),
        metadata_url: String::from("some url"),
        metadata_mutability: Mutability::Mutable,
        metadata: json,
        slot_updated: 0,
    };

    v1_content_from_json(&asset_data, cdn_prefix).unwrap()
}

#[tokio::test]
async fn simple_content() {
    let cdn_prefix = None;
    let j = load_test_json("mad_lad.json").await;
    let parsed = parse_offchain_json(j, cdn_prefix).await;
    assert_eq!(
        parsed.files,
        Some(vec![
            File {
                uri: Some("https://madlads.s3.us-west-2.amazonaws.com/images/1.png".to_string()),
                mime: Some("image/png".to_string()),
                quality: None,
                contexts: None,
            },
            File {
                uri: Some(
                    "https://arweave.net/qJ5B6fx5hEt4P7XbicbJQRyTcbyLaV-OQNA1KjzdqOQ/1.png"
                        .to_string(),
                ),
                mime: Some("image/png".to_string()),
                quality: None,
                contexts: None,
            }
        ])
    )
}

#[tokio::test]
async fn simple_content_with_cdn() {
    let cdn_prefix = Some("https://cdn.foobar.blah".to_string());
    let j = load_test_json("mad_lad.json").await;
    let parsed = parse_offchain_json(j, cdn_prefix).await;
    assert_eq!(
        parsed.files,
        Some(vec![
            File {
                uri: Some("https://cdn.foobar.blah//https://madlads.s3.us-west-2.amazonaws.com/images/1.png".to_string()),
                mime: Some("image/png".to_string()),
                quality: None,
                contexts: None,
            },
            File {
                uri: Some(
                    "https://cdn.foobar.blah//https://arweave.net/qJ5B6fx5hEt4P7XbicbJQRyTcbyLaV-OQNA1KjzdqOQ/1.png"
                        .to_string(),
                ),
                mime: Some("image/png".to_string()),
                quality: None,
                contexts: None,
            }
        ])
    )
}

#[tokio::test]
async fn complex_content() {
    let cdn_prefix = None;
    let j = load_test_json("infinite_fungi.json").await;
    let parsed = parse_offchain_json(j, cdn_prefix).await;
    assert_eq!(
        parsed.files,
        Some(vec![
            File {
                uri: Some(
                    "https://arweave.net/_a4sXT6fOHI-5VHFOHLEF73wqKuZtJgE518Ciq9DGyI?ext=gif"
                        .to_string(),
                ),
                mime: Some("image/gif".to_string()),
                quality: None,
                contexts: None,
            },
            File {
                uri: Some(
                    "https://arweave.net/HVOJ3bTpqMJJJtd5nW2575vPTekLa_SSDsQc7AqV_Ho?ext=mp4"
                        .to_string()
                ),
                mime: Some("video/mp4".to_string()),
                quality: None,
                contexts: None,
            },
        ])
    )
}

#[tokio::test]
async fn complex_content_with_cdn() {
    let cdn_prefix = Some("https://cdn.foobar.blah".to_string());
    let j = load_test_json("infinite_fungi.json").await;
    let parsed = parse_offchain_json(j, cdn_prefix).await;
    assert_eq!(
        parsed.files,
        Some(vec![
            File {
                uri: Some(
                    "https://cdn.foobar.blah//https://arweave.net/_a4sXT6fOHI-5VHFOHLEF73wqKuZtJgE518Ciq9DGyI?ext=gif"
                        .to_string(),
                ),
                mime: Some("image/gif".to_string()),
                quality: None,
                contexts: None,
            },
            File {
                uri: Some("https://arweave.net/HVOJ3bTpqMJJJtd5nW2575vPTekLa_SSDsQc7AqV_Ho?ext=mp4".to_string()),
                mime: Some("video/mp4".to_string()),
                quality: None,
                contexts: None,
            },
        ])
    )
}
