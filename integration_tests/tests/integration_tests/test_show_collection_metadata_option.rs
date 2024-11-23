use function_name::named;

use das_api::api::{self, ApiContract};

use itertools::Itertools;

use serial_test::serial;

use super::common::*;

#[tokio::test]
#[serial]
#[named]

async fn test_get_asset_with_show_collection_metadata_option() {
    let name = trim_test_name(function_name!());

    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Mainnet),
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(
        ["F9Lw3ki3hJ7PF9HQXsBzoY8GyE6sPoEZZdXJBsTTD2rk"]
    );

    apply_migrations_and_delete_data(setup.db.clone()).await;

    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"
    {
    "id": "F9Lw3ki3hJ7PF9HQXsBzoY8GyE6sPoEZZdXJBsTTD2rk",
    "displayOptions" : {
        "showCollectionMetadata": true
        }
    }
    "#;

    let request: api::GetAsset = serde_json::from_str(request).unwrap();

    let response = setup.das_api.get_asset(request).await.unwrap();

    insta::assert_json_snapshot!(name, response);
}
