use super::common::*;
use das_api::api::{self, ApiContract};
use function_name::named;
use itertools::Itertools;
use serial_test::serial;

#[tokio::test]
#[serial]
#[named]
async fn test_token_extensions_get_asset_scenario_1() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["BPU5vrAHafRuVeK33CgfdwTKSsmC4p6t3aqyav3cFF7Y"]);

    apply_migrations_and_delete_data(setup.db.clone()).await;
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"        
    {
        "id": "BPU5vrAHafRuVeK33CgfdwTKSsmC4p6t3aqyav3cFF7Y"
    }
    "#;

    let request: api::GetAsset = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_asset(request).await.unwrap();

    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_token_extensions_get_asset_scenario_2() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Mainnet),
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["HVbpJAQGNpkgBaYBZQBR1t7yFdvaYVp2vCQQfKKEN4tM"]);

    apply_migrations_and_delete_data(setup.db.clone()).await;
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"        
    {
        "id": "HVbpJAQGNpkgBaYBZQBR1t7yFdvaYVp2vCQQfKKEN4tM"
    }
    "#;

    let request: api::GetAsset = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_asset(request).await.unwrap();

    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_token_extensions_get_asset_scenario_3() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Mainnet),
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo"]);

    apply_migrations_and_delete_data(setup.db.clone()).await;
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"        
    {
        "id": "2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo"
    }
    "#;

    let request: api::GetAsset = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_asset(request).await.unwrap();

    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_fungible_token_get_asset_scenario_1() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Mainnet),
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts([
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "5x38Kp4hvdomTCnCrAny4UtMUt5rQBdB6px2K1Ui45Wq",
    ]);

    apply_migrations_and_delete_data(setup.db.clone()).await;
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"        
    {
        "id": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    }
    "#;

    let request: api::GetAsset = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_asset(request).await.unwrap();

    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_token_keg_fungible_with_no_metadata() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Devnet),
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts(["wKocBVvHQoVaiwWoCs9JYSVye4YZRrv5Cucf7fDqnz1"]);

    apply_migrations_and_delete_data(setup.db.clone()).await;
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"        
    {
        "id": "wKocBVvHQoVaiwWoCs9JYSVye4YZRrv5Cucf7fDqnz1"
    }
    "#;

    let request: api::GetAsset = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_asset(request).await.unwrap();

    insta::assert_json_snapshot!(name, response);
}

#[tokio::test]
#[serial]
#[named]
async fn test_fungibles_assets_with_no_ta_specific_data() {
    let name = trim_test_name(function_name!());
    let setup = TestSetup::new_with_options(
        name.clone(),
        TestSetupOptions {
            network: Some(Network::Mainnet),
        },
    )
    .await;

    let seeds: Vec<SeedEvent> = seed_accounts([
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
        "5x38Kp4hvdomTCnCrAny4UtMUt5rQBdB6px2K1Ui45Wq",
        "6B8k8At9879r5EsZAWg8W6DEpxzJ798Cwj48twu3pq4b",
        "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs",
        "DqqUY2u3jZ3ay8aNUbtTu63NPBVtyGsVVUEr45WP1i2h",
        "2SPhVHYYFaXQFRubBkU9fgmm4qC4X41CAR14Yd5EVhKa",
        "EvVddRSpjtk3Xqsi4MG9u6K527XH1wR2Z5dSaUP8Er1S",
    ]);

    apply_migrations_and_delete_data(setup.db.clone()).await;
    index_seed_events(&setup, seeds.iter().collect_vec()).await;

    let request = r#"        
    {
        "options" : {
				"showFungible" : true
			},
        "ids": [
				"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs"
        ]
    }
    "#;

    let request: api::GetAssets = serde_json::from_str(request).unwrap();
    let response = setup.das_api.get_assets(request).await.unwrap();

    insta::assert_json_snapshot!(name, response);

    assert_eq!(response.len(), 2);
    let assets = response.into_iter().map(|a| a.unwrap()).collect::<Vec<_>>();

    assert!(assets[0]
        .token_info
        .as_ref()
        .unwrap()
        .associated_token_address
        .is_none());

    assert!(assets[1]
        .token_info
        .as_ref()
        .unwrap()
        .associated_token_address
        .is_none());
}
