use digital_asset_types::rpc::{
    filter::{AssetSorting, SearchConditionType},
    Interface, OwnershipModel, RoyaltyModel,
};
use log::debug;

use crate::{api::*, DasApiError, RpcModule};
pub struct RpcApiBuilder;

impl RpcApiBuilder {
    pub fn build(
        contract: Box<dyn ApiContract>,
    ) -> Result<RpcModule<Box<dyn ApiContract>>, DasApiError> {
        let mut module = RpcModule::new(contract);
        module.register_async_method("healthz", |_rpc_params, rpc_context| async move {
            debug!("Checking Health");
            rpc_context.check_health().await.map_err(Into::into)
        })?;

        module.register_async_method("get_asset_proof", |rpc_params, rpc_context| async move {
            let payload = rpc_params.parse::<GetAsset>()?;
            rpc_context
                .get_asset_proof(payload)
                .await
                .map_err(Into::into)
        })?;
        module.register_alias("getAssetProof", "get_asset_proof")?;

        module.register_async_method("get_asset", |rpc_params, rpc_context| async move {
            let payload = rpc_params.parse::<GetAsset>()?;
            rpc_context.get_asset(payload).await.map_err(Into::into)
        })?;
        module.register_alias("getAsset", "get_asset")?;

        module.register_async_method(
            "get_assets_by_owner",
            |rpc_params, rpc_context| async move {
                let payload: GetAssetsByOwner;
                if let Ok(parsed_payload) = rpc_params.parse::<GetAssetsByOwner>() {
                    payload = parsed_payload;
                } else {
                    let mut sequence_parser = rpc_params.sequence();

                    let owner_address = match sequence_parser.next::<String>() {
                        Ok(address) => address,
                        Err(_) => {
                            return Err(DasApiError::ValidationError(
                                "'owner_address' is missing or invalid".to_string(),
                            )
                            .into())
                        }
                    };

                    payload = GetAssetsByOwner {
                        owner_address,
                        sort_by: sequence_parser
                            .optional_next::<AssetSorting>()
                            .unwrap_or(None),
                        limit: sequence_parser.optional_next::<u32>().unwrap_or(None),
                        page: sequence_parser.optional_next::<u32>().unwrap_or(None),
                        before: sequence_parser.optional_next::<String>().unwrap_or(None),
                        after: sequence_parser.optional_next::<String>().unwrap_or(None),
                    }
                }
                rpc_context
                    .get_assets_by_owner(payload)
                    .await
                    .map_err(Into::into)
            },
        )?;
        module.register_alias("getAssetsByOwner", "get_assets_by_owner")?;

        module.register_async_method(
            "get_assets_by_creator",
            |rpc_params, rpc_context| async move {
                let payload: GetAssetsByCreator;
                if let Ok(parsed_payload) = rpc_params.parse::<GetAssetsByCreator>() {
                    payload = parsed_payload
                } else {
                    let mut sequence_parser = rpc_params.sequence();

                    let creator_address = match sequence_parser.next::<String>() {
                        Ok(address) => address,
                        Err(_) => {
                            return Err(DasApiError::ValidationError(
                                "'creator_address' is missing or invalid".to_string(),
                            )
                            .into());
                        }
                    };

                    payload = GetAssetsByCreator {
                        creator_address,
                        only_verified: sequence_parser.optional_next::<bool>().unwrap_or(None),
                        sort_by: sequence_parser
                            .optional_next::<AssetSorting>()
                            .unwrap_or(None),
                        limit: sequence_parser.optional_next::<u32>().unwrap_or(None),
                        page: sequence_parser.optional_next::<u32>().unwrap_or(None),
                        before: sequence_parser.optional_next::<String>().unwrap_or(None),
                        after: sequence_parser.optional_next::<String>().unwrap_or(None),
                    }
                }
                rpc_context
                    .get_assets_by_creator(payload)
                    .await
                    .map_err(Into::into)
            },
        )?;
        module.register_alias("getAssetsByCreator", "get_assets_by_creator")?;

        module.register_async_method(
            "getAssetsByAuthority",
            |rpc_params, rpc_context| async move {
                let payload: GetAssetsByAuthority;
                if let Ok(parsed_payload) = rpc_params.parse::<GetAssetsByAuthority>() {
                    payload = parsed_payload;
                } else {
                    let mut sequence_parser = rpc_params.sequence();

                    let authority_address = match sequence_parser.next::<String>() {
                        Ok(address) => address,
                        Err(_) => {
                            return Err(DasApiError::ValidationError(
                                "'authority_address' is missing or invalid".to_string(),
                            )
                            .into());
                        }
                    };

                    payload = GetAssetsByAuthority {
                        authority_address,
                        sort_by: sequence_parser
                            .optional_next::<AssetSorting>()
                            .unwrap_or(None),
                        limit: sequence_parser.optional_next::<u32>().unwrap_or(None),
                        page: sequence_parser.optional_next::<u32>().unwrap_or(None),
                        before: sequence_parser.optional_next::<String>().unwrap_or(None),
                        after: sequence_parser.optional_next::<String>().unwrap_or(None),
                    }
                }
                rpc_context
                    .get_assets_by_authority(payload)
                    .await
                    .map_err(Into::into)
            },
        )?;

        module.register_async_method(
            "get_assets_by_group",
            |rpc_params, rpc_context| async move {
                let payload: GetAssetsByGroup;
                if let Ok(parsed_payload) = rpc_params.parse::<GetAssetsByGroup>() {
                    payload = parsed_payload;
                } else {
                    let mut sequence_parser = rpc_params.sequence();

                    let group_key = match sequence_parser.next::<String>() {
                        Ok(key) => key,
                        Err(_) => {
                            return Err(DasApiError::ValidationError(
                                "'group_key' is missing or invalid".to_string(),
                            )
                            .into());
                        }
                    };

                    let group_value = match sequence_parser.next::<String>() {
                        Ok(value) => value,
                        Err(_) => {
                            return Err(DasApiError::ValidationError(
                                "'group_value' is missing or invalid".to_string(),
                            )
                            .into());
                        }
                    };

                    payload = GetAssetsByGroup {
                        group_key,
                        group_value,
                        sort_by: sequence_parser
                            .optional_next::<AssetSorting>()
                            .unwrap_or(None),
                        limit: sequence_parser.optional_next::<u32>().unwrap_or(None),
                        page: sequence_parser.optional_next::<u32>().unwrap_or(None),
                        before: sequence_parser.optional_next::<String>().unwrap_or(None),
                        after: sequence_parser.optional_next::<String>().unwrap_or(None),
                    }
                }
                rpc_context
                    .get_assets_by_group(payload)
                    .await
                    .map_err(Into::into)
            },
        )?;

        module.register_async_method(
            "getSignaturesForAsset",
            |rpc_params, rpc_context| async move {
                let payload: GetSignaturesForAsset;
                if let Ok(parsed_payload) = rpc_params.parse::<GetSignaturesForAsset>() {
                    payload = parsed_payload;
                } else {
                    let mut sequence_parser = rpc_params.sequence();

                    let id = match sequence_parser.next::<String>() {
                        Ok(id) => id,
                        Err(_) => {
                            return Err(DasApiError::ValidationError(
                                "'id' is missing or invalid".to_string(),
                            )
                            .into());
                        }
                    };

                    payload = GetSignaturesForAsset {
                        id,
                        limit: sequence_parser.optional_next::<u32>().unwrap_or(None),
                        page: sequence_parser.optional_next::<u32>().unwrap_or(None),
                        before: sequence_parser.optional_next::<String>().unwrap_or(None),
                        after: sequence_parser.optional_next::<String>().unwrap_or(None),
                    }
                }
                rpc_context
                    .get_signatures_for_asset(payload)
                    .await
                    .map_err(Into::into)
            },
        )?;

        module.register_alias("getAssetsByGroup", "get_assets_by_group")?;

        module.register_async_method("search_assets", |rpc_params, rpc_context| async move {
            let payload: SearchAssets;
            if let Ok(parsed_payload) = rpc_params.parse::<SearchAssets>() {
                payload = parsed_payload;
            } else {
                let mut sequence_parser = rpc_params.sequence();
                payload = SearchAssets {
                    negate: sequence_parser.optional_next::<bool>().unwrap_or(None),
                    condition_type: sequence_parser
                        .optional_next::<SearchConditionType>()
                        .unwrap_or(None),
                    interface: sequence_parser.optional_next::<Interface>().unwrap_or(None),
                    owner_address: sequence_parser.optional_next::<String>().unwrap_or(None),
                    owner_type: sequence_parser
                        .optional_next::<OwnershipModel>()
                        .unwrap_or(None),
                    creator_address: sequence_parser.optional_next::<String>().unwrap_or(None),
                    creator_verified: sequence_parser.optional_next::<bool>().unwrap_or(None),
                    authority_address: sequence_parser.optional_next::<String>().unwrap_or(None),
                    grouping: sequence_parser
                        .optional_next::<(String, String)>()
                        .unwrap_or(None),
                    delegate: sequence_parser.optional_next::<Vec<u8>>().unwrap_or(None),
                    frozen: sequence_parser.optional_next::<bool>().unwrap_or(None),
                    supply: sequence_parser.optional_next::<u64>().unwrap_or(None),
                    supply_mint: sequence_parser.optional_next::<String>().unwrap_or(None),
                    compressed: sequence_parser.optional_next::<bool>().unwrap_or(None),
                    compressible: sequence_parser.optional_next::<bool>().unwrap_or(None),
                    royalty_target_type: sequence_parser
                        .optional_next::<RoyaltyModel>()
                        .unwrap_or(None),
                    royalty_target: sequence_parser.optional_next::<String>().unwrap_or(None),
                    royalty_amount: sequence_parser.optional_next::<u32>().unwrap_or(None),
                    burnt: sequence_parser.optional_next::<bool>().unwrap_or(None),
                    sort_by: sequence_parser
                        .optional_next::<AssetSorting>()
                        .unwrap_or(None),
                    limit: sequence_parser.optional_next::<u32>().unwrap_or(None),
                    page: sequence_parser.optional_next::<u32>().unwrap_or(None),
                    before: sequence_parser.optional_next::<String>().unwrap_or(None),
                    after: sequence_parser.optional_next::<String>().unwrap_or(None),
                }
            }
            rpc_context.search_assets(payload).await.map_err(Into::into)
        })?;
        module.register_alias("searchAssets", "search_assets")?;

        module.register_async_method("schema", |_, rpc_context| async move {
            Ok(rpc_context.schema())
        })?;

        Ok(module)
    }
}
