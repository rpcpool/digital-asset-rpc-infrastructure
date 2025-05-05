use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Options {
    #[serde(default)]
    pub show_unverified_collections: bool,
    #[serde(default)]
    pub show_collection_metadata: bool,
    #[serde(default)]
    pub show_zero_balance: bool,
    #[serde(default)]
    pub show_inscription: bool,
    #[serde(default)]
    pub show_fungible: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetAssetOptions {
    #[serde(default)]
    pub show_unverified_collections: bool,
    #[serde(default)]
    pub show_collection_metadata: bool,
    #[serde(default)]
    pub show_inscription: bool,
    #[serde(default)]
    pub show_fungible: bool,
    #[serde(default)]
    #[deprecated(note = "this field is deprecated and will be removed in the future")]
    pub show_zero_balance: bool,
}

impl From<GetAssetOptions> for Options {
    fn from(options: GetAssetOptions) -> Self {
        Options {
            show_unverified_collections: options.show_unverified_collections,
            show_collection_metadata: options.show_collection_metadata,
            show_inscription: options.show_inscription,
            show_fungible: options.show_fungible,
            show_zero_balance: false,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct GetByMethodsOptions {
    #[serde(default)]
    pub show_unverified_collections: bool,
    #[serde(default)]
    pub show_collection_metadata: bool,
    #[serde(default)]
    pub show_inscription: bool,
    #[serde(default)]
    pub show_zero_balance: bool,
    #[serde(default)]
    pub show_fungible: bool,
}

impl From<GetByMethodsOptions> for Options {
    fn from(options: GetByMethodsOptions) -> Self {
        Options {
            show_unverified_collections: options.show_unverified_collections,
            show_collection_metadata: options.show_collection_metadata,
            show_zero_balance: options.show_zero_balance,
            show_inscription: options.show_inscription,
            show_fungible: options.show_fungible,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, JsonSchema, Default)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SearchAssetsOptions {
    #[serde(default)]
    pub show_unverified_collections: bool,
    #[serde(default)]
    pub show_collection_metadata: bool,
    #[serde(default)]
    pub show_inscription: bool,
    #[serde(default)]
    pub show_zero_balance: bool,
}

impl From<SearchAssetsOptions> for Options {
    fn from(options: SearchAssetsOptions) -> Self {
        Options {
            show_unverified_collections: options.show_unverified_collections,
            show_collection_metadata: options.show_collection_metadata,
            show_zero_balance: options.show_zero_balance,
            show_inscription: options.show_inscription,
            show_fungible: false,
        }
    }
}
