//! SeaORM Entity. Generated by sea-orm-codegen 0.9.3

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Default, Debug, DeriveEntity)]
pub struct Entity;

impl EntityName for Entity {
    fn table_name(&self) -> &str {
        "tokens"
    }
}

#[derive(Clone, Debug, PartialEq, DeriveModel, DeriveActiveModel, Serialize, Deserialize)]
pub struct Model {
    pub mint: Vec<u8>,
    pub supply: Decimal,
    pub decimals: i32,
    pub token_program: Vec<u8>,
    pub mint_authority: Option<Vec<u8>>,
    pub freeze_authority: Option<Vec<u8>>,
    pub close_authority: Option<Vec<u8>>,
    pub extension_data: Option<Vec<u8>>,
    pub slot_updated: i64,
    pub extensions: Option<Json>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveColumn)]
pub enum Column {
    Mint,
    Supply,
    Decimals,
    TokenProgram,
    MintAuthority,
    FreezeAuthority,
    CloseAuthority,
    ExtensionData,
    SlotUpdated,
    Extensions,
}

#[derive(Copy, Clone, Debug, EnumIter, DerivePrimaryKey)]
pub enum PrimaryKey {
    Mint,
}

impl PrimaryKeyTrait for PrimaryKey {
    type ValueType = Vec<u8>;
    fn auto_increment() -> bool {
        false
    }
}

#[derive(Copy, Clone, Debug, EnumIter)]
pub enum Relation {}

impl ColumnTrait for Column {
    type EntityName = Entity;
    fn def(&self) -> ColumnDef {
        match self {
            Self::Mint => ColumnType::Binary.def(),
            Self::Supply => ColumnType::BigInteger.def(),
            Self::Decimals => ColumnType::Integer.def(),
            Self::TokenProgram => ColumnType::Binary.def(),
            Self::MintAuthority => ColumnType::Binary.def().null(),
            Self::FreezeAuthority => ColumnType::Binary.def().null(),
            Self::CloseAuthority => ColumnType::Binary.def().null(),
            Self::ExtensionData => ColumnType::Binary.def().null(),
            Self::SlotUpdated => ColumnType::BigInteger.def(),
            Self::Extensions => ColumnType::JsonBinary.def().null(),
        }
    }
}

impl RelationTrait for Relation {
    fn def(&self) -> RelationDef {
        panic!("No RelationDef")
    }
}

impl ActiveModelBehavior for ActiveModel {}
