//! SeaORM Entity. Generated by sea-orm-codegen 0.9.3

use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Default, Debug, DeriveEntity)]
pub struct Entity;

impl EntityName for Entity {
    fn table_name(&self) -> &str {
        "cl_audits"
    }
}

#[derive(Clone, Debug, PartialEq, DeriveModel, DeriveActiveModel, Serialize, Deserialize)]
pub struct Model {
    pub id: i64,
    pub tree: Vec<u8>,
    pub node_idx: i64,
    pub leaf_idx: Option<i64>,
    pub seq: i64,
    pub level: i64,
    pub hash: Vec<u8>,
    pub created_at: DateTime,
    pub tx: String,
    pub instruction: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveColumn)]
pub enum Column {
    Id,
    Tree,
    NodeIdx,
    LeafIdx,
    Seq,
    Level,
    Hash,
    CreatedAt,
    Tx,
    #[sea_orm(column_name = "Instruction")]
    Instruction,
}

#[derive(Copy, Clone, Debug, EnumIter, DerivePrimaryKey)]
pub enum PrimaryKey {
    Id,
}

impl PrimaryKeyTrait for PrimaryKey {
    type ValueType = i64;
    fn auto_increment() -> bool {
        true
    }
}

#[derive(Copy, Clone, Debug, EnumIter)]
pub enum Relation {}

impl ColumnTrait for Column {
    type EntityName = Entity;
    fn def(&self) -> ColumnDef {
        match self {
            Self::Id => ColumnType::BigInteger.def(),
            Self::Tree => ColumnType::Binary.def(),
            Self::NodeIdx => ColumnType::BigInteger.def(),
            Self::LeafIdx => ColumnType::BigInteger.def().null(),
            Self::Seq => ColumnType::BigInteger.def(),
            Self::Level => ColumnType::BigInteger.def(),
            Self::Hash => ColumnType::Binary.def(),
            Self::CreatedAt => ColumnType::DateTime.def(),
            Self::Tx => ColumnType::String(None).def(),
            Self::Instruction => ColumnType::String(None).def().null(),
        }
    }
}

impl RelationTrait for Relation {
    fn def(&self) -> RelationDef {
        panic!("No RelationDef")
    }
}

impl ActiveModelBehavior for ActiveModel {}

impl From<crate::dao::cl_items::ActiveModel> for ActiveModel {
    fn from(item: crate::dao::cl_items::ActiveModel) -> Self {
        return ActiveModel {
            tree: item.tree,
            level: item.level,
            node_idx: item.node_idx,
            hash: item.hash,
            seq: item.seq,
            leaf_idx: item.leaf_idx,
            ..Default::default()
        };
    }
}
