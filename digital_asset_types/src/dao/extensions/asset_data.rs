use crate::dao::sea_orm_active_enums::{ChainMutability, Mutability};
use crate::dao::{asset, asset_data};
use sea_orm::{EntityTrait, EnumIter, Related, RelationDef, RelationTrait};

#[derive(Copy, Clone, Debug, EnumIter)]
pub enum Relation {
    Asset,
}

impl RelationTrait for Relation {
    fn def(&self) -> RelationDef {
        match self {
            Self::Asset => asset_data::Entity::has_many(asset::Entity).into(),
        }
    }
}

impl Related<asset::Entity> for asset_data::Entity {
    fn to() -> RelationDef {
        Relation::Asset.def()
    }
}

impl Default for ChainMutability {
    fn default() -> Self {
        Self::Unknown
    }
}

impl Default for Mutability {
    fn default() -> Self {
        Self::Unknown
    }
}
