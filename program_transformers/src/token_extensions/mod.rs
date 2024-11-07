use {
    crate::{
        asset_upserts::{
            upsert_assets_mint_account_columns, upsert_assets_token_account_columns,
            AssetMintAccountColumns, AssetTokenAccountColumns,
        },
        error::{ProgramTransformerError, ProgramTransformerResult},
        AccountInfo,
    },
    blockbuster::programs::token_extensions::{
        extension::ShadowMetadata, MintAccount, TokenAccount, TokenExtensionsProgramAccount,
    },
    digital_asset_types::dao::{
        asset, asset_data,
        sea_orm_active_enums::{ChainMutability, OwnerType},
        token_accounts, tokens,
    },
    sea_orm::{
        entity::{ActiveValue, ColumnTrait},
        query::{QueryFilter, QueryTrait},
        sea_query::query::OnConflict,
        ConnectionTrait, DatabaseConnection, DbBackend, EntityTrait, TransactionTrait,
    },
    serde_json::Value,
    solana_sdk::program_option::COption,
    spl_token_2022::state::AccountState,
};

pub async fn handle_token_extensions_program_account<'a, 'b, 'c>(
    account_info: &'a AccountInfo,
    parsing_result: &'b TokenExtensionsProgramAccount,
    db: &'c DatabaseConnection,
) -> ProgramTransformerResult<()> {
    let account_key = account_info.pubkey.to_bytes().to_vec();
    let account_owner = account_info.owner.to_bytes().to_vec();
    let slot = account_info.slot as i64;
    match parsing_result {
        TokenExtensionsProgramAccount::TokenAccount(ta) => {
            let TokenAccount {
                account,
                extensions,
            } = ta;
            let ta = account;

            let extensions: Option<Value> = if extensions.is_some() {
                Some(
                    serde_json::to_value(extensions.clone())
                        .map_err(|e| ProgramTransformerError::SerializatonError(e.to_string()))?,
                )
            } else {
                None
            };

            let mint = ta.mint.to_bytes().to_vec();
            let delegate: Option<Vec<u8>> = match ta.delegate {
                COption::Some(d) => Some(d.to_bytes().to_vec()),
                COption::None => None,
            };
            let frozen = matches!(ta.state, AccountState::Frozen);
            let owner = ta.owner.to_bytes().to_vec();
            let model = token_accounts::ActiveModel {
                pubkey: ActiveValue::Set(account_key.clone()),
                mint: ActiveValue::Set(mint.clone()),
                delegate: ActiveValue::Set(delegate.clone()),
                owner: ActiveValue::Set(owner.clone()),
                frozen: ActiveValue::Set(frozen),
                delegated_amount: ActiveValue::Set(ta.delegated_amount as i64),
                token_program: ActiveValue::Set(account_owner.clone()),
                slot_updated: ActiveValue::Set(slot),
                amount: ActiveValue::Set(ta.amount as i64),
                close_authority: ActiveValue::Set(None),
                extensions: ActiveValue::Set(extensions.clone()),
            };

            let mut query = token_accounts::Entity::insert(model)
                .on_conflict(
                    OnConflict::columns([token_accounts::Column::Pubkey])
                        .update_columns([
                            token_accounts::Column::Mint,
                            token_accounts::Column::DelegatedAmount,
                            token_accounts::Column::Delegate,
                            token_accounts::Column::Amount,
                            token_accounts::Column::Frozen,
                            token_accounts::Column::TokenProgram,
                            token_accounts::Column::Owner,
                            token_accounts::Column::CloseAuthority,
                            token_accounts::Column::SlotUpdated,
                        ])
                        .to_owned(),
                )
                .build(DbBackend::Postgres);
            query.sql = format!(
                "{} WHERE excluded.slot_updated > token_accounts.slot_updated",
                query.sql
            );
            db.execute(query).await?;
            let txn = db.begin().await?;
            let asset_update: Option<asset::Model> = asset::Entity::find_by_id(mint.clone())
                .filter(asset::Column::OwnerType.eq(OwnerType::Single))
                .one(&txn)
                .await?;
            if let Some(_asset) = asset_update {
                // will only update owner if token account balance is non-zero
                // since the asset is marked as single then the token account balance can only be 1. Greater implies a fungible token in which case no si
                // TODO: this does not guarantee in case when wallet receives an amount of 1 for a token but its supply is more. is unlikely since mints often have a decimal
                if ta.amount == 1 {
                    upsert_assets_token_account_columns(
                        AssetTokenAccountColumns {
                            mint: mint.clone(),
                            owner: Some(owner.clone()),
                            frozen,
                            delegate,
                            slot_updated_token_account: Some(slot),
                            extensions,
                        },
                        &txn,
                    )
                    .await?;
                }
            }
            txn.commit().await?;
            Ok(())
        }
        TokenExtensionsProgramAccount::MintAccount(m) => {
            println!("MintAccount extns:{:#?}", m.extensions);
            let MintAccount {
                account,
                extensions,
            } = m;

            let extensions: Option<Value> = if extensions.is_some() {
                if let Some(metadata) = &m.extensions.metadata {
                    upsert_asset_data(metadata, account_key.clone(), slot, db).await?;
                }

                Some(
                    serde_json::to_value(extensions.clone())
                        .map_err(|e| ProgramTransformerError::SerializatonError(e.to_string()))?,
                )
            } else {
                None
            };

            let m = account;
            let freeze_auth: Option<Vec<u8>> = match m.freeze_authority {
                COption::Some(d) => Some(d.to_bytes().to_vec()),
                COption::None => None,
            };
            let mint_auth: Option<Vec<u8>> = match m.mint_authority {
                COption::Some(d) => Some(d.to_bytes().to_vec()),
                COption::None => None,
            };
            let model = tokens::ActiveModel {
                mint: ActiveValue::Set(account_key.clone()),
                token_program: ActiveValue::Set(account_owner),
                slot_updated: ActiveValue::Set(slot),
                supply: ActiveValue::Set(m.supply.into()),
                decimals: ActiveValue::Set(m.decimals as i32),
                close_authority: ActiveValue::Set(None),
                extension_data: ActiveValue::Set(None),
                mint_authority: ActiveValue::Set(mint_auth),
                freeze_authority: ActiveValue::Set(freeze_auth),
                extensions: ActiveValue::Set(extensions.clone()),
            };

            let mut query = tokens::Entity::insert(model)
                .on_conflict(
                    OnConflict::columns([tokens::Column::Mint])
                        .update_columns([
                            tokens::Column::Supply,
                            tokens::Column::TokenProgram,
                            tokens::Column::MintAuthority,
                            tokens::Column::CloseAuthority,
                            tokens::Column::ExtensionData,
                            tokens::Column::SlotUpdated,
                            tokens::Column::Decimals,
                            tokens::Column::FreezeAuthority,
                        ])
                        .to_owned(),
                )
                .build(DbBackend::Postgres);
            query.sql = format!(
                "{} WHERE excluded.slot_updated >= tokens.slot_updated",
                query.sql
            );
            db.execute(query).await?;
            let txn = db.begin().await?;

            //TODO :  handle checking nft , fungible asset and fungible token and updading specifcation_version and specification_class

            let asset_update: Option<asset::Model> = asset::Entity::find_by_id(account_key.clone())
                .filter(
                    asset::Column::OwnerType
                        .eq(OwnerType::Single)
                        .or(asset::Column::OwnerType
                            .eq(OwnerType::Unknown)
                            .and(asset::Column::Supply.eq(1))),
                )
                .one(db)
                .await?;
            if let Some(_asset) = asset_update {
                upsert_assets_mint_account_columns(
                    AssetMintAccountColumns {
                        mint: account_key.clone(),
                        supply_mint: Some(account_key.clone()),
                        supply: m.supply.into(),
                        slot_updated_mint_account: slot as u64,
                        extensions: extensions.clone(),
                    },
                    &txn,
                )
                .await?;

                txn.commit().await?;
            }
            Ok(())
        }
        _ => Err(ProgramTransformerError::NotImplemented),
    }
}

async fn upsert_asset_data(
    metadata: &ShadowMetadata,
    key_bytes: Vec<u8>,
    slot: i64,
    db: &DatabaseConnection,
) -> ProgramTransformerResult<()> {
    let metadata_json = serde_json::to_value(metadata.clone())
        .map_err(|e| ProgramTransformerError::SerializatonError(e.to_string()))?;
    let asset_data_model = asset_data::ActiveModel {
        metadata_url: ActiveValue::Set(metadata.uri.clone()),
        metadata: ActiveValue::Set(Value::String("processing".to_string())),
        id: ActiveValue::Set(key_bytes.clone()),
        chain_data_mutability: ActiveValue::Set(ChainMutability::Mutable),
        chain_data: ActiveValue::Set(metadata_json),
        slot_updated: ActiveValue::Set(slot),
        base_info_seq: ActiveValue::Set(Some(0)),
        raw_name: ActiveValue::Set(Some(metadata.name.clone().into_bytes().to_vec())),
        raw_symbol: ActiveValue::Set(Some(metadata.symbol.clone().into_bytes().to_vec())),
        ..Default::default()
    };
    let mut asset_data_query = asset_data::Entity::insert(asset_data_model)
        .on_conflict(
            OnConflict::columns([asset_data::Column::Id])
                .update_columns([
                    asset_data::Column::ChainDataMutability,
                    asset_data::Column::ChainData,
                    asset_data::Column::MetadataUrl,
                    asset_data::Column::SlotUpdated,
                    asset_data::Column::BaseInfoSeq,
                ])
                .to_owned(),
        )
        .build(DbBackend::Postgres);
    asset_data_query.sql = format!(
        "{} WHERE excluded.slot_updated >= asset_data.slot_updated",
        asset_data_query.sql
    );
    db.execute(asset_data_query).await?;
    Ok(())
}
