use {
    crate::{
        asset_upserts::{
            upsert_assets_mint_account_columns, upsert_assets_token_account_columns,
            AssetMintAccountColumns, AssetTokenAccountColumns,
        },
        error::ProgramTransformerResult,
        AccountInfo, DownloadMetadataNotifier,
    },
    blockbuster::programs::token_account::TokenProgramAccount,
    digital_asset_types::dao::{token_accounts, tokens},
    sea_orm::{
        entity::ActiveValue,
        query::QueryTrait,
        sea_query::query::OnConflict,
        sea_query::{Alias, Condition, Expr},
        ConnectionTrait, DatabaseConnection, DbBackend, EntityTrait, TransactionTrait,
    },
    solana_sdk::program_option::COption,
    spl_token::state::AccountState,
};

pub async fn handle_token_program_account<'a, 'b>(
    account_info: &AccountInfo,
    parsing_result: &'a TokenProgramAccount,
    db: &'b DatabaseConnection,
    _download_metadata_notifier: &DownloadMetadataNotifier,
) -> ProgramTransformerResult<()> {
    let account_key = account_info.pubkey.to_bytes().to_vec();
    let account_owner = account_info.owner.to_bytes().to_vec();
    match &parsing_result {
        TokenProgramAccount::TokenAccount(ta) => {
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
                slot_updated: ActiveValue::Set(account_info.slot as i64),
                amount: ActiveValue::Set(ta.amount as i64),
                close_authority: ActiveValue::Set(None),
            };

            let txn = db.begin().await?;

            token_accounts::Entity::insert(model)
                .on_conflict(
                    OnConflict::column(token_accounts::Column::Pubkey)
                        .update_columns([
                            token_accounts::Column::Mint,
                            token_accounts::Column::DelegatedAmount,
                            token_accounts::Column::Delegate,
                            token_accounts::Column::Amount,
                            token_accounts::Column::Frozen,
                            token_accounts::Column::TokenProgram,
                            token_accounts::Column::Owner,
                            token_accounts::Column::SlotUpdated,
                        ])
                        .action_cond_where(
                            Condition::all()
                                .add(
                                    Condition::any()
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                token_accounts::Column::Mint,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    token_accounts::Entity,
                                                    token_accounts::Column::Mint,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                token_accounts::Column::DelegatedAmount,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    token_accounts::Entity,
                                                    token_accounts::Column::DelegatedAmount,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                token_accounts::Column::Delegate,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    token_accounts::Entity,
                                                    token_accounts::Column::Delegate,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                token_accounts::Column::Amount,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    token_accounts::Entity,
                                                    token_accounts::Column::Amount,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                token_accounts::Column::Frozen,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    token_accounts::Entity,
                                                    token_accounts::Column::Frozen,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                token_accounts::Column::TokenProgram,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    token_accounts::Entity,
                                                    token_accounts::Column::TokenProgram,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                token_accounts::Column::Owner,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    token_accounts::Entity,
                                                    token_accounts::Column::Owner,
                                                ),
                                            ),
                                        ),
                                )
                                .add(
                                    Expr::tbl(
                                        token_accounts::Entity,
                                        token_accounts::Column::SlotUpdated,
                                    )
                                    .lte(account_info.slot as i64),
                                ),
                        )
                        .to_owned(),
                )
                .exec_without_returning(&txn)
                .await?;

            if ta.amount == 1 {
                upsert_assets_token_account_columns(
                    AssetTokenAccountColumns {
                        mint: mint.clone(),
                        owner: Some(owner.clone()),
                        frozen,
                        delegate,
                        slot_updated_token_account: Some(account_info.slot as i64),
                    },
                    &txn,
                )
                .await?;
            }

            txn.commit().await?;
            Ok(())
        }
        TokenProgramAccount::Mint(m) => {
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
                slot_updated: ActiveValue::Set(account_info.slot as i64),
                supply: ActiveValue::Set(m.supply.into()),
                decimals: ActiveValue::Set(m.decimals as i32),
                close_authority: ActiveValue::Set(None),
                extension_data: ActiveValue::Set(None),
                mint_authority: ActiveValue::Set(mint_auth),
                freeze_authority: ActiveValue::Set(freeze_auth),
            };

            let txn = db.begin().await?;

            tokens::Entity::insert(model)
                .on_conflict(
                    OnConflict::columns([tokens::Column::Mint])
                        .update_columns([
                            tokens::Column::Supply,
                            tokens::Column::TokenProgram,
                            tokens::Column::MintAuthority,
                            tokens::Column::SlotUpdated,
                            tokens::Column::Decimals,
                            tokens::Column::FreezeAuthority,
                        ])
                        .action_cond_where(
                            Condition::all()
                                .add(
                                    Condition::any()
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                tokens::Column::Supply,
                                            )
                                            .ne(Expr::tbl(tokens::Entity, tokens::Column::Supply)),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                tokens::Column::TokenProgram,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    tokens::Entity,
                                                    tokens::Column::TokenProgram,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                tokens::Column::MintAuthority,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    tokens::Entity,
                                                    tokens::Column::MintAuthority,
                                                ),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                tokens::Column::Decimals,
                                            )
                                            .ne(
                                                Expr::tbl(tokens::Entity, tokens::Column::Decimals),
                                            ),
                                        )
                                        .add(
                                            Expr::tbl(
                                                Alias::new("excluded"),
                                                tokens::Column::FreezeAuthority,
                                            )
                                            .ne(
                                                Expr::tbl(
                                                    tokens::Entity,
                                                    tokens::Column::FreezeAuthority,
                                                ),
                                            ),
                                        ),
                                )
                                .add(
                                    Expr::tbl(tokens::Entity, tokens::Column::SlotUpdated)
                                        .lte(account_info.slot as i64),
                                ),
                        )
                        .to_owned(),
                )
                .exec_without_returning(&txn)
                .await?;

            upsert_assets_mint_account_columns(
                AssetMintAccountColumns {
                    mint: account_key.clone(),
                    supply_mint: Some(account_key),
                    supply: m.supply.into(),
                    slot_updated_mint_account: account_info.slot,
                },
                &txn,
            )
            .await?;

            txn.commit().await?;

            Ok(())
        }
    }
}
