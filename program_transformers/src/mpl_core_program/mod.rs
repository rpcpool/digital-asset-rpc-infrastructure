use {
    crate::{
        error::{ProgramTransformerError, ProgramTransformerResult},
        mpl_core_program::v1_asset::{burn_v1_asset, save_v1_asset},
        skip_metadata_json_download, AccountInfo, DownloadMetadataNotifier,
    },
    blockbuster::programs::mpl_core_program::{MplCoreAccountData, MplCoreAccountState},
    sea_orm::DatabaseConnection,
};

mod v1_asset;

pub async fn handle_mpl_core_account<'a, 'b, 'c>(
    account_info: &AccountInfo,
    parsing_result: &'a MplCoreAccountState,
    db: &'b DatabaseConnection,
    download_metadata_notifier: &DownloadMetadataNotifier,
) -> ProgramTransformerResult<()> {
    match &parsing_result.data {
        MplCoreAccountData::EmptyAccount => {
            burn_v1_asset(db, account_info.pubkey, account_info.slot).await?;
            Ok(())
        }
        MplCoreAccountData::Asset(_) | MplCoreAccountData::Collection(_) => {
            if let Some(info) = save_v1_asset(
                db,
                account_info.pubkey,
                &parsing_result.data,
                account_info.slot,
            )
            .await?
            {
                if skip_metadata_json_download(&info, db).await {
                    return Ok(());
                }
                download_metadata_notifier(info)
                    .await
                    .map_err(ProgramTransformerError::DownloadMetadataNotify)?;
            }
            Ok(())
        }
        _ => Err(ProgramTransformerError::NotImplemented),
    }?;
    Ok(())
}
