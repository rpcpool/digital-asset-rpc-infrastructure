use blockbuster::{
    self,
    instruction::InstructionBundle,
    programs::bubblegum::{BubblegumInstruction, InstructionName},
};
use log::{debug, error, info};
use sea_orm::{ConnectionTrait, TransactionTrait};

mod mint_v1;

use crate::error::IngesterError;

pub async fn handle_bubblegum_instruction<'c, T>(
    parsing_result: &'c BubblegumInstruction,
    bundle: &'c InstructionBundle<'c>,
    txn: &T,
) -> Result<(), IngesterError>
where
    T: ConnectionTrait + TransactionTrait,
{
    let ix_type = &parsing_result.instruction;

    // @TODO this would be much better served by implemneting Debug trait on the InstructionName
    // or wrapping it into something that can display it more neatly.
    let ix_str = match ix_type {
        InstructionName::Unknown => "Unknown",
        InstructionName::MintV1 => "MintV1",
        InstructionName::MintToCollectionV1 => "MintToCollectionV1",
        InstructionName::Redeem => "Redeem",
        InstructionName::CancelRedeem => "CancelRedeem",
        InstructionName::Transfer => "Transfer",
        InstructionName::Delegate => "Delegate",
        InstructionName::DecompressV1 => "DecompressV1",
        InstructionName::Compress => "Compress",
        InstructionName::Burn => "Burn",
        InstructionName::CreateTree => "CreateTree",
        InstructionName::VerifyCreator => "VerifyCreator",
        InstructionName::UnverifyCreator => "UnverifyCreator",
        InstructionName::VerifyCollection => "VerifyCollection",
        InstructionName::UnverifyCollection => "UnverifyCollection",
        InstructionName::SetAndVerifyCollection => "SetAndVerifyCollection",
    };
    info!("BGUM instruction txn={:?}: {:?}", ix_str, bundle.txn_id);

    match ix_type {
        InstructionName::MintV1 | InstructionName::MintToCollectionV1 => {
            let result = mint_v1::update_name_symbol(&parsing_result, bundle, txn).await;

            match result {
                Ok(_) => info!("Successfully updated name and symbol"),
                Err(e) => {
                    error!("Error updating name and symbol: {:?}", e);
                }
            }
        }
        _ => debug!("Bubblegum: Not Implemented Instruction"),
    }
    Ok(())
}
