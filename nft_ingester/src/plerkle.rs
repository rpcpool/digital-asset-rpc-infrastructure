use {
    flatbuffers::{ForwardsUOffset, Vector},
    plerkle_serialization::{
        deserializer::*, CompiledInnerInstructions as FBCompiledInnerInstructions,
        CompiledInstruction as FBCompiledInstruction, InnerInstructions as FBInnerInstructions,
    },
    program_transformers::{error::ProgramTransformerError, AccountInfo, TransactionInfo},
    solana_message::compiled_instruction::CompiledInstruction,
    solana_transaction_status::{InnerInstruction, InnerInstructions},
};

// Plerkle's own deserializers build Solana 3.x types. These rebuild the same
// values at the 4.x types this workspace uses. See core/README.md.
fn deserialize_compiled_instruction(
    cix: FBCompiledInstruction<'_>,
) -> Result<CompiledInstruction, PlerkleDeserializerError> {
    Ok(CompiledInstruction {
        program_id_index: cix.program_id_index(),
        accounts: cix
            .accounts()
            .ok_or(PlerkleDeserializerError::NotFound)?
            .bytes()
            .to_vec(),
        data: cix
            .data()
            .ok_or(PlerkleDeserializerError::NotFound)?
            .bytes()
            .to_vec(),
    })
}

fn deserialize_compiled_instructions(
    vec_cix: Vector<'_, ForwardsUOffset<FBCompiledInstruction<'_>>>,
) -> Result<Vec<CompiledInstruction>, PlerkleDeserializerError> {
    vec_cix
        .iter()
        .map(deserialize_compiled_instruction)
        .collect()
}

fn deserialize_compiled_inner_instructions(
    vec_ixs: Vector<'_, ForwardsUOffset<FBCompiledInnerInstructions<'_>>>,
) -> Result<Vec<InnerInstructions>, PlerkleDeserializerError> {
    vec_ixs
        .iter()
        .map(|ixs| {
            let instructions = ixs
                .instructions()
                .ok_or(PlerkleDeserializerError::NotFound)?
                .iter()
                .map(|ix| {
                    Ok(InnerInstruction {
                        instruction: deserialize_compiled_instruction(
                            ix.compiled_instruction()
                                .ok_or(PlerkleDeserializerError::NotFound)?,
                        )?,
                        stack_height: Some(u32::from(ix.stack_height())),
                    })
                })
                .collect::<Result<Vec<_>, PlerkleDeserializerError>>()?;
            Ok(InnerInstructions {
                index: ixs.index(),
                instructions,
            })
        })
        .collect()
}

fn deserialize_legacy_inner_instructions(
    vec_ixs: Vector<'_, ForwardsUOffset<FBInnerInstructions<'_>>>,
) -> Result<Vec<InnerInstructions>, PlerkleDeserializerError> {
    vec_ixs
        .iter()
        .map(|iixs| {
            let instructions = iixs
                .instructions()
                .ok_or(PlerkleDeserializerError::NotFound)?
                .iter()
                .map(|cix| {
                    Ok(InnerInstruction {
                        instruction: deserialize_compiled_instruction(cix)?,
                        stack_height: Some(0),
                    })
                })
                .collect::<Result<Vec<_>, PlerkleDeserializerError>>()?;
            Ok(InnerInstructions {
                index: iixs.index(),
                instructions,
            })
        })
        .collect()
}

pub fn into_program_transformer_err(e: PlerkleDeserializerError) -> ProgramTransformerError {
    ProgramTransformerError::DeserializationError(e.to_string())
}

#[derive(thiserror::Error, Clone, Debug)]
pub enum PlerkleDeserializerError {
    #[error("Not found")]
    NotFound,
    #[error("Solana error: {0}")]
    Solana(#[from] SolanaDeserializerError),
}

pub struct PlerkleAccountInfo<'a>(pub plerkle_serialization::AccountInfo<'a>);

impl TryFrom<PlerkleAccountInfo<'_>> for AccountInfo {
    type Error = PlerkleDeserializerError;

    fn try_from(value: PlerkleAccountInfo) -> Result<Self, Self::Error> {
        let account_info = value.0;

        Ok(Self {
            slot: account_info.slot(),
            pubkey: account_info
                .pubkey()
                .ok_or(PlerkleDeserializerError::NotFound)?
                .try_into()?,
            owner: account_info
                .owner()
                .ok_or(PlerkleDeserializerError::NotFound)?
                .try_into()?,
            data: PlerkleOptionalU8Vector(account_info.data()).try_into()?,
        })
    }
}

pub struct PlerkleTransactionInfo<'a>(pub plerkle_serialization::TransactionInfo<'a>);

impl<'a> TryFrom<PlerkleTransactionInfo<'a>> for TransactionInfo {
    type Error = PlerkleDeserializerError;

    fn try_from(value: PlerkleTransactionInfo<'a>) -> Result<Self, Self::Error> {
        let tx_info = value.0;

        let slot = tx_info.slot();
        let signature = PlerkleOptionalStr(tx_info.signature()).try_into()?;
        let account_keys = PlerkleOptionalPubkeyVector(tx_info.account_keys()).try_into()?;
        let message_instructions = deserialize_compiled_instructions(
            tx_info
                .outer_instructions()
                .ok_or(PlerkleDeserializerError::NotFound)?,
        )?;
        let compiled = tx_info.compiled_inner_instructions();
        let inner = tx_info.inner_instructions();
        let meta_inner_instructions = if let Some(c) = compiled {
            deserialize_compiled_inner_instructions(c)
        } else {
            deserialize_legacy_inner_instructions(inner.ok_or(PlerkleDeserializerError::NotFound)?)
        }?;

        Ok(Self {
            slot,
            signature,
            account_keys,
            message_instructions,
            meta_inner_instructions,
        })
    }
}
