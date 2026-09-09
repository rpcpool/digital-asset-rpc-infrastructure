// V1-aware replacement for Plerkle's transaction serializer.
// See core/README.md for why this does not use plerkle_serialization's own.
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use plerkle_serialization::{
    error::PlerkleSerializationError, CompiledInnerInstruction, CompiledInnerInstructionArgs,
    CompiledInnerInstructions, CompiledInnerInstructionsArgs, CompiledInstruction,
    CompiledInstructionArgs, Pubkey as FBPubkey, TransactionInfo, TransactionInfoArgs,
    TransactionVersion,
};
use solana_sdk::{message::VersionedMessage, transaction::VersionedTransaction};
use solana_transaction_status::{
    option_serializer::OptionSerializer, EncodedConfirmedTransactionWithStatusMeta, UiInstruction,
    UiTransactionStatusMeta,
};

/// Plerkle's schema predates transaction v1, but the enum is an open `i8` on the
/// wire and its verifier only range-checks as `i8`. Value 2 extends Legacy (0)
/// and V0 (1) without changing the FlatBuffer layout.
pub const PLERKLE_TRANSACTION_VERSION_V1: TransactionVersion = TransactionVersion(2);

fn serialization_error(e: impl ToString) -> PlerkleSerializationError {
    PlerkleSerializationError::SerializationError(e.to_string())
}

/// Serialize an RPC transaction into the Plerkle transaction stream format.
pub fn serialize_encoded_transaction_with_status<'a>(
    mut builder: FlatBufferBuilder<'a>,
    tx: EncodedConfirmedTransactionWithStatusMeta,
) -> Result<FlatBufferBuilder<'a>, PlerkleSerializationError> {
    let meta: UiTransactionStatusMeta = tx.transaction.meta.ok_or_else(|| {
        PlerkleSerializationError::SerializationError(
            "Missing meta data for transaction".to_string(),
        )
    })?;

    let ui_transaction: VersionedTransaction =
        tx.transaction.transaction.decode().ok_or_else(|| {
            PlerkleSerializationError::SerializationError(
                "Transaction cannot be decoded".to_string(),
            )
        })?;

    let msg = ui_transaction.message;
    let atl_keys = msg.address_table_lookups();
    let sig = ui_transaction.signatures[0].to_string();

    // Static keys first, then lookup-table loaded addresses. v1 has no lookup
    // tables, so `atl_keys` is None and only the static keys are used.
    let account_keys = {
        let mut account_keys_fb_vec = msg
            .static_account_keys()
            .iter()
            .map(|key| FBPubkey(key.to_bytes()))
            .collect::<Vec<_>>();

        if atl_keys.is_some() {
            if let OptionSerializer::Some(ad) = &meta.loaded_addresses {
                for i in ad.writable.iter().chain(ad.readonly.iter()) {
                    let mut output: [u8; 32] = [0; 32];
                    bs58::decode(i)
                        .into(&mut output)
                        .map_err(serialization_error)?;
                    account_keys_fb_vec.push(FBPubkey(output));
                }
            }
        }

        (!account_keys_fb_vec.is_empty()).then(|| builder.create_vector(&account_keys_fb_vec))
    };

    let log_messages = if let OptionSerializer::Some(log_messages) = &meta.log_messages {
        let mut log_messages_fb_vec = Vec::with_capacity(log_messages.len());
        for message in log_messages {
            log_messages_fb_vec.push(builder.create_string(message));
        }
        Some(builder.create_vector(&log_messages_fb_vec))
    } else {
        None
    };

    let inner_instructions = if let OptionSerializer::Some(inner_instructions_vec) =
        meta.inner_instructions.as_ref()
    {
        let mut overall_fb_vec = Vec::with_capacity(inner_instructions_vec.len());
        for inner_instructions in inner_instructions_vec.iter() {
            let index = inner_instructions.index;
            let mut instructions_fb_vec = Vec::with_capacity(inner_instructions.instructions.len());
            for ui_instruction in inner_instructions.instructions.iter() {
                if let UiInstruction::Compiled(ui_compiled_instruction) = ui_instruction {
                    let program_id_index = ui_compiled_instruction.program_id_index;
                    let accounts = Some(builder.create_vector(&ui_compiled_instruction.accounts));
                    let data = bs58::decode(&ui_compiled_instruction.data)
                        .into_vec()
                        .map_err(serialization_error)?;
                    let data = Some(builder.create_vector(&data));
                    let compiled = CompiledInstruction::create(
                        &mut builder,
                        &CompiledInstructionArgs {
                            program_id_index,
                            accounts,
                            data,
                        },
                    );
                    instructions_fb_vec.push(CompiledInnerInstruction::create(
                        &mut builder,
                        &CompiledInnerInstructionArgs {
                            compiled_instruction: Some(compiled),
                            stack_height: 0, // Unused by DAS consumers
                        },
                    ));
                }
            }

            let instructions = Some(builder.create_vector(&instructions_fb_vec));
            overall_fb_vec.push(CompiledInnerInstructions::create(
                &mut builder,
                &CompiledInnerInstructionsArgs {
                    index,
                    instructions,
                },
            ));
        }

        Some(builder.create_vector(&overall_fb_vec))
    } else {
        let empty: Vec<WIPOffset<CompiledInnerInstructions>> = Vec::new();
        Some(builder.create_vector(empty.as_slice()))
    };

    let outer_instructions = msg.instructions();
    let outer_instructions = if !outer_instructions.is_empty() {
        let mut instructions_fb_vec = Vec::with_capacity(outer_instructions.len());
        for compiled_instruction in outer_instructions.iter() {
            let program_id_index = compiled_instruction.program_id_index;
            let accounts = Some(builder.create_vector(&compiled_instruction.accounts));
            let data = Some(builder.create_vector(&compiled_instruction.data));
            instructions_fb_vec.push(CompiledInstruction::create(
                &mut builder,
                &CompiledInstructionArgs {
                    program_id_index,
                    accounts,
                    data,
                },
            ));
        }
        Some(builder.create_vector(&instructions_fb_vec))
    } else {
        None
    };

    let version = match msg {
        VersionedMessage::Legacy(_) => TransactionVersion::Legacy,
        VersionedMessage::V0(_) => TransactionVersion::V0,
        VersionedMessage::V1(_) => PLERKLE_TRANSACTION_VERSION_V1,
    };

    let sig_db = builder.create_string(&sig);
    let transaction_info = TransactionInfo::create(
        &mut builder,
        &TransactionInfoArgs {
            is_vote: false,
            account_keys,
            log_messages,
            inner_instructions: None,
            outer_instructions,
            slot: tx.slot,
            seen_at: 0,
            slot_index: None,
            signature: Some(sig_db),
            compiled_inner_instructions: inner_instructions,
            version,
        },
    );

    builder.finish(transaction_info, None);
    Ok(builder)
}
