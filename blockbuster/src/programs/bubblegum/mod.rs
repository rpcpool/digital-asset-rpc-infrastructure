use crate::{
    error::BlockbusterError,
    instruction::InstructionBundle,
    program_handler::{NotUsed, ParseResult, ProgramParser},
    programs::ProgramParseResult,
};
use borsh::de::BorshDeserialize;
use log::warn;
use mpl_bubblegum::{
    get_instruction_type,
    instructions::{
        MintV2InstructionArgs, UnverifyCreatorInstructionArgs, UnverifyCreatorV2InstructionArgs,
        UpdateMetadataInstructionArgs, UpdateMetadataV2InstructionArgs,
        VerifyCreatorInstructionArgs, VerifyCreatorV2InstructionArgs,
    },
    types::{BubblegumEventType, MetadataArgs, UpdateArgs},
};
pub use mpl_bubblegum::{
    types::{LeafSchema, UseMethod},
    InstructionName, LeafSchemaEvent, ID,
};
use solana_sdk::pubkey::Pubkey;

#[derive(Eq, PartialEq)]
pub enum Payload {
    Unknown,
    Mint {
        args: MetadataArgs,
        authority: Pubkey,
        tree_id: Pubkey,
    },
    Decompress {
        args: MetadataArgs,
    },
    CancelRedeem {
        root: Pubkey,
    },
    CreatorVerification {
        metadata: MetadataArgs,
        creator: Pubkey,
        verify: bool,
    },
    CollectionVerification {
        collection: Pubkey,
        verify: bool,
    },
    UpdateMetadata {
        current_metadata: MetadataArgs,
        update_args: UpdateArgs,
        tree_id: Pubkey,
    },
}
//TODO add more of the parsing here to minimize program transformer code
pub struct BubblegumInstruction {
    pub instruction: InstructionName,
    pub tree_update: Option<spl_account_compression::events::ChangeLogEventV1>,
    pub leaf_update: Option<LeafSchemaEvent>,
    pub payload: Option<Payload>,
}

impl BubblegumInstruction {
    pub fn new(ix: InstructionName) -> Self {
        BubblegumInstruction {
            instruction: ix,
            tree_update: None,
            leaf_update: None,
            payload: None,
        }
    }
}

impl ParseResult for BubblegumInstruction {
    fn result_type(&self) -> ProgramParseResult {
        ProgramParseResult::Bubblegum(self)
    }
    fn result(&self) -> &Self
    where
        Self: Sized,
    {
        self
    }
}

pub struct BubblegumParser;

impl ProgramParser for BubblegumParser {
    fn key(&self) -> Pubkey {
        ID
    }

    fn key_match(&self, key: &Pubkey) -> bool {
        key == &ID
    }
    fn handles_account_updates(&self) -> bool {
        false
    }

    fn handles_instructions(&self) -> bool {
        true
    }
    fn handle_account(
        &self,
        _account_data: &[u8],
    ) -> Result<Box<(dyn ParseResult + 'static)>, BlockbusterError> {
        Ok(Box::new(NotUsed::new()))
    }

    fn handle_instruction(
        &self,
        bundle: &InstructionBundle,
    ) -> Result<Box<(dyn ParseResult + 'static)>, BlockbusterError> {
        let InstructionBundle {
            txn_id,
            instruction,
            inner_ix,
            keys,
            ..
        } = bundle;
        let outer_ix_data = match instruction {
            Some(cix) => cix.data.as_ref(),
            _ => return Err(BlockbusterError::DeserializationError),
        };
        let ix_type = get_instruction_type(outer_ix_data);
        let mut b_inst = BubblegumInstruction::new(ix_type);
        if let Some(ixs) = inner_ix {
            for (pid, cix) in ixs.iter() {
                if pid == &spl_noop::id() && !cix.data.is_empty() {
                    use spl_account_compression::events::{
                        AccountCompressionEvent::{self, ApplicationData, ChangeLog},
                        ApplicationDataEvent, ChangeLogEvent,
                    };

                    match AccountCompressionEvent::try_from_slice(&cix.data) {
                        Ok(result) => match result {
                            ChangeLog(changelog_event) => {
                                let ChangeLogEvent::V1(changelog_event) = changelog_event;
                                b_inst.tree_update = Some(changelog_event);
                            }
                            ApplicationData(app_data) => {
                                let ApplicationDataEvent::V1(app_data) = app_data;
                                let app_data = app_data.application_data;
                                b_inst.leaf_update =
                                    Some(get_bubblegum_leaf_schema_event(app_data)?);
                            }
                        },
                        Err(e) => {
                            warn!(
                                "Error while deserializing txn {:?} with spl-noop data: {:?}",
                                txn_id, e
                            );
                        }
                    }
                } else if pid == &mpl_noop::id() && !cix.data.is_empty() {
                    use mpl_account_compression::events::{
                        AccountCompressionEvent::{self, ApplicationData, ChangeLog},
                        ApplicationDataEvent, ChangeLogEvent,
                    };

                    match AccountCompressionEvent::try_from_slice(&cix.data) {
                        Ok(result) => match result {
                            ChangeLog(mpl_changelog_event) => {
                                let ChangeLogEvent::V1(mpl_changelog_event) = mpl_changelog_event;
                                let spl_change_log_event =
                                    convert_mpl_to_spl_change_log_event(mpl_changelog_event);
                                b_inst.tree_update = Some(spl_change_log_event);
                            }
                            ApplicationData(app_data) => {
                                let ApplicationDataEvent::V1(app_data) = app_data;
                                let app_data = app_data.application_data;
                                b_inst.leaf_update =
                                    Some(get_bubblegum_leaf_schema_event(app_data)?);
                            }
                        },
                        Err(e) => {
                            warn!(
                                "Error while deserializing txn {:?} with mpl-noop data: {:?}",
                                txn_id, e
                            );
                        }
                    }
                }
            }
        }

        if outer_ix_data.len() >= 8 {
            let ix_data = &outer_ix_data[8..];
            if !ix_data.is_empty() {
                match b_inst.instruction {
                    InstructionName::MintV1 => {
                        b_inst.payload = Some(build_mint_v1_payload(keys, ix_data, false)?);
                    }

                    InstructionName::MintToCollectionV1 => {
                        b_inst.payload = Some(build_mint_v1_payload(keys, ix_data, true)?);
                    }
                    InstructionName::DecompressV1 => {
                        let args: MetadataArgs = MetadataArgs::try_from_slice(ix_data)?;
                        b_inst.payload = Some(Payload::Decompress { args });
                    }
                    InstructionName::CancelRedeem => {
                        let slice: [u8; 32] = ix_data
                            .try_into()
                            .map_err(|_e| BlockbusterError::InstructionParsingError)?;
                        let root = Pubkey::new_from_array(slice);
                        b_inst.payload = Some(Payload::CancelRedeem { root });
                    }
                    InstructionName::VerifyCreator => {
                        b_inst.payload =
                            Some(build_creator_verification_payload(keys, ix_data, true)?);
                    }
                    InstructionName::UnverifyCreator => {
                        b_inst.payload =
                            Some(build_creator_verification_payload(keys, ix_data, false)?);
                    }
                    InstructionName::VerifyCollection | InstructionName::SetAndVerifyCollection => {
                        b_inst.payload = Some(build_collection_verification_payload(keys, true)?);
                    }
                    InstructionName::UnverifyCollection => {
                        b_inst.payload = Some(build_collection_verification_payload(keys, false)?);
                    }
                    InstructionName::UpdateMetadata => {
                        b_inst.payload = Some(build_update_metadata_payload(keys, ix_data)?);
                    }
                    InstructionName::MintV2 => {
                        b_inst.payload = Some(build_mint_v2_payload(keys, ix_data)?);
                    }
                    InstructionName::SetCollectionV2 => {
                        b_inst.payload = Some(build_set_collection_v2_payload(keys)?);
                    }
                    InstructionName::VerifyCreatorV2 => {
                        b_inst.payload =
                            Some(build_creator_verification_v2_payload(keys, ix_data, true)?);
                    }
                    InstructionName::UnverifyCreatorV2 => {
                        b_inst.payload =
                            Some(build_creator_verification_v2_payload(keys, ix_data, false)?);
                    }
                    InstructionName::UpdateMetadataV2 => {
                        b_inst.payload = Some(build_update_metadata_v2_payload(keys, ix_data)?);
                    }
                    InstructionName::UpdateAssetDataV2 => {} // Not supported
                    _ => {}
                };
            }
        }

        Ok(Box::new(b_inst))
    }
}

fn get_bubblegum_leaf_schema_event(app_data: Vec<u8>) -> Result<LeafSchemaEvent, BlockbusterError> {
    let event_type_byte = if !app_data.is_empty() {
        &app_data[0..1]
    } else {
        return Err(BlockbusterError::DeserializationError);
    };

    match BubblegumEventType::try_from_slice(event_type_byte)? {
        BubblegumEventType::Uninitialized => Err(BlockbusterError::MissingBubblegumEventData),
        BubblegumEventType::LeafSchemaEvent => Ok(LeafSchemaEvent::try_from_slice(&app_data)?),
    }
}

// Convert from mpl-account-compression `ChangeLogEventV1` to
// spl-account-compression `ChangeLogEventV1`.
fn convert_mpl_to_spl_change_log_event(
    mpl_changelog_event: mpl_account_compression::events::ChangeLogEventV1,
) -> spl_account_compression::events::ChangeLogEventV1 {
    spl_account_compression::events::ChangeLogEventV1 {
        id: mpl_changelog_event.id,
        path: mpl_changelog_event
            .path
            .iter()
            .map(|path_node| spl_account_compression::state::PathNode {
                node: path_node.node,
                index: path_node.index,
            })
            .collect(),
        seq: mpl_changelog_event.seq,
        index: mpl_changelog_event.index,
    }
}

// See Bubblegum documentation for offsets and positions:
// https://github.com/metaplex-foundation/mpl-bubblegum/blob/main/programs/bubblegum/README.md#-verify_creator-and-unverify_creator
fn build_creator_verification_payload(
    keys: &[Pubkey],
    ix_data: &[u8],
    verify: bool,
) -> Result<Payload, BlockbusterError> {
    let metadata = if verify {
        VerifyCreatorInstructionArgs::try_from_slice(ix_data)?.metadata
    } else {
        UnverifyCreatorInstructionArgs::try_from_slice(ix_data)?.metadata
    };

    let creator = *keys
        .get(5)
        .ok_or(BlockbusterError::InstructionParsingError)?;

    Ok(Payload::CreatorVerification {
        metadata,
        creator,
        verify,
    })
}

// See Bubblegum for offsets and positions:
// https://github.com/metaplex-foundation/mpl-bubblegum/blob/main/programs/bubblegum/README.md#-verify_collection-unverify_collection-and-set_and_verify_collection
// This uses the account.  The collection is only provided as an argument for `set_and_verify_collection`.
fn build_collection_verification_payload(
    keys: &[Pubkey],
    verify: bool,
) -> Result<Payload, BlockbusterError> {
    let collection = *keys
        .get(8)
        .ok_or(BlockbusterError::InstructionParsingError)?;
    Ok(Payload::CollectionVerification { collection, verify })
}

// See Bubblegum for offsets and positions:
// https://github.com/metaplex-foundation/mpl-bubblegum/blob/main/programs/bubblegum/README.md
fn build_mint_v1_payload(
    keys: &[Pubkey],
    ix_data: &[u8],
    set_verify: bool,
) -> Result<Payload, BlockbusterError> {
    let mut args: MetadataArgs = MetadataArgs::try_from_slice(ix_data)?;
    if set_verify {
        if let Some(ref mut col) = args.collection {
            col.verified = true;
        }
    }

    let authority = *keys
        .first()
        .ok_or(BlockbusterError::InstructionParsingError)?;

    let tree_id = *keys
        .get(3)
        .ok_or(BlockbusterError::InstructionParsingError)?;

    Ok(Payload::Mint {
        args,
        authority,
        tree_id,
    })
}

// See Bubblegum for offsets and positions:
// https://github.com/metaplex-foundation/mpl-bubblegum/blob/main/programs/bubblegum/README.md
fn build_update_metadata_payload(
    keys: &[Pubkey],
    ix_data: &[u8],
) -> Result<Payload, BlockbusterError> {
    let args = UpdateMetadataInstructionArgs::try_from_slice(ix_data)?;

    let tree_id = *keys
        .get(8)
        .ok_or(BlockbusterError::InstructionParsingError)?;

    Ok(Payload::UpdateMetadata {
        current_metadata: args.current_metadata,
        update_args: args.update_args,
        tree_id,
    })
}

// See Bubblegum for offsets and positions:
// https://github.com/metaplex-foundation/mpl-bubblegum/blob/main/programs/bubblegum/README.md
fn build_mint_v2_payload(keys: &[Pubkey], ix_data: &[u8]) -> Result<Payload, BlockbusterError> {
    let args: MintV2InstructionArgs = MintV2InstructionArgs::try_from_slice(ix_data)?;

    let authority = *keys
        .first()
        .ok_or(BlockbusterError::InstructionParsingError)?;

    let tree_id = *keys
        .get(6)
        .ok_or(BlockbusterError::InstructionParsingError)?;

    Ok(Payload::Mint {
        args: args.metadata.into(),
        authority,
        tree_id,
    })
}

// See Bubblegum for offsets and positions:
// https://github.com/metaplex-foundation/mpl-bubblegum/blob/main/programs/bubblegum/README.md#-verify_collection-unverify_collection-and-set_and_verify_collection
// This uses the account.  The collection is only provided as an argument for `set_and_verify_collection`.
fn build_set_collection_v2_payload(keys: &[Pubkey]) -> Result<Payload, BlockbusterError> {
    let collection = *keys
        .get(8)
        .ok_or(BlockbusterError::InstructionParsingError)?;
    Ok(Payload::CollectionVerification {
        collection,
        verify: true,
    })
}

// See Bubblegum documentation for offsets and positions:
// https://github.com/metaplex-foundation/mpl-bubblegum/blob/main/programs/bubblegum/README.md#-verify_creator-and-unverify_creator
fn build_creator_verification_v2_payload(
    keys: &[Pubkey],
    ix_data: &[u8],
    verify: bool,
) -> Result<Payload, BlockbusterError> {
    let metadata = if verify {
        VerifyCreatorV2InstructionArgs::try_from_slice(ix_data)?.metadata
    } else {
        UnverifyCreatorV2InstructionArgs::try_from_slice(ix_data)?.metadata
    };

    let payer = *keys
        .get(1)
        .ok_or(BlockbusterError::InstructionParsingError)?;

    let creator = *keys
        .get(2)
        .ok_or(BlockbusterError::InstructionParsingError)?;

    // Creator is optional in V2, None being signified by the program ID.
    // Creator defaults to the payer.
    let creator = if creator == mpl_bubblegum::ID {
        payer
    } else {
        creator
    };

    Ok(Payload::CreatorVerification {
        metadata: metadata.into(),
        creator,
        verify,
    })
}

// See Bubblegum for offsets and positions:
// https://github.com/metaplex-foundation/mpl-bubblegum/blob/main/programs/bubblegum/README.md
fn build_update_metadata_v2_payload(
    keys: &[Pubkey],
    ix_data: &[u8],
) -> Result<Payload, BlockbusterError> {
    let args = UpdateMetadataV2InstructionArgs::try_from_slice(ix_data)?;

    let tree_id = *keys
        .get(5)
        .ok_or(BlockbusterError::InstructionParsingError)?;

    Ok(Payload::UpdateMetadata {
        current_metadata: args.current_metadata.into(),
        update_args: args.update_args,
        tree_id,
    })
}
