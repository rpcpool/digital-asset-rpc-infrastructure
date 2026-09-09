// Snapshot deserialization helpers. Mirrors the (private) types in
// `solana_runtime::serde_snapshot` so we can read snapshot bank/accounts-db
// metadata files without depending on those private structs.
//
// Field order, types, and `default_on_eof` decorators MUST stay in lockstep
// with `solana-runtime`'s `serde_snapshot` module — bincode serializes by
// position, not by name, so a drift here would silently misparse snapshots.
// Sourced from solana-runtime 3.1.x.
//
// These snapshot types are marked deprecated upstream (Agave unstable API) but
// are intentionally mirrored here to deserialize snapshots, so suppress the lint
// for the whole module.
#![allow(deprecated)]
use {
    serde::Deserialize,
    solana_accounts_db::blockhash_queue::BlockhashQueue,
    solana_hard_forks::HardForks,
    solana_program::{
        clock::{Epoch, Slot, UnixTimestamp},
        epoch_schedule::EpochSchedule,
        fee_calculator::{FeeCalculator, FeeRateGovernor},
        hash::Hash,
    },
    solana_runtime::{
        bank::BankHashStats, rent_collector::RentCollector, stake_history::StakeHistory,
    },
    solana_sdk::{deserialize_utils::default_on_eof, inflation::Inflation, pubkey::Pubkey},
    solana_stake_interface::state::Delegation,
    solana_vote::vote_account::VoteAccounts,
    std::collections::{HashMap, HashSet},
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize)]
pub struct SerializableAccountStorageEntry {
    pub id: usize, // SerializedAccountsFileId
    pub accounts_current_len: usize,
}

#[derive(Clone, Deserialize, Debug)]
#[allow(dead_code)]
pub struct DeserializableVersionedBank {
    pub blockhash_queue: BlockhashQueue,
    pub _unused_ancestors: HashMap<Slot, usize>,
    pub hash: Hash,
    pub parent_hash: Hash,
    pub parent_slot: Slot,
    pub hard_forks: HardForks,
    pub transaction_count: u64,
    pub tick_height: u64,
    pub signature_count: u64,
    pub capitalization: u64,
    pub max_tick_height: u64,
    pub hashes_per_tick: Option<u64>,
    pub ticks_per_slot: u64,
    pub ns_per_slot: u128,
    pub genesis_creation_time: UnixTimestamp,
    pub slots_per_year: f64,
    pub accounts_data_len: u64,
    pub slot: Slot,
    pub epoch: Epoch,
    pub block_height: u64,
    pub collector_id: Pubkey,
    pub collector_fees: u64,
    pub _fee_calculator: FeeCalculator,
    pub fee_rate_governor: FeeRateGovernor,
    pub _collected_rent: u64,
    pub rent_collector: RentCollector,
    pub epoch_schedule: EpochSchedule,
    pub inflation: Inflation,
    pub stakes: DeserializableStakes,
    pub unused_accounts: UnusedAccounts,
    pub unused_epoch_stakes: HashMap<Epoch, ()>,
    pub is_delta: bool,
}

/// Mirrors `solana_runtime::serde_snapshot::DeserializableStakes`, which is
/// crate-private. `Stakes<T>` itself is no longer `Deserialize`.
#[derive(Clone, Debug, Deserialize)]
#[allow(dead_code)]
pub struct DeserializableStakes {
    pub vote_accounts: VoteAccounts,
    pub stake_delegations: Vec<(Pubkey, Delegation)>,
    pub unused: u64,
    pub epoch: Epoch,
    pub stake_history: StakeHistory,
}

#[derive(Default, Clone, PartialEq, Eq, Debug, Deserialize)]
pub struct UnusedAccounts {
    unused1: HashSet<Pubkey>,
    unused2: HashSet<Pubkey>,
    unused3: HashMap<Pubkey, u64>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct AccountsDbFields<T>(
    /// Careful! This contains entries for all historical slots with accounts, not only the slots
    ///  for the snapshot (even if it's incremental)
    pub HashMap<Slot, Vec<T>>,
    pub u64, // obsolete, formerly write_version
    pub Slot,
    pub BankHashInfo,
    /// all slots that were roots within the last epoch
    #[serde(deserialize_with = "default_on_eof")]
    pub Vec<Slot>,
    /// slots that were roots within the last epoch for which we care about the hash value
    #[serde(deserialize_with = "default_on_eof")]
    pub Vec<(Slot, Hash)>,
);

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
pub struct BankHashInfo {
    obsolete_accounts_delta_hash: [u8; 32],
    obsolete_accounts_hash: [u8; 32],
    stats: BankHashStats,
}
