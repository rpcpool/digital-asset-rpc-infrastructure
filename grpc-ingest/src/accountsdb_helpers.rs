use std::collections::{HashMap, HashSet};

use serde::Deserialize;
use solana_accounts_db::{
    accounts_db::BankHashStats,
    accounts_hash::{SerdeAccountsDeltaHash, SerdeAccountsHash},
    ancestors::AncestorsForSerialization,
    blockhash_queue::BlockhashQueue,
};
use solana_program::clock::{Epoch, Slot, UnixTimestamp};
use solana_sdk::deserialize_utils::default_on_eof;
use solana_sdk::pubkey::Pubkey;

// Serializable version of AccountStorageEntry for snapshot format
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Deserialize)]
pub struct SerializableAccountStorageEntry {
    pub id: usize, // SerializedAccountsFileId
    pub accounts_current_len: usize,
}

#[derive(Clone, Deserialize, Debug)]
#[allow(dead_code)]
#[allow(deprecated)]
pub struct DeserializableVersionedBank {
    pub blockhash_queue: BlockhashQueue,
    pub ancestors: AncestorsForSerialization,
    pub hash: solana_program::hash::Hash,
    pub parent_hash: solana_program::hash::Hash,
    pub parent_slot: Slot,
    pub hard_forks: solana_sdk::hard_forks::HardForks,
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
    pub _fee_calculator: solana_sdk::fee_calculator::FeeCalculator,
    pub fee_rate_governor: solana_sdk::fee_calculator::FeeRateGovernor,
    pub collected_rent: u64,
    pub rent_collector: solana_accounts_db::rent_collector::RentCollector,
    pub epoch_schedule: solana_sdk::epoch_schedule::EpochSchedule,
    pub inflation: solana_sdk::inflation::Inflation,
    pub stakes: solana_runtime::stakes::Stakes<solana_sdk::stake::state::Delegation>,
    #[allow(dead_code)]
    pub unused_accounts: UnusedAccounts,
    pub epoch_stakes: HashMap<Epoch, solana_runtime::epoch_stakes::EpochStakes>,
    pub is_delta: bool,
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
    #[allow(private_interfaces)] pub BankHashInfo,
    /// all slots that were roots within the last epoch
    #[serde(deserialize_with = "default_on_eof")]
    pub Vec<Slot>,
    /// slots that were roots within the last epoch for which we care about the hash value
    #[serde(deserialize_with = "default_on_eof")]
    pub Vec<(Slot, solana_program::hash::Hash)>,
);

#[derive(Clone, Default, Debug, Deserialize, PartialEq, Eq)]
pub struct BankHashInfo {
    accounts_delta_hash: SerdeAccountsDeltaHash,
    accounts_hash: SerdeAccountsHash,
    stats: BankHashStats,
}
