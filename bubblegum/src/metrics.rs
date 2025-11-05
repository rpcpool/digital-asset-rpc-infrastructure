use prometheus::{Counter, CounterVec, Opts};

lazy_static::lazy_static! {
    /// It uses the u8 representation of the error type to label the metric.
    pub static ref BUBBLEGUM_PROGRAM_TRANSFORMER_ERROR_COUNT: CounterVec = CounterVec::new(
    Opts::new("bubblegum_program_transformer_error_count", "Total number of program transformer errors"),
    &["error_type"],
    ).unwrap();

    pub static ref BUBBLEGUM_TREE_GAP_COUNT: Counter = Counter::new(
        "bubblegum_tree_gap_count", "Total number of tree gaps",
    ).unwrap();

    pub static ref BUBBLEGUM_RPC_GET_TRANSACTION_COUNT: Counter = Counter::new(
        "bubblegum_rpc_get_transaction_count", "Total number of RPC get transaction calls",
    ).unwrap();

    pub static ref BUBBLEGUM_RPC_GET_SIGNATURES_FOR_ADDRESS_COUNT: Counter = Counter::new(
        "bubblegum_rpc_get_signatures_for_address_count", "Total number of RPC get signatures for address calls",
    ).unwrap();

    pub static ref BUBBLEGUM_RPC_GET_SIGNATURES_FOR_ADDRESS_TOTAL_COUNT: Counter = Counter::new(
        "bubblegum_rpc_get_signatures_for_address_total_count", "Total number of RPC get signatures for address total calls",
    ).unwrap();
}
