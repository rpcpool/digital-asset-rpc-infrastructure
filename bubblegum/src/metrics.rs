use prometheus::{Counter, CounterVec, Histogram, HistogramOpts, Opts};

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

    pub static ref BUBBLEGUM_GAPS_MONITOR_GAPS_PER_TREE: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "bubblegum_gaps_monitor_gaps_per_tree",
            "Number of gaps found in the bubblegum index for a tree"
        )
        .buckets(vec![
            0.0,
            1.0,
            2.0,
            4.0,
            8.0,
            16.0,
            32.0,
            64.0,
            128.0,
            256.0,
            512.0,
            1024.0,
            2048.0,
            4096.0,
            8192.0,
            16384.0,
            32768.0,
            65536.0,
            131072.0,
            262144.0,
            524288.0,
        ]),
    ).unwrap();

    pub static ref BUBBLEGUM_GAPS_MONITOR_LENGTH_PER_GAP: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "bubblegum_gaps_monitor_length_per_gap",
            "Length of a gap in the bubblegum index"
        )
        .buckets(vec![
            0.0,
            1.0,
            2.0,
            4.0,
            8.0,
            16.0,
            32.0,
            64.0,
            128.0,
            256.0,
            512.0,
            1024.0,
            2048.0,
            4096.0,
            8192.0,
            16384.0,
            32768.0,
            65536.0,
            131072.0,
            262144.0,
            524288.0,
        ]),
    ).unwrap();

    pub static ref BUBBLEGUM_GAPS_MONITOR_TIME_PER_TREE: Histogram = Histogram::with_opts(
    HistogramOpts::new(
        "bubblegum_gaps_monitor_time_per_tree",
        "Time taken to monitor gaps for a tree"
    )
    ).unwrap();
}
