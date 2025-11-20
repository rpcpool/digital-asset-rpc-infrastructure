use prometheus::{Counter, CounterVec, Histogram, HistogramOpts, IntGauge, IntGaugeVec, Opts};

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

    pub static ref SCANNED_TREES_COUNT: IntGauge = IntGauge::new(
        "scanned_trees_count", "Total number of trees scanned"
    ).unwrap();

    pub static ref TOTAL_TREES_COUNT: IntGaugeVec = IntGaugeVec::new(
        Opts::new("total_trees_count", "Total number of trees by source (DB or RPC)"),
        &["source"]
    ).unwrap();

    pub static ref BUBBLEGUM_GAPS_MONITOR_TOTAL_GAPS_COUNT: IntGaugeVec = IntGaugeVec::new(
        Opts::new("bubblegum_gaps_monitor_total_gaps_count", "Total number of gaps by type (trees with gaps, gaps, and gaps length)"),
        &["type"]
    ).unwrap();

    pub static ref BUBBLEGUM_GAPS_MONITOR_LAST_GAPS_LENGTH: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "bubblegum_gaps_monitor_last_gaps_length",
            "Length of the last gap in the bubblegum index"
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

    pub static ref BUBBLEGUM_GAPS_MONITOR_TOTAL_CL_AUDITS_V2_COUNT: IntGauge = IntGauge::new(
        "bubblegum_gaps_monitor_total_cl_audits_v2_count", "Total number of cl_audits_v2 in the DB"
    ).unwrap();
}
