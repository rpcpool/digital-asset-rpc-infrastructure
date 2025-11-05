use prometheus::{Counter, CounterVec, Opts};

lazy_static::lazy_static! {
    /// It uses the instruction name to label the metric.
    pub static ref BUBBLEGUM_PROGRAM_TRANSFORMER_SUCCESS_COUNT: CounterVec = CounterVec::new(
        Opts::new("bubblegum_program_transformer_success_count", "Total number of program transformer successes"),
        &["instruction"],
    ).unwrap();

    pub static ref BUBBLEGUM_DOWNLOAD_METADATA_NOTIFIER_ERROR_COUNT: Counter = Counter::new(
        "bubblegum_download_metadata_notifier_error_count", "Total number of download metadata notifier errors",
    ).unwrap();
}
