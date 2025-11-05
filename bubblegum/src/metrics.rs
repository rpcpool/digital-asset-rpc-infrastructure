use prometheus::{CounterVec, Opts};

lazy_static::lazy_static! {
    /// It uses the u8 representation of the error type to label the metric.
    pub static ref BUBBLEGUM_PROGRAM_TRANSFORMER_ERROR_COUNT: CounterVec = CounterVec::new(
    Opts::new("bubblegum_program_transformer_error_count", "Total number of program transformer errors"),
    &["error_type"],
    ).unwrap();
}
