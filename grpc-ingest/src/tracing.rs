use {
    opentelemetry_sdk::trace::{self, Sampler},
    std::env,
    tracing::Dispatch,
    tracing_subscriber::{filter::EnvFilter, layer::SubscriberExt},
    tracing_timing::{
        group::{ByMessage, ByName},
        LayerDowncaster,
    },
};

pub fn init() -> anyhow::Result<TracingTimingLayer> {
    let open_tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_service_name(env::var("CARGO_PKG_NAME")?)
        .with_auto_split_batch(true)
        .with_trace_config(trace::config().with_sampler(Sampler::TraceIdRatioBased(0.25)))
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;
    let jeager_layer = tracing_opentelemetry::layer().with_tracer(open_tracer);

    let env_filter = EnvFilter::builder()
        .parse(env::var(EnvFilter::DEFAULT_ENV).unwrap_or_else(|_| "info,sqlx=warn".to_owned()))?;

    let is_atty = atty::is(atty::Stream::Stdout) && atty::is(atty::Stream::Stderr);
    let io_layer = tracing_subscriber::fmt::layer().with_ansi(is_atty);

    let tracing_timing_layer = tracing_timing::Builder::default()
        .layer(|| tracing_timing::Histogram::new_with_max(1_000_000 * 60_000, 2).unwrap());

    let downcaster = tracing_timing_layer.downcaster();

    let registry = tracing_subscriber::registry()
        .with(jeager_layer)
        .with(env_filter)
        .with(tracing_timing_layer)
        .with(io_layer);

    let d = if env::var_os("RUST_LOG_JSON").is_some() {
        let json_layer = tracing_subscriber::fmt::layer().json().flatten_event(true);
        Dispatch::new(registry.with(json_layer))
    } else {
        Dispatch::new(registry)
    };

    let tracing_timing_layer = TracingTimingLayer {
        downcaster,
        dispatch: d.clone(),
    };

    tracing::dispatcher::set_global_default(d).unwrap();

    Ok(tracing_timing_layer)
}

#[derive(Clone)]
pub struct TracingTimingLayer {
    pub downcaster: LayerDowncaster<ByName, ByMessage>,
    pub dispatch: Dispatch,
}

impl TracingTimingLayer {
    pub fn get_tracing_timing(self) {
        let tracing_timing_layer = self.downcaster.downcast(&self.dispatch).unwrap();

        tracing_timing_layer.force_synchronize();

        tracing_timing_layer.with_histograms(|hs| {

            for (span_group, hs) in hs {
                for (event_group, h) in hs {
                    // h.refresh();

                    let mean = h.mean() / 1_000_000.0;
                    let p90 = h.value_at_quantile(0.9) / 1_000_000;
                    let samples = h.len();

                    tracing::warn!(
                        "###### '{span_group}'::'{event_group}' | mean: {mean:.0}ms | P90: {p90}ms | samples_qty: {samples}",
                    );
                }
            }
        });
    }
}
