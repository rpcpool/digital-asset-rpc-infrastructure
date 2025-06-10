use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;
use std::{net::SocketAddr, sync::Once};

use crate::error::DasApiError;
use hyper::{
    server::conn::AddrStream,
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server, StatusCode,
};
use pin_project::pin_project;
use prometheus::{HistogramOpts, HistogramVec, IntCounterVec, Opts, Registry, TextEncoder};
use tracing::{error, info};

lazy_static::lazy_static! {
    static ref REGISTRY: Registry = Registry::new();

    static ref API_CALL_STATUS_COUNT:IntCounterVec = IntCounterVec::new(
        Opts::new("api_call_status_count", "Total number of API calls, grouped by method and status"),
        &["method", "status"]
    ).unwrap();

    pub static ref API_LATENCY_IN_MS: HistogramVec = HistogramVec::new(
        HistogramOpts::new("api_latency_in_milliseconds", "API request latency in milliseconds")
            .buckets(vec![
                1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0,
                1000.0, 2500.0, 5000.0, 10000.0, 30000.0, 60000.0
            ]),
        &["method"]
    ).unwrap();

    pub static ref API_ERRORS: IntCounterVec = IntCounterVec::new(
        Opts::new("api_errors_total", "Number of API errors grouped by method and status code"),
        &["method", "code"]
    ).unwrap();
}

pub fn run_server(address: SocketAddr) -> anyhow::Result<()> {
    static REGISTER: Once = Once::new();

    REGISTER.call_once(|| {
        macro_rules! register {
            ($collector:ident) => {
                REGISTRY
                    .register(Box::new($collector.clone()))
                    .expect("collector can't be registered");
            };
        }
        register!(API_CALL_STATUS_COUNT);
        register!(API_LATENCY_IN_MS);
    });

    let make_service = make_service_fn(move |_: &AddrStream| async move {
        Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| async move {
            let response = match req.uri().path() {
                "/metrics" => metrics_handler(),
                _ => not_found_handler(),
            };
            Ok::<_, hyper::Error>(response)
        }))
    });

    let server = Server::try_bind(&address)?.serve(make_service);
    info!("prometheus server started: http://{address:?}/metrics");

    tokio::spawn(async move {
        if let Err(error) = server.await {
            error!("prometheus server failed: {error:?}");
        }
    });

    Ok(())
}

fn metrics_handler() -> Response<Body> {
    let metrics = TextEncoder::new()
        .encode_to_string(&REGISTRY.gather())
        .unwrap_or_else(|error| {
            error!("could not encode custom metrics: {}", error);
            String::new()
        });
    Response::builder()
        .header("content-type", "text/plain")
        .body(Body::from(metrics))
        .unwrap()
}

fn not_found_handler() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::empty())
        .unwrap()
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum DasApiMethod {
    CheckHealth,
    GetSlot,
    GetAssetProof,
    GetAssetProofs,
    GetAsset,
    GetAssets,
    GetAssetsByOwner,
    GetAssetsByGroup,
    GetAssetsByCreator,
    GetAssetsByAuthority,
    SearchAssets,
    GetAssetSignatures,
    GetTokenAccounts,
    GetNftEditions,
    GetTokenLargestAccounts,
    GetTokenSupply,
    GetTokenAccountsByOwner,
    GetTokenAccountsByDelegate,
}

impl DasApiMethod {
    fn as_str(&self) -> &str {
        match self {
            DasApiMethod::CheckHealth => "checkHealth",
            DasApiMethod::GetSlot => "getSlot",
            DasApiMethod::GetAssetProof => "getAssetProof",
            DasApiMethod::GetAssetProofs => "getAssetProofs",
            DasApiMethod::GetAsset => "getAsset",
            DasApiMethod::GetAssets => "getAssets",
            DasApiMethod::GetAssetsByOwner => "getAssetsByOwner",
            DasApiMethod::GetAssetsByGroup => "getAssetsByGroup",
            DasApiMethod::GetAssetsByCreator => "getAssetsByCreator",
            DasApiMethod::GetAssetsByAuthority => "getAssetsByAuthority",
            DasApiMethod::SearchAssets => "searchAssets",
            DasApiMethod::GetAssetSignatures => "getAssetSignatures",
            DasApiMethod::GetTokenAccounts => "getTokenAccounts",
            DasApiMethod::GetNftEditions => "getNftEditions",
            DasApiMethod::GetTokenLargestAccounts => "getTokenLargestAccounts",
            DasApiMethod::GetTokenSupply => "getTokenSupply",
            DasApiMethod::GetTokenAccountsByOwner => "getTokenAccountsByOwner",
            DasApiMethod::GetTokenAccountsByDelegate => "getTokenAccountsByDelegate",
        }
    }
}

pub fn inc_api_status_count(method: &DasApiMethod, status: &str) {
    API_CALL_STATUS_COUNT
        .with_label_values(&[method.as_str(), status])
        .inc();
}

pub fn record_api_latency(method: &DasApiMethod, time_elapsed_ms: f64) {
    API_LATENCY_IN_MS
        .with_label_values(&[method.as_str()])
        .observe(time_elapsed_ms);
}

pub trait MetricsRecorderExt: Sized {
    fn record_metrics(self, method: DasApiMethod) -> MetricsRecorder<Self>;
}

#[pin_project]
pub struct MetricsRecorder<Fut> {
    method: DasApiMethod,
    #[pin]
    future: Fut,
    start: Option<Instant>,
}

impl<Fut, T> MetricsRecorderExt for Fut
where
    Fut: Future<Output = Result<T, DasApiError>> + Sized,
{
    fn record_metrics(self, method: DasApiMethod) -> MetricsRecorder<Fut> {
        MetricsRecorder {
            method,
            future: self,
            start: None,
        }
    }
}

impl<Fut, T> Future for MetricsRecorder<Fut>
where
    Fut: Future<Output = Result<T, DasApiError>>,
{
    type Output = Result<T, DasApiError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let method = self.method;
        let this = self.project();

        if this.start.is_none() {
            *this.start = Some(Instant::now());
        }

        match this.future.poll(cx) {
            Poll::Ready(result) => {
                let elapsed_ms = this.start.unwrap().elapsed().as_secs_f64() * 1000.0;
                println!("secs :{}", elapsed_ms / 1000.0);
                record_api_latency(&method, elapsed_ms);

                match &result {
                    Ok(_) => inc_api_status_count(&method, "success"),
                    Err(_err) => {
                        //TODO: handle error codes
                        inc_api_status_count(&method, "error")
                    }
                };
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
