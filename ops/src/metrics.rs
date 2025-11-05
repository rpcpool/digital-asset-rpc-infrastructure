use {
    das_bubblegum::metrics::{BUBBLEGUM_PROGRAM_TRANSFORMER_ERROR_COUNT, BUBBLEGUM_TREE_GAP_COUNT},
    das_core::{METADATA_JSON_DOWNLOAD_ERROR_COUNT, METADATA_JSON_DOWNLOAD_SUCCESS_COUNT},
    http_body_util::Full,
    hyper::{
        body::{Bytes, Incoming},
        service::service_fn,
        Request, Response,
    },
    hyper_util::{rt::TokioIo, server::conn::auto},
    program_transformers::metrics::{
        BUBBLEGUM_DOWNLOAD_METADATA_NOTIFIER_ERROR_COUNT,
        BUBBLEGUM_PROGRAM_TRANSFORMER_SUCCESS_COUNT,
    },
    prometheus::{Registry, TextEncoder},
    std::{convert::Infallible, net::SocketAddr, str::FromStr, sync::Once},
    tokio::net::TcpListener,
    tracing::{error, info},
};

lazy_static::lazy_static! {
    static ref REGISTRY: Registry = Registry::new();
}

fn metrics_handler() -> Result<Response<Full<Bytes>>, Infallible> {
    let metrics = TextEncoder::new()
        .encode_to_string(&REGISTRY.gather())
        .unwrap_or_else(|error| {
            error!("could not encode custom metrics: {error}");
            String::new()
        });

    Ok(Response::builder()
        .header("content-type", "text/plain")
        .body(Full::new(Bytes::from(metrics)))
        .unwrap())
}

async fn handle_metrics_request(
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    match req.uri().path() {
        "/metrics" => metrics_handler(),
        _ => Ok(not_found_handler()),
    }
}

fn not_found_handler() -> Response<Full<Bytes>> {
    Response::builder()
        .status(404)
        .body(Full::new(Bytes::from("Not Found")))
        .unwrap()
}

pub fn run_metrics_server(address: String) -> anyhow::Result<()> {
    let address = SocketAddr::from_str(&address).expect("Invalid prometheus address");

    // Register once
    static REGISTER: Once = Once::new();
    REGISTER.call_once(|| {
        macro_rules! register {
            ($collector:ident) => {
                REGISTRY
                    .register(Box::new($collector.clone()))
                    .expect("collector can't be registered");
            };
        }

        register!(METADATA_JSON_DOWNLOAD_SUCCESS_COUNT);
        register!(METADATA_JSON_DOWNLOAD_ERROR_COUNT);
        register!(BUBBLEGUM_PROGRAM_TRANSFORMER_ERROR_COUNT);
        register!(BUBBLEGUM_PROGRAM_TRANSFORMER_SUCCESS_COUNT);
        register!(BUBBLEGUM_DOWNLOAD_METADATA_NOTIFIER_ERROR_COUNT);
        register!(BUBBLEGUM_TREE_GAP_COUNT);
    });

    tokio::spawn(async move {
        let listener = match TcpListener::bind(address).await {
            Ok(l) => {
                info!("Prometheus server started at http://{address}/metrics");
                l
            }
            Err(e) => {
                error!("Failed to bind Prometheus server: {e:?}");
                return;
            }
        };

        loop {
            let (stream, _) = match listener.accept().await {
                Ok(pair) => pair,
                Err(e) => {
                    error!("Prometheus accept failed: {e:?}");
                    continue;
                }
            };

            let io = TokioIo::new(stream);
            let service = service_fn(move |req: Request<Incoming>| handle_metrics_request(req));

            tokio::spawn(async move {
                let builder = auto::Builder::new(hyper_util::rt::TokioExecutor::new());
                let conn = builder.serve_connection(io, service);
                if let Err(e) = conn.await {
                    error!("Prometheus connection failed: {e:?}");
                }
            });
        }
    });

    Ok(())
}
