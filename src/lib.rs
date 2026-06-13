//! # Elastic APM/OpenTelemetry Tracing Library for Axum
//!
//! This library provides helper functions to configure OpenTelemetry tracing for Elastic APM
//! in Axum applications (both REST APIs and reverse proxies).
//!
//! It supports standard Elastic APM environment variables:
//! - `ELASTIC_APM_SERVER_URL` (default: `http://localhost:8200`)
//! - `ELASTIC_APM_SERVICE_NAME` (default: `rust-service`)
//! - `ELASTIC_APM_SERVICE_VERSION` (default: `1.0.0`)
//! - `ELASTIC_APM_ENVIRONMENT` (default: `production`)
//! - `ELASTIC_APM_SECRET_TOKEN` (authorization bearer token)
//! - `ELASTIC_APM_API_KEY` (authorization api key)

use axum::{
    extract::{ConnectInfo, Request, State},
    http::{header, Version, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use opentelemetry::{global, KeyValue};
use opentelemetry_otlp::{WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{trace::SdkTracerProvider, Resource};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tower_http::trace::TraceLayer;
use tracing::{info, info_span, Span};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Application state containing the HTTP client and upstream configuration.
#[derive(Clone)]
pub struct AppState {
    pub client: ProxyClient,
    pub upstream_url: Arc<String>,
}

/// Helper function to extract a header value as a string.
fn extract_header(headers: &axum::http::HeaderMap, header_name: axum::http::header::HeaderName) -> String {
    headers
        .get(header_name)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("")
        .to_string()
}

/// Initializes the OpenTelemetry tracing provider and registers it globally
/// alongside a standard terminal logging layer.
///
/// Returns the `SdkTracerProvider` which should be kept alive for the duration
/// of the application to ensure spans are correctly flushed.
pub fn init_apm() -> Result<SdkTracerProvider, Box<dyn std::error::Error>> {
    let service_name = std::env::var("ELASTIC_APM_SERVICE_NAME")
        .unwrap_or_else(|_| "rust-service".to_string());
    let service_version = std::env::var("ELASTIC_APM_SERVICE_VERSION")
        .unwrap_or_else(|_| "1.0.0".to_string());
    let apm_environment = std::env::var("ELASTIC_APM_ENVIRONMENT")
        .unwrap_or_else(|_| "production".to_string());
    let apm_server_url = std::env::var("ELASTIC_APM_SERVER_URL")
        .unwrap_or_else(|_| "http://localhost:8200".to_string());

    // Get node name from system hostname (cross-platform)
    let apm_node_name = hostname::get()
        .ok()
        .map(|h| h.to_string_lossy().into_owned());

    // Construct the OTLP HTTP traces target endpoint (Elastic APM listens on /v1/traces)
    let traces_endpoint = format!("{}/v1/traces", apm_server_url.trim_end_matches('/'));

    // Extract authorization credentials if provided in ELASTIC_APM_SECRET_TOKEN or ELASTIC_APM_API_KEY
    let mut headers = HashMap::new();
    if let Ok(token) = std::env::var("ELASTIC_APM_SECRET_TOKEN") {
        let token = token.trim();
        if !token.is_empty() {
            headers.insert("Authorization".to_string(), format!("Bearer {}", token));
        }
    } else if let Ok(api_key) = std::env::var("ELASTIC_APM_API_KEY") {
        let api_key = api_key.trim();
        if !api_key.is_empty() {
            headers.insert("Authorization".to_string(), format!("ApiKey {}", api_key));
        }
    }

    info!(
        "Initializing OpenTelemetry OTLP Exporter targeting: {}",
        traces_endpoint
    );

    // 1. Build the OTLP Span Exporter using HTTP
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(traces_endpoint)
        .with_headers(headers)
        .build()?;

    // Build resource attributes
    let mut attributes = vec![
        KeyValue::new("service.version", service_version),
        KeyValue::new("deployment.environment", apm_environment),
    ];
    if let Some(node_name) = apm_node_name {
        attributes.push(KeyValue::new("service.node.name", node_name));
    }

    // 2. Build the TracerProvider using a batch processor
    let provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_service_name(service_name.clone())
                .with_attributes(attributes)
                .build(),
        )
        .build();

    // 3. Set global tracer provider
    global::set_tracer_provider(provider.clone());

    // 4. Configure local logging filter
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer();

    // 5. Connect tracing subscriber with OpenTelemetry registry layer
    use opentelemetry::trace::TracerProvider;
    let tracer = provider.tracer(service_name.clone());
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    Ok(provider)
}

/// Creates a `TraceLayer` pre-configured to record standard Elastic APM / OpenTelemetry
/// semantic conventions for incoming HTTP requests and responses.
pub fn apm_trace_layer() -> TraceLayer<
    tower_http::classify::SharedClassifier<tower_http::classify::ServerErrorsAsFailures>,
    impl Fn(&Request) -> Span + Clone,
    tower_http::trace::DefaultOnRequest,
    impl Fn(&Response, std::time::Duration, &Span) + Clone,
    tower_http::trace::DefaultOnBodyChunk,
    tower_http::trace::DefaultOnEos,
    tower_http::trace::DefaultOnFailure,
> {
    TraceLayer::new_for_http()
        .make_span_with(|request: &Request<_>| {
            let path = request.uri().path();
            let query = request.uri().query().unwrap_or("");
            let method = request.method().as_str();
            let scheme = request.uri().scheme_str().unwrap_or("http");

            // Map standard HTTP version
            let http_version = match request.version() {
                Version::HTTP_09 => "0.9",
                Version::HTTP_10 => "1.0",
                Version::HTTP_11 => "1.1",
                Version::HTTP_2 => "2.0",
                Version::HTTP_3 => "3.0",
                _ => "1.1",
            };

            // Extract domain and port from the request Host header or URI
            let mut domain = request.uri().host().unwrap_or("").to_string();
            let mut port = request.uri().port_u16().unwrap_or(0);

            let host_header = request.headers()
                .get(header::HOST)
                .and_then(|h| h.to_str().ok());

            let host = match host_header {
                Some(h) => h.to_string(),
                None => {
                    if domain.is_empty() {
                        "localhost".to_string()
                    } else {
                        domain.clone()
                    }
                }
            };

            if domain.is_empty() {
                let parts: Vec<&str> = host.split(':').collect();
                domain = parts[0].to_string();
                if parts.len() > 1 {
                    port = parts[1].parse::<u16>().unwrap_or(0);
                }
            }
            if port == 0 {
                port = if scheme == "https" { 443 } else { 80 };
            }

            let path_query = request.uri().path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
            let url_full = format!("{}://{}{}", scheme, host, path_query);

            // Extract client IP and port from ConnectInfo (injected by axum::serve)
            let (client_ip, client_port) = request.extensions()
                .get::<ConnectInfo<SocketAddr>>()
                .map(|ConnectInfo(addr)| (addr.ip().to_string(), addr.port()))
                .unwrap_or_else(|| {
                    // Fallback to X-Forwarded-For if socket connection info is not available
                    let ip = request.headers()
                        .get("x-forwarded-for")
                        .and_then(|h| h.to_str().ok())
                        .and_then(|s| s.split(',').next())
                        .map(|s| s.trim().to_string())
                        .unwrap_or_default();
                    (ip, 0)
                });

            // Extract User Agent
            let user_agent = extract_header(request.headers(), header::USER_AGENT);

            // Extract Referrer header
            let referrer = extract_header(request.headers(), header::REFERER);

            // Extract Content-Type
            let content_type = extract_header(request.headers(), header::CONTENT_TYPE);

            // Extract Accept header
            let accept = extract_header(request.headers(), header::ACCEPT);

            // Format transaction name dynamically as "GET /path"
            let otel_name = format!("{} {}", method, path);

            // Declare and record standard OTel/ECS HTTP semantic fields
            info_span!(
                "http_request",
                "otel.name" = otel_name,
                "http.request.method" = method,
                "url.full" = url_full,
                "url.path" = path,
                "url.query" = query,
                "url.scheme" = scheme,
                "url.domain" = domain.clone(),
                "url.port" = port as i64,
                "server.address" = domain,
                "server.port" = port as i64,
                "client.ip" = client_ip.clone(),
                "client.address" = client_ip.clone(),
                "client.port" = client_port as i64,
                "network.peer.address" = client_ip,
                "network.peer.port" = client_port as i64,
                "user_agent.original" = user_agent,
                "http.request.header.referer" = referrer,
                "http.request.header.content-type" = content_type,
                "http.request.header.accept" = accept,
                "http.version" = http_version,
                "network.protocol.name" = "http",
                "network.protocol.version" = http_version,
                "http.response.status_code" = tracing::field::Empty,
                "http.response.body.bytes" = tracing::field::Empty,
                "http.response.header.content-type" = tracing::field::Empty,
            )
        })
        .on_response(|response: &Response<_>, _latency: std::time::Duration, span: &Span| {
            // Record status code
            span.record("http.response.status_code", response.status().as_u16());

            // Record response size
            if let Some(content_length) = response.headers().get(header::CONTENT_LENGTH)
                .and_then(|h| h.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok()) {
                span.record("http.response.body.bytes", content_length);
            }

            // Record response Content-Type
            if let Some(content_type) = response.headers().get(header::CONTENT_TYPE)
                .and_then(|h| h.to_str().ok()) {
                span.record("http.response.header.content-type", content_type);
            }
        })
}

/// Type alias for the Hyper legacy client used to forward requests.
pub type ProxyClient = hyper_util::client::legacy::Client<
    hyper_util::client::legacy::connect::HttpConnector,
    axum::body::Body,
>;

/// Default handler for proxying incoming HTTP requests to an upstream target.
///
/// Uses the upstream URL from `AppState` instead of reading environment variables
/// on each request for better performance.
#[tracing::instrument(skip(state, req), fields(upstream_uri))]
pub async fn proxy_handler(
    State(state): State<AppState>,
    mut req: Request,
) -> Result<Response, StatusCode> {
    // Retrieve path and query from the incoming request URI
    let path_query = req
        .uri()
        .path_and_query()
        .map(|v| v.as_str())
        .unwrap_or("/");

    // Construct the destination URI pointing to the upstream server
    let uri_str = format!("{}{}", state.upstream_url.trim_end_matches('/'), path_query);
    let target_uri = uri_str.parse::<Uri>().map_err(|e| {
        tracing::error!("Failed to parse target URI: {}", e);
        StatusCode::BAD_REQUEST
    })?;

    // Record target URI and details in the current tracing span
    let span = Span::current();
    span.record("upstream_uri", &uri_str);
    
    // Update the request URI to target the upstream server
    *req.uri_mut() = target_uri;

    // Rewrite the Host header to match the new destination
    if let Some(host) = req.uri().host() {
        let host_val = if let Some(port) = req.uri().port_u16() {
            format!("{}:{}", host, port)
        } else {
            host.to_string()
        };

        if let Ok(header_val) = header::HeaderValue::from_str(&host_val) {
            req.headers_mut().insert(header::HOST, header_val);
        }
    }

    // Forward the request to the upstream using the hyper client
    let response = state.client.request(req).await.map_err(|e| {
        tracing::error!("Failed to proxy request: {}", e);
        StatusCode::BAD_GATEWAY
    })?;

    Ok(response.into_response())
}
