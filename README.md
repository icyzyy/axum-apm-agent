# Axum Elastic APM OpenTelemetry Telemetry Library

A lightweight, production-grade telemetry library designed to integrate **Axum** applications (either **REST APIs** or **Reverse Proxies**) with **Elastic APM** via native **OpenTelemetry (OTLP/HTTP)**.

It captures comprehensive standard OTel/ECS (Elastic Common Schema) HTTP metadata, automatically handles auth tokens and API keys, and dynamically names APM transactions (e.g., `GET /api/v1/users`).

---

## Features

- **Standard OTel & ECS Semantic Conventions**: Automatically records full URL, host, protocol details, user agent, referer, response body bytes, client IP (from socket socket/`ConnectInfo` or `X-Forwarded-For`), and HTTP status codes.
- **Dynamic APM Transaction Naming**: Bypasses compile-time static string limits to format spans dynamically matching their HTTP pathing (e.g., `GET /path`).
- **Flexible Authentication**: Reads native `ELASTIC_APM_SECRET_TOKEN` and `ELASTIC_APM_API_KEY` to automatically structure HTTP headers (`Bearer` or `ApiKey`).

---

## Configuration Environment Variables

Configure your application using the following environment variables:

| Variable | Description | Default |
| :--- | :--- | :--- |
| `ELASTIC_APM_SERVER_URL` | Base URL of the Elastic APM intake server | `http://localhost:8200` |
| `ELASTIC_APM_SERVICE_NAME` | Service name registered in Elastic APM | `rust-service` |
| `ELASTIC_APM_SERVICE_VERSION` | Version of the service | `1.0.0` |
| `ELASTIC_APM_ENVIRONMENT` | Deployment environment (e.g., `production`, `staging`) | `production` |
| `ELASTIC_APM_SECRET_TOKEN` | Bearer Token for APM authentication | None |
| `ELASTIC_APM_API_KEY` | API Key for APM authentication | None |

---

## Integration Guide

### 1. Cargo Dependencies

Add these dependencies to your `Cargo.toml`:

```toml
[dependencies]
axum = { version = "0.8", features = ["macros"] }
tokio = { version = "1.52", features = ["full"] }
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "registry"] }
tower-http = { version = "0.6", features = ["trace"] }
opentelemetry = { version = "0.32", features = ["trace"] }
opentelemetry_sdk = { version = "0.32", features = ["rt-tokio"] }
opentelemetry-otlp = { version = "0.32", features = ["http-proto", "reqwest-blocking-client"] }
tracing-opentelemetry = "0.33"
# The library dependency (if integrated locally or published)
rust-reverse-proxy = { path = "." }
```

### 2. Usage in a REST API

Here is how you would use this library in a typical Axum REST API:

```rust
use axum::{routing::get, Json, Router};
use rust_reverse_proxy::{apm_trace_layer, init_apm};
use serde_json::{json, Value};
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize OpenTelemetry APM provider
    let _tracer_provider = init_apm()?;

    // 2. Define routes and apply the tracing middleware layer
    let app = Router::new()
        .route("/api/hello", get(hello_handler))
        .route("/api/status", get(status_handler))
        .layer(apm_trace_layer());

    // 3. Serve the application with socket connections
    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("REST API listening on {}", addr);
    
    axum::serve(listener, app).await?;

    // Optional: explicit flush/shutdown on exit
    let _ = _tracer_provider.shutdown();
    Ok(())
}

async fn hello_handler() -> Json<Value> {
    Json(json!({ "message": "Hello, world!" }))
}

async fn status_handler() -> Json<Value> {
    Json(json!({ "status": "healthy" }))
}
```

### 3. Usage in a Reverse Proxy

Here is how you would use this library in a dynamic reverse proxy:

```rust
use axum::{
    extract::{Request, State},
    http::{header, StatusCode, Uri},
    response::{IntoResponse, Response},
    Router,
};
use hyper_util::{client::legacy::connect::HttpConnector, rt::TokioExecutor};
use rust_reverse_proxy::{apm_trace_layer, init_apm, AppState, ProxyClient};
use std::net::SocketAddr;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize OpenTelemetry APM provider
    let _tracer_provider = init_apm()?;

    // 2. Create the proxy HTTP client
    let client: ProxyClient = hyper_util::client::legacy::Client::<(), ()>::builder(TokioExecutor::new())
        .build(HttpConnector::new());

    // 3. Read upstream URL once at startup
    let upstream_url = std::env::var("UPSTREAM_URL")
        .unwrap_or_else(|_| "https://httpbin.org".to_string());

    // 4. Create application state
    let state = AppState {
        client,
        upstream_url: Arc::new(upstream_url),
    };

    // 5. Apply the fallback handler and the APM tracing layer
    let app = Router::new()
        .fallback(proxy_handler)
        .with_state(state)
        .layer(apm_trace_layer());

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("Reverse proxy listening on {}", addr);

    axum::serve(listener, app).await?;
    
    let _ = _tracer_provider.shutdown();
    Ok(())
}

async fn proxy_handler(
    State(state): State<AppState>,
    mut req: Request,
) -> Result<Response, StatusCode> {
    let path_query = req.uri().path_and_query().map(|v| v.as_str()).unwrap_or("/");
    let uri_str = format!("{}{}", state.upstream_url.trim_end_matches('/'), path_query);
    
    *req.uri_mut() = uri_str.parse::<Uri>().map_err(|_| StatusCode::BAD_REQUEST)?;

    if let Some(host) = req.uri().host() {
        let host_val = if let Some(port) = req.uri().port_u16() {
            format!("{}:{}", host, port)
        } else {
            host.to_string()
        };
        if let Ok(val) = header::HeaderValue::from_str(&host_val) {
            req.headers_mut().insert(header::HOST, val);
        }
    }

    let response = state.client.request(req).await.map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(response.into_response())
}
```
