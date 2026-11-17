# Axum Elastic APM OpenTelemetry Telemetry Library

A lightweight, production-grade telemetry library designed to integrate **Axum** applications with **Elastic APM** via native **OpenTelemetry (OTLP/HTTP)**.

It captures comprehensive standard OTel/ECS (Elastic Common Schema) HTTP metadata, automatically handles auth tokens and API keys, dynamically names APM transactions (e.g., `GET /api/v1/users`), and propagates W3C trace context (`traceparent`) across service boundaries.

---

## Features

- **Standard OTel & ECS Semantic Conventions**: Automatically records full URL, host, protocol details, user agent, referer, response body bytes (when the response sends `Content-Length`), client IP (from the socket via `ConnectInfo` or `X-Forwarded-For`), and HTTP status codes.
- **Route-Pattern Transaction Naming**: Names transactions by the matched **route pattern** (e.g. `GET /users/{id}`) via axum's `MatchedPath`, keeping the APM transaction view bounded instead of fragmenting it per concrete path. Falls back to the actual path when no route matched, and records `http.route = "unknown route"` for 404/405 — mirroring the Go agent's `apmechov4` naming.
- **Panic & Error Capture**: An optional `apm_panic_recovery_layer()` recovers handler panics into a `500` response (instead of dropping the connection) and records them as APM errors (`exception` span event + error status). 5xx handler responses are already captured as APM errors via the span status.
- **Zero-Overhead Disable Switch**: `ELASTIC_APM_ACTIVE` / `ELASTIC_APM_RECORDING` (both default `true`) short-circuit the trace layer to a no-op when disabled, so APM can be turned off with no tracing/OTel overhead (no OTel span is created or exported).
- **Server Span Kind**: HTTP request spans are exported with `SpanKind::Server` so APM categorizes them correctly in the service map.
- **W3C Trace Context Propagation**: Incoming `traceparent` headers continue the caller's trace; [`inject_trace_context`](https://docs.rs/axum-apm/latest/axum_apm/fn.inject_trace_context.html) carries it into outgoing requests (e.g. when you call an upstream service), so end-to-end traces stay connected.
- **Flexible Authentication**: Reads native `ELASTIC_APM_SECRET_TOKEN` and `ELASTIC_APM_API_KEY` to automatically structure HTTP headers (`Bearer` or `ApiKey`).
- **Transaction Sampling**: Configurable URL ignore patterns (exact, glob, regex) and per-transaction sample rate to reduce APM overhead on health checks and high-traffic endpoints.

> **Note on auth tokens:** the OTLP exporter URL-decodes header values before sending. If your secret token or API key contains `%XX` sequences, they will be altered in transit — avoid such characters in credentials.

---

## Configuration Environment Variables

Configure your application using the following environment variables:

| Variable | Description | Default |
| :--- | :--- | :--- |
| `ELASTIC_APM_SERVER_URL` | Base URL of the Elastic APM intake server | `http://localhost:8200` |
| `ELASTIC_APM_SERVICE_NAME` | Service name registered in Elastic APM | `rust-service` |
| `ELASTIC_APM_SERVICE_VERSION` | Version of the service | `1.0.0` |
| `ELASTIC_APM_ENVIRONMENT` | Deployment environment (e.g., `production`, `staging`) | `production` |
| `ELASTIC_APM_NODE_NAME` | Node name reported to APM | system hostname |
| `ELASTIC_APM_SECRET_TOKEN` | Bearer Token for APM authentication | None |
| `ELASTIC_APM_API_KEY` | API Key for APM authentication | None |
| `ELASTIC_APM_TRANSACTION_IGNORE_URLS` | Comma-separated URL patterns to exclude from tracing | (none) |
| `ELASTIC_APM_TRANSACTION_SAMPLE_RATE` | Float 0.0–1.0, sampling rate for transactions | `1.0` |
| `ELASTIC_APM_ACTIVE` | Master switch; when `false` the trace layer creates and exports no spans | `true` |
| `ELASTIC_APM_RECORDING` | When `false` the trace layer creates and exports no spans (agent active but not recording) | `true` |

---

## Transaction Sampling

### Ignoring URLs

Use `ELASTIC_APM_TRANSACTION_IGNORE_URLS` to exclude specific URLs from being traced. This is useful for health checks, metrics endpoints, and other high-frequency, low-value traffic:

```bash
# Exact match
ELASTIC_APM_TRANSACTION_IGNORE_URLS=/health,/metrics

# Glob patterns with wildcard
ELASTIC_APM_TRANSACTION_IGNORE_URLS=/health,/metrics,/status/*

# Regular expressions
ELASTIC_APM_TRANSACTION_IGNORE_URLS=^/api/v1/health.*,/health

# Mix of all pattern types
ELASTIC_APM_TRANSACTION_IGNORE_URLS=/health,/metrics,^/api/v[0-9]+/internal.*
```

**Pattern types:**
- **Exact match**: `/health` — matches only the exact path (not `/healthcheck` or `/health/live`)
- **Glob**: `/status/*` or `/api/v1/**` — `*` matches any sequence of characters, `?` matches exactly one character
- **Regex**: `^/api/v1/health.*` — full regex support; patterns starting with `^` or ending with `$` are treated as regex

### Sampling Rate

Use `ELASTIC_APM_TRANSACTION_SAMPLE_RATE` to control what fraction of transactions are sampled:

```bash
# Sample all transactions (default)
ELASTIC_APM_TRANSACTION_SAMPLE_RATE=1.0

# Sample 50% of transactions
ELASTIC_APM_TRANSACTION_SAMPLE_RATE=0.5

# Sample 10% of transactions
ELASTIC_APM_TRANSACTION_SAMPLE_RATE=0.1

# Disable sampling (drop all transactions)
ELASTIC_APM_TRANSACTION_SAMPLE_RATE=0.0
```

The sampler uses **trace-id-ratio-based** sampling, which is compatible with distributed tracing:
- **Respects parent decisions**: If a parent span is sampled, all child spans are sampled (ensures trace consistency)
- **Deterministic per trace**: The same trace ID always produces the same sampling decision
- **Ignores first**: URLs matching `ELASTIC_APM_TRANSACTION_IGNORE_URLS` are dropped before the sampling rate is applied
- **Fail-safe**: An invalid value (e.g. `0,1`, `nan`, `2.0`) logs a warning and falls back to `1.0`

---

## Transaction Naming & Error Capture

### Route-Pattern Transaction Naming

By default, APM transactions are named by the **matched route pattern** (e.g. `GET /users/{id}`) rather than the concrete path (e.g. `GET /users/123`). This keeps the APM transaction view bounded — a single transaction per route, not one per ID.

The library reads axum's [`MatchedPath`](https://docs.rs/axum/latest/axum/extract/struct.MatchedPath.html) request extension, which axum populates after route matching. Because the tracing layer is applied to the router (via `.layer(...)`), axum runs the layer *after* matching, so the pattern is available when the span is created.

- **Matched route** → span named `METHOD + " " + pattern`, and `http.route` set to the pattern.
- **No route matched** (e.g. a reverse-proxy fallback) → span named by the actual path.
- **404 / 405** → `http.route` recorded as `"unknown route"`, mirroring the Go agent's `UnknownRouteRequestName`.

> **Note:** a matched route whose *handler* returns 404/405 (e.g. "resource not found") has its `http.route` overwritten to `"unknown route"` at response time; the span name (the APM transaction name) remains the route pattern, so the transaction view is unaffected.

### Panic & Error Capture

Handler panics are not recovered by axum by default — the connection is dropped and nothing reaches APM. The optional `apm_panic_recovery_layer()` fixes this:

- Recovers a handler panic into a `500 Internal Server Error` response (the connection is not dropped).
- Records the panic on the APM span: an `exception` span event (`exception.type = "#Panic"`, `exception.message = <panic message>`) plus `error.type` / `error.message` fields, so it appears in APM's **Errors** section.

Apply it *inside* `apm_trace_layer()` so the APM span is active when the panic is caught:

```rust
let app = Router::new()
    .route("/users/{id}", get(user_handler))
    .layer(axum_apm::apm_panic_recovery_layer()) // inner: recovers panics
    .layer(axum_apm::apm_trace_layer());         // outer: creates the span
```

5xx handler responses are already captured as APM errors (the trace layer sets the span status to `Error` for server errors), so no extra layer is needed for non-panic errors.

### Disabling APM

Set `ELASTIC_APM_ACTIVE=false` or `ELASTIC_APM_RECORDING=false` to turn APM off. When disabled, the trace layer returns a no-op span, so no OTel span is created or exported — a zero-overhead short-circuit (mirroring the Go agent's `Recording()` check). This is useful for local development or for temporarily disabling APM without code changes.

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
serde_json = "1"
# The library dependency (if integrated locally or published)
axum-apm = { path = "." }
```

### 2. Usage in a REST API

Here is how you would use this library in a typical Axum REST API:

```rust
use axum::{routing::get, Json, Router};
use axum_apm::{apm_otel_layer, apm_panic_recovery_layer, apm_trace_layer, init_apm};
use serde_json::{json, Value};
use std::net::SocketAddr;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize the OpenTelemetry APM provider
    let (tracer_provider, config) = init_apm()?;

    // 2. Compose the OTel layer into your own tracing subscriber
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .with(apm_otel_layer(&tracer_provider))
        .init();

    // Surface configuration warnings now that the subscriber can receive them
    for warning in &config.warnings {
        tracing::warn!("{warning}");
    }

    // 3. Define routes and apply the APM layers:
    //    - apm_panic_recovery_layer (inner): recovers handler panics into a 500
    //      and records them as APM errors.
    //    - apm_trace_layer (outer): creates the APM span (named by the route
    //      pattern when the request matches a route).
    let app = Router::new()
        .route("/api/hello", get(hello_handler))
        .route("/api/status", get(status_handler))
        .layer(apm_panic_recovery_layer())
        .layer(apm_trace_layer());

    // 4. Serve the application
    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("REST API listening on {}", addr);

    let server = axum::serve(listener, app);
    let shutdown_signal = async {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Received shutdown signal, starting graceful shutdown...");
    };
    server.with_graceful_shutdown(shutdown_signal).await?;

    // Flush remaining spans before exit
    let _ = tracer_provider.shutdown();
    Ok(())
}

async fn hello_handler() -> Json<Value> {
    Json(json!({ "message": "Hello, world!" }))
}

async fn status_handler() -> Json<Value> {
    Json(json!({ "status": "healthy" }))
}
```

### 3. Propagating the Trace to Outgoing Requests

If your application calls other services (an upstream API, a database gateway, ...), use `inject_trace_context` to write the current W3C trace context (`traceparent`/`tracestate`) into the outgoing request headers, so the downstream service continues the same trace:

```rust
use axum::extract::Request;
use axum_apm::inject_trace_context;
use opentelemetry::Context;
use tracing::Span;

async fn call_upstream(mut req: Request) -> Result<axum::response::Response, axum::http::StatusCode> {
    // The APM span created by apm_trace_layer is the current span, so its
    // context carries the (possibly continued) trace ID.
    let cx = Span::current().context();
    inject_trace_context(&cx, &mut req);
    // ... send `req` with your HTTP client ...
}
```

> `inject_trace_context` is a no-op for the headers when the current span has no valid trace context (e.g. APM is disabled), so it is safe to call unconditionally.

---

## Local Development

A `docker-compose.yml` is provided with a local Elastic stack (APM Server + Elasticsearch + Kibana, all 8.19.x) for testing:

```bash
docker compose up -d
```

- APM Server: `http://localhost:8200` (OTLP/HTTP intake at `/v1/traces`)
- Kibana: `http://localhost:5601` (APM app)
- Elasticsearch: `http://localhost:9200`

> **APM Server version requirement:** OTLP/HTTP intake requires APM Server **8.3.0 or newer**. The compose file pins 8.19.x.

Then build a small Axum app with the recipe in [Usage in a REST API](#2-usage-in-a-rest-api) (it listens on port 8080) and hit it:

```bash
# in your own app crate that depends on axum-apm:
cargo run
curl http://localhost:8080/api/hello
```

Spans appear in Kibana → Observability → APM within a few seconds (batch export).
