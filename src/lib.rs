//! # Elastic APM/OpenTelemetry Tracing Library for Axum
//!
//! This library provides helper functions to configure OpenTelemetry tracing for Elastic APM
//! in Axum applications.
//!
//! It supports standard Elastic APM environment variables:
//! - `ELASTIC_APM_SERVER_URL` (default: `http://localhost:8200`)
//! - `ELASTIC_APM_SERVICE_NAME` (default: `rust-service`)
//! - `ELASTIC_APM_SERVICE_VERSION` (default: `1.0.0`)
//! - `ELASTIC_APM_ENVIRONMENT` (default: `production`)
//! - `ELASTIC_APM_NODE_NAME` (node name; defaults to the system hostname)
//! - `ELASTIC_APM_SECRET_TOKEN` (authorization bearer token)
//! - `ELASTIC_APM_API_KEY` (authorization api key)
//! - `ELASTIC_APM_TRANSACTION_IGNORE_URLS` (comma-separated URL patterns to exclude from tracing)
//! - `ELASTIC_APM_TRANSACTION_SAMPLE_RATE` (float 0.0-1.0, sampling rate for transactions)
//! - `ELASTIC_APM_ACTIVE` (bool, default `true`; when `false` the trace layer is a no-op)
//! - `ELASTIC_APM_RECORDING` (bool, default `true`; when `false` the trace layer is a no-op)
//!
//! # W3C trace context propagation
//!
//! `init_apm` installs the W3C `TraceContext` propagator globally, which is
//! what lets [`apm_trace_layer`] continue an incoming `traceparent` (and
//! lets [`inject_trace_context`] carry it into outgoing requests, e.g. when
//! you proxy to an upstream). If you build your own `SdkTracerProvider` and
//! skip `init_apm`, install a propagator yourself (e.g.
//! `opentelemetry_sdk::propagation::TraceContextPropagator`) or trace
//! propagation will silently no-op.

mod config;
mod panic_recovery;
mod propagation;
mod provider;
mod sampling;
mod trace_layer;

pub use config::{ApmConfig, ApmError};
pub use panic_recovery::{
    ApmPanicRecoveryFuture, ApmPanicRecoveryLayer, ApmPanicRecoveryService,
    apm_panic_recovery_layer,
};
pub use propagation::inject_trace_context;
pub use provider::{apm_otel_layer, apm_otel_layer_with_tracer_name, init_apm};
pub use trace_layer::{ApmTraceLayer, apm_trace_layer, apm_trace_layer_with_body};

/// Whether APM recording is enabled. Set from `ELASTIC_APM_ACTIVE` and
/// `ELASTIC_APM_RECORDING` (both default `true`) by [`init_apm`]. When `false`,
/// [`make_http_span`](crate::trace_layer::make_http_span) returns a no-op span
/// so the trace layer is zero-overhead, mirroring the Go agent's
/// `Recording()` short-circuit.
pub(crate) static RECORDING: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(true);

/// Shared helpers for the unit tests spread across the modules: the locks
/// serialize tests that touch process-global state (the default `tracing`
/// subscriber and environment variables), which run in parallel threads of
/// one process.
#[cfg(test)]
pub(crate) mod test_support {
    /// Tests that install a default `tracing` subscriber must hold this lock:
    /// the default subscriber is process-global and the tests run in parallel.
    /// Async-aware because the guard is held across await points.
    pub static SUBSCRIBER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// Tests that mutate process environment variables must hold this lock:
    /// tests run in parallel threads of one process, and env vars are global.
    pub static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
}
