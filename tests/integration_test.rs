use axum::Router;
use axum_apm::{AppState, ProxyClient, apm_otel_layer, apm_trace_layer, init_apm, proxy_handler};
use hyper_util::{client::legacy::connect::HttpConnector, rt::TokioExecutor};
use std::sync::Arc;
use tracing_subscriber::layer::SubscriberExt;

fn test_client() -> ProxyClient {
    hyper_util::client::legacy::Client::<(), ()>::builder(TokioExecutor::new())
        .build(HttpConnector::new())
}

#[test]
fn init_apm_returns_a_working_provider() {
    // init_apm reads ELASTIC_APM_* env vars with sane defaults, so it must
    // succeed without any configuration and must not panic.
    let result = std::panic::catch_unwind(init_apm);
    let (provider, config) = result
        .expect("init_apm should not panic")
        .expect("init_apm should succeed");
    // A clean configuration produces no warnings.
    assert!(
        config.warnings.is_empty(),
        "default configuration should not warn: {:?}",
        config.warnings
    );
    // The provider must shut down cleanly (flushes the batch processor).
    provider
        .shutdown()
        .expect("provider shutdown should succeed");
}

#[test]
fn apm_trace_layer_creation() {
    let _layer = apm_trace_layer();
}

#[test]
fn app_state_creation() {
    let state = AppState {
        client: test_client(),
        upstream_url: Arc::new("https://httpbin.org".to_string()),
    };

    assert_eq!(state.upstream_url.as_str(), "https://httpbin.org");
}

#[test]
fn router_with_state() {
    let state = AppState {
        client: test_client(),
        upstream_url: Arc::new("https://httpbin.org".to_string()),
    };

    let app: Router = Router::new()
        .fallback(proxy_handler)
        .with_state(state)
        .layer(apm_trace_layer());

    // Just verify the router can be created
    let _ = app;
}

#[tokio::test]
async fn proxy_handler_invalid_upstream_uri() {
    // proxy_handler returns BAD_REQUEST when the composed upstream URI is invalid
    let state = AppState {
        client: test_client(),
        upstream_url: Arc::new("invalid-url".to_string()),
    };

    let req = axum::http::Request::builder()
        .uri("http://localhost:3000")
        .body(axum::body::Body::empty())
        .unwrap();

    let result = proxy_handler(axum::extract::State(state), req).await;
    assert_eq!(result.unwrap_err(), axum::http::StatusCode::BAD_REQUEST);
}

/// Exercises the documented public-API recipe end to end:
/// `init_apm` -> `apm_otel_layer` -> subscriber -> `apm_trace_layer` -> request.
#[tokio::test]
async fn full_public_api_recipe_serves_a_request() {
    use axum::routing::get;
    use tower::ServiceExt as _;

    let (provider, _config) = init_apm().expect("init_apm should succeed");

    let subscriber = tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new("info"))
        .with(tracing_subscriber::fmt::layer())
        .with(apm_otel_layer(&provider));
    let _guard = tracing::subscriber::set_default(subscriber);

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .layer(apm_trace_layer());

    let req = axum::http::Request::builder()
        .uri("/health")
        .body(axum::body::Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.expect("request should succeed");
    assert_eq!(resp.status(), axum::http::StatusCode::OK);
    let _ = axum::body::to_bytes(resp.into_body(), 1024)
        .await
        .expect("body should be readable");

    // No APM server is running in the test environment, so the final flush
    // may time out; that is expected and not a wiring failure.
    let _ = provider.shutdown();
}
