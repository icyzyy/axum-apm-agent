use rust_reverse_proxy::{init_apm, apm_trace_layer, proxy_handler, AppState, ProxyClient};
use axum::Router;
use hyper_util::{client::legacy::connect::HttpConnector, rt::TokioExecutor};
use std::sync::Arc;

#[test]
fn test_init_apm() {
    // Test that init_apm can be called without panicking
    // Note: This will fail if ELASTIC_APM_SERVER_URL is not set, but shouldn't panic
    let result = std::panic::catch_unwind(|| {
        // Reset any previous global state
        let _ = init_apm();
    });
    assert!(result.is_ok(), "init_apm should not panic");
}

#[test]
fn test_apm_trace_layer_creation() {
    // Test that apm_trace_layer can be created
    let _layer = apm_trace_layer();
}

#[test]
fn test_app_state_creation() {
    let client: ProxyClient = hyper_util::client::legacy::Client::<(), ()>::builder(TokioExecutor::new())
        .build(HttpConnector::new());
    
    let state = AppState {
        client,
        upstream_url: Arc::new("https://httpbin.org".to_string()),
    };
    
    assert_eq!(state.upstream_url.as_str(), "https://httpbin.org");
}

#[test]
fn test_router_with_state() {
    let client: ProxyClient = hyper_util::client::legacy::Client::<(), ()>::builder(TokioExecutor::new())
        .build(HttpConnector::new());
    
    let state = AppState {
        client,
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
async fn test_proxy_handler_missing_upstream() {
    // Test that proxy_handler returns BAD_REQUEST for invalid URIs
    let client: ProxyClient = hyper_util::client::legacy::Client::<(), ()>::builder(TokioExecutor::new())
        .build(HttpConnector::new());
    
    let state = AppState {
        client,
        upstream_url: Arc::new("invalid-url".to_string()),
    };
    
    // Create a test request with an invalid path that would create an invalid URI
    let req = axum::http::Request::builder()
        .uri("http://localhost:3000")
        .body(axum::body::Body::empty())
        .unwrap();
    
    let result = proxy_handler(axum::extract::State(state), req).await;
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), axum::http::StatusCode::BAD_REQUEST);
}