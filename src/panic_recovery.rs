use axum::{
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::Service;
use tracing::Span;

/// A tower [`Layer`](tower::Layer) that recovers panics from the wrapped
/// service, records them as APM errors on the current span, and returns a
/// `500 Internal Server Error` response instead of dropping the connection.
///
/// Apply it *inside* [`apm_trace_layer`](crate::apm_trace_layer) so the APM
/// span is active when a panic is caught:
///
/// ```ignore
/// let app = Router::new()
///     .route("/users/{id}", get(handler))
///     .layer(axum_apm::apm_panic_recovery_layer()) // inner: recovers panics
///     .layer(axum_apm::apm_trace_layer());         // outer: creates the span
/// ```
///
/// This mirrors the Go agent's `apmechov4` middleware, which recovers panics
/// and reports them to APM (and can replace the framework's own recover).
#[derive(Clone, Debug, Default)]
pub struct ApmPanicRecoveryLayer;

impl<S> tower::Layer<S> for ApmPanicRecoveryLayer {
    type Service = ApmPanicRecoveryService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        ApmPanicRecoveryService { inner }
    }
}

/// The [`Service`] produced by [`ApmPanicRecoveryLayer`].
///
/// Panics raised while polling the request future are recovered; note the
/// limits of that protection:
///
/// - `poll_ready` is forwarded to the inner service *without* panic
///   protection (a panic there propagates to the caller — harmless for
///   axum's `Router`, whose `poll_ready` is infallible).
/// - The `S::Error = Infallible` bound means this layer cannot wrap services
///   that report errors (e.g. `tower::timeout::Timeout` or
///   `tower::limit::ConcurrencyLimit`).
/// - Panics raised while *dropping* the inner future (e.g. on client
///   disconnect) are not caught; that is inherent to `catch_unwind` (the
///   same limitation as Go's `recover`).
#[derive(Clone, Debug)]
pub struct ApmPanicRecoveryService<S> {
    inner: S,
}

impl<S> Service<Request> for ApmPanicRecoveryService<S>
where
    S: Service<Request, Response = Response, Error = std::convert::Infallible>
        + Clone
        + Send
        + 'static,
    <S as Service<Request>>::Future: Send + 'static,
{
    type Response = Response;
    type Error = std::convert::Infallible;
    type Future = ApmPanicRecoveryFuture;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let inner_future = self.inner.call(req);
        let boxed: BoxedResponseFuture = Box::pin(inner_future);
        ApmPanicRecoveryFuture::new(boxed)
    }
}

/// A boxed, `Send` future producing an HTTP response (the inner service's
/// future, boxed so [`ApmPanicRecoveryFuture`] is always `Unpin`).
type BoxedResponseFuture =
    Pin<Box<dyn Future<Output = Result<Response, std::convert::Infallible>> + Send>>;

/// A future that wraps an inner future and catches any panic it raises,
/// converting the panic into a recorded APM error plus a `500` response.
///
/// Not directly constructible; produced by [`ApmPanicRecoveryService::call`].
///
/// The inner future is boxed so this wrapper is always `Unpin` (the inner
/// future may not be).
pub struct ApmPanicRecoveryFuture {
    inner: Option<BoxedResponseFuture>,
}

impl ApmPanicRecoveryFuture {
    fn new(inner: BoxedResponseFuture) -> Self {
        Self { inner: Some(inner) }
    }
}

impl Future for ApmPanicRecoveryFuture {
    type Output = Result<Response, std::convert::Infallible>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let inner = this
            .inner
            .as_mut()
            .expect("ApmPanicRecoveryFuture polled after completion");
        // Catch a panic raised while polling the inner future. `AssertUnwindSafe`
        // is required because `&mut Context` is not `UnwindSafe`; the panic is
        // fully contained (we do not resume the inner future).
        let result = panic::catch_unwind(AssertUnwindSafe(|| inner.as_mut().poll(cx)));
        match result {
            Ok(Poll::Ready(Ok(response))) => {
                this.inner = None;
                Poll::Ready(Ok(response))
            }
            // Unreachable: the inner service's error is `Infallible`.
            Ok(Poll::Ready(Err(e))) => match e {},
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => {
                this.inner = None;
                record_panic_on_current_span(&payload);
                let response =
                    (StatusCode::INTERNAL_SERVER_ERROR, "internal server error").into_response();
                Poll::Ready(Ok(response))
            }
        }
    }
}

/// Records a caught panic on the current [`Span`] (the APM request span when
/// this layer is composed inside [`apm_trace_layer`](crate::apm_trace_layer)):
/// sets the error fields and emits an `exception` span event, mirroring the
/// Go agent's panic reporting.
fn record_panic_on_current_span(payload: &Box<dyn std::any::Any + Send>) {
    let span = Span::current();
    let message = extract_panic_message(payload);
    span.record("error.type", "#Panic");
    span.record("error.message", message.as_str());
    tracing::error!(
        parent: &span,
        exception.type = "#Panic",
        exception.message = message.as_str(),
        "exception"
    );
}

/// Extracts a human-readable message from a panic payload (the `&str` or
/// `String` message when available, otherwise a generic description).
fn extract_panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "handler panicked".to_string()
    }
}

/// Creates an [`ApmPanicRecoveryLayer`]. See the type's docs for usage.
#[must_use]
pub fn apm_panic_recovery_layer() -> ApmPanicRecoveryLayer {
    ApmPanicRecoveryLayer
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apm_trace_layer;
    use crate::test_support::SUBSCRIBER_LOCK;
    use opentelemetry::global;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tracing_subscriber::layer::SubscriberExt;

    /// A handler that panics must not drop the connection: the panic is
    /// recovered into a 500 response, and the panic is recorded on the APM
    /// span (status Error + an `exception` event), mirroring the Go agent's
    /// panic-recovery behavior.
    #[tokio::test]
    async fn panic_in_handler_is_recovered_and_recorded_as_an_error() {
        use axum::Router;
        use axum::routing::get;
        use opentelemetry::trace::{Status, TracerProvider as _};
        use tower::ServiceExt as _;

        let _guard = SUBSCRIBER_LOCK.lock().await;
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());

        let tracer = provider.tracer("test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _sub_guard = tracing::subscriber::set_default(subscriber);

        async fn panicking_handler() -> &'static str {
            panic!("boom");
        }

        // The panic-recovery layer sits *inside* the trace layer so the APM
        // span is active when the panic is caught.
        let app = Router::new()
            .route("/panic", get(panicking_handler))
            .layer(apm_panic_recovery_layer())
            .layer(apm_trace_layer());

        let req = Request::builder()
            .uri("/panic")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        // The panic is recovered into a 500 response (connection not dropped).
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let _ = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        let http_span = spans
            .iter()
            .find(|s| s.name == "GET /panic")
            .unwrap_or_else(|| {
                panic!(
                    "expected the http span to be exported, got: {:?}",
                    spans.iter().map(|s| s.name.to_string()).collect::<Vec<_>>()
                )
            });
        // The span must be marked as an error.
        assert!(
            matches!(http_span.status, Status::Error { .. }),
            "expected the span status to be Error, got: {:?}",
            http_span.status
        );
        // The panic must be recorded as an `exception` span event.
        let exception_event = http_span
            .events
            .iter()
            .find(|e| e.name == "exception")
            .expect("an `exception` span event should be recorded for the panic");
        let ev_attr = |key: &str| {
            exception_event
                .attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.as_str().into_owned())
        };
        assert_eq!(ev_attr("exception.type").as_deref(), Some("#Panic"));
        assert_eq!(ev_attr("exception.message").as_deref(), Some("boom"));
        // The span itself carries the error fields (the APM error view).
        let span_attr = |key: &str| {
            http_span
                .attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.as_str().into_owned())
        };
        assert_eq!(span_attr("error.type").as_deref(), Some("#Panic"));
        assert_eq!(span_attr("error.message").as_deref(), Some("boom"));
    }

    /// A panic raised on a *later* poll (after the handler has already
    /// returned `Pending`) must also be recovered: `catch_unwind` wraps every
    /// poll of the inner future, not just the first.
    #[tokio::test]
    async fn panic_on_a_later_poll_is_recovered() {
        use axum::Router;
        use axum::routing::get;
        use opentelemetry::trace::TracerProvider as _;
        use tower::ServiceExt as _;

        let _guard = SUBSCRIBER_LOCK.lock().await;
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());

        let tracer = provider.tracer("test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _sub_guard = tracing::subscriber::set_default(subscriber);

        async fn late_panicking_handler() -> &'static str {
            // Suspend first so the panic happens on a later poll.
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            panic!("late boom");
        }

        let app = Router::new()
            .route("/late-panic", get(late_panicking_handler))
            .layer(apm_panic_recovery_layer())
            .layer(apm_trace_layer());

        let req = Request::builder()
            .uri("/late-panic")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let _ = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        let http_span = spans
            .iter()
            .find(|s| s.name == "GET /late-panic")
            .unwrap_or_else(|| {
                panic!(
                    "expected the http span to be exported, got: {:?}",
                    spans.iter().map(|s| s.name.to_string()).collect::<Vec<_>>()
                )
            });
        let exception_event = http_span
            .events
            .iter()
            .find(|e| e.name == "exception")
            .expect("an `exception` span event should be recorded for the panic");
        let ev_attr = |key: &str| {
            exception_event
                .attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.as_str().into_owned())
        };
        assert_eq!(ev_attr("exception.type").as_deref(), Some("#Panic"));
        assert_eq!(ev_attr("exception.message").as_deref(), Some("late boom"));
    }

    /// A panic with a non-string payload (e.g. `panic!(42)`) still produces a
    /// usable `exception.message` via the fallback text.
    #[tokio::test]
    async fn panic_with_non_string_payload_uses_the_fallback_message() {
        use axum::Router;
        use axum::routing::get;
        use opentelemetry::trace::TracerProvider as _;
        use tower::ServiceExt as _;

        let _guard = SUBSCRIBER_LOCK.lock().await;
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        global::set_tracer_provider(provider.clone());

        let tracer = provider.tracer("test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _sub_guard = tracing::subscriber::set_default(subscriber);

        async fn non_string_panicking_handler() -> &'static str {
            // `panic_any` raises a panic whose payload is not a `&str` or
            // `String` (here an `i32`), exercising the fallback message.
            std::panic::panic_any(42i32);
        }

        let app = Router::new()
            .route("/non-string-panic", get(non_string_panicking_handler))
            .layer(apm_panic_recovery_layer())
            .layer(apm_trace_layer());

        let req = Request::builder()
            .uri("/non-string-panic")
            .body(axum::body::Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let _ = axum::body::to_bytes(resp.into_body(), 1024).await.unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        let http_span = spans
            .iter()
            .find(|s| s.name == "GET /non-string-panic")
            .unwrap_or_else(|| {
                panic!(
                    "expected the http span to be exported, got: {:?}",
                    spans.iter().map(|s| s.name.to_string()).collect::<Vec<_>>()
                )
            });
        let exception_event = http_span
            .events
            .iter()
            .find(|e| e.name == "exception")
            .expect("an `exception` span event should be recorded for the panic");
        let ev_attr = |key: &str| {
            exception_event
                .attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.as_str().into_owned())
        };
        assert_eq!(ev_attr("exception.type").as_deref(), Some("#Panic"));
        assert_eq!(
            ev_attr("exception.message").as_deref(),
            Some("handler panicked")
        );
    }
}
