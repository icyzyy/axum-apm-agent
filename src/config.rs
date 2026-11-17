/// The effective APM configuration parsed from the `ELASTIC_APM_*`
/// environment variables, as returned by [`init_apm`](crate::init_apm).
///
/// `init_apm` runs before your `tracing` subscriber exists, so it cannot
/// log configuration problems itself: log [`warnings`](Self::warnings)
/// after installing the subscriber.
#[derive(Debug, Clone)]
pub struct ApmConfig {
    /// `ELASTIC_APM_SERVICE_NAME` (default `rust-service`).
    pub service_name: String,
    /// `ELASTIC_APM_SERVICE_VERSION` (default `1.0.0`).
    pub service_version: String,
    /// `ELASTIC_APM_ENVIRONMENT` (default `production`).
    pub environment: String,
    /// `ELASTIC_APM_NODE_NAME`, or the system hostname when unset/blank.
    pub node_name: Option<String>,
    /// Effective transaction sample rate (always a finite value in `0.0..=1.0`).
    pub sample_rate: f64,
    /// Effective `ELASTIC_APM_TRANSACTION_IGNORE_URLS` patterns.
    pub ignore_patterns: Vec<String>,
    /// Human-readable warnings from configuration parsing (e.g. an invalid
    /// sample rate that fell back to `1.0`). Empty when the configuration
    /// is valid.
    pub warnings: Vec<String>,
}

/// Parses the `ELASTIC_APM_TRANSACTION_SAMPLE_RATE` value.
///
/// Returns `(rate, warnings)`. The rate defaults to `1.0` when the variable
/// is unset, and also falls back to `1.0` (with a warning in the returned
/// vector) when the value is not a finite number in `0.0..=1.0`, so a typo
/// can never silently change the sampling behavior.
pub(crate) fn parse_sample_rate(raw: Option<String>) -> (f64, Vec<String>) {
    match raw {
        None => (1.0, Vec::new()),
        Some(s) => match s.trim().parse::<f64>() {
            Ok(v) if v.is_finite() && (0.0..=1.0).contains(&v) => (v, Vec::new()),
            Ok(v) => (
                1.0,
                vec![format!(
                    "Invalid ELASTIC_APM_TRANSACTION_SAMPLE_RATE {v} (must be a finite number between 0.0 and 1.0); falling back to 1.0"
                )],
            ),
            Err(e) => (
                1.0,
                vec![format!(
                    "Failed to parse ELASTIC_APM_TRANSACTION_SAMPLE_RATE {s:?}: {e}; falling back to 1.0"
                )],
            ),
        },
    }
}

/// Errors returned by the fallible entry points of this library.
#[derive(Debug, thiserror::Error)]
pub enum ApmError {
    /// The OTLP exporter could not be built.
    #[error("failed to build the OTLP exporter: {0}")]
    ExporterBuild(#[from] opentelemetry_otlp::ExporterBuildError),
}

/// Resolves the APM node name: the `ELASTIC_APM_NODE_NAME` environment
/// variable when set to a non-blank value, otherwise the system hostname.
pub(crate) fn resolve_node_name() -> Option<String> {
    if let Ok(name) = std::env::var("ELASTIC_APM_NODE_NAME") {
        let name = name.trim().to_string();
        if !name.is_empty() {
            return Some(name);
        }
    }
    hostname::get()
        .ok()
        .map(|h| h.to_string_lossy().into_owned())
}

/// Parses a boolean environment variable.
///
/// Returns `default` when the variable is unset or blank. Recognizes the
/// common truthy/falsy spellings (`true`/`false`, `1`/`0`, `yes`/`no`,
/// `on`/`off`, case-insensitive); any other non-blank value falls back to
/// `default` (so a typo cannot silently flip the switch).
pub(crate) fn parse_bool_env(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "" => default,
            "true" | "1" | "yes" | "on" => true,
            "false" | "0" | "no" | "off" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

/// Parses a comma-separated string of URL patterns into a Vec<String>.
/// Patterns can be exact matches, glob patterns (with * and ?), or regex.
pub(crate) fn parse_url_patterns(patterns_env: Option<String>) -> Vec<String> {
    patterns_env
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::ENV_LOCK;

    #[test]
    fn parse_bool_env_defaults_when_unset_or_blank() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("TEST_BOOL_ENV") };
        assert!(parse_bool_env("TEST_BOOL_ENV", true));
        assert!(!parse_bool_env("TEST_BOOL_ENV", false));
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::set_var("TEST_BOOL_ENV", "   ") };
        assert!(parse_bool_env("TEST_BOOL_ENV", true));
        assert!(!parse_bool_env("TEST_BOOL_ENV", false));
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("TEST_BOOL_ENV") };
    }

    #[test]
    fn parse_bool_env_recognizes_truthy_and_falsy_spellings() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        for (value, expected) in [
            ("true", true),
            ("TRUE", true),
            ("1", true),
            ("yes", true),
            ("on", true),
            ("false", false),
            ("False", false),
            ("0", false),
            ("no", false),
            ("off", false),
        ] {
            // SAFETY: env mutation serialized by ENV_LOCK.
            unsafe { std::env::set_var("TEST_BOOL_ENV", value) };
            assert_eq!(
                parse_bool_env("TEST_BOOL_ENV", !expected),
                expected,
                "value: {value}"
            );
        }
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("TEST_BOOL_ENV") };
    }

    #[test]
    fn parse_bool_env_falls_back_to_default_on_unrecognized_value() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::set_var("TEST_BOOL_ENV", "maybe") };
        assert!(parse_bool_env("TEST_BOOL_ENV", true));
        assert!(!parse_bool_env("TEST_BOOL_ENV", false));
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("TEST_BOOL_ENV") };
    }

    #[test]
    fn sample_rate_parses_valid_values_without_warnings() {
        for (input, expected) in [
            (None, 1.0),
            (Some("0.5"), 0.5),
            (Some("0"), 0.0),
            (Some("1"), 1.0),
            (Some(" 0.25 "), 0.25),
        ] {
            let (rate, warnings) = parse_sample_rate(input.map(str::to_string));
            assert_eq!(rate, expected, "input: {input:?}");
            assert!(
                warnings.is_empty(),
                "no warnings expected for {input:?}: {warnings:?}"
            );
        }
    }

    #[test]
    fn sample_rate_falls_back_to_one_and_warns_on_invalid_values() {
        for invalid in ["abc", "0,1", "nan", "inf", "-0.1", "2.0", "1.5"] {
            let (rate, warnings) = parse_sample_rate(Some(invalid.to_string()));
            assert_eq!(rate, 1.0, "{invalid:?} should fall back to 1.0");
            assert!(
                warnings
                    .iter()
                    .any(|w| w.contains("ELASTIC_APM_TRANSACTION_SAMPLE_RATE")),
                "{invalid:?} should produce a warning naming the variable: {warnings:?}"
            );
        }
    }

    #[test]
    fn node_name_prefers_env_var_over_hostname() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::set_var("ELASTIC_APM_NODE_NAME", "test-node") };
        assert_eq!(resolve_node_name().as_deref(), Some("test-node"));
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("ELASTIC_APM_NODE_NAME") };
    }

    #[test]
    fn node_name_ignores_blank_env_var() {
        let _guard = ENV_LOCK.lock().unwrap_or_else(|p| p.into_inner());
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::set_var("ELASTIC_APM_NODE_NAME", "   ") };
        // Blank value must not be used; falls back to the system hostname.
        assert_ne!(resolve_node_name().as_deref(), Some("   "));
        // SAFETY: env mutation serialized by ENV_LOCK.
        unsafe { std::env::remove_var("ELASTIC_APM_NODE_NAME") };
    }
}
