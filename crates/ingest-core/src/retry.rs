//! Retry wrapper with exponential backoff + jitter (FR-1.3). Generic over
//! any async operation returning `Result<T, E>`.

use rand::Rng;
use std::future::Future;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    pub max_retries: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
    /// Fraction of the computed delay to randomize, e.g. 0.2 = +/-20%.
    /// Prevents synchronized retry storms across concurrent workers hitting
    /// the same transient failure at the same time.
    pub jitter_factor: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 5,
            base_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(10),
            jitter_factor: 0.2,
        }
    }
}

/// Runs `op` up to `config.max_retries + 1` times, applying exponential
/// backoff with jitter between attempts. `should_retry` decides whether a
/// given error is transient (retry) or terminal (fail immediately) — e.g.
/// a malformed-request error should NOT be retried, but a timeout should.
pub async fn retry_with_backoff<T, E, Fut, F, ShouldRetry>(
    config: RetryConfig,
    mut op: F,
    should_retry: ShouldRetry,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    ShouldRetry: Fn(&E) -> bool,
{
    let mut attempt: u32 = 0;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if attempt >= config.max_retries || !should_retry(&e) {
                    return Err(e);
                }
                let exp_delay = config.base_delay.as_millis() as f64 * 2f64.powi(attempt as i32);
                let capped = exp_delay.min(config.max_delay.as_millis() as f64);
                let jitter_range = capped * config.jitter_factor;
                let jitter = rand::thread_rng().gen_range(-jitter_range..=jitter_range);
                let delay_ms = (capped + jitter).max(0.0) as u64;

                tracing::warn!(attempt, delay_ms, "retrying after transient error");
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn retries_transient_errors_then_succeeds() {
        let attempts = AtomicU32::new(0);
        let cfg = RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
            jitter_factor: 0.0,
        };
        let out = retry_with_backoff(
            cfg,
            || {
                let n = attempts.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n < 2 {
                        Err("transient")
                    } else {
                        Ok(7u8)
                    }
                }
            },
            |_| true,
        )
        .await;
        assert_eq!(out, Ok(7));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn does_not_retry_terminal_errors() {
        let attempts = AtomicU32::new(0);
        let cfg = RetryConfig {
            max_retries: 5,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
            jitter_factor: 0.0,
        };
        let out: Result<(), &str> = retry_with_backoff(
            cfg,
            || {
                attempts.fetch_add(1, Ordering::SeqCst);
                async { Err("terminal") }
            },
            |_| false,
        )
        .await;
        assert_eq!(out, Err("terminal"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
