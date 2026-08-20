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
