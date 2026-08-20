//! Client-side token-bucket rate limiter. Enforced regardless of what the
//! RPC provider allows — this is a stated assignment requirement (FR-1.2),
//! not just a courtesy to the provider.
//!
//! NOTE: uses tokio's async Mutex + Instant; requires the tokio runtime.
//! Not unit-testable purely (needs real time), so correctness here is
//! reasoned through carefully rather than proven via synthetic fixtures —
//! confirm actual throughput against a real clock in Cursor
//! (`cargo test -- --nocapture` with a timed integration test, or just
//! watch the observed req/s during a real ingest run).

use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

pub struct TokenBucketLimiter {
    inner: Arc<Mutex<Inner>>,
    capacity: f64,
    refill_per_sec: f64,
}

struct Inner {
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucketLimiter {
    /// `rate_per_sec` = sustained requests/sec allowed (e.g. 10.0).
    /// `burst` = max tokens that can accumulate (allows short bursts up to
    /// this many requests instantly; set equal to rate_per_sec for a
    /// conservative, minimally-bursty limiter).
    pub fn new(rate_per_sec: f64, burst: f64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner { tokens: burst, last_refill: Instant::now() })),
            capacity: burst,
            refill_per_sec: rate_per_sec,
        }
    }

    /// Blocks (async) until a token is available, then consumes one.
    pub async fn acquire(&self) {
        loop {
            let wait = {
                let mut inner = self.inner.lock().await;
                let now = Instant::now();
                let elapsed = now.duration_since(inner.last_refill).as_secs_f64();
                inner.tokens = (inner.tokens + elapsed * self.refill_per_sec).min(self.capacity);
                inner.last_refill = now;

                if inner.tokens >= 1.0 {
                    inner.tokens -= 1.0;
                    None
                } else {
                    let deficit = 1.0 - inner.tokens;
                    Some(Duration::from_secs_f64(deficit / self.refill_per_sec))
                }
            };

            match wait {
                None => return,
                Some(d) => tokio::time::sleep(d).await,
            }
        }
    }
}

impl Clone for TokenBucketLimiter {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone(), capacity: self.capacity, refill_per_sec: self.refill_per_sec }
    }
}
