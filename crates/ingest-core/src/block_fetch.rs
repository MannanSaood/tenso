//! Orchestrates concurrent, rate-limited, bounded fetching across a slot
//! range (FR-1.1, FR-1.6). This is the piece that ties rpc_client +
//! rate_limiter + retry together; it does NOT touch storage — the caller
//! (cli's `ingest` subcommand) is responsible for persisting each outcome.

use crate::rate_limiter::TokenBucketLimiter;
use crate::retry::{retry_with_backoff, RetryConfig};
use crate::rpc_client::{is_transient, RpcClient, RpcError};
use crate::types::DecodedBlock;
use futures::stream::{FuturesUnordered, StreamExt};
use std::sync::Arc;

pub enum FetchOutcome {
    Ingested(DecodedBlock),
    Skipped { slot: u64 },
    Failed { slot: u64, error: String },
}

/// Fetch every slot in `start_slot..start_slot + count`, bounded to at most
/// `max_concurrency` in-flight requests at once, all sharing one rate
/// limiter (so total throughput across all workers stays under the cap,
/// not per-worker).
///
/// Returns one `FetchOutcome` per slot via the provided callback as soon as
/// it's ready (not batched at the end) — this lets the caller start writing
/// to storage while later slots are still being fetched, which matters for
/// the backpressure experiment (FR-5.1): the caller's storage-writer stage
/// is a separate, boundable consumer of this stream.
pub async fn fetch_block_range<F>(
    client: Arc<RpcClient>,
    limiter: Arc<TokenBucketLimiter>,
    start_slot: u64,
    count: u64,
    max_concurrency: usize,
    mut on_outcome: F,
) where
    F: FnMut(FetchOutcome) + Send,
{
    let retry_config = RetryConfig::default();
    let mut slots: Vec<u64> = (start_slot..start_slot + count).collect();
    let mut in_flight = FuturesUnordered::new();

    // Prime the pipeline up to max_concurrency, then keep it full as tasks
    // complete — a standard bounded-concurrency worker pool pattern.
    while in_flight.len() < max_concurrency {
        if let Some(slot) = slots.pop() {
            in_flight.push(fetch_one(client.clone(), limiter.clone(), slot, retry_config));
        } else {
            break;
        }
    }

    while let Some(outcome) = in_flight.next().await {
        on_outcome(outcome);
        if let Some(slot) = slots.pop() {
            in_flight.push(fetch_one(client.clone(), limiter.clone(), slot, retry_config));
        }
    }
}

async fn fetch_one(
    client: Arc<RpcClient>,
    limiter: Arc<TokenBucketLimiter>,
    slot: u64,
    retry_config: RetryConfig,
) -> FetchOutcome {
    limiter.acquire().await;

    let result = retry_with_backoff(
        retry_config,
        || {
            let client = client.clone();
            async move { client.get_block(slot).await }
        },
        is_transient,
    )
    .await;

    match result {
        Ok(block) => FetchOutcome::Ingested(block),
        Err(RpcError::SlotSkipped) => FetchOutcome::Skipped { slot },
        Err(e) => FetchOutcome::Failed { slot, error: e.to_string() },
    }
}
