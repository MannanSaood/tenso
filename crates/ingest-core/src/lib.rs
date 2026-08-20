//! RPC client, rate limiting, retry/backoff, and account-key resolution for
//! fetching Solana blocks. Deliberately contains NO database code — every
//! public function returns plain typed structs; `storage` owns persistence.

pub mod account_resolution;
pub mod block_fetch;
pub mod rate_limiter;
pub mod retry;
pub mod rpc_client;
pub mod types;

pub use account_resolution::{resolve_account_locks, MessageHeader};
pub use block_fetch::{fetch_block_range, FetchOutcome};
pub use rate_limiter::TokenBucketLimiter;
pub use retry::{retry_with_backoff, RetryConfig};
pub use rpc_client::{RpcClient, RpcError};
pub use types::*;
