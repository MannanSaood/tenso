//! Shared types passed between ingest-core and its caller (the cli's
//! `ingest` subcommand, which hands these to `storage`). Kept separate from
//! the raw RPC JSON shapes in `rpc_client.rs` so the rest of the workspace
//! depends on a stable, hand-picked type rather than the RPC's full schema.

use serde::{Deserialize, Serialize};

/// A single resolved account lock for one transaction (post ALT-resolution).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountLock {
    pub account: String,
    pub is_writable: bool,
}

/// Balance change data needed by the `ohlcv` crate — kept minimal on purpose.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenBalanceChange {
    pub mint: String,
    pub pre_amount: u64,
    pub post_amount: u64,
    pub decimals: u8,
}

/// One fully-decoded transaction, ready to be persisted by `storage` and
/// consumed by `contention` (via `locks`) and `ohlcv` (via `token_deltas`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecodedTransaction {
    pub signature: String,
    pub slot: u64,
    pub block_time: Option<i64>,
    pub program_ids: Vec<String>,
    pub locks: Vec<AccountLock>,
    pub token_deltas: Vec<TokenBalanceChange>,
    /// True if the transaction's `meta.err` field was non-null. Failed
    /// transactions still consumed compute/locks and should still be
    /// counted for contention purposes, but are excluded from OHLCV.
    pub failed: bool,
}

/// Status of a single slot after an ingest attempt — persisted by `storage`
/// in `ingestion_status` so re-runs know what's already done (FR-1.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SlotStatus {
    Ingested,
    Skipped, // leader skipped this slot — expected, not an error (FR-1.3)
    Failed,  // exhausted retries — needs manual attention or a later re-run
}

#[derive(Debug, Clone)]
pub struct DecodedBlock {
    pub slot: u64,
    pub block_time: Option<i64>,
    pub transactions: Vec<DecodedTransaction>,
}
