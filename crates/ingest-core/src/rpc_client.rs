//! Minimal JSON-RPC client for Solana's `getBlock` and raw-response parsing
//! into our `DecodedBlock`/`DecodedTransaction` types.
//!
//! Live-checked against a Helius `getBlock` (`encoding=json`,
//! `maxSupportedTransactionVersion=0`). Extra keys in the envelope (e.g.
//! `blockhash`, `version`, `addressTableLookups`, `logMessages`, `costUnits`)
//! are ignored on purpose — serde's default is to skip unknown fields. The
//! `Raw*` structs only name fields we actually decode.

use crate::account_resolution::{resolve_account_locks, MessageHeader};
use crate::types::{AccountLock, DecodedBlock, DecodedTransaction, TokenBalanceChange};
use serde::Deserialize;
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("rpc returned an error: {0}")]
    RpcReturnedError(String),
    #[error("slot was skipped by the leader")]
    SlotSkipped,
    #[error("unexpected response shape: {0}")]
    UnexpectedShape(String),
}

/// Transient errors are worth retrying (network hiccups, rate limit
/// responses); a skipped slot or a malformed request are not.
pub fn is_transient(e: &RpcError) -> bool {
    matches!(e, RpcError::Transport(_))
}

pub struct RpcClient {
    http: reqwest::Client,
    endpoint: String,
}

impl RpcClient {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self { http: reqwest::Client::new(), endpoint: endpoint.into() }
    }

    /// Current finalized slot. Used to pick a real slot for live schema checks.
    pub async fn get_slot(&self) -> Result<u64, RpcError> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSlot",
            "params": [{ "commitment": "finalized" }]
        });
        let resp: serde_json::Value =
            self.http.post(&self.endpoint).json(&body).send().await?.json().await?;
        resp.get("result")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| RpcError::UnexpectedShape(format!("getSlot: {resp}")))
    }

    /// Same JSON-RPC request as [`Self::get_block`], but returns the envelope
    /// as `serde_json::Value` so we can inspect the live shape before trusting
    /// the `Raw*` structs.
    pub async fn get_block_raw(&self, slot: u64) -> Result<serde_json::Value, RpcError> {
        let resp: serde_json::Value = self
            .http
            .post(&self.endpoint)
            .json(&get_block_body(slot))
            .send()
            .await?
            .json()
            .await?;
        Ok(resp)
    }

    /// Fetch and decode one slot. Returns `Err(RpcError::SlotSkipped)` for a
    /// leader-skipped slot — callers must treat this as an expected, non-
    /// error outcome (FR-1.3), not something to log as a failure.
    pub async fn get_block(&self, slot: u64) -> Result<DecodedBlock, RpcError> {
        let value = self.get_block_raw(slot).await?;
        decode_get_block_json(slot, value)
    }
}

/// Decode a `getBlock` JSON-RPC envelope into a [`DecodedBlock`].
/// Schema errors are `UnexpectedShape` (not retried as transport failures).
fn decode_get_block_json(slot: u64, value: serde_json::Value) -> Result<DecodedBlock, RpcError> {
    if let Some(err) = value.get("error") {
        let message = err
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("rpc error")
            .to_string();
        // Solana RPC signals a skipped slot via a specific error message
        // ("Slot ... was skipped, or missing due to ledger jump to
        // recent snapshot"); anything else is a genuine error.
        if message.contains("skipped") || message.contains("missing") {
            return Err(RpcError::SlotSkipped);
        }
        return Err(RpcError::RpcReturnedError(message));
    }

    let resp: RpcEnvelope<RawBlock> = serde_json::from_value(value)
        .map_err(|e| RpcError::UnexpectedShape(e.to_string()))?;
    let raw = resp
        .result
        .ok_or_else(|| RpcError::UnexpectedShape("missing result".into()))?;
    Ok(decode_block(slot, raw))
}

fn get_block_body(slot: u64) -> serde_json::Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlock",
        "params": [
            slot,
            {
                "encoding": "json",
                "maxSupportedTransactionVersion": 0,
                "transactionDetails": "full",
                "rewards": false
            }
        ]
    })
}

fn decode_block(slot: u64, raw: RawBlock) -> DecodedBlock {
    let transactions = raw
        .transactions
        .into_iter()
        .filter_map(|t| decode_transaction(slot, raw.block_time, t))
        .collect();

    DecodedBlock { slot, block_time: raw.block_time, transactions }
}

fn decode_transaction(
    slot: u64,
    block_time: Option<i64>,
    raw: RawTxWithMeta,
) -> Option<DecodedTransaction> {
    let message = &raw.transaction.message;
    let header = MessageHeader {
        num_required_signatures: message.header.num_required_signatures,
        num_readonly_signed_accounts: message.header.num_readonly_signed_accounts,
        num_readonly_unsigned_accounts: message.header.num_readonly_unsigned_accounts,
    };

    let (loaded_writable, loaded_readonly) = raw
        .meta
        .as_ref()
        .and_then(|m| m.loaded_addresses.clone())
        .map(|la| (la.writable, la.readonly))
        .unwrap_or_default();

    let locks: Vec<AccountLock> =
        resolve_account_locks(&message.account_keys, header, &loaded_writable, &loaded_readonly);

    let program_ids = extract_program_ids(&message.account_keys, &loaded_writable, &loaded_readonly, &message.instructions);

    let failed = raw.meta.as_ref().map(|m| m.err.is_some()).unwrap_or(false);

    let token_deltas = raw
        .meta
        .as_ref()
        .map(|m| build_token_deltas(&m.pre_token_balances, &m.post_token_balances))
        .unwrap_or_default();

    let signature = raw.transaction.signatures.first().cloned().unwrap_or_default();

    Some(DecodedTransaction {
        signature,
        slot,
        block_time,
        program_ids,
        locks,
        token_deltas,
        failed,
    })
}

fn extract_program_ids(
    static_keys: &[String],
    loaded_writable: &[String],
    loaded_readonly: &[String],
    instructions: &[RawInstruction],
) -> Vec<String> {
    // Program IDs are referenced by index into the FULL account list, in the
    // same order resolve_account_locks builds it: static accounts (as they
    // appear in accountKeys, unsplit) then loaded-writable then
    // loaded-readonly. NOTE: this uses the *unsplit* static_keys order,
    // which matches how Solana indexes instructions' programIdIndex — do
    // not reuse the write/readonly-split order from account_resolution here.
    let mut full_list: Vec<&str> = static_keys.iter().map(|s| s.as_str()).collect();
    full_list.extend(loaded_writable.iter().map(|s| s.as_str()));
    full_list.extend(loaded_readonly.iter().map(|s| s.as_str()));

    let mut ids: Vec<String> = instructions
        .iter()
        .filter_map(|ix| full_list.get(ix.program_id_index as usize).map(|s| s.to_string()))
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

fn build_token_deltas(
    pre: &[RawTokenBalance],
    post: &[RawTokenBalance],
) -> Vec<TokenBalanceChange> {
    // Match pre/post by account_index (the index into the tx's account
    // list) — this is how Solana correlates the two lists.
    let mut deltas = Vec::new();
    for post_bal in post {
        let pre_bal = pre.iter().find(|p| p.account_index == post_bal.account_index);
        let pre_amount: u64 = pre_bal
            .map(|p| p.ui_token_amount.amount.parse().unwrap_or(0))
            .unwrap_or(0);
        let post_amount: u64 = post_bal.ui_token_amount.amount.parse().unwrap_or(0);
        let decimals = post_bal.ui_token_amount.decimals;
        let mint = post_bal.mint.clone();

        if pre_amount != post_amount {
            deltas.push(TokenBalanceChange { mint, pre_amount, post_amount, decimals });
        }
    }
    // Accounts present in `pre` but fully closed (absent from `post`) went
    // to zero — represent that explicitly rather than silently dropping it.
    for pre_bal in pre {
        if !post.iter().any(|p| p.account_index == pre_bal.account_index) {
            let pre_amount: u64 = pre_bal.ui_token_amount.amount.parse().unwrap_or(0);
            if pre_amount != 0 {
                deltas.push(TokenBalanceChange {
                    mint: pre_bal.mint.clone(),
                    pre_amount,
                    post_amount: 0,
                    decimals: pre_bal.ui_token_amount.decimals,
                });
            }
        }
    }
    deltas
}

// ---------------- Raw RPC response shapes ----------------

#[derive(Debug, Deserialize)]
struct RpcEnvelope<T> {
    result: Option<T>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawBlock {
    block_time: Option<i64>,
    #[serde(default)]
    transactions: Vec<RawTxWithMeta>,
}

#[derive(Debug, Deserialize)]
struct RawTxWithMeta {
    // Live Helius also sends `version`: `"legacy"` | `0`. Ignored.
    transaction: RawTransaction,
    meta: Option<RawMeta>,
}

#[derive(Debug, Deserialize)]
struct RawTransaction {
    message: RawMessage,
    #[serde(default)]
    signatures: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawMessage {
    // Confirmed live: `encoding=json` returns base58 strings, not
    // `{pubkey,signer,writable}` objects (`jsonParsed` would).
    account_keys: Vec<String>,
    header: RawHeader,
    #[serde(default)]
    instructions: Vec<RawInstruction>,
    // Live v0 messages also have `recentBlockhash` and `addressTableLookups`.
    // ALT pubkeys come from `meta.loadedAddresses`, already resolved.
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawHeader {
    num_required_signatures: u8,
    num_readonly_signed_accounts: u8,
    num_readonly_unsigned_accounts: u8,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawInstruction {
    program_id_index: u8,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawLoadedAddresses {
    #[serde(default)]
    writable: Vec<String>,
    #[serde(default)]
    readonly: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawMeta {
    err: Option<serde_json::Value>,
    #[serde(default)]
    pre_token_balances: Vec<RawTokenBalance>,
    #[serde(default)]
    post_token_balances: Vec<RawTokenBalance>,
    #[serde(default)]
    loaded_addresses: Option<RawLoadedAddresses>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawTokenBalance {
    account_index: u32,
    mint: String,
    ui_token_amount: RawUiTokenAmount,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawUiTokenAmount {
    amount: String, // raw amount as a string, per Solana RPC convention
    decimals: u8,
}

#[cfg(test)]
mod live_debug {
    use super::*;
    use std::path::PathBuf;

    fn helius_url() -> String {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.env");
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some(rest) = line.strip_prefix("HELIUS_URL=") {
                let url = rest.trim().trim_matches('"');
                assert!(!url.is_empty(), "HELIUS_URL is empty in .env");
                return url.to_string();
            }
        }
        panic!("HELIUS_URL not found in {}", path.display());
    }

    /// Temporary live probe: print one real getBlock JSON envelope.
    /// Run with: cargo test -p ingest-core print_raw_get_block -- --ignored --nocapture
    #[tokio::test]
    #[ignore = "hits live Helius; run with --ignored --nocapture"]
    async fn print_raw_get_block() {
        let client = RpcClient::new(helius_url());
        let tip = client.get_slot().await.expect("getSlot");
        let slot = tip.saturating_sub(32);
        eprintln!("fetching getBlock for slot {slot} (finalized tip {tip})");

        let raw = client.get_block_raw(slot).await.expect("getBlock raw");
        let pretty = serde_json::to_string_pretty(&raw).expect("pretty-print JSON");
        println!("{pretty}");
    }

    #[test]
    fn live_fixture_deserializes_into_raw_structs() {
        let value: serde_json::Value =
            serde_json::from_str(include_str!("getblock_live_fixture.json")).unwrap();
        let block = decode_get_block_json(440494625, value).expect("fixture must match Raw* structs");
        assert_eq!(block.block_time, Some(1787237909));
        assert_eq!(block.transactions.len(), 3);
        assert!(!block.transactions[0].failed);
        assert!(block.transactions[1].failed);
        assert!(!block.transactions[2].token_deltas.is_empty());
        assert!(!block.transactions[2].locks.is_empty());
    }

    #[tokio::test]
    #[ignore = "hits live Helius; run with --ignored --nocapture"]
    async fn parse_live_get_block() {
        let client = RpcClient::new(helius_url());
        let tip = client.get_slot().await.expect("getSlot");
        let slot = tip.saturating_sub(32);
        let block = client.get_block(slot).await.expect("get_block parse");
        let failed = block.transactions.iter().filter(|t| t.failed).count();
        let with_tokens = block.transactions.iter().filter(|t| !t.token_deltas.is_empty()).count();
        eprintln!(
            "parsed slot {slot}: {} txs, {failed} failed, {with_tokens} with token deltas",
            block.transactions.len()
        );
        assert!(!block.transactions.is_empty());
    }
}
