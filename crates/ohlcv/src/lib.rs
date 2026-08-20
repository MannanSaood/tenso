//! Pure logic, zero I/O. Infers per-trade prices from paired SPL-token /
//! wrapped-SOL balance deltas within a single transaction, and aggregates
//! those inferred trades into OHLCV candles at a given interval.
//!
//! Deliberately does NOT decode any DEX/DeFi instruction — per the
//! assignment, only `preTokenBalances`/`postTokenBalances` and
//! `preBalances`/`postBalances` are used. This means every price here is an
//! *inference*, not ground truth, and the documented exclusion rules below
//! exist specifically to keep that inference honest rather than silently
//! wrong.
//!
//! Excluded, on purpose (see each check below for the specific rule):
//!   - SOL <-> wSOL wrap/unwrap only (no non-SOL counter-asset — not a trade)
//!   - Same-direction deltas (e.g. LP add/remove: both legs increase or both
//!     decrease together — not a two-sided swap)
//!   - No wSOL leg at all (e.g. a multi-hop route with no direct SOL leg —
//!     out of scope, we don't attempt multi-hop reconstruction)
//!   - Dust trades below a documented minimum SOL-equivalent volume

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Wrapped SOL mint address — used as the "SOL leg" identifier since native
/// SOL balance changes are noisy (fees hit every transaction's fee payer
/// regardless of whether a trade occurred), while wSOL balance changes are
/// clean, trade-specific signals.
pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Minimum SOL-equivalent volume for a trade to be counted; anything smaller
/// is treated as dust and excluded. Documented, adjustable constant.
pub const DUST_THRESHOLD_SOL: f64 = 0.0005;

/// One mint's balance change within a single transaction, already
/// decimal-adjusted is NOT done here — raw amounts + decimals are kept
/// separate so the conversion is explicit and testable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenDelta {
    pub mint: String,
    pub pre_amount: u64,
    pub post_amount: u64,
    pub decimals: u8,
}

impl TokenDelta {
    /// Signed, decimal-adjusted delta. Positive = balance increased (bought),
    /// negative = balance decreased (sold).
    pub fn delta_ui(&self) -> f64 {
        (self.post_amount as f64 - self.pre_amount as f64) / 10f64.powi(self.decimals as i32)
    }
}

/// All token balance changes observed in one transaction, plus the block
/// time needed for candle bucketing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxSnapshot {
    pub signature: String,
    pub block_time: i64,
    pub token_deltas: Vec<TokenDelta>,
}

/// One inferred trade: some mint moved against a wSOL leg in the same tx.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferredTrade {
    pub signature: String,
    pub block_time: i64,
    pub mint: String,
    pub price_in_sol: f64,
    pub volume_sol: f64,
}

/// Infer zero or more trades from a single transaction's balance deltas.
pub fn infer_trades(tx: &TxSnapshot) -> Vec<InferredTrade> {
    let mut trades = Vec::new();

    let wsol = tx.token_deltas.iter().find(|d| d.mint == WSOL_MINT);
    let wsol = match wsol {
        Some(w) if w.delta_ui() != 0.0 => w,
        _ => return trades, // no SOL leg -> excluded (wrap-only or no wSOL touched at all)
    };
    let wsol_delta = wsol.delta_ui();

    for d in &tx.token_deltas {
        if d.mint == WSOL_MINT {
            continue;
        }
        let token_delta = d.delta_ui();
        if token_delta == 0.0 {
            continue;
        }
        // A real two-sided swap has opposite-signed legs: give X, receive SOL
        // (or vice versa). Same-direction deltas (both up or both down) look
        // like LP add/remove or another non-swap balance change — excluded.
        if (token_delta > 0.0) == (wsol_delta > 0.0) {
            continue;
        }
        let volume_sol = wsol_delta.abs();
        if volume_sol < DUST_THRESHOLD_SOL {
            continue; // dust
        }
        let price_in_sol = volume_sol / token_delta.abs();
        trades.push(InferredTrade {
            signature: tx.signature.clone(),
            block_time: tx.block_time,
            mint: d.mint.clone(),
            price_in_sol,
            volume_sol,
        });
    }

    trades
}

/// One OHLCV candle for a fixed time bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candle {
    pub bucket_start: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}

/// Aggregate inferred trades into per-mint candle series at a fixed interval
/// (e.g. 60 for 1m, 300 for 5m). Trades must carry a block_time; bucketing is
/// a simple floor-division into fixed-width windows.
pub fn build_candles(trades: &[InferredTrade], interval_seconds: i64) -> HashMap<String, Vec<Candle>> {
    let mut by_mint: HashMap<&str, Vec<&InferredTrade>> = HashMap::new();
    for t in trades {
        by_mint.entry(t.mint.as_str()).or_default().push(t);
    }

    let mut result: HashMap<String, Vec<Candle>> = HashMap::new();
    for (mint, mut mtrades) in by_mint {
        mtrades.sort_by_key(|t| t.block_time);

        let mut buckets: HashMap<i64, Vec<&InferredTrade>> = HashMap::new();
        for t in &mtrades {
            let bucket_start = t.block_time.div_euclid(interval_seconds) * interval_seconds;
            buckets.entry(bucket_start).or_default().push(t);
        }

        let mut bucket_keys: Vec<i64> = buckets.keys().copied().collect();
        bucket_keys.sort();

        let mut candles = Vec::with_capacity(bucket_keys.len());
        for bucket_start in bucket_keys {
            let bt = &buckets[&bucket_start]; // already time-ordered (mtrades was pre-sorted)
            let prices: Vec<f64> = bt.iter().map(|t| t.price_in_sol).collect();
            let volume: f64 = bt.iter().map(|t| t.volume_sol).sum();
            candles.push(Candle {
                bucket_start,
                open: prices[0],
                close: *prices.last().unwrap(),
                high: prices.iter().cloned().fold(f64::MIN, f64::max),
                low: prices.iter().cloned().fold(f64::MAX, f64::min),
                volume,
            });
        }
        result.insert(mint.to_string(), candles);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn approx(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn simple_swap_infers_one_trade() {
        let tx = TxSnapshot {
            signature: "sig1".into(),
            block_time: 1000,
            token_deltas: vec![
                TokenDelta { mint: "TOKENMINT".into(), pre_amount: 100_000000, post_amount: 0, decimals: 6 },
                TokenDelta { mint: WSOL_MINT.into(), pre_amount: 0, post_amount: 1_000000000, decimals: 9 },
            ],
        };
        let trades = infer_trades(&tx);
        assert_eq!(trades.len(), 1);
        assert!(approx(trades[0].price_in_sol, 0.01));
        assert!(approx(trades[0].volume_sol, 1.0));
    }

    #[test]
    fn wrap_only_excluded() {
        let tx = TxSnapshot {
            signature: "sig2".into(),
            block_time: 1000,
            token_deltas: vec![
                TokenDelta { mint: WSOL_MINT.into(), pre_amount: 0, post_amount: 5_000000000, decimals: 9 },
            ],
        };
        assert!(infer_trades(&tx).is_empty());
    }

    #[test]
    fn dust_trade_excluded() {
        let tx = TxSnapshot {
            signature: "sig3".into(),
            block_time: 1000,
            token_deltas: vec![
                TokenDelta { mint: "TOKENMINT".into(), pre_amount: 10_000000, post_amount: 0, decimals: 6 },
                TokenDelta { mint: WSOL_MINT.into(), pre_amount: 0, post_amount: 100_000, decimals: 9 }, // 0.0001 SOL
            ],
        };
        assert!(infer_trades(&tx).is_empty());
    }

    #[test]
    fn same_direction_deltas_excluded() {
        // Both legs increase together — looks like an LP add, not a swap.
        let tx = TxSnapshot {
            signature: "sig4".into(),
            block_time: 1000,
            token_deltas: vec![
                TokenDelta { mint: "TOKENMINT".into(), pre_amount: 0, post_amount: 100_000000, decimals: 6 },
                TokenDelta { mint: WSOL_MINT.into(), pre_amount: 0, post_amount: 1_000000000, decimals: 9 },
            ],
        };
        assert!(infer_trades(&tx).is_empty());
    }

    #[test]
    fn no_wsol_leg_excluded() {
        let tx = TxSnapshot {
            signature: "sig5".into(),
            block_time: 1000,
            token_deltas: vec![
                TokenDelta { mint: "TOKENMINT_A".into(), pre_amount: 100_000000, post_amount: 0, decimals: 6 },
                TokenDelta { mint: "TOKENMINT_B".into(), pre_amount: 0, post_amount: 50_000000, decimals: 6 },
            ],
        };
        assert!(infer_trades(&tx).is_empty());
    }

    #[test]
    fn candle_bucketing_and_aggregation() {
        let mint = "TOKENMINT".to_string();
        let trades = vec![
            InferredTrade { signature: "s1".into(), block_time: 960, mint: mint.clone(), price_in_sol: 0.01, volume_sol: 1.0 },
            InferredTrade { signature: "s2".into(), block_time: 980, mint: mint.clone(), price_in_sol: 0.012, volume_sol: 2.0 },
            InferredTrade { signature: "s3".into(), block_time: 1010, mint: mint.clone(), price_in_sol: 0.009, volume_sol: 0.5 },
            InferredTrade { signature: "s4".into(), block_time: 1090, mint: mint.clone(), price_in_sol: 0.011, volume_sol: 1.5 },
        ];
        let candles = build_candles(&trades, 60);
        let c = &candles[&mint];
        assert_eq!(c.len(), 2);
        assert!(approx(c[0].open, 0.01));
        assert!(approx(c[0].close, 0.009));
        assert!(approx(c[0].high, 0.012));
        assert!(approx(c[0].low, 0.009));
        assert!(approx(c[0].volume, 3.5));
        assert_eq!(c[1].bucket_start, 1080);
    }

    #[test]
    fn empty_trades_produce_no_candles() {
        let candles = build_candles(&[], 60);
        assert!(candles.is_empty());
    }
}
