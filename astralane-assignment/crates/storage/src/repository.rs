//! Typed repository layer. All SQL lives here; callers (cli, api) never see
//! raw queries.
//!
//! Idempotency strategy (FR-1.5): DuckDB's Appender API is fast for bulk
//! inserts but does NOT enforce ON CONFLICT semantics the way a plain
//! prepared-statement INSERT can. Rather than fight that, we use a
//! delete-then-insert pattern per slot, wrapped in one DuckDB transaction:
//! before writing a slot's data, delete any existing rows for that slot,
//! then Appender-insert the fresh set. Re-running ingestion for an
//! already-ingested slot always converges to the same end state — that's
//! the actual idempotency guarantee, not a row-level upsert.
//!
//! Write batching (ADR-1 caveat): `replace_slot_data` is written to accept
//! a *batch* of blocks, not one block at a time — call it once per N
//! fetched blocks (see the FINDINGS.md write-path-contention experiment)
//! rather than once per block, or DuckDB's per-Appender-call overhead will
//! dominate.

use crate::StorageError;
use contention::ScheduleResult;
use duckdb::{params, Connection};
use ohlcv::Candle;
use std::collections::HashMap;

pub type TransactionRow = (String, bool, Vec<String>, Option<i32>);
pub type AccountLockRow = (String, String, bool);
pub type TokenBalanceRow = (String, String, u64, u64, u8);
pub type SlotBatchRow = (
    u64,
    Option<i64>,
    Vec<TransactionRow>,
    Vec<AccountLockRow>,
    Vec<TokenBalanceRow>,
);
/// One `token_balance_changes` row. CLI maps these into `ohlcv::TxSnapshot`.
pub type BalanceChangeRow = (
    String,      // tx_signature
    u64,         // slot
    Option<i64>, // block_time
    String,      // mint
    u64,         // pre_amount
    u64,         // post_amount
    u8,          // decimals
);

pub struct Repository<'a> {
    conn: &'a Connection,
}

impl<'a> Repository<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self { conn }
    }

    /// Record the outcome of attempting a slot (ingested / skipped / failed).
    /// Always safe to call repeatedly for the same slot — last write wins,
    /// which is correct here since ingestion_status just reflects the most
    /// recent attempt's outcome.
    pub fn upsert_ingestion_status(
        &self,
        slot: u64,
        status: &str,
        note: Option<&str>,
    ) -> Result<(), StorageError> {
        self.conn.execute(
            "INSERT INTO ingestion_status (slot, status, note) VALUES (?, ?, ?)
             ON CONFLICT (slot) DO UPDATE SET status = excluded.status, note = excluded.note",
            params![slot as i64, status, note],
        )?;
        Ok(())
    }

    /// Delete + re-insert all data for one slot's transactions, locks, and
    /// token balance changes. Called per-slot from within a batch
    /// transaction by `replace_slots_batch` below.
    fn replace_slot_data_inner(
        &self,
        slot: u64,
        block_time: Option<i64>,
        transactions: &[TransactionRow],
        locks: &[AccountLockRow],
        token_changes: &[TokenBalanceRow],
    ) -> Result<(), StorageError> {
        let slot_i = slot as i64;

        self.conn.execute("DELETE FROM account_locks WHERE slot = ?", params![slot_i])?;
        self.conn.execute(
            "DELETE FROM token_balance_changes WHERE slot = ?",
            params![slot_i],
        )?;
        self.conn.execute("DELETE FROM transactions WHERE slot = ?", params![slot_i])?;
        self.conn.execute(
            "INSERT INTO blocks (slot, block_time, tx_count) VALUES (?, ?, ?)
             ON CONFLICT (slot) DO UPDATE SET block_time = excluded.block_time, tx_count = excluded.tx_count",
            params![slot_i, block_time, transactions.len() as i32],
        )?;

        // Transactions + locks + token balance changes: batched via Appender
        // for throughput (see ADR-1 caveat — never insert one row at a time).
        {
            let mut appender = self.conn.appender("transactions")?;
            for (sig, failed, program_ids, step) in transactions {
                let program_ids_json = serde_json::to_string(program_ids)?;
                appender.append_row(params![
                    sig,
                    slot_i,
                    block_time,
                    *failed,
                    program_ids_json,
                    *step
                ])?;
            }
            appender.flush()?;
        }
        {
            let mut appender = self.conn.appender("account_locks")?;
            for (sig, account, is_writable) in locks {
                appender.append_row(params![slot_i, sig, account, *is_writable])?;
            }
            appender.flush()?;
        }
        {
            let mut appender = self.conn.appender("token_balance_changes")?;
            for (sig, mint, pre, post, decimals) in coalesce_token_balance_rows(token_changes) {
                appender.append_row(params![
                    sig,
                    slot_i,
                    block_time,
                    mint,
                    pre as i64,
                    post as i64,
                    decimals as i32
                ])?;
            }
            appender.flush()?;
        }

        Ok(())
    }

    /// Batch entry point: replace data for multiple slots inside a single
    /// DuckDB transaction. This is the write-path-contention lever —
    /// tune the batch size the caller uses (e.g. every 20-50 blocks) based
    /// on the FINDINGS.md experiment rather than calling this per-block.
    pub fn replace_slots_batch(&mut self, slots: Vec<SlotBatchRow>) -> Result<(), StorageError> {
        self.conn.execute_batch("BEGIN TRANSACTION")?;
        let result = (|| {
            for (slot, block_time, transactions, locks, token_changes) in slots {
                self.replace_slot_data_inner(
                    slot,
                    block_time,
                    &transactions,
                    &locks,
                    &token_changes,
                )?;
            }
            self.conn.execute_batch("COMMIT")?;
            Ok(())
        })();
        if result.is_err() {
            // Constraint failures abort the DuckDB txn; without ROLLBACK the
            // next statement on this connection fails with "transaction is
            // aborted" instead of the original error.
            let _ = self.conn.execute_batch("ROLLBACK");
        }
        result
    }

    /// Write back schedule step assignments computed by the `contention`
    /// crate for one block's transactions (signatures must match rows
    /// already inserted via `replace_slots_batch`).
    pub fn write_schedule_steps(
        &self,
        signatures: &[String],
        schedule: &ScheduleResult,
    ) -> Result<(), StorageError> {
        for (sig, step) in signatures.iter().zip(schedule.steps.iter()) {
            self.conn.execute(
                "UPDATE transactions SET step = ? WHERE signature = ?",
                params![*step as i32, sig],
            )?;
        }
        Ok(())
    }

    /// Upsert a batch of candles for one mint+interval (called after
    /// `ohlcv::build_candles`).
    pub fn upsert_candles(
        &self,
        mint: &str,
        interval_sec: i32,
        candles: &[Candle],
    ) -> Result<(), StorageError> {
        for c in candles {
            self.conn.execute(
                "INSERT INTO candles (mint, interval_sec, bucket_start, open, high, low, close, volume)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                 ON CONFLICT (mint, interval_sec, bucket_start) DO UPDATE SET
                     open = excluded.open, high = excluded.high, low = excluded.low,
                     close = excluded.close, volume = excluded.volume",
                params![mint, interval_sec, c.bucket_start, c.open, c.high, c.low, c.close, c.volume],
            )?;
        }
        Ok(())
    }

    // ---------------- Read-side queries for the API ----------------

    /// Contention metrics for a slot range (FR-2.3 / GET /api/contention).
    pub fn query_contention_summary(
        &self,
        from_slot: u64,
        to_slot: u64,
    ) -> Result<ContentionSummary, StorageError> {
        let depth: i64 = self.conn.query_row(
            "SELECT COALESCE(MAX(step), -1) + 1 FROM transactions WHERE slot BETWEEN ? AND ?",
            params![from_slot as i64, to_slot as i64],
            |row| row.get(0),
        )?;

        let mut stmt = self.conn.prepare(
            "SELECT account, COUNT(*) as conflict_count
             FROM account_locks
             WHERE slot BETWEEN ? AND ? AND is_writable = true
             GROUP BY account
             ORDER BY conflict_count DESC
             LIMIT 20",
        )?;
        let top_accounts: Vec<(String, i64)> = stmt
            .query_map(params![from_slot as i64, to_slot as i64], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })?
            .collect::<Result<_, _>>()?;

        Ok(ContentionSummary { depth: depth.max(0) as u64, top_conflicting_accounts: top_accounts })
    }

    /// Mints that have at least one stored candle (GET /api/tokens).
    /// The dashboard dropdown must not list balance-activity mints with no
    /// inferred OHLCV — those would show "No data for this range".
    pub fn query_tokens(&self) -> Result<Vec<TokenOption>, StorageError> {
        let mut stmt = self.conn.prepare(
            "SELECT mint,
                    SUM(CASE WHEN interval_sec = 60 THEN 1 ELSE 0 END) AS candles_1m,
                    SUM(CASE WHEN interval_sec = 300 THEN 1 ELSE 0 END) AS candles_5m
             FROM candles
             GROUP BY mint
             HAVING SUM(CASE WHEN interval_sec = 60 THEN 1 ELSE 0 END) > 0
                 OR SUM(CASE WHEN interval_sec = 300 THEN 1 ELSE 0 END) > 0
             ORDER BY (SUM(CASE WHEN interval_sec = 60 THEN 1 ELSE 0 END)
                     + SUM(CASE WHEN interval_sec = 300 THEN 1 ELSE 0 END)) DESC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(TokenOption {
                    mint: row.get(0)?,
                    candles_1m: row.get(1)?,
                    candles_5m: row.get(2)?,
                })
            })?
            .collect::<Result<_, _>>()?;
        Ok(rows)
    }

    /// Inclusive slot range. Callers (CLI) group by signature into snapshots.
    pub fn query_balance_changes_for_range(
        &self,
        from_slot: u64,
        to_slot: u64,
    ) -> Result<Vec<BalanceChangeRow>, StorageError> {
        let mut stmt = self.conn.prepare(
            "SELECT tx_signature, slot, block_time, mint, pre_amount, post_amount, decimals
             FROM token_balance_changes
             WHERE slot BETWEEN ? AND ?
             ORDER BY slot ASC, tx_signature ASC",
        )?;
        let rows = stmt
            .query_map(params![from_slot as i64, to_slot as i64], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)? as u64,
                    row.get::<_, Option<i64>>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)? as u64,
                    row.get::<_, i64>(5)? as u64,
                    row.get::<_, i32>(6)? as u8,
                ))
            })?
            .collect::<Result<_, _>>()?;
        Ok(rows)
    }

    /// Candle series for one mint+interval (GET /api/ohlcv).
    pub fn query_ohlcv(&self, mint: &str, interval_sec: i32) -> Result<Vec<Candle>, StorageError> {
        let mut stmt = self.conn.prepare(
            "SELECT bucket_start, open, high, low, close, volume
             FROM candles
             WHERE mint = ? AND interval_sec = ?
             ORDER BY bucket_start ASC",
        )?;
        let rows = stmt
            .query_map(params![mint, interval_sec], |row| {
                Ok(Candle {
                    bucket_start: row.get(0)?,
                    open: row.get(1)?,
                    high: row.get(2)?,
                    low: row.get(3)?,
                    close: row.get(4)?,
                    volume: row.get(5)?,
                })
            })?
            .collect::<Result<_, _>>()?;
        Ok(rows)
    }

    /// Counts used by `stats` / FINDINGS (coverage, throughput denominators).
    pub fn query_db_stats(&self) -> Result<DbStats, StorageError> {
        let blocks: i64 = self.conn.query_row("SELECT COUNT(*) FROM blocks", [], |r| r.get(0))?;
        let transactions: i64 =
            self.conn.query_row("SELECT COUNT(*) FROM transactions", [], |r| r.get(0))?;
        let failed_transactions: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM transactions WHERE failed = true",
            [],
            |r| r.get(0),
        )?;
        let token_change_rows: i64 =
            self.conn.query_row("SELECT COUNT(*) FROM token_balance_changes", [], |r| r.get(0))?;
        let distinct_mints: i64 = self.conn.query_row(
            "SELECT COUNT(DISTINCT mint) FROM token_balance_changes",
            [],
            |r| r.get(0),
        )?;
        let distinct_tx_with_balances: i64 = self.conn.query_row(
            "SELECT COUNT(DISTINCT tx_signature) FROM token_balance_changes",
            [],
            |r| r.get(0),
        )?;
        let tx_with_wsol: i64 = self.conn.query_row(
            "SELECT COUNT(DISTINCT tx_signature) FROM token_balance_changes
             WHERE mint = 'So11111111111111111111111111111111111111112'",
            [],
            |r| r.get(0),
        )?;
        let candles_1m: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM candles WHERE interval_sec = 60",
            [],
            |r| r.get(0),
        )?;
        let candles_5m: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM candles WHERE interval_sec = 300",
            [],
            |r| r.get(0),
        )?;
        Ok(DbStats {
            blocks,
            transactions,
            failed_transactions,
            token_change_rows,
            distinct_mints,
            distinct_tx_with_balances,
            tx_with_wsol,
            candles_1m,
            candles_5m,
        })
    }
}

/// Net multiple token-account deltas for the same (tx, mint) so the
/// `(tx_signature, mint)` primary key holds. A swap often moves two ATAs of
/// the same mint; OHLCV only needs the net mint delta.
fn coalesce_token_balance_rows(rows: &[TokenBalanceRow]) -> Vec<TokenBalanceRow> {
    let mut net: HashMap<(String, String), (i128, u8)> = HashMap::new();
    let mut order: Vec<(String, String)> = Vec::new();
    for (sig, mint, pre, post, decimals) in rows {
        let key = (sig.clone(), mint.clone());
        let delta = *post as i128 - *pre as i128;
        if let Some((n, _)) = net.get_mut(&key) {
            *n += delta;
        } else {
            order.push(key.clone());
            net.insert(key, (delta, *decimals));
        }
    }
    order
        .into_iter()
        .filter_map(|key| {
            let (n, decimals) = net.get(&key).copied()?;
            if n == 0 {
                return None;
            }
            let (pre, post) = if n > 0 { (0, n as u64) } else { ((-n) as u64, 0) };
            Some((key.0, key.1, pre, post, decimals))
        })
        .collect()
}

#[derive(Debug, serde::Serialize)]
pub struct ContentionSummary {
    pub depth: u64,
    pub top_conflicting_accounts: Vec<(String, i64)>,
}

/// One mint in the dashboard token dropdown: only mints with stored candles.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TokenOption {
    pub mint: String,
    pub candles_1m: i64,
    pub candles_5m: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DbStats {
    pub blocks: i64,
    pub transactions: i64,
    pub failed_transactions: i64,
    pub token_change_rows: i64,
    pub distinct_mints: i64,
    pub distinct_tx_with_balances: i64,
    pub tx_with_wsol: i64,
    pub candles_1m: i64,
    pub candles_5m: i64,
}
