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
        transactions: &[(
            String,      // signature
            bool,        // failed
            Vec<String>, // program_ids
            Option<i32>, // step (from contention scheduler, if computed yet)
        )],
        locks: &[(String, String, bool)], // (tx_signature, account, is_writable)
        token_changes: &[(String, String, u64, u64, u8)], // (tx_sig, mint, pre, post, decimals)
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
            for (sig, mint, pre, post, decimals) in token_changes {
                appender.append_row(params![
                    sig,
                    slot_i,
                    block_time,
                    mint,
                    *pre as i64,
                    *post as i64,
                    *decimals as i32
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
    pub fn replace_slots_batch(
        &mut self,
        slots: Vec<(
            u64,
            Option<i64>,
            Vec<(String, bool, Vec<String>, Option<i32>)>,
            Vec<(String, String, bool)>,
            Vec<(String, String, u64, u64, u8)>,
        )>,
    ) -> Result<(), StorageError> {
        self.conn.execute_batch("BEGIN TRANSACTION")?;
        for (slot, block_time, transactions, locks, token_changes) in slots {
            self.replace_slot_data_inner(slot, block_time, &transactions, &locks, &token_changes)?;
        }
        self.conn.execute_batch("COMMIT")?;
        Ok(())
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

    /// Indexed mints with activity counts (GET /api/tokens).
    pub fn query_tokens(&self) -> Result<Vec<(String, i64)>, StorageError> {
        let mut stmt = self.conn.prepare(
            "SELECT mint, COUNT(*) as activity_count
             FROM token_balance_changes
             GROUP BY mint
             ORDER BY activity_count DESC",
        )?;
        let rows = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
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
}

#[derive(Debug, serde::Serialize)]
pub struct ContentionSummary {
    pub depth: u64,
    pub top_conflicting_accounts: Vec<(String, i64)>,
}
