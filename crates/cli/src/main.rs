//! Thin binary. Two subcommands: `ingest` (wires ingest-core + contention +
//! ohlcv + storage) and `serve` (wires api). This is the ONLY crate allowed
//! to use anyhow / unwrap / expect outside tests — every other crate stays
//! pure and typed.
//!
//! Ingest runs a 3-stage bounded mpsc pipeline (FR-5.1): fetch → parse → store.
//! Send policy is block (`Sender::send().await`), not shed or unbounded buffer.
//! Remaining permits are logged via `Sender::capacity()`; `--simulate-pause-secs`
//! pauses the store stage once so that drop toward 0 is observable.
//!
//! `--serve` starts the HTTP API in the same process as ingest (FR-4.5).
//! Parse/store DuckDB work uses `spawn_blocking` unless `--cpu-inline` is set
//! (FR-5.2 before/after comparison).

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use ingest_core::{DecodedBlock, FetchOutcome, RpcClient, TokenBucketLimiter};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use storage::Repository;

/// Bounded mpsc capacity between pipeline stages (fetch→parse and parse→store).
///
/// Live `getBlock` of a busy slot takes multiple seconds (~3–4 completions per
/// 10s even with `--max-concurrency 8`), so a 10s store pause only queues a
/// handful of blocks. Capacity is 2: one slot can sit buffered without
/// immediately stalling fetch, and `--simulate-pause-secs 10` still drives
/// `Sender::capacity()` to 0 (FR-5.1 block policy). The DuckDB batch itself
/// lives in the store task's `Vec`, not in this channel.
const PIPELINE_CAPACITY: usize = 2;

#[derive(Parser)]
#[command(name = "block-analysis")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Fetch a slot range, compute contention + OHLCV, persist to DuckDB.
    Ingest(IngestArgs),
    /// Serve the API + dashboard.
    Serve {
        #[arg(long, default_value = "blocks.duckdb")]
        db_path: String,
        #[arg(long, default_value_t = 8080)]
        port: u16,
    },
}

#[derive(Args)]
struct IngestArgs {
    #[arg(long)]
    rpc_endpoint: String,
    #[arg(long)]
    start_slot: u64,
    #[arg(long, default_value_t = 1000)]
    count: u64,
    #[arg(long, default_value_t = 10.0)]
    rate_per_sec: f64,
    #[arg(long, default_value_t = 8)]
    max_concurrency: usize,
    #[arg(long, default_value_t = 25)]
    batch_size: usize,
    #[arg(long, default_value = "blocks.duckdb")]
    db_path: String,
    /// Pause the store stage once, in seconds, so fetch/parse fill the
    /// bounded channels (FR-5.1). 0 disables the pause.
    #[arg(long, default_value_t = 0)]
    simulate_pause_secs: u64,
    /// Serve the dashboard/API in this process while ingesting (FR-4.5).
    #[arg(long)]
    serve: bool,
    #[arg(long, default_value_t = 8080)]
    port: u16,
    /// Run parse/contention CPU on the tokio worker instead of spawn_blocking
    /// (FR-5.2 "before" measurement). Default is spawn_blocking.
    #[arg(long)]
    cpu_inline: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Command::Ingest(args) => run_ingest(args).await,
        Command::Serve { db_path, port } => run_serve(db_path, port).await,
    }
}

async fn run_ingest(args: IngestArgs) -> Result<()> {
    // DuckDB's Connection is Send but not Sync (interior RefCell). Shared
    // access goes through Mutex — same pattern as the API server. Store and
    // API both take this lock; `/api/health` does not (FR-4.5).
    let ingest_started = std::time::Instant::now();
    log_process_rss("ingest_start");
    let conn = Arc::new(Mutex::new(storage::open(&args.db_path).context("opening duckdb")?));
    let client = Arc::new(RpcClient::new(args.rpc_endpoint));
    let limiter = Arc::new(TokenBucketLimiter::new(args.rate_per_sec, args.rate_per_sec));

    let server_handle = if args.serve {
        let state = api::AppState { conn: Arc::clone(&conn) };
        let addr = format!("0.0.0.0:{}", args.port);
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .with_context(|| format!("binding {addr}"))?;
        tracing::info!(%addr, "serving during ingest (FR-4.5)");
        Some(tokio::spawn(async move {
            axum::serve(listener, api::build_router(state))
                .await
                .map_err(anyhow::Error::from)
        }))
    } else {
        None
    };

    let (fetch_tx, mut fetch_rx) = tokio::sync::mpsc::channel::<FetchOutcome>(PIPELINE_CAPACITY);
    let (parse_tx, mut parse_rx) = tokio::sync::mpsc::channel::<ParsedOutcome>(PIPELINE_CAPACITY);

    let fetch = tokio::spawn(async move {
        ingest_core::fetch_block_range(
            client,
            limiter,
            args.start_slot,
            args.count,
            args.max_concurrency,
            |outcome| {
                let tx = fetch_tx.clone();
                async move {
                    let slot = fetch_outcome_slot(&outcome);
                    let remaining = tx.capacity();
                    tracing::info!(
                        stage = "fetch→parse",
                        slot,
                        remaining,
                        pipeline_capacity = PIPELINE_CAPACITY,
                        "mpsc remaining permits before blocking send"
                    );
                    if remaining == 0 {
                        tracing::warn!(
                            stage = "fetch→parse",
                            slot,
                            "channel full; blocking send (FR-5.1 backpressure)"
                        );
                    }
                    if tx.send(outcome).await.is_err() {
                        tracing::warn!(slot, "parse receiver dropped");
                    }
                }
            },
        )
        .await;
    });

    let cpu_inline = args.cpu_inline;
    let parse = tokio::spawn(async move {
        while let Some(outcome) = fetch_rx.recv().await {
            let parsed = if cpu_inline {
                parse_outcome(outcome)
            } else {
                tokio::task::spawn_blocking(move || parse_outcome(outcome))
                    .await
                    .context("parse worker panicked")?
            };
            let slot = parsed.slot();
            let remaining = parse_tx.capacity();
            tracing::info!(
                stage = "parse→store",
                slot,
                remaining,
                pipeline_capacity = PIPELINE_CAPACITY,
                "mpsc remaining permits before blocking send"
            );
            if remaining == 0 {
                tracing::warn!(
                    stage = "parse→store",
                    slot,
                    "channel full; blocking send (FR-5.1 backpressure)"
                );
            }
            if parse_tx.send(parsed).await.is_err() {
                anyhow::bail!("store receiver dropped");
            }
        }
        Ok::<_, anyhow::Error>(())
    });

    let store_conn = Arc::clone(&conn);
    let store = tokio::spawn(async move {
        let mut pending_batch = Vec::new();
        let mut ingested = 0u64;
        let mut ingested_txs = 0u64;
        let mut skipped = 0u64;
        let mut failed = 0u64;
        let mut did_pause = false;

        while let Some(parsed) = parse_rx.recv().await {
            if !did_pause && args.simulate_pause_secs > 0 {
                did_pause = true;
                tracing::warn!(
                    secs = args.simulate_pause_secs,
                    "store stage pausing once (FR-5.1 backpressure)"
                );
                tokio::time::sleep(std::time::Duration::from_secs(args.simulate_pause_secs)).await;
                tracing::warn!("store stage resumed");
            }

            match parsed {
                ParsedOutcome::Ingested(block) => {
                    let slot = block.slot;
                    db_blocking(Arc::clone(&store_conn), move |conn| {
                        Repository::new(conn)
                            .upsert_ingestion_status(slot, "ingested", None)
                            .map_err(anyhow::Error::from)
                    })
                    .await?;
                    ingested_txs += block.transactions.len() as u64;
                    pending_batch.push(block);
                    ingested += 1;
                }
                ParsedOutcome::Skipped { slot } => {
                    db_blocking(Arc::clone(&store_conn), move |conn| {
                        Repository::new(conn)
                            .upsert_ingestion_status(slot, "skipped", None)
                            .map_err(anyhow::Error::from)
                    })
                    .await?;
                    skipped += 1;
                }
                ParsedOutcome::Failed { slot, error } => {
                    let note = error.clone();
                    db_blocking(Arc::clone(&store_conn), move |conn| {
                        Repository::new(conn)
                            .upsert_ingestion_status(slot, "failed", Some(&note))
                            .map_err(anyhow::Error::from)
                    })
                    .await?;
                    failed += 1;
                    tracing::error!(slot, error, "slot failed");
                }
            }

            if pending_batch.len() >= args.batch_size {
                let conn = Arc::clone(&store_conn);
                let batch = std::mem::take(&mut pending_batch);
                match tokio::task::spawn_blocking(move || flush_parsed_batch(&conn, batch))
                    .await
                    .context("flush worker panicked")?
                {
                    Ok(()) => log_process_rss("after_batch_flush"),
                    Err(e) => tracing::error!(?e, "failed to flush batch"),
                }
                tracing::info!(ingested, ingested_txs, skipped, failed, "ingest progress");
            }
        }

        if !pending_batch.is_empty() {
            let conn = Arc::clone(&store_conn);
            let batch = pending_batch;
            tokio::task::spawn_blocking(move || flush_parsed_batch(&conn, batch))
                .await
                .context("flush worker panicked")??;
            log_process_rss("after_final_flush");
        }
        Ok::<_, anyhow::Error>((ingested, skipped, failed, ingested_txs))
    });

    fetch.await.context("fetch stage join")?;
    parse.await.context("parse stage join")??;
    let (ingested, skipped, failed, ingested_txs) = store.await.context("store stage join")??;

    let elapsed = ingest_started.elapsed();
    let attempted = ingested + skipped + failed;
    let ingest_secs = elapsed.as_secs_f64().max(1e-6);
    let slots_per_sec = attempted as f64 / ingest_secs;
    let txs_per_sec = ingested_txs as f64 / ingest_secs;
    tracing::info!(
        ingested,
        skipped,
        failed,
        ingested_txs,
        elapsed_secs = ingest_secs,
        slots_per_sec,
        txs_per_sec,
        "ingestion complete"
    );
    log_process_rss("after_store");

    // Build OHLCV candles across the whole ingested range once ingestion is
    // done. For a live/streaming variant you'd do this incrementally per
    // batch instead — left as a whole-range pass here for simplicity.
    let from_slot = args.start_slot;
    let to_slot = args.start_slot.saturating_add(args.count.saturating_sub(1));
    let candle_conn = Arc::clone(&conn);
    tokio::task::spawn_blocking(move || build_and_store_candles(&candle_conn, from_slot, to_slot))
        .await
        .context("candle worker panicked")??;

    let stats_conn = Arc::clone(&conn);
    let stats = tokio::task::spawn_blocking(move || {
        let guard = stats_conn.lock().unwrap();
        Repository::new(&guard).query_db_stats().map_err(anyhow::Error::from)
    })
    .await
    .context("stats worker panicked")??;
    tracing::info!(
        blocks = stats.blocks,
        transactions = stats.transactions,
        failed_transactions = stats.failed_transactions,
        token_change_rows = stats.token_change_rows,
        distinct_mints = stats.distinct_mints,
        distinct_tx_with_balances = stats.distinct_tx_with_balances,
        tx_with_wsol = stats.tx_with_wsol,
        candles_1m = stats.candles_1m,
        candles_5m = stats.candles_5m,
        txs_per_sec = stats.transactions as f64 / ingest_secs,
        "db stats after ingest"
    );
    log_process_rss("after_candles");

    if let Some(handle) = server_handle {
        tracing::info!("ingestion complete; dashboard serving until Ctrl+C");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("shutting down");
            }
            res = handle => {
                res.context("server join")??;
            }
        }
    }

    Ok(())
}

async fn db_blocking<T, F>(conn: Arc<Mutex<duckdb::Connection>>, f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce(&duckdb::Connection) -> Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let guard = conn.lock().unwrap();
        f(&guard)
    })
    .await
    .context("db worker panicked")?
}

fn fetch_outcome_slot(outcome: &FetchOutcome) -> u64 {
    match outcome {
        FetchOutcome::Ingested(block) => block.slot,
        FetchOutcome::Skipped { slot } | FetchOutcome::Failed { slot, .. } => *slot,
    }
}

enum ParsedOutcome {
    Ingested(ParsedBlock),
    Skipped { slot: u64 },
    Failed { slot: u64, error: String },
}

impl ParsedOutcome {
    fn slot(&self) -> u64 {
        match self {
            ParsedOutcome::Ingested(block) => block.slot,
            ParsedOutcome::Skipped { slot } | ParsedOutcome::Failed { slot, .. } => *slot,
        }
    }
}

struct ParsedBlock {
    slot: u64,
    block_time: Option<i64>,
    transactions: Vec<(String, bool, Vec<String>, Option<i32>)>,
    locks: Vec<(String, String, bool)>,
    token_changes: Vec<(String, String, u64, u64, u8)>,
    signatures: Vec<String>,
    schedule: contention::ScheduleResult,
}

fn parse_outcome(outcome: FetchOutcome) -> ParsedOutcome {
    match outcome {
        FetchOutcome::Skipped { slot } => ParsedOutcome::Skipped { slot },
        FetchOutcome::Failed { slot, error } => ParsedOutcome::Failed { slot, error },
        FetchOutcome::Ingested(block) => ParsedOutcome::Ingested(parse_block(block)),
    }
}

fn parse_block(block: DecodedBlock) -> ParsedBlock {
    let transactions: Vec<_> = block
        .transactions
        .iter()
        .map(|t| (t.signature.clone(), t.failed, t.program_ids.clone(), None))
        .collect();
    let locks: Vec<_> = block
        .transactions
        .iter()
        .flat_map(|t| {
            t.locks
                .iter()
                .map(move |l| (t.signature.clone(), l.account.clone(), l.is_writable))
        })
        .collect();
    let token_changes: Vec<_> = block
        .transactions
        .iter()
        .filter(|t| !t.failed) // OHLCV excludes failed transactions
        .flat_map(|t| {
            t.token_deltas.iter().map(move |d| {
                (t.signature.clone(), d.mint.clone(), d.pre_amount, d.post_amount, d.decimals)
            })
        })
        .collect();

    // Contention scheduling, per block (schedules don't cross block
    // boundaries — each block is independently scheduled).
    let tx_locks: Vec<contention::TxLocks> = block
        .transactions
        .iter()
        .map(|t| contention::TxLocks {
            signature: t.signature.clone(),
            program_ids: t.program_ids.clone(),
            locks: t
                .locks
                .iter()
                .map(|l| contention::AccountLock {
                    account: l.account.clone(),
                    is_writable: l.is_writable,
                })
                .collect(),
        })
        .collect();
    let schedule = contention::build_schedule(&tx_locks);
    let signatures: Vec<String> = block.transactions.iter().map(|t| t.signature.clone()).collect();

    ParsedBlock {
        slot: block.slot,
        block_time: block.block_time,
        transactions,
        locks,
        token_changes,
        signatures,
        schedule,
    }
}

fn flush_parsed_batch(conn: &Mutex<duckdb::Connection>, blocks: Vec<ParsedBlock>) -> Result<()> {
    let conn = conn.lock().unwrap();
    let mut repo = Repository::new(&conn);

    let mut schedules = Vec::with_capacity(blocks.len());
    let mut slot_rows = Vec::with_capacity(blocks.len());
    for block in blocks {
        slot_rows.push((
            block.slot,
            block.block_time,
            block.transactions,
            block.locks,
            block.token_changes,
        ));
        schedules.push((block.signatures, block.schedule));
    }
    repo.replace_slots_batch(slot_rows)?;

    for (signatures, schedule) in &schedules {
        repo.write_schedule_steps(signatures, schedule)?;
    }

    Ok(())
}

fn build_and_store_candles(
    conn: &Mutex<duckdb::Connection>,
    from_slot: u64,
    to_slot: u64,
) -> Result<()> {
    let conn = conn.lock().unwrap();
    let repo = Repository::new(&conn);

    let rows = repo.query_balance_changes_for_range(from_slot, to_slot)?;
    let snapshots = snapshots_from_balance_rows(rows);

    let mut trades = Vec::new();
    for tx in &snapshots {
        trades.extend(ohlcv::infer_trades(tx));
    }

    for interval_sec in [60_i32, 300] {
        let by_mint = ohlcv::build_candles(&trades, i64::from(interval_sec));
        for (mint, candles) in by_mint {
            repo.upsert_candles(&mint, interval_sec, &candles)?;
        }
    }

    tracing::info!(
        from_slot,
        to_slot,
        snapshots = snapshots.len(),
        trades = trades.len(),
        "stored OHLCV candles"
    );
    Ok(())
}

/// Group stored balance rows into one snapshot per transaction.
/// Rows with no `block_time` are dropped — candle buckets need a timestamp.
fn snapshots_from_balance_rows(rows: Vec<storage::BalanceChangeRow>) -> Vec<ohlcv::TxSnapshot> {
    let mut by_sig: HashMap<String, ohlcv::TxSnapshot> = HashMap::new();
    for (signature, _slot, block_time, mint, pre_amount, post_amount, decimals) in rows {
        let Some(block_time) = block_time else {
            continue;
        };
        let tx = by_sig.entry(signature.clone()).or_insert_with(|| ohlcv::TxSnapshot {
            signature,
            block_time,
            token_deltas: Vec::new(),
        });
        tx.token_deltas.push(ohlcv::TokenDelta {
            mint,
            pre_amount,
            post_amount,
            decimals,
        });
    }
    by_sig.into_values().collect()
}

/// Current and OS-tracked peak working set. Sampled while `getBlock` is in
/// flight so FINDINGS can report peak RSS on the assignment-sized ingest.
fn process_rss_bytes() -> Option<(u64, u64)> {
    #[cfg(windows)]
    {
        #[repr(C)]
        struct ProcessMemoryCounters {
            cb: u32,
            page_fault_count: u32,
            peak_working_set_size: usize,
            working_set_size: usize,
            quota_peak_paged_pool_usage: usize,
            quota_paged_pool_usage: usize,
            quota_peak_non_paged_pool_usage: usize,
            quota_non_paged_pool_usage: usize,
            pagefile_usage: usize,
            peak_pagefile_usage: usize,
        }

        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn GetCurrentProcess() -> *mut core::ffi::c_void;
            fn K32GetProcessMemoryInfo(
                process: *mut core::ffi::c_void,
                ppsmem_counters: *mut ProcessMemoryCounters,
                cb: u32,
            ) -> i32;
        }

        unsafe {
            let mut pmc = std::mem::zeroed::<ProcessMemoryCounters>();
            pmc.cb = std::mem::size_of::<ProcessMemoryCounters>() as u32;
            if K32GetProcessMemoryInfo(GetCurrentProcess(), &mut pmc, pmc.cb) == 0 {
                return None;
            }
            Some((pmc.working_set_size as u64, pmc.peak_working_set_size as u64))
        }
    }
    #[cfg(not(windows))]
    {
        None
    }
}

fn log_process_rss(at: &'static str) {
    if let Some((ws, peak)) = process_rss_bytes() {
        tracing::info!(
            at,
            working_set_bytes = ws,
            peak_working_set_bytes = peak,
            working_set_mb = ws as f64 / 1_048_576.0,
            peak_working_set_mb = peak as f64 / 1_048_576.0,
            "process rss"
        );
    }
}

async fn run_serve(db_path: String, port: u16) -> Result<()> {
    let conn = storage::open(&db_path).context("opening duckdb")?;
    let state = api::AppState { conn: Arc::new(Mutex::new(conn)) };
    let router = api::build_router(state);

    let addr = format!("0.0.0.0:{port}");
    tracing::info!(%addr, "serving");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, router).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_rss_reports_nonzero_working_set() {
        let (ws, peak) = process_rss_bytes().expect("rss available on Windows");
        assert!(ws > 0);
        assert!(peak >= ws);
    }

    #[test]
    fn snapshots_group_by_signature_and_drop_null_block_time() {
        let rows = vec![
            ("sig-a".into(), 1, Some(100), "MINT".into(), 10, 0, 6),
            ("sig-a".into(), 1, Some(100), ohlcv::WSOL_MINT.into(), 0, 1_000_000_000, 9),
            ("sig-b".into(), 2, None, "MINT".into(), 1, 0, 6),
        ];
        let snaps = snapshots_from_balance_rows(rows);
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].signature, "sig-a");
        assert_eq!(snaps[0].token_deltas.len(), 2);
    }

    #[tokio::test]
    async fn bounded_mpsc_blocks_when_full() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<u8>(PIPELINE_CAPACITY);
        tx.try_send(1).unwrap();
        tx.try_send(2).unwrap();
        assert_eq!(tx.capacity(), 0);

        let send = tokio::spawn(async move { tx.send(3).await });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        assert!(!send.is_finished(), "send must block while channel is full (FR-5.1)");

        assert_eq!(rx.recv().await, Some(1));
        send.await.unwrap().unwrap();
        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(rx.recv().await, Some(3));
    }

    #[test]
    fn parse_block_excludes_failed_tx_from_ohlcv_rows() {
        let block = DecodedBlock {
            slot: 1,
            block_time: Some(1),
            transactions: vec![
                ingest_core::DecodedTransaction {
                    signature: "ok".into(),
                    slot: 1,
                    block_time: Some(1),
                    program_ids: vec![],
                    locks: vec![],
                    token_deltas: vec![ingest_core::TokenBalanceChange {
                        mint: "MINT".into(),
                        pre_amount: 10,
                        post_amount: 0,
                        decimals: 6,
                    }],
                    failed: false,
                },
                ingest_core::DecodedTransaction {
                    signature: "bad".into(),
                    slot: 1,
                    block_time: Some(1),
                    program_ids: vec![],
                    locks: vec![],
                    token_deltas: vec![ingest_core::TokenBalanceChange {
                        mint: "MINT".into(),
                        pre_amount: 10,
                        post_amount: 0,
                        decimals: 6,
                    }],
                    failed: true,
                },
            ],
        };
        let parsed = parse_block(block);
        assert_eq!(parsed.token_changes.len(), 1);
        assert_eq!(parsed.token_changes[0].0, "ok");
        assert_eq!(parsed.transactions.len(), 2);
    }
}
