//! Thin binary. Two subcommands: `ingest` (wires ingest-core + contention +
//! ohlcv + storage) and `serve` (wires api). This is the ONLY crate allowed
//! to use anyhow / unwrap / expect outside tests — every other crate stays
//! pure and typed.
//!
//! Ingest runs a 3-stage bounded mpsc pipeline (FR-5.1): fetch → parse → store.
//! Send policy is block (`Sender::send().await`), not shed or unbounded buffer.
//! Remaining permits are logged via `Sender::capacity()`; `--simulate-pause-secs`
//! pauses the store stage once so that drop toward 0 is observable.

use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use ingest_core::{DecodedBlock, FetchOutcome, RpcClient, TokenBucketLimiter};
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
#[command(name = "astralane-assignment")]
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
        #[arg(long, default_value = "astralane.duckdb")]
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
    #[arg(long, default_value = "astralane.duckdb")]
    db_path: String,
    /// Pause the store stage once, in seconds, so fetch/parse fill the
    /// bounded channels (FR-5.1). 0 disables the pause.
    #[arg(long, default_value_t = 0)]
    simulate_pause_secs: u64,
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
    // access goes through Mutex — same pattern as the API server. Only the
    // store stage touches it; fetch/parse stay I/O-free aside from RPC.
    let conn = Arc::new(Mutex::new(storage::open(&args.db_path).context("opening duckdb")?));
    let client = Arc::new(RpcClient::new(args.rpc_endpoint));
    let limiter = Arc::new(TokenBucketLimiter::new(args.rate_per_sec, args.rate_per_sec));

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

    let parse = tokio::spawn(async move {
        while let Some(outcome) = fetch_rx.recv().await {
            let parsed = parse_outcome(outcome);
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
                    {
                        let guard = store_conn.lock().unwrap();
                        let repo = Repository::new(&guard);
                        let _ = repo.upsert_ingestion_status(block.slot, "ingested", None);
                    }
                    pending_batch.push(block);
                    ingested += 1;
                }
                ParsedOutcome::Skipped { slot } => {
                    let guard = store_conn.lock().unwrap();
                    let repo = Repository::new(&guard);
                    let _ = repo.upsert_ingestion_status(slot, "skipped", None);
                    skipped += 1;
                }
                ParsedOutcome::Failed { slot, error } => {
                    let guard = store_conn.lock().unwrap();
                    let repo = Repository::new(&guard);
                    let _ = repo.upsert_ingestion_status(slot, "failed", Some(&error));
                    failed += 1;
                    tracing::error!(slot, error, "slot failed");
                }
            }

            if pending_batch.len() >= args.batch_size {
                match flush_parsed_batch(&store_conn, std::mem::take(&mut pending_batch)) {
                    Ok(()) => {}
                    Err(e) => tracing::error!(?e, "failed to flush batch"),
                }
            }
        }

        if !pending_batch.is_empty() {
            flush_parsed_batch(&store_conn, pending_batch)?;
        }
        Ok::<_, anyhow::Error>((ingested, skipped, failed))
    });

    fetch.await.context("fetch stage join")?;
    parse.await.context("parse stage join")??;
    let (ingested, skipped, failed) = store.await.context("store stage join")??;

    tracing::info!(ingested, skipped, failed, "ingestion complete");

    // Build OHLCV candles across the whole ingested range once ingestion is
    // done. For a live/streaming variant you'd do this incrementally per
    // batch instead — left as a whole-range pass here for simplicity.
    build_and_store_candles(&conn)?;

    Ok(())
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

fn build_and_store_candles(conn: &Mutex<duckdb::Connection>) -> Result<()> {
    let conn = conn.lock().unwrap();
    let repo = Repository::new(&conn);
    let tokens = repo.query_tokens()?;

    // NOTE: re-deriving trades from stored token_balance_changes here would
    // need an additional storage query function to fetch raw balance rows
    // back out (not yet written — `query_tokens` only returns mint +
    // activity count, not the underlying deltas). Left as a clearly-marked
    // TODO rather than a guessed-at, unverified query: add a
    // `query_balance_changes_for_range` to Repository, map its rows into
    // `ohlcv::TxSnapshot`, then call `ohlcv::infer_trades` +
    // `ohlcv::build_candles` per mint for both 60s and 300s intervals, then
    // `repo.upsert_candles`.
    tracing::warn!(
        token_count = tokens.len(),
        "TODO: wire query_balance_changes_for_range -> ohlcv::infer_trades -> build_candles -> upsert_candles"
    );
    Ok(())
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
