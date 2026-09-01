# Solana Block Analysis

Ingest consecutive Solana slots over JSON-RPC, reconstruct a per-block
transaction-conflict schedule, derive 1-minute and 5-minute OHLCV candles
from token-balance metadata, and serve both from one process: HTTP API plus
a vanilla-JS dashboard.

![Contention for slot 440522383](gallery/contention.png)
![OHLCV 1-minute candles](gallery/ohlcv.png)

## What it does

- **Ingest** — `getBlock` over Helius (or any JSON-RPC), client-side 10 req/s
  token bucket, retries, skip-slot handling, v0 / ALT account resolution.
- **Contention** — order-preserving schedule from account locks (heuristic;
  RPC does not expose the validator’s real timetable).
- **OHLCV** — 1m / 5m candles from `preTokenBalances` / `postTokenBalances`
  only. No DEX instruction decode.
- **Serve** — axum + DuckDB in one process. `/api/health` does not take the
  DB mutex. Dashboard is embedded in the binary (`rust-embed`).

Storage is DuckDB (Appender batch writes, delete-then-insert per slot).
Ingest and the API share one connection behind a mutex.

## Requirements

Rust **1.85+** (edition 2024). RPC URL in a gitignored `.env`:

```
HELIUS_URL=https://mainnet.helius-rpc.com/?api-key=YOUR_KEY
```

Copy `.env.example` if needed. Rate limiting is enforced **in this process**
(`ingest-core::TokenBucketLimiter`), not by relying on the provider.

```
cargo build --release
cargo test
```

On Windows, if `cargo test` hits MSVC `LNK1318`, run crates one at a time
or `powershell -File scripts\verify.ps1`.

## Ingest window

Default range (finalized mainnet, 20 Aug 2026): **1,000 consecutive slots**
`440522383`–`440523382` (tip `440524383` minus 2,000 so the window stays in
long-term storage).

| | |
|---|---|
| Start | `440522383` |
| End (inclusive) | `440523382` |
| Count | 1,000 |

## Usage

PowerShell (do **not** use bash `\` line continuation):

```powershell
# ingest the default 1,000-slot window (loads .env)
powershell -File scripts\ingest.ps1

# ingest and keep the dashboard up (Ctrl+C to stop)
powershell -File scripts\ingest.ps1 -Serve

# serve an existing database
.\target\release\block-analysis.exe serve --db-path blocks.duckdb --port 8080
```

macOS / Linux:

```bash
cargo run --release -- ingest \
  --rpc-endpoint "$HELIUS_URL" \
  --start-slot 440522383 \
  --count 1000 \
  --rate-per-sec 10 \
  --max-concurrency 8 \
  --batch-size 25 \
  --db-path blocks.duckdb

cargo run --release -- serve --db-path blocks.duckdb --port 8080
```

Open http://127.0.0.1:8080. The token list is **mints that already have
candles**. Contention defaults to slot `440522383`.

A full 1,000-slot DuckDB is on the order of **9 GB** and is not checked in.
Rebuild it with `scripts\ingest.ps1`, or run `cargo test` without RPC.
Benchmarks: [FINDINGS.md](FINDINGS.md). More commands: [HOW_TO_TEST.md](HOW_TO_TEST.md).

## Architecture

```
cli (one binary)
  ingest ── fetch (RPC, rate limit, retry)
         ── parse (locks + schedule + token deltas)
         ── store (DuckDB Appender, batches of 25)
  serve  ── axum  /  /api/health  /api/contention  /api/tokens  /api/ohlcv
```

Fetch → parse → store uses a bounded `mpsc` (capacity 2, **block** on send).
Parse and DB work run on `spawn_blocking` unless `--cpu-inline`.

## Contention model

- **Conflict:** a **write** on an account conflicts with a later **read or
  write** on the same account. Two **reads** never conflict.
- **Step:** `1 + max(blocking step among earlier conflicting locks)`, or `0`
  if nothing blocks. Depth is `1 + max(step)` (0 if no txs).
- **Heuristic.** The schedule **preserves in-block order**. It is not the
  validator’s actual schedule and not a graph-coloring minimum.

See `contention::build_schedule`. `cargo test -p contention`.

## OHLCV model

- Intervals: **60 s** and **300 s**.
- Inputs: transaction metadata only (`preTokenBalances` / `postTokenBalances`).
- Price: opposing SPL mint vs wrapped-SOL (`So111…112`) in the same successful
  tx, decimal-adjusted. Volume in SOL.
- Dropped: wrap/unwrap-only, same-direction deltas (LP-like), no wSOL leg,
  dust below `0.0005` SOL, failed txs (`meta.err`).

## Tests

```powershell
powershell -File scripts\verify.ps1
```

| Area | Command |
|---|---|
| Conflict detection | `cargo test -p contention` |
| v0 / ALT accounts | `cargo test -p ingest-core v0_account_resolution` |
| 1m / 5m candles | `cargo test -p ohlcv candle` |
| Idempotent ingest | `cargo test -p storage replacing_the_same_slot_twice_does_not_duplicate_rows` |
| Full offline suite | `cargo test` or `scripts\verify.ps1` |
