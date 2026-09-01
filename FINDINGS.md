# Performance notes

Host: Windows 10, PowerShell, `rustc 1.90.0`. RPC: Helius mainnet `getBlock`
(`encoding=json`, `maxSupportedTransactionVersion=0`). Client rate cap:
**10 req/s token bucket in-process**.

**Measured window:** slots **`440522383`–`440523382`** (1,000 consecutive).
Picked 20 Aug 2026 from finalized tip `440524383` minus 2,000 so the range
stays in long-term storage.

Correctness is `cargo test` / `scripts\verify.ps1`. This file is wall-clock
and memory from live ingest.

## Expected vs actual

Before live RPC we assumed:

- 10 req/s means 1,000 slots cannot finish faster than 100 s.
- Busy `getBlock` JSON would dominate, so wall time would be tens of minutes
  even with `--max-concurrency 8`.
- Contention depth modest except on hot writable accounts; read/read never
  conflicts.
- Metadata-only OHLCV would under-count true DEX volume.

| Run | Slots | Result |
|---|---|---|
| Smoke | 50 from `440503794` | `ingested=50 skipped=0 failed=0` in ~2 min. Channel capacity was 32; backpressure did not show. |
| Backpressure | 12, `--simulate-pause-secs 10` | `ingested=12 skipped=0 failed=0`. `parse→store remaining` hit **0**, then `fetch→parse remaining` hit **0**. `channel full; blocking send`. No drops. |
| Full window | **1,000** from `440522383`–`440523382` | `ingested=1000 skipped=0 failed=0 ingested_txs=1938449`. Wall **14 528 s** (~4 h 2 min). One slot was **12.6 MB JSON / 2 415 txs**. |

Finished-window rate: **0.069 slots/s** (~14.6 s/slot at concurrency 8).
**RPC- and payload-bound**, not the 10/s bucket. Early 50-slot smokes looked
like 0.3–0.4 slots/s because those blocks were smaller and the DuckDB file
was still tiny.

## Throughput

Measured 21 Aug 2026 04:20 IST after ingest:

| Metric | Value |
|---|---|
| Slots | **1,000** ingested, 0 skipped, 0 failed |
| Transactions | **1,938,449** (mean **1,938** txs/slot) |
| Failed txs (`meta.err`) | **615,853** (31.8%) |
| Wall | **14 528 s** (fetch+parse+store; candle pass **~4 s**) |
| **Slots/s** | **0.069** |
| **Txs/s** | **133.4** |
| DuckDB file | **9.34 GB** (`blocks.duckdb`) |

- Bound is **Helius `getBlock` of ~12 MB JSON**, not the limiter.
- Concurrency 8 already keeps several large payloads in flight. More workers
  fight the same process-wide bucket and RAM.
- Appender batches of **25** slots. Early flushes ~4 min apart; later ~7–8 min
  as the file grew. RPC wait still dominated.

## API latency

`cargo test -p api api_latency_percentiles_at_rest -- --nocapture`
(20 Aug 2026): **80 sequential `oneshot` samples**, in-memory DuckDB, debug
binary, in-process axum — handler time, not a full HTTP stack.

| Endpoint | p50 | p95 | p99 |
|---|---|---|---|
| `GET /api/health` | **0.028 ms** | **0.030 ms** | **0.037 ms** |
| `GET /api/contention?from=&to=` | **13.8 ms** | **15.0 ms** | **15.5 ms** |
| `GET /api/tokens` | **6.0 ms** | **7.1 ms** | **7.8 ms** |
| `GET /api/ohlcv` (1m) | **6.8 ms** | **8.1 ms** | **8.7 ms** |

With the DuckDB mutex held 400 ms:

| Endpoint | Observation |
|---|---|
| `/api/health` | 200 in **&lt; 150 ms** (no mutex) |
| `/api/contention` | waits on the mutex for the rest of the flush |

Health stays on the tokio worker; contention/OHLCV serialize behind ingest
writes. That is the DuckDB single-writer tradeoff. Live TCP adds kernel/HTTP
overhead on top.

## Peak memory

Sampled every 2 s during the 1,000-slot run (`scripts\ingest-measure.ps1`)
plus `K32GetProcessMemoryInfo` after each 25-slot flush.

| Condition | Working set |
|---|---|
| Release `serve`, empty DuckDB, idle ~3 s | **18.4 MB** |
| Ingest start (before first `getBlock`) | **9.0 MB** |
| After first 25-slot flush | WS **299 MB**, OS peak **822 MB** |
| Steady mid-run | WS **~0.5–1.3 GB** |
| **Peak while fetching** | **5.42 GB** (`PeakWorkingSet64`; sampled max WS **5.41 GB**) |
| After store / after candles | WS **1.12 / 1.15 GB**; OS peak still **5.42 GB** |

Peak is **DuckDB + concurrent ~12 MB JSON**, not the 2-deep mpsc. Working
set dropped toward ~1.1 GB after the last flush. No OOM.

## Profile

Wall-clock logs and a capacity experiment; `cargo flamegraph` was not used.

**Dominant cost: waiting on `getBlock`.**

1. A 10 s store pause only queued a handful of blocks at concurrency 8.
2. `PIPELINE_CAPACITY = 32` never hit 0 in that pause; we cut it to **2** so
   backpressure was visible.
3. The limiter *can* delay in tests; live ingest averaged **0.069 slots/s**.
4. One slot in this window was **12.6 MB / 2 415 txs**. Eight in flight
   already explain hundreds of MB of RSS before DuckDB grows.

JSON decode and `contention::build_schedule` are in the noise next to RPC.
Candle rebuild over the whole range was **~4 s**. Parse/DB still go through
`spawn_blocking` (`--cpu-inline` is the comparison case). Later flushes
slowed as the file grew to 9 GB — secondary to RPC.

## Serialization

| Source | Effect | Change |
|---|---|---|
| Client token bucket (10/s) | Caps RPC even if Helius would allow more | Process-wide, not per-task |
| `Mutex<duckdb::Connection>` | Ingest writes and API reads cannot overlap | One process; `/api/health` skips the lock; handlers use `spawn_blocking` |
| Appender + DELETE per slot, batch of 25 | Mutex held for a whole batch | Smaller batch → snappier API, more commits. Failed batch **ROLLBACK** |
| Hot writable accounts / shared fee-payer | Schedule depth grows along that account | Order-preserving heuristic |
| Bounded mpsc (cap 2), **block** send | Full channel stalls fetch; no drop | Capacity 32 hid backpressure; 2 makes a 10 s pause fill the pipe |

## OHLCV modelling

Candles at **1 minute** and **5 minutes**. Inputs are **transaction
metadata** only. Failed txs dropped. No DEX program decode.

- **Price:** opposing SPL vs wrapped-SOL in the same tx, decimal-adjusted.
- **Volume:** `|wSOL delta|`.
- **Dust:** `DUST_THRESHOLD_SOL = 0.0005`.
- Also dropped: wrap-only, same-direction deltas, no wSOL leg.
- Same mint on several ATAs in one tx is **netted** before the
  `(tx_signature, mint)` primary key.

Coverage on the 1,000-slot DB:

| | |
|---|---|
| Token-balance rows | **129,237** |
| Distinct mints | **1,329** |
| Txs with any balance snapshot | **125,973** |
| Of those, with a wSOL leg | **121,147** |
| Inferred priced trades | **615** |
| Candles 1m / 5m | **134 / 76** |

**615** metadata-only wSOL↔SPL trades from **1.94 M** txs. The rest of the
wSOL activity is wrap/LP/dust/no opposing mint.

## What we discarded

1. **Provider throttling as the rate cap** — then throughput would measure
   Helius, not the client. Cap is `TokenBucketLimiter` at 10 req/s.
2. **mpsc capacity 32** — live `getBlock` never filled it in a 10 s pause.
   Capacity **2**.
3. **Appender of two ATAs of the same mint** — PK `(tx_signature, mint)`
   failed. Coalesce net delta first.
4. **No ROLLBACK after a failed DuckDB transaction** — next statement died
   with “transaction is aborted”.
5. **`ingest` and `serve` as two processes** — DuckDB is a poor multi-process
   writer. `ingest --serve` shares one mutex.
6. **PowerShell bashisms** — `\` continuation and `$HELIUS_URL` expansion do
   not work. Use `scripts\ingest.ps1`. Do not paste the API key in the shell.
7. **Graph-coloring “minimum steps”** — ignores in-block order and over-claims
   parallelism. RPC does not expose the validator schedule.
