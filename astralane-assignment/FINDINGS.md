# FINDINGS

Host: Windows 10, PowerShell, `rustc 1.90.0`. RPC: Helius mainnet `getBlock`
(`encoding=json`, `maxSupportedTransactionVersion=0`). Client rate cap:
**10 req/s token bucket in-process**, not provider throttling.

**Chosen window (also in README):** slots **`440522383`–`440523382`**
(1,000 consecutive). Picked 20 Aug 2026 from finalized tip `440524383`
minus 2,000 so the whole range is finalized.

This note is the load-experiment write-up. Offline tests
(`cargo test` / `scripts\verify.ps1`) are the correctness suite.

## 1. Expected vs. actual

**Expected, before any live RPC:**

- A hard 10 req/s cap means 1,000 slots cannot finish faster than 100 s.
- Real `getBlock` of a busy mainnet slot is a large JSON payload. We
  expected wall time to be dominated by **RPC wait**, not decode or
  DuckDB, and therefore **tens of minutes** for 1,000 slots even with
  `--max-concurrency 8`.
- Contention depth should stay modest except on hot writable accounts
  (shared fee-payer, AMM pools). Read/read should never conflict.
- OHLCV from metadata only would **under-count** true DEX volume
  (no multi-hop reconstruction).

**Actual:**

| Run | Slots | Result |
|---|---|---|
| Smoke | 50 from `440503794` | `ingested=50 skipped=0 failed=0` in ~2 min. Channel capacity was still 32; backpressure did **not** show. |
| Backpressure | 12, `--simulate-pause-secs 10` | `ingested=12 skipped=0 failed=0`. `parse→store remaining` hit **0**, then `fetch→parse remaining` hit **0**. `channel full; blocking send`. No drops. |
| Assignment-sized | **1,000** from `440522383`–`440523382` | `ingested=1000 skipped=0 failed=0 ingested_txs=1938449`. Wall **14 528 s** (~4 h 2 min). Exit 0. One slot of `getBlock` JSON in this window was **12.6 MB** / 2 415 txs — that is why the run is hours, not minutes. |

Effective slot rate on the finished 1,000-slot window: **0.069 slots/s**
(~14.6 s per slot at `--max-concurrency 8`). That is **RPC- and payload-bound**,
not the 10/s token-bucket ceiling. Early 50-slot smokes looked like 0.3–0.4
slots/s because those blocks were smaller / the DuckDB file was still tiny.

`cargo build --release` succeeds. `cargo test` passes (Windows: run per
crate if the linker hits `LNK1318`). Required suites: contention
(conflict detection), ingest-core `v0_account_resolution_*` (v0/ALT),
ohlcv `candle_*` / `five_minute_candles_*` (1m and 5m), storage
`replacing_the_same_slot_twice_*` (idempotent ingest).

## 2. Throughput

Measured on the finished DB (`db stats after ingest`, 21 Aug 2026 04:20 IST):

| Metric | Value |
|---|---|
| Slots | **1,000** ingested, 0 skipped, 0 failed |
| Transactions | **1,938,449** (mean **1,938** txs/slot) |
| Failed txs (`meta.err`) | **615,853** (31.8%) |
| Wall | **14 528 s** (fetch+parse+store; candle pass **~4 s** after that) |
| **Slots/s** | **0.069** |
| **Txs/s** | **133.4** |
| DuckDB file | **9.34 GB** (`astralane.duckdb`) |

- Bound is **Helius `getBlock` of ~12 MB JSON**, not the 10 req/s limiter.
  The bucket would allow 10/s; we never got close.
- **`--max-concurrency`:** 8 already keeps several large payloads in flight.
  More workers would fight the same process-wide bucket and RAM.
- **Write path:** Appender batches of **25** slots. Early flushes ~4 min
  apart; later ones ~7–8 min as the file grew past several GB. RPC wait
  still dominated; DuckDB was not idle, but it was not the 4-hour cause.

## 3. API latency percentiles

Measured 20 Aug 2026 with `cargo test -p api api_latency_percentiles_at_rest -- --nocapture`:
**80 sequential `oneshot` samples** against an in-memory seeded DuckDB
(debug test binary, in-process axum — no TCP). That is a lower bound on
handler time, not a full HTTP stack.

| Endpoint | p50 | p95 | p99 |
|---|---|---|---|
| `GET /api/health` | **0.028 ms** | **0.030 ms** | **0.037 ms** |
| `GET /api/contention?from=&to=` | **13.8 ms** | **15.0 ms** | **15.5 ms** |
| `GET /api/tokens` | **6.0 ms** | **7.1 ms** | **7.8 ms** |
| `GET /api/ohlcv` (1m) | **6.8 ms** | **8.1 ms** | **8.7 ms** |

**During a held DuckDB mutex (400 ms exclusive lock)** — FR-4.5 / write
contention, not a percentile sweep:

| Endpoint | Observation |
|---|---|
| `/api/health` | 200 in **&lt; 150 ms** (does not take the mutex) |
| `/api/contention` | waits on the mutex for the remainder of the flush |

So: health stays on the tokio worker; contention/OHLCV serialize behind
ingest writes. That is the intended DuckDB single-writer tradeoff, not
accept-loop death. Live TCP + `ingest --serve` will add kernel/HTTP
overhead on top of the table above.

## 4. Peak memory

Sampled **every 2 s** during the 1,000-slot `getBlock` run
(`scripts\ingest-measure.ps1` → `logs/ingest-rss.csv`), plus in-process
`K32GetProcessMemoryInfo` after every 25-slot flush.

| Condition | Working set |
|---|---|
| Release `serve`, empty DuckDB, idle ~3 s | **18.4 MB** |
| Ingest start (before first `getBlock`) | **9.0 MB** |
| After first 25-slot flush | WS **299 MB**, OS peak **822 MB** |
| Steady mid-run (hundreds of slots) | WS **~0.5–1.3 GB** |
| **Peak while getting blocks** | **5.42 GB** (`PeakWorkingSet64` = 5 683 388 416; sampled max WS **5.41 GB**) |
| After store / after candles | WS **1.12 / 1.15 GB**; OS peak still **5.42 GB** |

Peak is **DuckDB + concurrent ~12 MB JSON**, not the 2-deep mpsc. Working
set dropped toward ~1.1 GB once the last batch flushed; the OS peak is
what FINDINGS reports. No OOM. File on disk ended at **9.34 GB**.

## 5. Performance profile

Tooling: **wall-clock logs + capacity experiment**, not `cargo flamegraph`
(not installed here).

**Dominant cost: waiting on `getBlock`.** Evidence:

1. A 10 s store pause only queued a handful of blocks at concurrency 8.
2. `PIPELINE_CAPACITY = 32` never reached 0 in that pause; we cut it to
   **2** so backpressure was observable at all.
3. Token-bucket tests show the limiter *can* delay; the 1,000-slot run
   averaged **0.069 slots/s**, not 10.
4. One finalized slot in this window was **12.6 MB JSON / 2 415 txs**.
   Eight of those in flight already explain hundreds of MB of RSS
   before DuckDB grows.

JSON decode and `contention::build_schedule` are in the noise next to
RPC. Candle rebuild over the whole range was **~4 s**. Parse/DB still
go through `spawn_blocking` (`--cpu-inline` is the before-case). Later
25-slot flushes slowed as `astralane.duckdb` grew to 9 GB — a secondary
cost, not the primary one.

## 6. Serialization — sources and effect of changes

| Source | Effect | What we changed |
|---|---|---|
| Client token bucket (10/s) | Caps RPC even if Helius would allow more | Required. Process-wide, not per-task. |
| `Mutex<duckdb::Connection>` | Ingest writes and API reads cannot overlap | One process (`ingest --serve`). `/api/health` skips the lock. Handlers use `spawn_blocking`. |
| Appender + DELETE per slot, batch of 25 | Mutex held for a whole batch | Smaller batch → snappier API, more commits. Default 25. Failed batch **ROLLBACK** (without it the connection stayed “transaction aborted”). |
| Hot writable accounts / shared fee-payer | Schedule depth grows along that account | Heuristic, order-preserving — not a parallel-coloring rewrite. |
| Bounded mpsc (cap 2), **block** send | Full channel stalls fetch; no drop | Capacity 32 hid FR-5.1; 2 makes a 10 s pause fill the pipe. |

## 7. OHLCV modelling and exclusions

Candles at **1 minute** and **5 minutes** only. Inputs are **transaction
metadata**: `preTokenBalances` / `postTokenBalances` (failed txs
dropped). No DEX program decode, no extra `getAccountInfo`.

- **Price:** opposing SPL mint vs wrapped-SOL (`So111…112`) delta in the
  same tx, decimal-adjusted.
- **Volume:** SOL-equivalent (`|wSOL delta|`).
- **Dust:** `DUST_THRESHOLD_SOL = 0.0005`. Below that the wSOL leg is
  wrap dust / rounding, not a priced swap (`dust_trade_excluded`).
- **Also excluded:** wrap-only (no counter-asset), same-direction
  deltas (LP add/remove), no wSOL leg (multi-hop out of scope).
- Same mint appearing on several ATAs in one tx is **netted** before
  the `(tx_signature, mint)` primary key.

Coverage on the finished 1,000-slot DB:

| | |
|---|---|
| Token-balance rows | **129,237** |
| Distinct mints | **1,329** |
| Txs with any balance snapshot | **125,973** |
| Of those, with a wSOL leg | **121,147** |
| Inferred priced trades | **615** |
| Candles 1m / 5m | **134 / 76** |

So **615** metadata-only wSOL↔SPL trades from **1.94 M** txs. Failed txs
are excluded by construction. The rest of the wSOL activity is wrap/LP/
dust/no opposing mint — the under-count we expected. Tests lock 1m
bucketing and 5m (`five_minute_candles_use_300s_buckets`).

## 8. Approaches that did not work

1. **Relying on the provider to throttle** — assignment forbids it.
   Throughput would then measure Helius, not us. We cap at 10 req/s in
   `TokenBucketLimiter`.
2. **mpsc capacity 32** — live `getBlock` never filled it in a 10 s
   pause, so FR-5.1 looked like a no-op. Capacity **2**.
3. **Appender of two ATAs of the same mint** — PK `(tx_signature, mint)`
   failed. Coalesce net delta first.
4. **No ROLLBACK after a failed DuckDB transaction** — next statement
   died with “transaction is aborted”.
5. **`ingest` and `serve` as two processes** — DuckDB is a poor
   multi-process writer. `ingest --serve` shares one mutex.
6. **PowerShell bashisms** — `\` continuation and `$HELIUS_URL` expansion
   do not work. `scripts\ingest.ps1` loads `.env` and pins start slot
   `440522383`. Do not paste the API key in the shell.
7. **Graph-coloring “minimum steps”** — would ignore in-block order and
   over-claim parallelism. We kept the order-preserving heuristic
   because RPC does not expose the validator schedule.
