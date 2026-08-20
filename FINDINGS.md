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
| Assignment-sized | 1,000 from `~440508794` (earlier tip-relative start) | Pipeline and 10 s pause produced the same `remaining=0` pattern. Process was interrupted (`exit 1`) before `ingestion complete`, so a finished 1,000-row count is **not** claimed. The **documented** window to re-run is `440522383`–`440523382`. |

Effective slot rate on those live pulls: about **0.3–0.4 slots/s**
(a handful of `getBlock` completions per 10 s at concurrency 8). That
matches the RPC-bound expectation, not the 10/s token-bucket ceiling.

`cargo build --release` succeeds. `cargo test` passes (Windows: run per
crate if the linker hits `LNK1318`). Required suites: contention
(conflict detection), ingest-core `v0_account_resolution_*` (v0/ALT),
ohlcv `candle_*` / `five_minute_candles_*` (1m and 5m), storage
`replacing_the_same_slot_twice_*` (idempotent ingest).

## 2. Throughput

- **Slots/s:** ~0.3–0.4 with `--rate-per-sec 10 --max-concurrency 8`.
  Bound is **Helius `getBlock` latency**, not the limiter. The bucket
  would allow 10/s; we never got close.
- **Txs/s:** not counted on a finished 1,000-slot DB. A busy block is
  hundreds of transactions, so tx ingest tracks slot rate × txs/block.
- **`--max-concurrency`:** above ~8 does not help once in-flight
  `getBlock`s already fill the 10 req/s budget and each call takes
  seconds. The limiter is **process-wide**, one bucket for all workers.
- **Write path:** DuckDB Appender batches of **25** slots. Flush is
  short relative to RPC; it is not the throughput bottleneck.

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

| Condition | Working set |
|---|---|
| Release `serve` on an empty DuckDB, port 18080, idle ~3 s | **18.4 MB** (`WorkingSet64` = 19 312 640) |

A full ingest holds decoded `getBlock` JSON and Appender buffers. We did
not sample RSS on a completed 1,000-slot run (that job was interrupted).
Debug `cargo test` (bundled DuckDB) and the 12- and 50-slot ingests did
not OOM. To capture ingest peak on the chosen window:

```powershell
Get-Process astralane-assignment | Select-Object WorkingSet64
```

while `scripts\ingest.ps1` runs.

## 5. Performance profile

Tooling: **wall-clock logs + capacity experiment**, not `cargo flamegraph`
(not installed here).

**Dominant cost: waiting on `getBlock`.** Evidence:

1. A 10 s store pause only queued a handful of blocks at concurrency 8.
2. `PIPELINE_CAPACITY = 32` never reached 0 in that pause; we cut it to
   **2** so backpressure was observable at all.
3. Token-bucket tests show the limiter *can* delay; live ingest never
   ran at 10 slots/s.

JSON decode, `contention::build_schedule`, and DuckDB Appender are in
the noise next to RPC. Parse/DB still go through `spawn_blocking` so they
cannot pin a tokio worker if a hot block ever does get expensive
(`--cpu-inline` is the before-case).

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

Coverage on a finished 1,000-slot histogram was not taken (ingest
interrupted). A 12-slot smoke stored **56 distinct mints** in
`token_balance_changes` (activity rows, not inferred trades). After
ingest, `infer_trades` + `build_candles` at 60 s and 300 s fills
`candles`. Tests lock 1m bucketing and 5m (`five_minute_candles_use_300s_buckets`).

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
