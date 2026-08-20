# FINDINGS

> Template — every section below needs real measurements. Nothing here is
> filled in with invented numbers; that's intentional. Replace each
> `TODO` with what you actually observe.

## 1. Expected vs. Actual

**Expected**, before running anything:
- Ingestion of 1,000 slots at a 10 req/s cap should take roughly
  `TODO: (slots / effective_slots_per_request) / 10` seconds at minimum,
  plus retry overhead for skipped/failed slots.
- Contention schedule depth should generally stay low (most Solana blocks
  have significant account-lock parallelism); a small number of "hot"
  accounts (e.g. popular AMM pools) likely dominate the conflict list.

**Actual:** TODO — fill in after a real ingestion run.

## 2. Throughput
- TODO: blocks/sec and transactions/sec observed during ingestion.
- TODO: how throughput changed (if at all) with `--max-concurrency` tuning.

## 3. API Latency Percentiles
- TODO: p50 / p95 / p99 for each endpoint, measured both at rest and while
  ingestion is actively running in the background (this second number is
  the one that actually matters for FR-4.5).

## 4. Peak Memory
- TODO: peak RSS during ingestion, and separately during API serving.

## 5. Profiling Summary
- TODO: what tool was used (e.g. `cargo flamegraph`, `tokio-console`), and
  what it showed as the dominant cost — RPC wait time, JSON deserialization,
  DuckDB writes, or something else.

## 6. Serialization Sources & Mitigation Effects
- TODO: identify points where work was unexpectedly serialized (e.g. a
  single-writer DuckDB connection behind a `Mutex`, or the fee-payer
  contention pattern observed in real blocks) and what, if anything, was
  changed in response.

## 7. Load Experiment Results

### 7.1 Backpressure (FR-5.1)
- Policy chosen: TODO (block / shed / buffer)
- Observed effect of pausing the DB-writer stage for 10s: TODO — did
  upstream fetch/parse stages block, drop data, or buffer? How large did
  the buffer grow? How long did it take to drain after resuming?

### 7.2 Async Starvation (FR-5.2)
- API p50/p99 latency with CPU-heavy contention/OHLCV work running inline
  on the tokio runtime: TODO
- API p50/p99 latency after moving that work to `spawn_blocking`/rayon: TODO
- Delta and interpretation: TODO

### 7.3 Write-Path Contention (FR-5.3)
- Batch size chosen and why: TODO
- Transaction boundary strategy: TODO
- Measured effect of concurrent API reads during active ingestion: TODO

## 8. OHLCV Modelling Decisions
- Dust threshold chosen: `ohlcv::DUST_THRESHOLD_SOL` = 0.0005 SOL — TODO:
  justify against real observed trade-size distribution once data exists.
- TODO: rough percentage of transactions with a wSOL leg vs. excluded
  entirely (no SOL leg / same-direction / dust), to give a sense of
  ingest-to-inferred-trade coverage.

## 9. Approaches That Didn't Work
- TODO — document any dead ends honestly here. This section is explicitly
  requested by the assignment and is a good place to show real engineering
  judgment, not just a clean final result.
