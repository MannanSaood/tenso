# Astralane Rust + Solana Engineering Assignment

## Status
Scaffolded and partially verified outside a Rust toolchain (see
`.cursor/rules/project.mdc` for the exact breakdown of what's proven vs.
unproven). `contention` and `ohlcv` have their core algorithms verified
against reference simulations before transcription; the rest is a careful
first draft awaiting a real `cargo build`.

## Setup
```
cargo build --release
cargo test
```

## Configuration
- `--rpc-endpoint`: Helius free-tier endpoint (primary). Public
  `api.mainnet-beta.solana.com` is a documented fallback only — not used for
  the timed load experiments, since its throttling would contaminate those
  measurements.
- `--start-slot`: TODO — fill in the actual chosen starting slot once ingestion
  has run, and briefly note why (e.g. "recent enough to reflect current
  network activity, chosen at time X").
- Rate cap: 10 req/s, enforced client-side via a token-bucket limiter
  (`ingest-core::TokenBucketLimiter`), independent of provider limits.

## Running
```
# Ingest 1000 slots starting at SLOT
cargo run --release -- ingest --rpc-endpoint https://... --start-slot SLOT --count 1000

# Serve the API + dashboard
cargo run --release -- serve --port 8080
```

## Contention model — definitions and assumptions
- **Conflict**: a write lock on an account conflicts with any other read or
  write lock on the same account; two read locks never conflict.
- **Step**: the position in an order-preserving heuristic schedule. A
  transaction is assigned to `step = 1 + max(blocking step across all its
  account locks)`, where the blocking step comes from the most recent
  conflicting access to that account among *earlier* transactions in the
  block.
- **Exactness**: this is a **heuristic reconstruction, not the validator's
  actual execution schedule** — the Solana RPC does not expose real
  scheduling decisions. The algorithm deliberately preserves original
  in-block transaction order (rather than a free graph-coloring minimum)
  because that mirrors how Solana's runtime actually processes a block,
  giving an honest answer to "how parallel could this realistically have
  run" rather than a theoretical best case.

## OHLCV model — assumptions and exclusions
- Price is inferred by matching an SPL token balance delta against an
  opposing wrapped-SOL (`So111...112`) balance delta within the **same
  transaction**, decimal-adjusted.
- Volume is denominated in SOL-equivalent terms for cross-token
  comparability.
- Explicitly excluded (see `ohlcv/src/lib.rs` doc comments for the exact
  logic):
  - SOL <-> wSOL wrap/unwrap only (no counter-asset — not a trade)
  - Same-direction deltas (both legs increase or decrease together — looks
    like an LP add/remove, not a two-sided swap)
  - Transactions with no wSOL leg at all (e.g. multi-hop routes with no
    direct SOL leg — out of scope, no multi-hop reconstruction attempted)
  - Dust trades below `ohlcv::DUST_THRESHOLD_SOL` (currently 0.0005 SOL,
    adjustable)
- Failed transactions (`meta.err` non-null) are excluded from OHLCV
  entirely, though still counted for contention purposes.

## Known gaps (see `.cursor/rules/project.mdc` for full detail)
- FR-5.1's explicit bounded-channel fetch/parse/store pipeline is not yet
  built — current ingestion uses bounded-concurrency fetching only.
- `build_and_store_candles` in `cli/main.rs` is a stub pending a
  `query_balance_changes_for_range` addition to `storage::Repository`.
- No RPC response has been validated against `rpc_client.rs`'s struct
  definitions yet — do this first with a real Helius key.

## Testing
- `cargo test -p contention -p ohlcv` — pure-logic unit tests, verified twice
  (once via Python reference simulation, once as compiled Rust tests).
- `cargo test -p storage` — schema/idempotency tests; requires a working
  DuckDB build in your environment.
- Dashboard chart layout math verified separately under Node — see
  `static/dashboard.js` and the corresponding test harness used during
  development (not shipped as part of the Rust test suite).

## FINDINGS.md
See `FINDINGS.md` — currently a template with placeholders; fill in with
real measurements from the three load experiments (backpressure, async
starvation, write-path contention) once the pipeline is complete.
