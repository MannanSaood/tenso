# How to test and run every FR

Prefer the verification script (Windows-safe: one crate at a time):

```powershell
powershell -File scripts\verify.ps1
```

```bash
bash scripts/verify.sh
```

## PowerShell: do not use bash `\`

This fails with `Missing expression after unary operator '--'`:

```powershell
cargo run --release -- ingest \
  --start-slot START --count 100
```

Also `--start-slot START` is the literal string `START`, not a variable.

Use the helper (loads `.env`, default start **`440522383`**, count **1000**):

```powershell
powershell -File scripts\ingest.ps1
powershell -File scripts\ingest.ps1 -Count 50 -SimulatePauseSecs 10
powershell -File scripts\ingest.ps1 -Count 100 -Serve
```

Or continue lines with a **backtick**:

```powershell
Get-Content .env | ForEach-Object {
  if ($_ -match '^\s*([^#=]+)=(.*)$') {
    Set-Item -Path "Env:$($matches[1].Trim())" -Value $matches[2].Trim().Trim('"')
  }
}
.\target\release\astralane-assignment.exe ingest `
  --rpc-endpoint $env:HELIUS_URL `
  --start-slot 440522383 `
  --count 100 `
  --db-path astralane.duckdb
```

Keep the key in `.env`. Do not paste it into the terminal.

---

## Environment

```powershell
# Windows PowerShell — prefer .env via scripts\ingest.ps1
$env:HELIUS_URL = "https://mainnet.helius-rpc.com/?api-key=YOUR_KEY"
```

```bash
# macOS / Linux
export HELIUS_URL="https://mainnet.helius-rpc.com/?api-key=YOUR_KEY"
```

```bat
:: Windows cmd
set HELIUS_URL=https://mainnet.helius-rpc.com/?api-key=YOUR_KEY
```

---

## Automated tests (no RPC)

| FR | What it proves | Command |
|---|---|---|
| **FR-1.1 / FR-1.6** | Empty slot range completes | `cargo test -p ingest-core empty_slot_range_completes_without_rpc` |
| **FR-1.2** | Token bucket delays after burst | `cargo test -p ingest-core token_bucket_enforces_sustained_rate` |
| **FR-1.3** | Retries vs skip-slot | `cargo test -p ingest-core retries_transient_errors_then_succeeds does_not_retry_terminal_errors rate_limit_rpc_errors_are_transient block_not_available_is_skipped skipped_slot_message_is_skipped` |
| **FR-1.5** | Same slot twice does not duplicate | `cargo test -p storage replacing_the_same_slot_twice_does_not_duplicate_rows` |
| **FR-1.x decode** | Live fixture parses | `cargo test -p ingest-core live_fixture_deserializes_into_raw_structs` |
| **FR-1.x ALT** | Writable/readonly ALT order | `cargo test -p ingest-core` (account_resolution) |
| **FR-2 / FR-2.3** | Schedule + hot accounts | `cargo test -p contention` and `cargo test -p storage contention_summary_reports_depth_and_writable_accounts` |
| **v0 account resolution** | ALT-loaded keys after static v0 header split | `cargo test -p ingest-core v0_account_resolution` |
| **FR-3 candles** | 1m (60s) and 5m (300s) buckets | `cargo test -p ohlcv candle` |
| **FR-3** | Trades, exclusions, candles | `cargo test -p ohlcv` ; `cargo test -p storage stored_swap_rebuilds_into_ohlcv_candles` ; `cargo test -p cli parse_block_excludes_failed_tx_from_ohlcv_rows` |
| **FR-4.1–4.4** | `/` and JSON APIs | `cargo test -p api health_dashboard_and_json_apis` |
| **FR-4.5 / FR-5.2** | Health ignores DB mutex | `cargo test -p api health_does_not_wait_on_db_mutex` |
| **FR-5.1** | Bounded mpsc blocks at cap 2 | `cargo test -p cli bounded_mpsc_blocks_when_full` |

On Windows, `cargo test --workspace` can fail at **link** with `LNK1318`
(parallel DuckDB PDBs). `scripts\verify.ps1` runs crates sequentially.

Ignored live RPC:

```powershell
cargo test -p ingest-core -- --ignored --nocapture
```

Needs `HELIUS_URL` in `.env`.

Optional 2-slot RPC smoke:

```powershell
powershell -File scripts\verify.ps1 -LiveSmoke
```

---

## Live dashboard (after ingest)

```powershell
.\target\release\astralane-assignment.exe serve --db-path astralane.duckdb --port 8080
```

Open http://127.0.0.1:8080

| Check | Where |
|---|---|
| FR-4.1 | Page loads from the binary (no frontend build) |
| FR-4.5 | http://127.0.0.1:8080/api/health → `{"ok":true}` |
| FR-2.3 | From/to slots → **Load** contention |
| FR-3 | Token + 1m/5m → **Load** candles |

```powershell
Invoke-RestMethod http://127.0.0.1:8080/api/health
Invoke-RestMethod http://127.0.0.1:8080/api/tokens
```

```bash
curl.exe -s http://127.0.0.1:8080/api/health
```

### FR-4.5 + FR-5.3 in one process

```powershell
powershell -File scripts\ingest.ps1 -Count 100 -Serve
```

In another terminal, `/api/health` should stay fast; `/api/contention`
may wait during a DuckDB flush (`--batch-size 25`).

### FR-5.1 backpressure

```powershell
powershell -File scripts\ingest.ps1 -Count 50 -SimulatePauseSecs 10
```

Look for `remaining=0` and `channel full; blocking send (FR-5.1 backpressure)`.

### FR-5.2 before vs after

Default uses `spawn_blocking`. Compare `--cpu-inline` on the exe if you
want the FINDINGS 7.2 “before” case (see `cli` `--help`).

---

## What the dashboard does not show

Rate limits, retries, skipped slots, backpressure, and Part 5 experiments
are **logs + FINDINGS.md + this file**.
