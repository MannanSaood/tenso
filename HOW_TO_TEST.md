# Testing and operations

## Quick check

Windows (one crate at a time, avoids MSVC `LNK1318`):

```powershell
powershell -File scripts\verify.ps1
```

Unix:

```bash
bash scripts/verify.sh
```

## PowerShell notes

Bash `\` continuation fails here (`Missing expression after unary operator '--'`).
`--start-slot START` is the literal string `START`, not a variable.

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
.\target\release\block-analysis.exe ingest `
  --rpc-endpoint $env:HELIUS_URL `
  --start-slot 440522383 `
  --count 100 `
  --db-path blocks.duckdb
```

Keep the key in `.env`. Do not paste it into the terminal.

## Environment

```powershell
# Windows PowerShell — prefer .env via scripts\ingest.ps1
$env:HELIUS_URL = "https://mainnet.helius-rpc.com/?api-key=YOUR_KEY"
```

```bash
export HELIUS_URL="https://mainnet.helius-rpc.com/?api-key=YOUR_KEY"
```

```bat
set HELIUS_URL=https://mainnet.helius-rpc.com/?api-key=YOUR_KEY
```

## Offline tests (no RPC)

| Area | Command |
|---|---|
| Empty slot range | `cargo test -p ingest-core empty_slot_range_completes_without_rpc` |
| Token bucket | `cargo test -p ingest-core token_bucket_enforces_sustained_rate` |
| Retry / skip-slot | `cargo test -p ingest-core retries_transient_errors_then_succeeds does_not_retry_terminal_errors rate_limit_rpc_errors_are_transient block_not_available_is_skipped skipped_slot_message_is_skipped` |
| Idempotent ingest | `cargo test -p storage replacing_the_same_slot_twice_does_not_duplicate_rows` |
| `getBlock` fixture | `cargo test -p ingest-core live_fixture_deserializes_into_raw_structs` |
| v0 / ALT locks | `cargo test -p ingest-core v0_account_resolution` |
| Schedule + hot accounts | `cargo test -p contention` ; `cargo test -p storage contention_summary_reports_depth_and_writable_accounts` |
| 1m / 5m candles | `cargo test -p ohlcv candle` |
| Trades and exclusions | `cargo test -p ohlcv` ; `cargo test -p storage stored_swap_rebuilds_into_ohlcv_candles` ; `cargo test -p cli parse_block_excludes_failed_tx_from_ohlcv_rows` |
| HTTP API + dashboard | `cargo test -p api health_dashboard_and_json_apis` |
| Health vs DB mutex | `cargo test -p api health_does_not_wait_on_db_mutex` |
| Bounded mpsc | `cargo test -p cli bounded_mpsc_blocks_when_full` |

On Windows, `cargo test --workspace` can fail at **link** with `LNK1318`
(parallel DuckDB PDBs). `scripts\verify.ps1` runs crates sequentially.

Live RPC (optional, needs `HELIUS_URL`):

```powershell
cargo test -p ingest-core -- --ignored --nocapture
powershell -File scripts\verify.ps1 -LiveSmoke
```

## Dashboard

```powershell
.\target\release\block-analysis.exe serve --db-path blocks.duckdb --port 8080
```

http://127.0.0.1:8080

| Check | Expected |
|---|---|
| Page | Served from the binary, no frontend build |
| Health | http://127.0.0.1:8080/api/health → `{"ok":true}` |
| Contention | Default slot `440522383`, **Load** |
| OHLCV | Dropdown is candle mints only; 1m / 5m follow that mint |

```powershell
Invoke-RestMethod http://127.0.0.1:8080/api/health
Invoke-RestMethod http://127.0.0.1:8080/api/tokens
```

### Ingest + serve in one process

```powershell
powershell -File scripts\ingest.ps1 -Count 100 -Serve
```

`/api/health` should stay fast; `/api/contention` may wait during a DuckDB
flush (`--batch-size 25`).

### Backpressure

```powershell
powershell -File scripts\ingest.ps1 -Count 50 -SimulatePauseSecs 10
```

Look for `remaining=0` and `channel full; blocking send`.

### CPU path

Default parse/DB work uses `spawn_blocking`. `--cpu-inline` runs that work on
the tokio worker (see `cli --help` and FINDINGS §5).

Rate limits, retries, skipped slots, and pipeline capacity show up in **logs**
and [FINDINGS.md](FINDINGS.md), not on the dashboard.
