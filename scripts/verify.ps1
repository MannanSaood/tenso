#Requires -Version 5.1
<#
.SYNOPSIS
  Run every offline FR verification that is safe on Windows.

.DESCRIPTION
  Runs crate tests one at a time to avoid MSVC LNK1318 PDB collisions when
  several DuckDB-linked binaries link in parallel. Prints a pass/fail table.

  Usage (from repo root):
    powershell -File scripts\verify.ps1
    powershell -File scripts\verify.ps1 -LiveSmoke

  -LiveSmoke needs HELIUS_URL in .env and fetches 2 real slots (slow).
#>
param(
    [switch]$LiveSmoke
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

function Import-DotEnv {
    $envFile = Join-Path $Root ".env"
    if (-not (Test-Path $envFile)) { return }
    Get-Content $envFile | ForEach-Object {
        $line = $_.Trim()
        if ($line -eq "" -or $line.StartsWith("#")) { return }
        $eq = $line.IndexOf("=")
        if ($eq -lt 1) { return }
        $name = $line.Substring(0, $eq).Trim()
        $value = $line.Substring($eq + 1).Trim().Trim('"')
        Set-Item -Path "Env:$name" -Value $value
    }
}

function Invoke-CrateTest {
    param([string]$Crate)
    Write-Host ""
    Write-Host "=== cargo test -p $Crate ===" -ForegroundColor Cyan
    cargo test -p $Crate
    if ($LASTEXITCODE -ne 0) {
        throw "FAILED: cargo test -p $Crate (exit $LASTEXITCODE)"
    }
}

Write-Host "FR verification (Windows)" -ForegroundColor Green
Write-Host "Repo: $Root"
Write-Host "Rustc: $(rustc --version)"

$sw = [System.Diagnostics.Stopwatch]::StartNew()

Invoke-CrateTest contention
Invoke-CrateTest ohlcv
Invoke-CrateTest ingest-core
Invoke-CrateTest storage
Invoke-CrateTest api
Invoke-CrateTest cli

Write-Host ""
Write-Host "=== cargo clippy (changed crates, -D warnings) ===" -ForegroundColor Cyan
cargo clippy -p cli -p api -p storage -p ingest-core --no-deps -- -D warnings
if ($LASTEXITCODE -ne 0) {
    throw "FAILED: clippy"
}

if ($LiveSmoke) {
    Import-DotEnv
    if (-not $env:HELIUS_URL) {
        throw "LiveSmoke requires HELIUS_URL in .env (do not paste the key on the command line)"
    }
    Write-Host ""
    Write-Host "=== live smoke: 2 slots ===" -ForegroundColor Cyan
    $body = '{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{"commitment":"finalized"}]}'
    $tip = (Invoke-RestMethod -Method Post -Uri $env:HELIUS_URL -ContentType "application/json" -Body $body).result
    $start = [int64]$tip - 64
    Write-Host "finalized tip $tip; ingest start_slot $start (2 slots)"
    cargo run --release -p cli -- ingest `
        --rpc-endpoint $env:HELIUS_URL `
        --start-slot $start `
        --count 2 `
        --rate-per-sec 10 `
        --max-concurrency 2 `
        --batch-size 2 `
        --db-path blocks-verify.duckdb
    if ($LASTEXITCODE -ne 0) {
        throw "FAILED: live smoke ingest"
    }
}

$sw.Stop()
Write-Host ""
Write-Host "====================================================" -ForegroundColor Green
Write-Host " VERIFICATION PASSED"
Write-Host " elapsed $($sw.Elapsed.ToString('mm\:ss'))"
Write-Host "====================================================" -ForegroundColor Green
Write-Host ""
Write-Host "FR map (what just ran):"
Write-Host "  FR-1.1/1.6  ingest-core empty range + ALT/decode fixture"
Write-Host "  FR-1.2      ingest-core token bucket"
Write-Host "  FR-1.3      ingest-core retry + skip-slot JSON"
Write-Host "  FR-1.5      storage replace_slots_batch twice"
Write-Host "  FR-2/2.3    contention unit tests + storage summary"
Write-Host "  FR-3        ohlcv unit tests + storage candles + cli failed-tx filter"
Write-Host "  FR-4.1-4.4  api HTTP tests (/, /api/*)"
Write-Host "  FR-4.5/5.2  api health while DuckDB mutex held"
Write-Host "  FR-5.1      cli bounded mpsc blocks at capacity"
Write-Host "  FR-5.3      storage batch + mutex (see FINDINGS.md for live ingest)"
Write-Host ""
Write-Host "Dashboard / 1000-slot ingest is NOT in this script. See HOW_TO_TEST.md"
Write-Host "PowerShell ingest example:  powershell -File scripts\ingest.ps1"
