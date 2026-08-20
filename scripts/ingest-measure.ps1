#Requires -Version 5.1
# 1,000-slot ingest with 2-second WorkingSet64 / PeakWorkingSet64 sampling.
param(
    [int64]$StartSlot = 440522383,
    [int64]$Count = 1000,
    [string]$DbPath = "astralane.duckdb"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$envFile = Join-Path $Root ".env"
if (-not (Test-Path $envFile)) { throw ".env missing - add HELIUS_URL there" }
Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq "" -or $line.StartsWith("#")) { return }
    $eq = $line.IndexOf("=")
    if ($eq -lt 1) { return }
    $name = $line.Substring(0, $eq).Trim()
    $value = $line.Substring($eq + 1).Trim().Trim([char]34)
    Set-Item -Path "Env:$name" -Value $value
}
if (-not $env:HELIUS_URL) { throw "HELIUS_URL not set in .env" }

$env:RUST_LOG = "info"
$exe = Join-Path $Root "target\release\astralane-assignment.exe"
if (-not (Test-Path $exe)) { throw "missing release exe - cargo build --release -p cli" }

$logDir = Join-Path $Root "logs"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$stdout = Join-Path $logDir "ingest-1000.stdout.log"
$stderr = Join-Path $logDir "ingest-1000.stderr.log"
$csv = Join-Path $logDir "ingest-rss.csv"

$ingestArgs = @(
    "ingest",
    "--rpc-endpoint", $env:HELIUS_URL,
    "--start-slot", "$StartSlot",
    "--count", "$Count",
    "--rate-per-sec", "10",
    "--max-concurrency", "8",
    "--batch-size", "25",
    "--simulate-pause-secs", "0",
    "--db-path", $DbPath
)

Write-Host "start_slot=$StartSlot count=$Count range=$StartSlot..$($StartSlot + $Count - 1)"
Write-Host "rss csv: $csv"

$p = Start-Process -FilePath $exe -ArgumentList $ingestArgs -WorkingDirectory $Root -PassThru `
    -RedirectStandardOutput $stdout -RedirectStandardError $stderr -WindowStyle Hidden

Set-Content -Path $csv -Encoding utf8 -Value "elapsed_s,working_set_bytes,peak_working_set_bytes,working_set_mb,peak_working_set_mb"

$start = Get-Date
$maxWs = [int64]0
$maxPeak = [int64]0
while (-not $p.HasExited) {
    Start-Sleep -Seconds 2
    try { $p.Refresh() } catch { break }
    $ws = [int64]$p.WorkingSet64
    $peak = [int64]$p.PeakWorkingSet64
    if ($ws -gt $maxWs) { $maxWs = $ws }
    if ($peak -gt $maxPeak) { $maxPeak = $peak }
    $elapsed = ((Get-Date) - $start).TotalSeconds
    $line = "{0:F1},{1},{2},{3:F2},{4:F2}" -f $elapsed, $ws, $peak, ($ws / 1MB), ($peak / 1MB)
    Add-Content -Path $csv -Value $line -Encoding utf8
}

$p.WaitForExit()
$elapsedTotal = ((Get-Date) - $start).TotalSeconds
Write-Host ("exit={0} elapsed_s={1:F1}" -f $p.ExitCode, $elapsedTotal)
Write-Host ("sampled_max_working_set_mb={0:F1} os_peak_working_set_mb={1:F1}" -f ($maxWs / 1MB), ($maxPeak / 1MB))
if (Test-Path $DbPath) {
    $db = Get-Item $DbPath
    Write-Host ("duckdb_mb={0:F1}" -f ($db.Length / 1MB))
}
exit $p.ExitCode
