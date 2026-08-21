#Requires -Version 5.1
<#
.SYNOPSIS
  Assignment-sized ingest using PowerShell line continuation (backtick), not bash `\`.

.EXAMPLE
  powershell -File scripts\ingest.ps1
  powershell -File scripts\ingest.ps1 -Count 50 -SimulatePauseSecs 10 -Serve
#>
param(
    [int64]$StartSlot = 440522383,
    [int64]$Count = 1000,
    [int]$SimulatePauseSecs = 0,
    [switch]$Serve,
    [uint16]$Port = 8080,
    [string]$DbPath = "astralane.duckdb"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$envFile = Join-Path $Root ".env"
if (-not (Test-Path $envFile)) { throw ".env missing — add HELIUS_URL=... there, do not paste the key in the shell" }
Get-Content $envFile | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq "" -or $line.StartsWith("#")) { return }
    $eq = $line.IndexOf("=")
    if ($eq -lt 1) { return }
    Set-Item -Path "Env:$($line.Substring(0, $eq).Trim())" -Value $line.Substring($eq + 1).Trim().Trim('"')
}
if (-not $env:HELIUS_URL) { throw "HELIUS_URL not set in .env" }

$env:RUST_LOG = "info"
Write-Host "start_slot=$StartSlot  count=$Count  (range $StartSlot .. $($StartSlot + $Count - 1))"

$exe = Join-Path $Root "target\release\astralane-assignment.exe"
if (-not (Test-Path $exe)) {
    cargo build --release -p cli
}

$ingestArgs = @(
    "ingest",
    "--rpc-endpoint", $env:HELIUS_URL,
    "--start-slot", "$StartSlot",
    "--count", "$Count",
    "--rate-per-sec", "10",
    "--max-concurrency", "8",
    "--batch-size", "25",
    "--simulate-pause-secs", "$SimulatePauseSecs",
    "--db-path", $DbPath
)
if ($Serve) {
    $ingestArgs += @("--serve", "--port", "$Port")
}

& $exe @ingestArgs
exit $LASTEXITCODE
