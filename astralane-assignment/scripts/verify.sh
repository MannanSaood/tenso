#!/usr/bin/env bash
# Offline FR verification for macOS/Linux. Run from anywhere:
#   bash scripts/verify.sh
#   bash scripts/verify.sh --live-smoke
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

live=0
if [[ "${1:-}" == "--live-smoke" ]]; then
  live=1
fi

run_crate() {
  echo ""
  echo "=== cargo test -p $1 ==="
  cargo test -p "$1"
}

echo "Astralane FR verification (Unix)"
echo "Repo: $ROOT"
rustc --version

run_crate contention
run_crate ohlcv
run_crate ingest-core
run_crate storage
run_crate api
run_crate cli

echo ""
echo "=== cargo clippy -D warnings ==="
cargo clippy -p cli -p api -p storage -p ingest-core --no-deps -- -D warnings

if [[ "$live" -eq 1 ]]; then
  if [[ -f .env ]]; then
    set -a
    # shellcheck disable=SC1091
    source .env
    set +a
  fi
  if [[ -z "${HELIUS_URL:-}" ]]; then
    echo "Live smoke needs HELIUS_URL in .env" >&2
    exit 1
  fi
  echo ""
  echo "=== live smoke: 2 slots ==="
  TIP="$(curl -s "$HELIUS_URL" -H 'content-type: application/json' \
    -d '{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{"commitment":"finalized"}]}' \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['result'])")"
  START=$((TIP - 64))
  cargo run --release -p cli -- ingest \
    --rpc-endpoint "$HELIUS_URL" \
    --start-slot "$START" \
    --count 2 \
    --rate-per-sec 10 \
    --max-concurrency 2 \
    --batch-size 2 \
    --db-path astralane-verify.duckdb
fi

echo ""
echo "===================================================="
echo " VERIFICATION PASSED"
echo "===================================================="
