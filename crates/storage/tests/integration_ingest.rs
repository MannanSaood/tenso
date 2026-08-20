//! Integration test for idempotent ingestion (FR-1.5). NOT runnable in the
//! environment that wrote this file (no network, no compiled DuckDB) — this
//! is a skeleton with the right shape and assertions; wire up a real Helius
//! endpoint (or a small recorded fixture) in Cursor to actually run it.
//!
//! Suggested approach once you have network access: either hit a live RPC
//! for a tiny, fixed slot range (slow, flaky in CI, but "real"), or record
//! a handful of real getBlock responses to local JSON fixtures once and
//! replay them through a mock RpcClient for a fast, deterministic test.

use std::path::PathBuf;

fn temp_db_path(name: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!("astralane_test_{name}_{}.duckdb", std::process::id()));
    p
}

#[test]
fn schema_migrations_are_idempotent() {
    // Running `storage::open` twice against the same file must not error —
    // this is the cheapest possible idempotency check and doesn't need
    // network access, only a working DuckDB build.
    let path = temp_db_path("schema");
    let path_str = path.to_str().unwrap();

    let conn1 = storage::open(path_str).expect("first open should succeed");
    drop(conn1);
    let conn2 = storage::open(path_str).expect("second open (re-run) should also succeed");
    drop(conn2);

    let _ = std::fs::remove_file(&path);
}

// TODO once network access is available in the dev environment:
//
// #[tokio::test]
// async fn re_ingesting_same_slot_range_does_not_duplicate_rows() {
//     let path = temp_db_path("reingest");
//     let path_str = path.to_str().unwrap();
//     let conn = storage::open(path_str).unwrap();
//
//     // 1. Ingest a small real (or fixture-replayed) slot range once.
//     // 2. Record row counts in `transactions`, `account_locks`,
//     //    `token_balance_changes` for that slot range.
//     // 3. Ingest the SAME range again.
//     // 4. Assert row counts are IDENTICAL (not doubled) — this is the
//     //    actual FR-1.5 guarantee the delete-then-insert pattern in
//     //    `storage::repository::replace_slot_data_inner` is supposed to
//     //    provide. This test is the real proof; write it first once you
//     //    have a working RpcClient against a live or fixture endpoint.
//
//     let _ = std::fs::remove_file(&path);
// }
