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

#[test]
fn duplicate_mint_rows_in_one_tx_coalesce_instead_of_pk_fail() {
    let path = temp_db_path("dup_mint");
    let path_str = path.to_str().unwrap();
    let conn = storage::open(path_str).expect("open");
    let mut repo = storage::Repository::new(&conn);

    let sig = "sig-dup-mint".to_string();
    let usdt = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB".to_string();
    repo.replace_slots_batch(vec![(
        1,
        Some(1),
        vec![(sig.clone(), false, vec!["prog".into()], None)],
        vec![],
        vec![
            (sig.clone(), usdt.clone(), 10, 20, 6),
            (sig.clone(), usdt.clone(), 5, 0, 6),
        ],
    )])
    .expect("same mint twice must coalesce");

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM token_balance_changes", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);
    let (pre, post): (i64, i64) = conn
        .query_row(
            "SELECT pre_amount, post_amount FROM token_balance_changes",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    // net +10 + (-5) = +5
    assert_eq!(pre, 0);
    assert_eq!(post, 5);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn failed_batch_rolls_back_so_the_connection_can_write_again() {
    let path = temp_db_path("rollback");
    let path_str = path.to_str().unwrap();
    let conn = storage::open(path_str).expect("open");
    let mut repo = storage::Repository::new(&conn);

    let sig = "same-sig".to_string();
    let first = repo.replace_slots_batch(vec![(
        1,
        Some(1),
        vec![
            (sig.clone(), false, vec![], None),
            (sig.clone(), false, vec![], None), // duplicate PK on transactions.signature
        ],
        vec![],
        vec![],
    )]);
    assert!(first.is_err(), "duplicate tx signature must fail the batch");

    let second = repo.replace_slots_batch(vec![(
        1,
        Some(1),
        vec![(sig, false, vec![], None)],
        vec![],
        vec![],
    )]);
    assert!(
        second.is_ok(),
        "after ROLLBACK the same connection must accept a valid batch, got {second:?}"
    );

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
