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
    p.push(format!("blocks_test_{name}_{}.duckdb", std::process::id()));
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

#[test]
fn query_balance_changes_for_range_is_inclusive_and_excludes_other_slots() {
    let path = temp_db_path("bal_range");
    let path_str = path.to_str().unwrap();
    let conn = storage::open(path_str).expect("open");
    let mut repo = storage::Repository::new(&conn);

    let mint = "TOKENMINT".to_string();
    repo.replace_slots_batch(vec![
        (
            10,
            Some(1_000),
            vec![("sig-a".into(), false, vec![], None)],
            vec![],
            vec![("sig-a".into(), mint.clone(), 100, 0, 6)],
        ),
        (
            20,
            Some(2_000),
            vec![("sig-b".into(), false, vec![], None)],
            vec![],
            vec![("sig-b".into(), mint, 0, 50, 6)],
        ),
    ])
    .expect("insert two slots");

    let rows = repo.query_balance_changes_for_range(10, 10).expect("query");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "sig-a");
    assert_eq!(rows[0].1, 10);
    assert_eq!(rows[0].2, Some(1_000));

    let both = repo.query_balance_changes_for_range(10, 20).expect("query both");
    assert_eq!(both.len(), 2);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn replacing_the_same_slot_twice_does_not_duplicate_rows() {
    let conn = storage::open_in_memory().expect("open");
    let mut repo = storage::Repository::new(&conn);
    let row = || {
        (
            42u64,
            Some(1_000i64),
            vec![("sig-1".into(), false, vec!["prog".into()], None)],
            vec![("sig-1".into(), "acct".into(), true)],
            vec![("sig-1".into(), "MINT".into(), 10, 0, 6)],
        )
    };
    repo.replace_slots_batch(vec![row()]).expect("first");
    repo.replace_slots_batch(vec![row()]).expect("second");

    let txs: i64 = conn
        .query_row("SELECT COUNT(*) FROM transactions", [], |r| r.get(0))
        .unwrap();
    let locks: i64 = conn
        .query_row("SELECT COUNT(*) FROM account_locks", [], |r| r.get(0))
        .unwrap();
    let bals: i64 = conn
        .query_row("SELECT COUNT(*) FROM token_balance_changes", [], |r| r.get(0))
        .unwrap();
    assert_eq!(txs, 1);
    assert_eq!(locks, 1);
    assert_eq!(bals, 1);
}

#[test]
fn contention_summary_reports_depth_and_writable_accounts() {
    let conn = storage::open_in_memory().expect("open");
    let mut repo = storage::Repository::new(&conn);
    repo.replace_slots_batch(vec![(
        7,
        Some(1),
        vec![
            ("t1".into(), false, vec![], None),
            ("t2".into(), false, vec![], None),
        ],
        vec![
            ("t1".into(), "hot".into(), true),
            ("t2".into(), "hot".into(), true),
        ],
        vec![],
    )])
    .expect("insert");
    repo.write_schedule_steps(
        &["t1".into(), "t2".into()],
        &contention::ScheduleResult {
            steps: vec![0, 1],
            depth: 2,
            width_per_step: Default::default(),
            conflicts: vec![],
        },
    )
    .expect("steps");

    let summary = repo.query_contention_summary(7, 7).expect("query");
    assert_eq!(summary.depth, 2);
    assert_eq!(summary.top_conflicting_accounts[0], ("hot".into(), 2));
}

#[test]
fn stored_swap_rebuilds_into_ohlcv_candles() {
    let conn = storage::open_in_memory().expect("open");
    let mut repo = storage::Repository::new(&conn);
    let mint = "TOKENMINT".to_string();
    let t = 1_700_000_060i64;
    repo.replace_slots_batch(vec![(
        9,
        Some(t),
        vec![("swap".into(), false, vec![], None)],
        vec![],
        vec![
            ("swap".into(), mint.clone(), 100_000_000, 0, 6),
            ("swap".into(), ohlcv::WSOL_MINT.into(), 0, 1_000_000_000, 9),
        ],
    )])
    .expect("insert swap");

    let rows = repo.query_balance_changes_for_range(9, 9).expect("rows");
    let mut by_sig: std::collections::HashMap<String, ohlcv::TxSnapshot> =
        std::collections::HashMap::new();
    for (signature, _slot, block_time, mint, pre_amount, post_amount, decimals) in rows {
        let tx = by_sig.entry(signature.clone()).or_insert_with(|| ohlcv::TxSnapshot {
            signature,
            block_time: block_time.expect("time"),
            token_deltas: Vec::new(),
        });
        tx.token_deltas.push(ohlcv::TokenDelta {
            mint,
            pre_amount,
            post_amount,
            decimals,
        });
    }
    let mut trades = Vec::new();
    for tx in by_sig.values() {
        trades.extend(ohlcv::infer_trades(tx));
    }
    assert_eq!(trades.len(), 1);
    let candles = ohlcv::build_candles(&trades, 60);
    let series = &candles[&mint];
    repo.upsert_candles(&mint, 60, series).expect("upsert");
    let stored = repo.query_ohlcv(&mint, 60).expect("ohlcv");
    assert_eq!(stored.len(), 1);
    assert!((stored[0].volume - 1.0).abs() < 1e-9);

    let tokens = repo.query_tokens().expect("tokens");
    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].mint, mint);
    assert_eq!(tokens[0].candles_1m, 1);
    assert_eq!(tokens[0].candles_5m, 0);
    assert!(
        !tokens.iter().any(|t| t.mint == ohlcv::WSOL_MINT),
        "quote mint must not appear in the OHLCV dropdown"
    );
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
