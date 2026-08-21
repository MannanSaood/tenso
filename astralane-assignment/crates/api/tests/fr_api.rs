//! HTTP-level coverage for FR-4 (API + dashboard) and FR-4.5 / FR-5.2
//! (health stays fast while the DuckDB mutex is held).

use api::{build_router, AppState};
use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tower::ServiceExt;

fn seeded_state() -> AppState {
    let conn = storage::open_in_memory().expect("open");
    let mut repo = storage::Repository::new(&conn);
    let mint = "TOKENMINT".to_string();
    repo.replace_slots_batch(vec![(
        10,
        Some(1_700_000_060),
        vec![
            ("t1".into(), false, vec![], None),
            ("t2".into(), false, vec![], None),
        ],
        vec![
            ("t1".into(), "hot".into(), true),
            ("t2".into(), "hot".into(), true),
        ],
        vec![
            ("t1".into(), mint.clone(), 100_000_000, 0, 6),
            ("t1".into(), ohlcv::WSOL_MINT.into(), 0, 1_000_000_000, 9),
        ],
    )])
    .expect("seed slots");
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

    let rows = repo.query_balance_changes_for_range(10, 10).expect("rows");
    let mut by_sig = std::collections::HashMap::new();
    for (signature, _, block_time, mint, pre, post, decimals) in rows {
        let tx = by_sig.entry(signature.clone()).or_insert_with(|| ohlcv::TxSnapshot {
            signature,
            block_time: block_time.unwrap(),
            token_deltas: Vec::new(),
        });
        tx.token_deltas.push(ohlcv::TokenDelta {
            mint,
            pre_amount: pre,
            post_amount: post,
            decimals,
        });
    }
    let mut trades = Vec::new();
    for tx in by_sig.values() {
        trades.extend(ohlcv::infer_trades(tx));
    }
    let candles = ohlcv::build_candles(&trades, 60);
    for (m, series) in candles {
        repo.upsert_candles(&m, 60, &series).expect("candles");
    }

    AppState { conn: Arc::new(Mutex::new(conn)) }
}

async fn body_json(response: axum::http::Response<Body>) -> serde_json::Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn health_dashboard_and_json_apis() {
    let app = build_router(seeded_state());

    let health = app
        .clone()
        .oneshot(Request::builder().uri("/api/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    assert_eq!(body_json(health).await["ok"], true);

    let index = app
        .clone()
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(index.status(), StatusCode::OK);

    let contention = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/contention?from=10&to=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(contention.status(), StatusCode::OK);
    let c = body_json(contention).await;
    assert_eq!(c["depth"], 2);

    let tokens = app
        .clone()
        .oneshot(Request::builder().uri("/api/tokens").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(tokens.status(), StatusCode::OK);
    let token_list = body_json(tokens).await;
    let arr = token_list.as_array().unwrap();
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["mint"], "TOKENMINT");
    assert!(arr[0]["candles_1m"].as_i64().unwrap() >= 1);

    let ohlcv = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/ohlcv?mint=TOKENMINT&interval=1m")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ohlcv.status(), StatusCode::OK);
    assert_eq!(body_json(ohlcv).await.as_array().unwrap().len(), 1);

    let bad = app
        .oneshot(
            Request::builder()
                .uri("/api/ohlcv?mint=TOKENMINT&interval=2h")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(bad.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn health_does_not_wait_on_db_mutex() {
    let conn = Arc::new(Mutex::new(storage::open_in_memory().expect("open")));
    let hold = Arc::clone(&conn);
    let started = Arc::new(std::sync::Barrier::new(2));
    let ready = Arc::clone(&started);
    let holder = std::thread::spawn(move || {
        let _guard = hold.lock().unwrap();
        ready.wait();
        std::thread::sleep(Duration::from_millis(400));
    });
    started.wait();

    let app = build_router(AppState { conn: Arc::clone(&conn) });
    let t0 = Instant::now();
    let health = app
        .oneshot(Request::builder().uri("/api/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let elapsed = t0.elapsed();
    assert_eq!(health.status(), StatusCode::OK);
    assert!(
        elapsed < Duration::from_millis(150),
        "health must not wait on DuckDB mutex, took {elapsed:?}"
    );
    holder.join().unwrap();
}

fn percentile(sorted_ms: &[f64], p: f64) -> f64 {
    assert!(!sorted_ms.is_empty());
    let idx = ((sorted_ms.len() - 1) as f64 * p).round() as usize;
    sorted_ms[idx]
}

async fn sample_uri_ms(app: axum::Router, uri: &str, n: usize) -> Vec<f64> {
    let mut samples = Vec::with_capacity(n);
    for _ in 0..n {
        let t0 = Instant::now();
        let resp = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let _ = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        samples.push(t0.elapsed().as_secs_f64() * 1000.0);
    }
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    samples
}

#[tokio::test]
async fn api_latency_percentiles_at_rest() {
    let app = build_router(seeded_state());
    let n = 80;
    let health = sample_uri_ms(app.clone(), "/api/health", n).await;
    let contention = sample_uri_ms(app.clone(), "/api/contention?from=10&to=10", n).await;
    let tokens = sample_uri_ms(app.clone(), "/api/tokens", n).await;
    let ohlcv = sample_uri_ms(app, "/api/ohlcv?mint=TOKENMINT&interval=1m", n).await;

    let report = |name: &str, s: &[f64]| {
        eprintln!(
            "API {name} n={n} p50={:.3}ms p95={:.3}ms p99={:.3}ms",
            percentile(s, 0.50),
            percentile(s, 0.95),
            percentile(s, 0.99)
        );
    };
    report("health", &health);
    report("contention", &contention);
    report("tokens", &tokens);
    report("ohlcv", &ohlcv);

    assert!(percentile(&health, 0.99) < 50.0, "health p99 should stay well under 50ms at rest");
    assert!(percentile(&contention, 0.99) < 200.0);
}
