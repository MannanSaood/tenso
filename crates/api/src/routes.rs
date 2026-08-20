//! JSON API handlers (FR-4.2–4.4) plus a lock-free health check (FR-4.5).
//! DuckDB work runs on `spawn_blocking` so a slow query cannot starve the
//! tokio worker that accepts other requests (FR-5.2).

use crate::AppState;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use duckdb::Connection;
use serde::Deserialize;
use storage::Repository;

#[derive(Debug, Deserialize)]
pub struct ContentionQuery {
    pub from: u64,
    pub to: u64,
}

/// Does not take the DuckDB mutex. Use this to show the runtime is still
/// accepting work while ingest holds the DB lock (FR-4.5 / FR-5.2).
pub async fn get_health() -> impl IntoResponse {
    Json(serde_json::json!({ "ok": true }))
}

pub async fn get_contention(
    State(state): State<AppState>,
    Query(q): Query<ContentionQuery>,
) -> Response {
    match run_db(state, move |conn| {
        Repository::new(conn)
            .query_contention_summary(q.from, q.to)
            .map_err(|e| e.to_string())
    })
    .await
    {
        Ok(summary) => Json(summary).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

pub async fn get_tokens(State(state): State<AppState>) -> Response {
    match run_db(state, |conn| {
        Repository::new(conn).query_tokens().map_err(|e| e.to_string())
    })
    .await
    {
        Ok(tokens) => Json(tokens).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct OhlcvQuery {
    pub mint: String,
    pub interval: String, // "1m" | "5m"
}

pub async fn get_ohlcv(State(state): State<AppState>, Query(q): Query<OhlcvQuery>) -> Response {
    let interval_sec = match q.interval.as_str() {
        "1m" => 60,
        "5m" => 300,
        _ => return (StatusCode::BAD_REQUEST, "interval must be 1m or 5m").into_response(),
    };
    match run_db(state, move |conn| {
        Repository::new(conn)
            .query_ohlcv(&q.mint, interval_sec)
            .map_err(|e| e.to_string())
    })
    .await
    {
        Ok(candles) => Json(candles).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}

/// Run a DuckDB read on a blocking thread so the async worker stays free.
async fn run_db<T, F>(state: AppState, f: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce(&Connection) -> Result<T, String> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let conn = state
            .conn
            .lock()
            .map_err(|e| format!("db mutex poisoned: {e}"))?;
        f(&conn)
    })
    .await
    .map_err(|e| format!("db worker panicked: {e}"))?
}
