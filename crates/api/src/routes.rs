//! The three JSON API handlers (FR-4.2-4.4). Each one: lock the shared
//! connection briefly, delegate to `storage::Repository`, serialize, done.
//! No business logic here — that all lives in `storage`, `contention`, and
//! `ohlcv`.

use crate::AppState;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde::Deserialize;
use storage::Repository;

#[derive(Debug, Deserialize)]
pub struct ContentionQuery {
    pub from: u64,
    pub to: u64,
}

pub async fn get_contention(
    State(state): State<AppState>,
    Query(q): Query<ContentionQuery>,
) -> impl IntoResponse {
    let conn = state.conn.lock().unwrap();
    let repo = Repository::new(&conn);
    match repo.query_contention_summary(q.from, q.to) {
        Ok(summary) => Json(summary).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

pub async fn get_tokens(State(state): State<AppState>) -> impl IntoResponse {
    let conn = state.conn.lock().unwrap();
    let repo = Repository::new(&conn);
    match repo.query_tokens() {
        Ok(tokens) => Json(tokens).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Debug, Deserialize)]
pub struct OhlcvQuery {
    pub mint: String,
    pub interval: String, // "1m" | "5m"
}

pub async fn get_ohlcv(
    State(state): State<AppState>,
    Query(q): Query<OhlcvQuery>,
) -> impl IntoResponse {
    let interval_sec = match q.interval.as_str() {
        "1m" => 60,
        "5m" => 300,
        _ => return (StatusCode::BAD_REQUEST, "interval must be 1m or 5m").into_response(),
    };
    let conn = state.conn.lock().unwrap();
    let repo = Repository::new(&conn);
    match repo.query_ohlcv(&q.mint, interval_sec) {
        Ok(candles) => Json(candles).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
