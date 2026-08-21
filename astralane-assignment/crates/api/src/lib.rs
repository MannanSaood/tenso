//! axum HTTP layer. Handlers call into `storage` and format responses only
//! — no business logic lives here (FR-4). Must stay responsive while
//! ingestion runs concurrently in the background (FR-4.5). DuckDB work is
//! offloaded with `spawn_blocking` (FR-5.2); `/api/health` never takes the
//! DB mutex so it stays fast during writes.

pub mod routes;
pub mod static_files;

use axum::{routing::get, Router};
use duckdb::Connection;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct AppState {
    pub conn: Arc<Mutex<Connection>>,
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/api/health", get(routes::get_health))
        .route("/api/contention", get(routes::get_contention))
        .route("/api/tokens", get(routes::get_tokens))
        .route("/api/ohlcv", get(routes::get_ohlcv))
        .route("/", get(static_files::serve_index))
        .route("/*path", get(static_files::serve_static))
        .with_state(state)
}
