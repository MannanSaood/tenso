//! axum HTTP layer. Handlers call into `storage` and format responses only
//! — no business logic lives here (FR-4). Must stay responsive while
//! ingestion runs concurrently in the background (FR-4.5) — this is why
//! `AppState` holds an `Arc<Mutex<Connection>>` rather than anything that
//! would let a slow query block the whole server; see FINDINGS.md for the
//! measured async-starvation experiment that validates this in practice.
//!
//! NOTE: not run against a live server from the environment that wrote this
//! (no toolchain — see chat context). Route wiring and handler logic are
//! written carefully but `cargo run` + hitting these endpoints for real is
//! the first thing to do with this crate in Cursor.

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
        .route("/", get(static_files::serve_index))
        .route("/*path", get(static_files::serve_static))
        .route("/api/contention", get(routes::get_contention))
        .route("/api/tokens", get(routes::get_tokens))
        .route("/api/ohlcv", get(routes::get_ohlcv))
        .with_state(state)
}
