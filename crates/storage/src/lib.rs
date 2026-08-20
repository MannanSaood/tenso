//! DuckDB-backed storage. This is the ONLY crate in the workspace that
//! contains raw SQL or touches the DuckDB Appender API — every other crate
//! goes through the typed functions here.
//!
//! NOTE: I do not have a working Rust/DuckDB toolchain in the environment
//! that wrote this file (see chat context), so none of this has been
//! compiled or run against a real DuckDB file. The schema and query logic
//! are written carefully and the idempotency/upsert reasoning is sound, but
//! this crate — more than contention/ohlcv — needs a real `cargo test` pass
//! in Cursor before you trust it. Start there.

pub mod repository;
pub mod schema;

pub use repository::Repository;
pub use schema::run_migrations;

use duckdb::Connection;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("duckdb error: {0}")]
    DuckDb(#[from] duckdb::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
}

/// Open (or create) the DuckDB file at `path` and run migrations. Call once
/// at startup; the returned `Connection` should be wrapped in an
/// `Arc<Mutex<_>>` or similar by the caller if shared across tokio tasks —
/// DuckDB's Rust bindings are not internally async-safe for concurrent
/// access from multiple tasks without external synchronization.
pub fn open(path: &str) -> Result<Connection, StorageError> {
    let conn = Connection::open(path)?;
    run_migrations(&conn)?;
    Ok(conn)
}
