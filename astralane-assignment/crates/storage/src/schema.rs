//! Schema + migrations, run once at startup. DuckDB doesn't have a
//! separate migration framework in common use the way e.g. sqlx does for
//! Postgres — `CREATE TABLE IF NOT EXISTS` is sufficient here since the
//! schema is fixed for the life of this assignment (no versioned migrations
//! needed).

use duckdb::Connection;

pub fn run_migrations(conn: &Connection) -> Result<(), duckdb::Error> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS blocks (
            slot        BIGINT PRIMARY KEY,
            block_time  BIGINT,
            tx_count    INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS transactions (
            signature    VARCHAR PRIMARY KEY,
            slot         BIGINT NOT NULL,
            block_time   BIGINT,
            failed       BOOLEAN NOT NULL,
            -- JSON array of strings. duckdb-rs cannot bind LIST/VARCHAR[] via
            -- ToSql or the Appender API ("binding List parameters is not yet
            -- supported"), so we persist the list as JSON text instead.
            program_ids  VARCHAR NOT NULL,
            step         INTEGER,               -- assigned by the contention scheduler, nullable until computed
        );

        CREATE TABLE IF NOT EXISTS account_locks (
            slot          BIGINT NOT NULL,
            tx_signature  VARCHAR NOT NULL,
            account       VARCHAR NOT NULL,
            is_writable   BOOLEAN NOT NULL,
            PRIMARY KEY (tx_signature, account)
        );

        CREATE TABLE IF NOT EXISTS token_balance_changes (
            tx_signature  VARCHAR NOT NULL,
            slot          BIGINT NOT NULL,
            block_time    BIGINT,
            mint          VARCHAR NOT NULL,
            pre_amount    UBIGINT NOT NULL,
            post_amount   UBIGINT NOT NULL,
            decimals      TINYINT NOT NULL,
            PRIMARY KEY (tx_signature, mint)
        );

        CREATE TABLE IF NOT EXISTS candles (
            mint          VARCHAR NOT NULL,
            interval_sec  INTEGER NOT NULL,     -- 60 or 300
            bucket_start  BIGINT NOT NULL,
            open          DOUBLE NOT NULL,
            high          DOUBLE NOT NULL,
            low           DOUBLE NOT NULL,
            close         DOUBLE NOT NULL,
            volume        DOUBLE NOT NULL,
            PRIMARY KEY (mint, interval_sec, bucket_start)
        );

        CREATE TABLE IF NOT EXISTS ingestion_status (
            slot    BIGINT PRIMARY KEY,
            status  VARCHAR NOT NULL,   -- 'ingested' | 'skipped' | 'failed'
            note    VARCHAR
        );

        CREATE INDEX IF NOT EXISTS idx_locks_account ON account_locks(account);
        CREATE INDEX IF NOT EXISTS idx_tx_slot ON transactions(slot);
        CREATE INDEX IF NOT EXISTS idx_balance_mint ON token_balance_changes(mint);
        CREATE INDEX IF NOT EXISTS idx_candles_mint_interval ON candles(mint, interval_sec);
        "#,
    )
}
