//! Pure logic, zero I/O. Reconstructs an order-preserving heuristic schedule
//! from account-lock data for a single Solana block.
//!
//! IMPORTANT — documented limitation: the Solana RPC does not expose the
//! validator's actual execution schedule. This is a heuristic reconstruction,
//! not the ground truth. It is deliberately order-preserving (a transaction
//! may only start after every *earlier* transaction it conflicts with has
//! completed) rather than a free graph-coloring minimum, because that
//! mirrors how Solana's runtime actually processes a block (in-order per
//! account), giving an honest answer to "how parallel could this realistically
//! have run" rather than a theoretical best case that ignores block order.
//!
//! Conflict rule: two reads of the same account never conflict. A write
//! conflicts with any read or write on the same account.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// One account lock held by a transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountLock {
    pub account: String,
    pub is_writable: bool,
}

impl AccountLock {
    pub fn write(account: impl Into<String>) -> Self {
        Self { account: account.into(), is_writable: true }
    }
    pub fn read(account: impl Into<String>) -> Self {
        Self { account: account.into(), is_writable: false }
    }
}

/// A single transaction's account locks, as decoded from a block.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxLocks {
    pub signature: String,
    pub program_ids: Vec<String>,
    pub locks: Vec<AccountLock>,
}

/// One reported conflict event, for the per-account / per-program breakdown
/// the assignment asks for (FR-2.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictEvent {
    pub account: String,
    pub tx_signature: String,
    pub program_ids: Vec<String>,
    pub step: usize,
    /// true if this event was a write conflicting with prior activity;
    /// reads are only ever *blocked by* a conflict, never the cause of one.
    pub caused_by_write: bool,
}

/// Result of scheduling one block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleResult {
    /// steps[i] = the step assigned to txs[i], in original input order.
    pub steps: Vec<usize>,
    /// Number of steps in the schedule (depth). 0 if there were no transactions.
    pub depth: usize,
    /// step index -> number of transactions running in that step.
    pub width_per_step: HashMap<usize, usize>,
    /// Every account-lock conflict observed, for reporting (FR-2.3).
    pub conflicts: Vec<ConflictEvent>,
}

#[derive(Default)]
struct AccountState {
    /// Step of the most recent write to this account, or None if never written.
    last_write_step: Option<usize>,
    /// Steps of reads that have occurred since the last write (reads don't
    /// conflict with each other, so several can accumulate here).
    reader_steps_since_write: Vec<usize>,
}

/// Reconstruct an order-preserving schedule for one block's transactions.
///
/// `txs` must be in original block (execution) order — this is load-bearing:
/// the algorithm's honesty depends on processing transactions in the order
/// they actually appeared in the block, not an arbitrary order.
pub fn build_schedule(txs: &[TxLocks]) -> ScheduleResult {
    let mut account_state: HashMap<&str, AccountState> = HashMap::new();
    let mut steps: Vec<usize> = Vec::with_capacity(txs.len());
    let mut conflicts: Vec<ConflictEvent> = Vec::new();

    for tx in txs {
        // Step 1: compute the earliest step this tx can run at, given every
        // earlier transaction already scheduled.
        let mut earliest = 0usize;
        for lock in &tx.locks {
            let state = account_state.entry(lock.account.as_str()).or_default();
            let blocking_step = if lock.is_writable {
                // A write must come after the last write AND after every
                // read that happened since that write.
                let mut b = state.last_write_step;
                for &r in &state.reader_steps_since_write {
                    b = Some(b.map_or(r, |cur| cur.max(r)));
                }
                b
            } else {
                // A read only needs to come after the last write.
                state.last_write_step
            };
            if let Some(b) = blocking_step {
                earliest = earliest.max(b + 1);
            }
        }

        let assigned_step = earliest;
        steps.push(assigned_step);

        // Step 2: record conflicts and update account state now that this
        // tx's step is finalized.
        for lock in &tx.locks {
            let state = account_state.get_mut(lock.account.as_str()).unwrap();
            let had_prior_activity =
                state.last_write_step.is_some() || !state.reader_steps_since_write.is_empty();

            if lock.is_writable {
                if had_prior_activity {
                    conflicts.push(ConflictEvent {
                        account: lock.account.clone(),
                        tx_signature: tx.signature.clone(),
                        program_ids: tx.program_ids.clone(),
                        step: assigned_step,
                        caused_by_write: true,
                    });
                }
                state.last_write_step = Some(assigned_step);
                state.reader_steps_since_write.clear();
            } else {
                state.reader_steps_since_write.push(assigned_step);
            }
        }
    }

    let mut width_per_step: HashMap<usize, usize> = HashMap::new();
    for &s in &steps {
        *width_per_step.entry(s).or_insert(0) += 1;
    }
    let depth = steps.iter().max().map(|m| m + 1).unwrap_or(0);

    ScheduleResult { steps, depth, width_per_step, conflicts }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tx(sig: &str, locks: Vec<AccountLock>) -> TxLocks {
        TxLocks {
            signature: sig.to_string(),
            program_ids: vec!["11111111111111111111111111111111".to_string()],
            locks,
        }
    }

    #[test]
    fn read_read_no_conflict() {
        let txs = vec![
            tx("tx1", vec![AccountLock::read("A")]),
            tx("tx2", vec![AccountLock::read("A")]),
        ];
        let r = build_schedule(&txs);
        assert_eq!(r.steps, vec![0, 0]);
        assert_eq!(r.depth, 1);
    }

    #[test]
    fn write_then_read_conflicts() {
        let txs = vec![
            tx("tx1", vec![AccountLock::write("A")]),
            tx("tx2", vec![AccountLock::read("A")]),
        ];
        let r = build_schedule(&txs);
        assert_eq!(r.steps, vec![0, 1]);
    }

    #[test]
    fn read_then_write_conflicts() {
        let txs = vec![
            tx("tx1", vec![AccountLock::read("A")]),
            tx("tx2", vec![AccountLock::write("A")]),
        ];
        let r = build_schedule(&txs);
        assert_eq!(r.steps, vec![0, 1]);
    }

    #[test]
    fn write_write_conflicts() {
        let txs = vec![
            tx("tx1", vec![AccountLock::write("A")]),
            tx("tx2", vec![AccountLock::write("A")]),
        ];
        let r = build_schedule(&txs);
        assert_eq!(r.steps, vec![0, 1]);
        assert_eq!(r.conflicts.len(), 1);
        assert_eq!(r.conflicts[0].tx_signature, "tx2");
    }

    #[test]
    fn independent_accounts_run_fully_parallel() {
        let txs = vec![
            tx("tx1", vec![AccountLock::write("A")]),
            tx("tx2", vec![AccountLock::write("B")]),
        ];
        let r = build_schedule(&txs);
        assert_eq!(r.steps, vec![0, 0]);
        assert_eq!(r.depth, 1);
    }

    #[test]
    fn write_read_write_chain() {
        let txs = vec![
            tx("tx1", vec![AccountLock::write("A")]),
            tx("tx2", vec![AccountLock::read("A")]),
            tx("tx3", vec![AccountLock::write("A")]),
        ];
        let r = build_schedule(&txs);
        assert_eq!(r.steps, vec![0, 1, 2]);
        assert_eq!(r.depth, 3);
    }

    #[test]
    fn multiple_readers_share_a_step_writer_waits_for_all() {
        let txs = vec![
            tx("tx1", vec![AccountLock::write("A")]),
            tx("tx2", vec![AccountLock::read("A")]),
            tx("tx3", vec![AccountLock::read("A")]),
            tx("tx4", vec![AccountLock::read("A")]),
            tx("tx5", vec![AccountLock::write("A")]),
        ];
        let r = build_schedule(&txs);
        assert_eq!(r.steps, vec![0, 1, 1, 1, 2]);
        assert_eq!(r.width_per_step.get(&1), Some(&3));
        assert_eq!(r.depth, 3);
    }

    #[test]
    fn multi_account_tx_takes_max_of_constraints() {
        let txs = vec![
            tx("tx1", vec![AccountLock::write("A")]),
            tx("tx2", vec![AccountLock::write("B")]),
            tx("tx3", vec![AccountLock::write("A"), AccountLock::write("B")]),
        ];
        let r = build_schedule(&txs);
        assert_eq!(r.steps, vec![0, 0, 1]);
    }

    #[test]
    fn shared_fee_payer_forces_sequential_execution() {
        // Two otherwise-independent transactions sharing the same fee payer
        // (a writable lock) cannot run in parallel — this is a realistic and
        // common case worth its own test, since it's easy to forget the fee
        // payer counts as a writable lock too.
        let txs = vec![
            tx("tx1", vec![AccountLock::write("payer"), AccountLock::read("A")]),
            tx("tx2", vec![AccountLock::write("payer"), AccountLock::read("B")]),
        ];
        let r = build_schedule(&txs);
        assert_eq!(r.steps, vec![0, 1]);
    }

    #[test]
    fn empty_block_has_zero_depth() {
        let r = build_schedule(&[]);
        assert_eq!(r.depth, 0);
        assert!(r.steps.is_empty());
        assert!(r.conflicts.is_empty());
    }

    #[test]
    fn single_transaction_no_conflicts() {
        let txs = vec![tx("tx1", vec![AccountLock::write("A"), AccountLock::read("B")])];
        let r = build_schedule(&txs);
        assert_eq!(r.steps, vec![0]);
        assert_eq!(r.depth, 1);
        assert!(r.conflicts.is_empty());
    }
}
