//! Pure logic: resolves the full, ordered account-lock list for a
//! transaction, including v0/address-lookup-table (ALT) accounts.
//!
//! Verified independently in Python before transcription — see
//! `reference_sim_alt.py` in the assignment planning materials. Covered
//! cases: mixed signed/unsigned + readonly/writable split, single-signer
//! transactions, and all-non-signer-readonly transactions.
//!
//! Solana's Message v0 static account layout (from the transaction's
//! `header` + `accountKeys`) is always ordered:
//!   [signed-writable][signed-readonly][unsigned-writable][unsigned-readonly]
//!
//! For v0 transactions, `meta.loadedAddresses.{writable,readonly}` from the
//! RPC response gives the ALT-resolved pubkeys already resolved server-side
//! (NOT indices) — we only need to concatenate them in the right order and
//! tag them with the correct writable/readonly flag. We deliberately do NOT
//! re-resolve lookup tables ourselves via separate `getAccountInfo` calls;
//! the RPC already did that work, and duplicating it would burn rate-limit
//! budget for no benefit (see ADR discussion).

use crate::types::AccountLock;

/// Mirrors the Solana transaction message header fields.
#[derive(Debug, Clone, Copy)]
pub struct MessageHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
}

/// Split a transaction's *static* account keys into writable/readonly lists,
/// per the Message v0 layout described above.
fn split_static_keys(
    static_keys: &[String],
    header: MessageHeader,
) -> (Vec<String>, Vec<String>) {
    let n = static_keys.len();
    let num_sig = header.num_required_signatures as usize;
    let num_readonly_signed = header.num_readonly_signed_accounts as usize;
    let num_readonly_unsigned = header.num_readonly_unsigned_accounts as usize;

    let signed_writable_count = num_sig.saturating_sub(num_readonly_signed);
    let non_signer_count = n.saturating_sub(num_sig);
    let unsigned_writable_count = non_signer_count.saturating_sub(num_readonly_unsigned);

    let signed_writable = &static_keys[0..signed_writable_count.min(n)];
    let signed_readonly = &static_keys[signed_writable_count.min(n)..num_sig.min(n)];
    let unsigned_writable_start = num_sig.min(n);
    let unsigned_writable_end = (num_sig + unsigned_writable_count).min(n);
    let unsigned_writable = &static_keys[unsigned_writable_start..unsigned_writable_end];
    let unsigned_readonly = &static_keys[unsigned_writable_end..n];

    let mut writable = Vec::with_capacity(signed_writable.len() + unsigned_writable.len());
    writable.extend_from_slice(signed_writable);
    writable.extend_from_slice(unsigned_writable);

    let mut readonly = Vec::with_capacity(signed_readonly.len() + unsigned_readonly.len());
    readonly.extend_from_slice(signed_readonly);
    readonly.extend_from_slice(unsigned_readonly);

    (writable, readonly)
}

/// Resolve the complete, ordered account-lock list for one transaction:
/// static accounts (split via the header) followed by ALT-loaded accounts
/// (already resolved to pubkeys by the RPC).
pub fn resolve_account_locks(
    static_keys: &[String],
    header: MessageHeader,
    loaded_writable: &[String],
    loaded_readonly: &[String],
) -> Vec<AccountLock> {
    let (static_writable, static_readonly) = split_static_keys(static_keys, header);

    let mut locks = Vec::with_capacity(
        static_writable.len() + static_readonly.len() + loaded_writable.len() + loaded_readonly.len(),
    );
    for a in static_writable.into_iter().chain(loaded_writable.iter().cloned()) {
        locks.push(AccountLock { account: a, is_writable: true });
    }
    for a in static_readonly.into_iter().chain(loaded_readonly.iter().cloned()) {
        locks.push(AccountLock { account: a, is_writable: false });
    }
    locks
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(strs: &[&str]) -> Vec<String> {
        strs.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn mixed_signed_unsigned_readonly_split() {
        let static_keys = keys(&["A0", "A1", "A2", "A3", "A4"]);
        let header = MessageHeader {
            num_required_signatures: 2,
            num_readonly_signed_accounts: 1,
            num_readonly_unsigned_accounts: 1,
        };
        let (w, r) = split_static_keys(&static_keys, header);
        assert_eq!(w, keys(&["A0", "A2", "A3"]));
        assert_eq!(r, keys(&["A1", "A4"]));
    }

    #[test]
    fn single_signer_no_readonly() {
        let static_keys = keys(&["Payer"]);
        let header = MessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
        };
        let (w, r) = split_static_keys(&static_keys, header);
        assert_eq!(w, keys(&["Payer"]));
        assert!(r.is_empty());
    }

    #[test]
    fn all_non_signers_readonly() {
        let static_keys = keys(&["Payer", "ProgramA", "Sysvar"]);
        let header = MessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 2,
        };
        let (w, r) = split_static_keys(&static_keys, header);
        assert_eq!(w, keys(&["Payer"]));
        assert_eq!(r, keys(&["ProgramA", "Sysvar"]));
    }

    #[test]
    fn full_resolution_appends_alt_accounts_writable_then_readonly() {
        let static_keys = keys(&["Payer", "ProgramA"]);
        let header = MessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 1,
        };
        let loaded_writable = keys(&["ALT_W1", "ALT_W2"]);
        let loaded_readonly = keys(&["ALT_R1"]);
        let locks = resolve_account_locks(&static_keys, header, &loaded_writable, &loaded_readonly);

        // Expect order: static-writable, ALT-writable, static-readonly, ALT-readonly
        let accounts: Vec<&str> = locks.iter().map(|l| l.account.as_str()).collect();
        assert_eq!(accounts, vec!["Payer", "ALT_W1", "ALT_W2", "ProgramA", "ALT_R1"]);
        assert!(locks[0].is_writable); // Payer
        assert!(locks[1].is_writable); // ALT_W1
        assert!(locks[2].is_writable); // ALT_W2
        assert!(!locks[3].is_writable); // ProgramA
        assert!(!locks[4].is_writable); // ALT_R1
    }

    #[test]
    fn no_alt_accounts_still_works() {
        let static_keys = keys(&["Payer"]);
        let header = MessageHeader {
            num_required_signatures: 1,
            num_readonly_signed_accounts: 0,
            num_readonly_unsigned_accounts: 0,
        };
        let locks = resolve_account_locks(&static_keys, header, &[], &[]);
        assert_eq!(locks.len(), 1);
        assert_eq!(locks[0].account, "Payer");
        assert!(locks[0].is_writable);
    }
}
