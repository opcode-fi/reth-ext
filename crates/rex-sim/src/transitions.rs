use hashbrown::HashMap;

use alloy_eips::eip4895::Withdrawals;
use reth_ethereum_primitives::Receipt;
use reth_primitives_traits::SealedHeader;
use reth_revm::revm::primitives::{Address, B256, U256};
use reth_revm::revm::state::{AccountInfo, Bytecode};

use crate::fetch::{AccountCache, BlockHashCache, CodeCache, StorageCache};
use crate::state::StateSnapshot;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CacheMergeStats {
    pub accounts_merged: usize,
    pub accounts_discarded: usize,
    pub storage_merged: usize,
    pub storage_discarded: usize,
    pub block_hashes_merged: usize,
    pub block_hashes_discarded: usize,
    pub code_merged: usize,
}

impl CacheMergeStats {
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, usize, usize)> {
        [
            ("accounts", self.accounts_merged, self.accounts_discarded),
            ("storage", self.storage_merged, self.storage_discarded),
            (
                "block_hashes",
                self.block_hashes_merged,
                self.block_hashes_discarded,
            ),
            ("code", self.code_merged, 0),
        ]
        .into_iter()
    }
}

#[derive(Default)]
pub struct Caches {
    pub accounts: AccountCache,
    pub code: CodeCache,
    pub storage: StorageCache,
    pub block_hashes: BlockHashCache,
}

#[derive(Debug, Clone, Default)]
pub struct AccountChange {
    pub info: Option<AccountInfo>,
    pub storage: HashMap<U256, U256>,
}

#[derive(Debug, Clone, Default)]
pub struct ExecutionChanges {
    pub accounts: HashMap<Address, AccountChange>,
    pub contracts: HashMap<B256, Bytecode>,
}

#[derive(Debug, Clone)]
pub struct BlockData {
    pub number: u64,
    pub hash: B256,
    pub header: SealedHeader,
    pub withdrawals: Option<Withdrawals>,
    pub receipts: Vec<Receipt>,
    pub tx_hashes: Vec<B256>,
}

#[derive(Debug, Clone, Copy)]
pub struct ChainTip {
    pub number: u64,
    pub hash: B256,
}

pub fn merge_caches(
    state: &mut StateSnapshot,
    caches: Caches,
    current_hash: B256,
) -> CacheMergeStats {
    let mut stats = CacheMergeStats::default();

    if caches.accounts.is_valid_for(current_hash) {
        stats.accounts_merged = caches.accounts.data.len();
        state.accounts.extend(caches.accounts.data);
    } else {
        stats.accounts_discarded = caches.accounts.data.len();
    }

    if caches.storage.is_valid_for(current_hash) {
        stats.storage_merged = caches.storage.data.len();
        state.storage.extend(caches.storage.data);
    } else {
        stats.storage_discarded = caches.storage.data.len();
    }

    if caches.block_hashes.is_valid_for(current_hash) {
        stats.block_hashes_merged = caches.block_hashes.data.len();
        state.block_hashes.extend(caches.block_hashes.data);
    } else {
        stats.block_hashes_discarded = caches.block_hashes.data.len();
    }

    stats.code_merged = caches.code.data.len();
    state.code.extend(caches.code.data);

    stats
}

pub fn apply_execution_changes(state: &mut StateSnapshot, changes: &ExecutionChanges) {
    for (address, account_change) in &changes.accounts {
        if let Some(info) = &account_change.info
            && let Some(known) = state.accounts.get_mut(address)
        {
            *known = info.clone();
        }

        for (slot, value) in &account_change.storage {
            if let Some(known) = state.storage.get_mut(&(*address, *slot)) {
                *known = *value;
            }
        }
    }

    for (code_hash, bytecode) in &changes.contracts {
        state.code.insert(*code_hash, bytecode.clone());
    }
}

pub fn apply_block_data(state: &mut StateSnapshot, blocks: &[BlockData]) {
    for block in blocks {
        state.push_receipts(
            block.number,
            block.receipts.clone(),
            block.tx_hashes.clone(),
        );
        state.block_hashes.insert(block.number, block.hash);
    }

    if let Some(tip) = blocks.last() {
        state.header = Some(tip.header.clone());
        state.withdrawals = tip.withdrawals.clone();
    }
}

pub fn build_commit_state(
    current: &StateSnapshot,
    caches: Caches,
    changes: &ExecutionChanges,
    blocks: &[BlockData],
    tip: ChainTip,
) -> (StateSnapshot, CacheMergeStats) {
    let mut new_state = current.clone();
    new_state.stale = false;

    let stats = merge_caches(&mut new_state, caches, current.block_hash);

    apply_execution_changes(&mut new_state, changes);

    apply_block_data(&mut new_state, blocks);

    let mut changed: Vec<Address> = changes.accounts.keys().copied().collect();
    changed.sort_unstable();
    new_state.push_changed_accounts(tip.number, changed);

    new_state.block = tip.number;
    new_state.block_hash = tip.hash;

    (new_state, stats)
}

fn copy_history_before(current: &StateSnapshot, cutoff: u64, new_state: &mut StateSnapshot) {
    for receipt_block in &current.receipts_history {
        if receipt_block.block <= cutoff {
            new_state.receipts_history.push_back(receipt_block.clone());
        }
    }
    for changed in &current.changed_accounts_history {
        if changed.block <= cutoff {
            new_state
                .changed_accounts_history
                .push_back(changed.clone());
        }
    }
    for (&block_num, &hash) in &current.block_hashes {
        if block_num <= cutoff {
            new_state.block_hashes.insert(block_num, hash);
        }
    }

    new_state.code = current.code.clone();
}

pub fn build_reorg_state(
    current: &StateSnapshot,
    common_ancestor: u64,
    changes: &ExecutionChanges,
    blocks: &[BlockData],
    tip: ChainTip,
) -> StateSnapshot {
    let mut new_state = StateSnapshot::default();
    copy_history_before(current, common_ancestor, &mut new_state);
    apply_execution_changes(&mut new_state, changes);
    apply_block_data(&mut new_state, blocks);

    let mut changed: Vec<Address> = changes.accounts.keys().copied().collect();
    changed.sort_unstable();
    new_state.push_changed_accounts(tip.number, changed);
    new_state.block = tip.number;
    new_state.block_hash = tip.hash;
    new_state
}

pub fn build_revert_state(current: &StateSnapshot, common_ancestor: u64) -> StateSnapshot {
    let mut new_state = StateSnapshot {
        stale: true,
        block: common_ancestor,
        block_hash: B256::ZERO,
        ..Default::default()
    };
    copy_history_before(current, common_ancestor, &mut new_state);
    new_state
}

pub fn mark_stale(state: &StateSnapshot) -> StateSnapshot {
    let mut new_state = state.clone();
    new_state.stale = true;
    new_state
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(n: u64) -> B256 {
        let mut bytes = [0u8; 32];
        bytes[24..32].copy_from_slice(&n.to_be_bytes());
        B256::from(bytes)
    }

    fn addr(n: u64) -> Address {
        let mut bytes = [0u8; 20];
        bytes[12..20].copy_from_slice(&n.to_be_bytes());
        Address::from(bytes)
    }

    fn test_header(number: u64, hash: B256) -> SealedHeader {
        use alloy_consensus::Header;
        let header = Header {
            number,
            ..Default::default()
        };
        SealedHeader::new(header, hash)
    }

    fn test_block_data(number: u64) -> BlockData {
        let h = hash(number);
        BlockData {
            number,
            hash: h,
            header: test_header(number, h),
            withdrawals: None,
            receipts: vec![],
            tx_hashes: vec![],
        }
    }

    fn account_info(balance: u64) -> AccountInfo {
        AccountInfo {
            balance: U256::from(balance),
            nonce: 0,
            code_hash: B256::ZERO,
            account_id: None,
            code: None,
        }
    }

    mod cache_merge {
        use super::*;

        #[test]
        fn merges_valid_caches() {
            let mut state = StateSnapshot {
                block: 100,
                block_hash: hash(100),
                ..Default::default()
            };

            let mut caches = Caches::default();
            caches.accounts.block_hash = hash(100);
            caches.accounts.data.insert(addr(1), account_info(1000));
            caches.storage.block_hash = hash(100);
            caches
                .storage
                .data
                .insert((addr(1), U256::from(0)), U256::from(42));

            let stats = merge_caches(&mut state, caches, hash(100));

            assert_eq!(stats.accounts_merged, 1);
            assert_eq!(stats.accounts_discarded, 0);
            assert_eq!(stats.storage_merged, 1);
            assert_eq!(stats.storage_discarded, 0);

            assert!(state.accounts.contains_key(&addr(1)));
            assert!(state.storage.contains_key(&(addr(1), U256::from(0))));
        }

        #[test]
        fn discards_stale_caches() {
            let mut state = StateSnapshot {
                block: 100,
                block_hash: hash(100),
                ..Default::default()
            };

            let mut caches = Caches::default();

            caches.accounts.block_hash = hash(99);
            caches.accounts.data.insert(addr(1), account_info(1000));
            caches.storage.block_hash = hash(99);
            caches
                .storage
                .data
                .insert((addr(1), U256::from(0)), U256::from(42));

            let stats = merge_caches(&mut state, caches, hash(100));

            assert_eq!(stats.accounts_merged, 0);
            assert_eq!(stats.accounts_discarded, 1);
            assert_eq!(stats.storage_merged, 0);
            assert_eq!(stats.storage_discarded, 1);

            assert!(state.accounts.is_empty());
            assert!(state.storage.is_empty());
        }

        #[test]
        fn code_always_merged() {
            let mut state = StateSnapshot::default();

            let mut caches = Caches::default();

            caches
                .code
                .data
                .insert(hash(1), Bytecode::new_raw(vec![0x60, 0x00].into()));

            let stats = merge_caches(&mut state, caches, hash(100));

            assert_eq!(stats.code_merged, 1);
            assert!(state.code.contains_key(&hash(1)));
        }

        #[test]
        fn empty_caches_are_valid() {
            let mut state = StateSnapshot {
                block: 100,
                block_hash: hash(100),
                ..Default::default()
            };

            let caches = Caches::default();
            let stats = merge_caches(&mut state, caches, hash(100));

            assert_eq!(stats.accounts_merged, 0);
            assert_eq!(stats.accounts_discarded, 0);
        }
    }

    mod execution_changes {
        use super::*;

        /// Only accounts the snapshot already holds take the block's writes; the rest stay lazy.
        #[test]
        fn applies_account_info_to_known_accounts_only() {
            let mut state = StateSnapshot::default();
            state.accounts.insert(addr(1), account_info(1000));

            let mut changes = ExecutionChanges::default();
            for (n, balance) in [(1, 5000), (2, 7000)] {
                changes.accounts.insert(
                    addr(n),
                    AccountChange {
                        info: Some(account_info(balance)),
                        storage: HashMap::new(),
                    },
                );
            }

            apply_execution_changes(&mut state, &changes);

            assert_eq!(
                state.accounts.get(&addr(1)).unwrap().balance,
                U256::from(5000)
            );
            assert!(!state.accounts.contains_key(&addr(2)));
        }

        /// Only slots the snapshot already holds take the block's writes; the rest stay lazy.
        #[test]
        fn applies_storage_changes_to_known_slots_only() {
            let mut state = StateSnapshot::default();
            state
                .storage
                .insert((addr(1), U256::from(0)), U256::from(50));

            let mut storage = HashMap::new();
            storage.insert(U256::from(0), U256::from(100));
            storage.insert(U256::from(1), U256::from(200));

            let mut changes = ExecutionChanges::default();
            changes.accounts.insert(
                addr(1),
                AccountChange {
                    info: None,
                    storage,
                },
            );

            apply_execution_changes(&mut state, &changes);

            assert_eq!(
                state.storage.get(&(addr(1), U256::from(0))),
                Some(&U256::from(100))
            );
            assert!(!state.storage.contains_key(&(addr(1), U256::from(1))));
        }

        #[test]
        fn applies_contracts() {
            let mut state = StateSnapshot::default();

            let mut changes = ExecutionChanges::default();
            changes
                .contracts
                .insert(hash(1), Bytecode::new_raw(vec![0x60, 0x00].into()));

            apply_execution_changes(&mut state, &changes);

            assert!(state.code.contains_key(&hash(1)));
        }

        #[test]
        fn overwrites_existing_values() {
            let mut state = StateSnapshot::default();
            state.accounts.insert(addr(1), account_info(1000));
            state
                .storage
                .insert((addr(1), U256::from(0)), U256::from(50));

            let mut storage = HashMap::new();
            storage.insert(U256::from(0), U256::from(999));

            let mut changes = ExecutionChanges::default();
            changes.accounts.insert(
                addr(1),
                AccountChange {
                    info: Some(account_info(5000)),
                    storage,
                },
            );

            apply_execution_changes(&mut state, &changes);

            assert_eq!(
                state.accounts.get(&addr(1)).unwrap().balance,
                U256::from(5000)
            );
            assert_eq!(
                state.storage.get(&(addr(1), U256::from(0))),
                Some(&U256::from(999))
            );
        }
    }

    mod block_data {
        use super::*;

        #[test]
        fn applies_block_hashes() {
            let mut state = StateSnapshot::default();

            let blocks = vec![test_block_data(100), test_block_data(101)];

            apply_block_data(&mut state, &blocks);

            assert_eq!(state.block_hashes.get(&100), Some(&hash(100)));
            assert_eq!(state.block_hashes.get(&101), Some(&hash(101)));
        }

        #[test]
        fn applies_receipts() {
            let mut state = StateSnapshot::default();

            let blocks = vec![test_block_data(100), test_block_data(101)];

            apply_block_data(&mut state, &blocks);

            assert_eq!(state.receipts_history.len(), 2);
            assert_eq!(state.receipts_history[0].block, 100);
            assert_eq!(state.receipts_history[1].block, 101);
        }
    }

    mod build_commit {
        use super::*;

        #[test]
        fn builds_from_current_state() {
            let mut current = StateSnapshot {
                block: 99,
                block_hash: hash(99),
                stale: true,
                ..Default::default()
            };
            current.accounts.insert(addr(1), account_info(1000));

            let (new_state, _) = build_commit_state(
                &current,
                Caches::default(),
                &ExecutionChanges::default(),
                &[],
                ChainTip {
                    number: 100,
                    hash: hash(100),
                },
            );

            assert!(!new_state.stale);
            assert_eq!(new_state.block, 100);
            assert_eq!(new_state.block_hash, hash(100));

            assert!(new_state.accounts.contains_key(&addr(1)));
        }

        #[test]
        fn execution_overwrites_cache() {
            let current = StateSnapshot {
                block: 99,
                block_hash: hash(99),
                ..Default::default()
            };

            let mut caches = Caches::default();
            caches.accounts.block_hash = hash(99);
            caches.accounts.data.insert(addr(1), account_info(1000));

            let mut changes = ExecutionChanges::default();
            changes.accounts.insert(
                addr(1),
                AccountChange {
                    info: Some(account_info(5000)),
                    storage: HashMap::new(),
                },
            );

            let (new_state, stats) = build_commit_state(
                &current,
                caches,
                &changes,
                &[],
                ChainTip {
                    number: 100,
                    hash: hash(100),
                },
            );

            assert_eq!(stats.accounts_merged, 1);

            assert_eq!(
                new_state.accounts.get(&addr(1)).unwrap().balance,
                U256::from(5000)
            );
        }

        #[test]
        fn records_changed_accounts_at_tip() {
            let current = StateSnapshot::default();
            let mut changes = ExecutionChanges::default();
            changes.accounts.insert(
                addr(1),
                AccountChange {
                    info: Some(account_info(5000)),
                    storage: HashMap::new(),
                },
            );
            changes.accounts.insert(
                addr(2),
                AccountChange {
                    info: Some(account_info(7000)),
                    storage: HashMap::new(),
                },
            );

            let (new_state, _) = build_commit_state(
                &current,
                Caches::default(),
                &changes,
                &[test_block_data(100)],
                ChainTip {
                    number: 100,
                    hash: hash(100),
                },
            );

            let mut got = new_state.changed_accounts(100).expect("recorded").to_vec();
            got.sort();
            let mut want = vec![addr(1), addr(2)];
            want.sort();
            assert_eq!(got, want);
        }
    }

    mod build_reorg {
        use super::*;

        #[test]
        fn clears_accounts_and_storage() {
            let mut current = StateSnapshot {
                block: 100,
                block_hash: hash(100),
                ..Default::default()
            };
            current.accounts.insert(addr(1), account_info(1000));
            current
                .storage
                .insert((addr(1), U256::from(0)), U256::from(42));

            let new_state = build_reorg_state(
                &current,
                95,
                &ExecutionChanges::default(),
                &[],
                ChainTip {
                    number: 98,
                    hash: hash(98),
                },
            );

            assert!(new_state.accounts.is_empty());
            assert!(new_state.storage.is_empty());
        }

        #[test]
        fn preserves_code() {
            let mut current = StateSnapshot::default();
            current
                .code
                .insert(hash(1), Bytecode::new_raw(vec![0x60].into()));

            let new_state = build_reorg_state(
                &current,
                95,
                &ExecutionChanges::default(),
                &[],
                ChainTip {
                    number: 98,
                    hash: hash(98),
                },
            );

            assert!(new_state.code.contains_key(&hash(1)));
        }

        #[test]
        fn truncates_receipts_at_common_ancestor() {
            let mut current = StateSnapshot::default();
            current.push_receipts(90, vec![], vec![]);
            current.push_receipts(95, vec![], vec![]);
            current.push_receipts(100, vec![], vec![]);

            let new_state = build_reorg_state(
                &current,
                95,
                &ExecutionChanges::default(),
                &[],
                ChainTip {
                    number: 98,
                    hash: hash(98),
                },
            );

            assert_eq!(new_state.receipts_history.len(), 2);
            assert_eq!(new_state.receipts_history[0].block, 90);
            assert_eq!(new_state.receipts_history[1].block, 95);
        }

        #[test]
        fn truncates_block_hashes_at_common_ancestor() {
            let mut current = StateSnapshot::default();
            current.block_hashes.insert(90, hash(90));
            current.block_hashes.insert(95, hash(95));
            current.block_hashes.insert(100, hash(100));

            let new_state = build_reorg_state(
                &current,
                95,
                &ExecutionChanges::default(),
                &[],
                ChainTip {
                    number: 98,
                    hash: hash(98),
                },
            );

            assert_eq!(new_state.block_hashes.len(), 2);
            assert!(new_state.block_hashes.contains_key(&90));
            assert!(new_state.block_hashes.contains_key(&95));
            assert!(!new_state.block_hashes.contains_key(&100));
        }

        /// A reorg empties the snapshot, so the new chain's diffs land on nothing and every read goes lazy.
        #[test]
        fn applies_new_chain_changes() {
            let current = StateSnapshot::default();

            let mut changes = ExecutionChanges::default();
            changes.accounts.insert(
                addr(1),
                AccountChange {
                    info: Some(account_info(5000)),
                    storage: HashMap::new(),
                },
            );

            let blocks = vec![test_block_data(98)];

            let new_state = build_reorg_state(
                &current,
                95,
                &changes,
                &blocks,
                ChainTip {
                    number: 98,
                    hash: hash(98),
                },
            );

            assert!(!new_state.accounts.contains_key(&addr(1)));

            assert!(new_state.block_hashes.contains_key(&98));
            assert_eq!(new_state.receipts_history.len(), 1);
        }

        #[test]
        fn records_new_chain_changed_accounts() {
            let current = StateSnapshot::default();
            let mut changes = ExecutionChanges::default();
            changes.accounts.insert(
                addr(1),
                AccountChange {
                    info: Some(account_info(5000)),
                    storage: HashMap::new(),
                },
            );

            let new_state = build_reorg_state(
                &current,
                95,
                &changes,
                &[test_block_data(98)],
                ChainTip {
                    number: 98,
                    hash: hash(98),
                },
            );

            assert_eq!(new_state.changed_accounts(98), Some([addr(1)].as_slice()));
        }

        #[test]
        fn truncates_changed_accounts_at_common_ancestor() {
            let mut current = StateSnapshot::default();
            current.push_changed_accounts(90, vec![addr(90)]);
            current.push_changed_accounts(95, vec![addr(95)]);
            current.push_changed_accounts(100, vec![addr(100)]);

            let new_state = build_reorg_state(
                &current,
                95,
                &ExecutionChanges::default(),
                &[],
                ChainTip {
                    number: 98,
                    hash: hash(98),
                },
            );

            assert!(new_state.changed_accounts(100).is_none());
            assert_eq!(new_state.changed_accounts(95), Some([addr(95)].as_slice()));
            assert_eq!(new_state.changed_accounts(90), Some([addr(90)].as_slice()));
        }
    }

    mod build_revert {
        use super::*;

        #[test]
        fn marks_state_as_stale() {
            let current = StateSnapshot::default();
            let new_state = build_revert_state(&current, 95);

            assert!(new_state.stale);
        }

        #[test]
        fn sets_block_hash_to_zero() {
            let current = StateSnapshot::default();
            let new_state = build_revert_state(&current, 95);

            assert_eq!(new_state.block, 95);
            assert_eq!(new_state.block_hash, B256::ZERO);
        }

        #[test]
        fn clears_accounts_and_storage() {
            let mut current = StateSnapshot::default();
            current.accounts.insert(addr(1), account_info(1000));
            current
                .storage
                .insert((addr(1), U256::from(0)), U256::from(42));

            let new_state = build_revert_state(&current, 95);

            assert!(new_state.accounts.is_empty());
            assert!(new_state.storage.is_empty());
        }

        #[test]
        fn preserves_code() {
            let mut current = StateSnapshot::default();
            current
                .code
                .insert(hash(1), Bytecode::new_raw(vec![0x60].into()));

            let new_state = build_revert_state(&current, 95);

            assert!(new_state.code.contains_key(&hash(1)));
        }

        #[test]
        fn truncates_at_common_ancestor() {
            let mut current = StateSnapshot::default();
            current.push_receipts(90, vec![], vec![]);
            current.push_receipts(95, vec![], vec![]);
            current.push_receipts(100, vec![], vec![]);
            current.block_hashes.insert(90, hash(90));
            current.block_hashes.insert(95, hash(95));
            current.block_hashes.insert(100, hash(100));

            let new_state = build_revert_state(&current, 95);

            assert_eq!(new_state.receipts_history.len(), 2);
            assert_eq!(new_state.block_hashes.len(), 2);
        }
    }

    mod scenarios {
        use super::*;

        #[test]
        fn commit_then_reorg_scenario() {
            let mut state = StateSnapshot {
                block: 100,
                block_hash: hash(100),
                stale: false,
                ..Default::default()
            };
            state.accounts.insert(addr(1), account_info(1000));
            state
                .storage
                .insert((addr(1), U256::from(0)), U256::from(42));
            state.block_hashes.insert(100, hash(100));

            let new_state = build_reorg_state(
                &state,
                95,
                &ExecutionChanges::default(),
                &[],
                ChainTip {
                    number: 98,
                    hash: hash(98),
                },
            );

            assert!(new_state.accounts.is_empty());
            assert!(new_state.storage.is_empty());
            assert!(!new_state.block_hashes.contains_key(&100));
            assert_eq!(new_state.block, 98);
            assert!(!new_state.stale);
        }

        #[test]
        fn revert_then_recover_scenario() {
            let mut reverted = build_revert_state(&StateSnapshot::default(), 95);
            assert!(reverted.stale);

            reverted.stale = true;
            let (recovered, _) = build_commit_state(
                &reverted,
                Caches::default(),
                &ExecutionChanges::default(),
                &[],
                ChainTip {
                    number: 100,
                    hash: hash(100),
                },
            );

            assert!(!recovered.stale);
            assert_eq!(recovered.block, 100);
        }

        #[test]
        fn stale_cache_discarded_on_commit() {
            let current = StateSnapshot {
                block: 99,
                block_hash: hash(99),
                ..Default::default()
            };

            let mut caches = Caches::default();
            caches.accounts.block_hash = hash(50);
            caches.accounts.data.insert(addr(1), account_info(1000));

            let (new_state, stats) = build_commit_state(
                &current,
                caches,
                &ExecutionChanges::default(),
                &[],
                ChainTip {
                    number: 100,
                    hash: hash(100),
                },
            );

            assert_eq!(stats.accounts_discarded, 1);
            assert_eq!(stats.accounts_merged, 0);
            assert!(new_state.accounts.is_empty());
        }
    }
}
