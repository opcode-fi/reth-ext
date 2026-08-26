use hashbrown::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use alloy_eips::eip4895::Withdrawals;
use arc_swap::ArcSwap;
use crossbeam_channel::{Receiver, RecvTimeoutError};
use reth_ethereum_primitives::Receipt;
use reth_primitives_traits::SealedHeader;
use reth_revm::DatabaseRef;
use reth_revm::revm::primitives::{Address, B256, U256};
use reth_revm::revm::state::{AccountInfo, Bytecode};
use reth_tracing::tracing::{error, warn};
use tokio::sync::mpsc;

use crate::error::{FetchKind, SimError};
use crate::fetch::{
    AccountFetchMessage, AccountRequest, BlockHashFetchMessage, BlockHashRequest, CodeFetchMessage,
    CodeRequest, FetchConfig, StorageFetchMessage, StorageRequest,
};

pub const RECEIPTS_HISTORY_DEPTH: usize = 64;

#[derive(Clone, Debug)]
pub struct BlockReceipts {
    pub block: u64,
    pub receipts: Vec<Receipt>,
    pub tx_hashes: Vec<B256>,
}

#[derive(Clone, Debug)]
pub struct BlockChangedAccounts {
    pub block: u64,
    pub accounts: Vec<Address>,
}

#[derive(Clone, Default)]
pub struct StateSnapshot {
    pub block: u64,
    pub block_hash: B256,
    pub header: Option<SealedHeader>,
    pub withdrawals: Option<Withdrawals>,
    pub accounts: HashMap<Address, AccountInfo>,
    pub code: HashMap<B256, Bytecode>,
    pub storage: HashMap<(Address, U256), U256>,
    pub block_hashes: HashMap<u64, B256>,
    pub receipts_history: VecDeque<BlockReceipts>,
    pub changed_accounts_history: VecDeque<BlockChangedAccounts>,
    pub stale: bool,
}

impl StateSnapshot {
    pub fn receipts(&self, block: u64) -> Option<&[Receipt]> {
        self.receipts_history
            .iter()
            .find(|r| r.block == block)
            .map(|r| r.receipts.as_slice())
    }

    pub fn receipts_and_tx_hashes(&self, block: u64) -> Option<(&[Receipt], &[B256])> {
        self.receipts_history
            .iter()
            .find(|r| r.block == block)
            .map(|r| (r.receipts.as_slice(), r.tx_hashes.as_slice()))
    }

    pub fn push_receipts(&mut self, block: u64, receipts: Vec<Receipt>, tx_hashes: Vec<B256>) {
        while self.receipts_history.len() >= RECEIPTS_HISTORY_DEPTH {
            self.receipts_history.pop_front();
        }
        self.receipts_history.push_back(BlockReceipts {
            block,
            receipts,
            tx_hashes,
        });
    }

    pub fn truncate_receipts(&mut self, from_block: u64) {
        self.receipts_history.retain(|r| r.block < from_block);
    }

    pub fn changed_accounts(&self, block: u64) -> Option<&[Address]> {
        self.changed_accounts_history
            .iter()
            .find(|c| c.block == block)
            .map(|c| c.accounts.as_slice())
    }

    pub fn push_changed_accounts(&mut self, block: u64, accounts: Vec<Address>) {
        while self.changed_accounts_history.len() >= RECEIPTS_HISTORY_DEPTH {
            self.changed_accounts_history.pop_front();
        }
        self.changed_accounts_history
            .push_back(BlockChangedAccounts { block, accounts });
    }

    pub fn truncate_changed_accounts(&mut self, from_block: u64) {
        self.changed_accounts_history
            .retain(|c| c.block < from_block);
    }
}

pub struct FetchChannels {
    pub account: mpsc::UnboundedSender<AccountFetchMessage>,
    pub code: mpsc::UnboundedSender<CodeFetchMessage>,
    pub storage: mpsc::UnboundedSender<StorageFetchMessage>,
    pub block_hash: mpsc::UnboundedSender<BlockHashFetchMessage>,
}

fn await_fetch<T>(
    rx: Receiver<Result<T, SimError>>,
    kind: FetchKind,
    timeout: Duration,
) -> Result<T, SimError> {
    match rx.recv_timeout(timeout) {
        Ok(response) => response,
        Err(RecvTimeoutError::Timeout) => {
            error!(target: "rex-sim", ?kind, ?timeout, "Fetch timed out waiting for fetch manager");
            Err(SimError::FetchTimeout {
                kind,
                elapsed: timeout,
            })
        }
        Err(RecvTimeoutError::Disconnected) => Err(SimError::FetcherClosed),
    }
}

pub struct StaticState {
    inner: ArcSwap<StateSnapshot>,
    generation: AtomicU64,
    channels: FetchChannels,
    fetch: FetchConfig,
}

impl StaticState {
    pub fn new(initial: StateSnapshot, channels: FetchChannels, fetch: FetchConfig) -> Self {
        Self {
            inner: ArcSwap::new(Arc::new(initial)),
            generation: AtomicU64::new(0),
            channels,
            fetch,
        }
    }

    pub fn block(&self) -> u64 {
        self.inner.load().block
    }

    pub fn block_hash(&self) -> B256 {
        self.inner.load().block_hash
    }

    pub fn header(&self) -> Option<SealedHeader> {
        self.inner.load().header.clone()
    }

    pub fn withdrawals(&self) -> Option<Withdrawals> {
        self.inner.load().withdrawals.clone()
    }

    pub fn is_stale(&self) -> bool {
        self.inner.load().stale
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn swap(&self, new_state: StateSnapshot) {
        self.inner.store(Arc::new(new_state));
        self.generation.fetch_add(1, Ordering::Release);
    }

    pub fn snapshot(&self) -> Arc<StateSnapshot> {
        self.inner.load_full()
    }

    pub fn receipts(&self, block: u64) -> Option<Vec<Receipt>> {
        self.inner.load().receipts(block).map(|r| r.to_vec())
    }

    pub fn receipts_and_tx_hashes(&self, block: u64) -> Option<(Vec<Receipt>, Vec<B256>)> {
        self.inner
            .load()
            .receipts_and_tx_hashes(block)
            .map(|(r, h)| (r.to_vec(), h.to_vec()))
    }

    pub fn changed_accounts(&self, block: u64) -> Option<Vec<Address>> {
        self.inner
            .load()
            .changed_accounts(block)
            .map(|a| a.to_vec())
    }

    pub fn guard(&self) -> StateGuard {
        StateGuard {
            generation: self.generation(),
            block_hash: self.block_hash(),
            stale: self.is_stale(),
        }
    }

    pub fn check_guard(&self, guard: &StateGuard) -> StateValidity {
        if guard.stale {
            return StateValidity::WasStale;
        }
        if self.is_stale() {
            return StateValidity::BecameStale;
        }
        let current_gen = self.generation();
        if guard.generation != current_gen {
            return StateValidity::Changed {
                old_gen: guard.generation,
                new_gen: current_gen,
            };
        }
        StateValidity::Valid
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StateGuard {
    pub generation: u64,
    pub block_hash: B256,
    pub stale: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateValidity {
    Valid,
    WasStale,
    BecameStale,
    Changed { old_gen: u64, new_gen: u64 },
}

impl DatabaseRef for StaticState {
    type Error = SimError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        let state = self.inner.load();

        if state.stale {
            warn!(target: "rex-sim", address = %address, "Fetch attempted on stale state");
            return Err(SimError::StaleState);
        }

        if let Some(info) = state.accounts.get(&address) {
            return Ok(Some(info.clone()));
        }

        let (tx, rx) = crossbeam_channel::bounded(1);

        self.channels
            .account
            .send(AccountFetchMessage {
                request: AccountRequest {
                    address,
                    block: state.block,
                    block_hash: state.block_hash,
                },
                respond_to: tx,
            })
            .map_err(|_| SimError::FetcherClosed)?;

        await_fetch(
            rx,
            FetchKind::Account,
            Duration::from_millis(self.fetch.wait_timeout_ms),
        )
    }

    fn code_by_hash_ref(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        let state = self.inner.load();

        if state.stale {
            warn!(target: "rex-sim", code_hash = %code_hash, "Fetch attempted on stale state");
            return Err(SimError::StaleState);
        }

        if let Some(code) = state.code.get(&code_hash) {
            return Ok(code.clone());
        }

        let (tx, rx) = crossbeam_channel::bounded(1);

        self.channels
            .code
            .send(CodeFetchMessage {
                request: CodeRequest {
                    code_hash,
                    block: state.block,
                    address_hint: None,
                },
                respond_to: tx,
            })
            .map_err(|_| SimError::FetcherClosed)?;

        await_fetch(
            rx,
            FetchKind::Code,
            Duration::from_millis(self.fetch.wait_timeout_ms),
        )
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        let state = self.inner.load();

        if state.stale {
            warn!(target: "rex-sim", address = %address, slot = %index, "Fetch attempted on stale state");
            return Err(SimError::StaleState);
        }

        if let Some(value) = state.storage.get(&(address, index)) {
            return Ok(*value);
        }

        let (tx, rx) = crossbeam_channel::bounded(1);

        self.channels
            .storage
            .send(StorageFetchMessage {
                request: StorageRequest {
                    address,
                    index,
                    block: state.block,
                    block_hash: state.block_hash,
                },
                respond_to: tx,
            })
            .map_err(|_| SimError::FetcherClosed)?;

        await_fetch(
            rx,
            FetchKind::Storage,
            Duration::from_millis(self.fetch.wait_timeout_ms),
        )
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        let state = self.inner.load();

        if state.stale {
            warn!(target: "rex-sim", block = number, "Fetch attempted on stale state");
            return Err(SimError::StaleState);
        }

        if let Some(hash) = state.block_hashes.get(&number) {
            return Ok(*hash);
        }

        let (tx, rx) = crossbeam_channel::bounded(1);

        self.channels
            .block_hash
            .send(BlockHashFetchMessage {
                request: BlockHashRequest {
                    number,
                    at_block_hash: state.block_hash,
                },
                respond_to: tx,
            })
            .map_err(|_| SimError::FetcherClosed)?;

        await_fetch(
            rx,
            FetchKind::BlockHash,
            Duration::from_millis(self.fetch.wait_timeout_ms),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(n: u64) -> Address {
        let mut bytes = [0u8; 20];
        bytes[12..20].copy_from_slice(&n.to_be_bytes());
        Address::from(bytes)
    }

    #[test]
    fn stores_and_retrieves_changed_accounts() {
        let mut s = StateSnapshot::default();
        s.push_changed_accounts(100, vec![addr(1), addr(2)]);
        s.push_changed_accounts(101, vec![addr(3)]);
        assert_eq!(s.changed_accounts(100), Some([addr(1), addr(2)].as_slice()));
        assert_eq!(s.changed_accounts(101), Some([addr(3)].as_slice()));
        assert_eq!(s.changed_accounts(102), None);
    }

    #[test]
    fn changed_accounts_respects_history_depth() {
        let mut s = StateSnapshot::default();
        for b in 0..(RECEIPTS_HISTORY_DEPTH as u64 + 10) {
            s.push_changed_accounts(b, vec![addr(b)]);
        }
        assert_eq!(s.changed_accounts_history.len(), RECEIPTS_HISTORY_DEPTH);

        assert!(s.changed_accounts(9).is_none());
        assert_eq!(s.changed_accounts(10), Some([addr(10)].as_slice()));
    }

    #[test]
    fn truncates_changed_accounts_from_block() {
        let mut s = StateSnapshot::default();
        s.push_changed_accounts(90, vec![addr(90)]);
        s.push_changed_accounts(95, vec![addr(95)]);
        s.push_changed_accounts(100, vec![addr(100)]);
        s.truncate_changed_accounts(96);
        assert_eq!(s.changed_accounts_history.len(), 2);
        assert!(s.changed_accounts(100).is_none());
        assert_eq!(s.changed_accounts(95), Some([addr(95)].as_slice()));
    }

    #[test]
    fn static_state_exposes_changed_accounts() {
        let (account_tx, _a) = tokio::sync::mpsc::unbounded_channel();
        let (code_tx, _c) = tokio::sync::mpsc::unbounded_channel();
        let (storage_tx, _s) = tokio::sync::mpsc::unbounded_channel();
        let (block_hash_tx, _b) = tokio::sync::mpsc::unbounded_channel();
        let channels = FetchChannels {
            account: account_tx,
            code: code_tx,
            storage: storage_tx,
            block_hash: block_hash_tx,
        };
        let mut snap = StateSnapshot::default();
        snap.push_changed_accounts(200, vec![addr(7)]);
        let state = StaticState::new(snap, channels, FetchConfig::default());
        assert_eq!(state.changed_accounts(200), Some(vec![addr(7)]));
        assert_eq!(state.changed_accounts(201), None);
    }
}
