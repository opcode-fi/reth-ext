use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::Receiver as Replies;
use reth_revm::revm::primitives::{Address, B256, Bytes, KECCAK_EMPTY, U256};
use reth_revm::revm::state::{AccountInfo, Bytecode};
use tokio::sync::{mpsc, oneshot};

use crate::error::SimError;
use crate::fetch::*;
use crate::managers::*;
use crate::rpc::RpcClient;
use crate::rpc::mock::MockRpc;

const RECV: Duration = Duration::from_secs(5);

fn addr(n: u64) -> Address {
    let mut bytes = [0u8; 20];
    bytes[12..20].copy_from_slice(&n.to_be_bytes());
    Address::from(bytes)
}

fn hash(n: u64) -> B256 {
    let mut bytes = [0u8; 32];
    bytes[24..32].copy_from_slice(&n.to_be_bytes());
    B256::from(bytes)
}

fn info(balance: u64, code_hash: B256) -> AccountInfo {
    AccountInfo {
        balance: U256::from(balance),
        nonce: balance,
        code_hash,
        account_id: None,
        code: None,
    }
}

fn bytecode(byte: u8) -> Bytecode {
    Bytecode::new_raw(Bytes::from(vec![0x60, byte]))
}

fn cfg(permits: usize, flush_ms: u64) -> FetchConfig {
    FetchConfig {
        account: permits,
        code: permits,
        storage: permits,
        block_hash: permits,
        rpc_timeout_ms: 1_000,
        wait_timeout_ms: 2_000,
        flush_timeout_ms: flush_ms,
    }
}

async fn until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..1_000 {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for: {what}");
}

struct AccountHarness {
    tx: mpsc::UnboundedSender<AccountFetchMessage>,
    flush_tx: mpsc::Sender<oneshot::Sender<AccountCache>>,
    regs: mpsc::UnboundedReceiver<CodeRegistration>,
    rpc: Arc<MockRpc>,
}

impl AccountHarness {
    fn spawn(rpc: Arc<MockRpc>, fetch: FetchConfig) -> Self {
        let (tx, fetch_rx) = mpsc::unbounded_channel();
        let (flush_tx, flush_rx) = mpsc::channel(1);
        let (reg_tx, regs) = mpsc::unbounded_channel();
        let manager = AccountFetchManager::new(
            fetch_rx,
            flush_rx,
            SideChannel::none(),
            reg_tx,
            RpcClient::mock(Arc::clone(&rpc)),
            fetch,
        );
        tokio::spawn(manager.run());
        Self {
            tx,
            flush_tx,
            regs,
            rpc,
        }
    }

    fn get(
        &self,
        address: Address,
        block: u64,
        block_hash: B256,
    ) -> Replies<Result<Option<AccountInfo>, SimError>> {
        let (respond_to, rx) = crossbeam_channel::bounded(1);
        self.tx
            .send(AccountFetchMessage {
                request: AccountRequest {
                    address,
                    block,
                    block_hash,
                },
                respond_to,
            })
            .unwrap();
        rx
    }

    async fn flush(&self) -> AccountCache {
        let (tx, rx) = oneshot::channel();
        self.flush_tx.send(tx).await.unwrap();
        rx.await.unwrap()
    }
}

struct CodeHarness {
    tx: mpsc::UnboundedSender<CodeFetchMessage>,
    flush_tx: mpsc::Sender<oneshot::Sender<CodeCache>>,
    reg_tx: mpsc::UnboundedSender<CodeRegistration>,
    rpc: Arc<MockRpc>,
}

impl CodeHarness {
    fn spawn(rpc: Arc<MockRpc>, fetch: FetchConfig) -> Self {
        let (tx, fetch_rx) = mpsc::unbounded_channel();
        let (flush_tx, flush_rx) = mpsc::channel(1);
        let (reg_tx, reg_rx) = mpsc::unbounded_channel();
        let manager = CodeFetchManager::new(
            fetch_rx,
            flush_rx,
            SideChannel::new(reg_rx),
            CodeHints::default(),
            RpcClient::mock(Arc::clone(&rpc)),
            fetch,
        );
        tokio::spawn(manager.run());
        Self {
            tx,
            flush_tx,
            reg_tx,
            rpc,
        }
    }

    fn get(
        &self,
        code_hash: B256,
        block: u64,
        address_hint: Option<Address>,
    ) -> Replies<Result<Bytecode, SimError>> {
        let (respond_to, rx) = crossbeam_channel::bounded(1);
        self.tx
            .send(CodeFetchMessage {
                request: CodeRequest {
                    code_hash,
                    block,
                    address_hint,
                },
                respond_to,
            })
            .unwrap();
        rx
    }

    async fn flush(&self) -> CodeCache {
        let (tx, rx) = oneshot::channel();
        self.flush_tx.send(tx).await.unwrap();
        rx.await.unwrap()
    }
}

struct StorageHarness {
    tx: mpsc::UnboundedSender<StorageFetchMessage>,
    flush_tx: mpsc::Sender<oneshot::Sender<StorageCache>>,
    rpc: Arc<MockRpc>,
}

impl StorageHarness {
    fn spawn(rpc: Arc<MockRpc>, fetch: FetchConfig) -> Self {
        let (tx, fetch_rx) = mpsc::unbounded_channel();
        let (flush_tx, flush_rx) = mpsc::channel(1);
        let manager = StorageFetchManager::new(
            fetch_rx,
            flush_rx,
            SideChannel::none(),
            (),
            RpcClient::mock(Arc::clone(&rpc)),
            fetch,
        );
        tokio::spawn(manager.run());
        Self { tx, flush_tx, rpc }
    }

    fn get(
        &self,
        address: Address,
        index: U256,
        block: u64,
        block_hash: B256,
    ) -> Replies<Result<U256, SimError>> {
        let (respond_to, rx) = crossbeam_channel::bounded(1);
        self.tx
            .send(StorageFetchMessage {
                request: StorageRequest {
                    address,
                    index,
                    block,
                    block_hash,
                },
                respond_to,
            })
            .unwrap();
        rx
    }

    async fn flush(&self) -> StorageCache {
        let (tx, rx) = oneshot::channel();
        self.flush_tx.send(tx).await.unwrap();
        rx.await.unwrap()
    }
}

struct BlockHashHarness {
    tx: mpsc::UnboundedSender<BlockHashFetchMessage>,
    flush_tx: mpsc::Sender<oneshot::Sender<BlockHashCache>>,
    rpc: Arc<MockRpc>,
}

impl BlockHashHarness {
    fn spawn(rpc: Arc<MockRpc>, fetch: FetchConfig) -> Self {
        let (tx, fetch_rx) = mpsc::unbounded_channel();
        let (flush_tx, flush_rx) = mpsc::channel(1);
        let manager = BlockHashFetchManager::new(
            fetch_rx,
            flush_rx,
            SideChannel::none(),
            (),
            RpcClient::mock(Arc::clone(&rpc)),
            fetch,
        );
        tokio::spawn(manager.run());
        Self { tx, flush_tx, rpc }
    }

    fn get(&self, number: u64, at_block_hash: B256) -> Replies<Result<B256, SimError>> {
        let (respond_to, rx) = crossbeam_channel::bounded(1);
        self.tx
            .send(BlockHashFetchMessage {
                request: BlockHashRequest {
                    number,
                    at_block_hash,
                },
                respond_to,
            })
            .unwrap();
        rx
    }

    async fn flush(&self) -> BlockHashCache {
        let (tx, rx) = oneshot::channel();
        self.flush_tx.send(tx).await.unwrap();
        rx.await.unwrap()
    }
}

/// A second request for a cached key is served without an RPC call.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_cache_hit_skips_rpc() {
    let rpc = MockRpc::new();
    rpc.set_account(addr(1), Some(info(10, KECCAK_EMPTY)));
    let h = AccountHarness::spawn(Arc::clone(&rpc), cfg(4, 5_000));

    let first = h.get(addr(1), 100, hash(1)).recv_timeout(RECV).unwrap();
    assert_eq!(first.unwrap().unwrap().balance, U256::from(10));
    assert_eq!(h.rpc.account_calls(), 1);

    let second = h.get(addr(1), 100, hash(1)).recv_timeout(RECV).unwrap();
    assert_eq!(second.unwrap().unwrap().balance, U256::from(10));
    assert_eq!(h.rpc.account_calls(), 1, "cache hit must not hit the RPC");
}

/// A completion whose epoch predates a cache-tag change is delivered to the
/// caller but not inserted into the cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_stale_epoch_completion_is_not_cached() {
    let (rpc, gate) = MockRpc::gated();
    rpc.set_account(addr(1), Some(info(10, hash(0xaa))));
    rpc.set_account(addr(2), Some(info(20, hash(0xbb))));
    let h = AccountHarness::spawn(Arc::clone(&rpc), cfg(8, 5_000));

    let stale = h.get(addr(1), 100, hash(1));
    until("account fetch 1 in flight", || h.rpc.account_calls() == 1).await;

    let fresh = h.get(addr(2), 101, hash(2));
    until("account fetch 2 in flight", || h.rpc.account_calls() == 2).await;

    gate.add_permits(64);

    assert_eq!(
        stale.recv_timeout(RECV).unwrap().unwrap().unwrap().balance,
        U256::from(10)
    );
    assert_eq!(
        fresh.recv_timeout(RECV).unwrap().unwrap().unwrap().balance,
        U256::from(20)
    );
    assert_eq!(h.rpc.account_calls(), 2);

    let refetch = h.get(addr(1), 101, hash(2)).recv_timeout(RECV).unwrap();
    assert_eq!(refetch.unwrap().unwrap().balance, U256::from(10));
    assert_eq!(
        h.rpc.account_calls(),
        3,
        "stale-epoch completion must not be cached"
    );

    let cached = h.get(addr(2), 101, hash(2)).recv_timeout(RECV).unwrap();
    assert_eq!(cached.unwrap().unwrap().balance, U256::from(20));
    assert_eq!(
        h.rpc.account_calls(),
        3,
        "current-epoch completion must be cached"
    );
}

/// The code-hash registration fires even for a completion the epoch guard
/// refuses to cache.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_registration_fires_even_when_epoch_is_stale() {
    let (rpc, gate) = MockRpc::gated();
    rpc.set_account(addr(1), Some(info(10, hash(0xaa))));
    rpc.set_account(addr(2), Some(info(20, hash(0xbb))));
    let mut h = AccountHarness::spawn(Arc::clone(&rpc), cfg(8, 5_000));

    let stale = h.get(addr(1), 100, hash(1));
    until("account fetch 1 in flight", || h.rpc.account_calls() == 1).await;
    let fresh = h.get(addr(2), 101, hash(2));
    until("account fetch 2 in flight", || h.rpc.account_calls() == 2).await;
    gate.add_permits(64);
    stale.recv_timeout(RECV).unwrap().unwrap();
    fresh.recv_timeout(RECV).unwrap().unwrap();

    let mut seen = Vec::new();
    while let Ok(reg) = h.regs.try_recv() {
        seen.push((reg.code_hash, reg.address));
    }
    seen.sort();
    assert_eq!(
        seen,
        vec![(hash(0xaa), addr(1)), (hash(0xbb), addr(2))],
        "code registration must fire outside the epoch guard"
    );
}

/// A negative account lookup is never memoized.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_none_result_is_not_cached() {
    let rpc = MockRpc::new();
    rpc.set_account(addr(1), None);
    let mut h = AccountHarness::spawn(Arc::clone(&rpc), cfg(4, 5_000));

    assert!(
        h.get(addr(1), 100, hash(1))
            .recv_timeout(RECV)
            .unwrap()
            .unwrap()
            .is_none()
    );
    assert_eq!(h.rpc.account_calls(), 1);

    assert!(
        h.get(addr(1), 100, hash(1))
            .recv_timeout(RECV)
            .unwrap()
            .unwrap()
            .is_none()
    );
    assert_eq!(
        h.rpc.account_calls(),
        2,
        "Ok(None) must not be cached, so the second lookup refetches"
    );

    assert!(
        h.regs.try_recv().is_err(),
        "Ok(None) carries no code hash, so no registration"
    );
}

/// `inflight` returns to zero after completions, so a later flush returns
/// without draining until `flush_timeout_ms`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn account_inflight_returns_to_zero_so_flush_does_not_wait() {
    let rpc = MockRpc::new();
    for n in 1..=8u64 {
        rpc.set_account(addr(n), Some(info(n, KECCAK_EMPTY)));
    }
    let h = AccountHarness::spawn(Arc::clone(&rpc), cfg(4, 5_000));

    let pending: Vec<_> = (1..=8u64).map(|n| h.get(addr(n), 100, hash(1))).collect();
    for rx in pending {
        rx.recv_timeout(RECV).unwrap().unwrap().unwrap();
    }
    assert_eq!(h.rpc.account_calls(), 8);

    let started = std::time::Instant::now();
    let cache = h.flush().await;
    let elapsed = started.elapsed();

    assert_eq!(cache.data.len(), 8);
    assert_eq!(cache.block_hash, hash(1));
    assert!(
        elapsed < Duration::from_secs(1),
        "flush drained for {elapsed:?}; inflight did not return to zero"
    );
}

/// Bytecode is content-addressed, so a completion that crosses a flush boundary
/// is still cached and no block change invalidates it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn code_completion_across_flush_boundary_is_cached() {
    let (rpc, gate) = MockRpc::gated();
    rpc.set_code(hash(0xcc), bytecode(1));
    let h = CodeHarness::spawn(Arc::clone(&rpc), cfg(4, 100));

    let pending = h.get(hash(0xcc), 100, Some(addr(1)));
    until("code fetch in flight", || h.rpc.code_calls() == 1).await;

    let flushed = h.flush().await;
    assert!(flushed.data.is_empty());

    gate.add_permits(64);
    assert_eq!(pending.recv_timeout(RECV).unwrap().unwrap(), bytecode(1));

    let again = h.get(hash(0xcc), 101, None).recv_timeout(RECV).unwrap();
    assert_eq!(again.unwrap(), bytecode(1));
    assert_eq!(
        h.rpc.code_calls(),
        1,
        "code completions are cached regardless of epoch/flush boundary"
    );
}

/// The registration channel back-fills the address hint the caller lacked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn code_registration_supplies_missing_address_hint() {
    let rpc = MockRpc::new();
    rpc.set_code(hash(0xcc), bytecode(2));
    let h = CodeHarness::spawn(Arc::clone(&rpc), cfg(4, 5_000));

    let err = h
        .get(hash(0xcc), 100, None)
        .recv_timeout(RECV)
        .unwrap()
        .unwrap_err();
    assert!(matches!(err, SimError::MissingAddressHint { .. }));
    assert_eq!(h.rpc.code_calls(), 1);

    h.reg_tx
        .send(CodeRegistration {
            code_hash: hash(0xcc),
            address: addr(7),
        })
        .unwrap();

    let ok = h.get(hash(0xcc), 100, None).recv_timeout(RECV).unwrap();
    assert_eq!(ok.unwrap(), bytecode(2));
    assert_eq!(h.rpc.code_calls(), 2);
    assert_eq!(
        *h.rpc.code_hints.lock().unwrap(),
        vec![None, Some(addr(7))],
        "registered address must be used as the hint"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_cache_hit_skips_rpc() {
    let rpc = MockRpc::new();
    rpc.set_storage(addr(1), U256::from(7), U256::from(42));
    let h = StorageHarness::spawn(Arc::clone(&rpc), cfg(4, 5_000));

    let first = h.get(addr(1), U256::from(7), 100, hash(1));
    assert_eq!(first.recv_timeout(RECV).unwrap().unwrap(), U256::from(42));
    assert_eq!(h.rpc.storage_calls(), 1);

    let second = h.get(addr(1), U256::from(7), 100, hash(1));
    assert_eq!(second.recv_timeout(RECV).unwrap().unwrap(), U256::from(42));
    assert_eq!(h.rpc.storage_calls(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_stale_epoch_completion_is_not_cached() {
    let (rpc, gate) = MockRpc::gated();
    rpc.set_storage(addr(1), U256::from(7), U256::from(42));
    rpc.set_storage(addr(2), U256::from(8), U256::from(43));
    let h = StorageHarness::spawn(Arc::clone(&rpc), cfg(8, 5_000));

    let stale = h.get(addr(1), U256::from(7), 100, hash(1));
    until("storage fetch 1 in flight", || h.rpc.storage_calls() == 1).await;
    let fresh = h.get(addr(2), U256::from(8), 101, hash(2));
    until("storage fetch 2 in flight", || h.rpc.storage_calls() == 2).await;
    gate.add_permits(64);

    assert_eq!(stale.recv_timeout(RECV).unwrap().unwrap(), U256::from(42));
    assert_eq!(fresh.recv_timeout(RECV).unwrap().unwrap(), U256::from(43));

    let refetch = h.get(addr(1), U256::from(7), 101, hash(2));
    assert_eq!(refetch.recv_timeout(RECV).unwrap().unwrap(), U256::from(42));
    assert_eq!(
        h.rpc.storage_calls(),
        3,
        "stale-epoch completion must not be cached"
    );

    let cached = h.get(addr(2), U256::from(8), 101, hash(2));
    assert_eq!(cached.recv_timeout(RECV).unwrap().unwrap(), U256::from(43));
    assert_eq!(h.rpc.storage_calls(), 3);
}

/// A completion abandoned by a flush must not be memoized when it lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_completion_abandoned_by_flush_is_not_cached_probed_by_second_flush_not_refetch() {
    let (rpc, gate) = MockRpc::gated();
    rpc.set_storage(addr(1), U256::from(7), U256::from(42));
    let h = StorageHarness::spawn(Arc::clone(&rpc), cfg(4, 100));

    let pending = h.get(addr(1), U256::from(7), 100, hash(1));
    until("storage fetch in flight", || h.rpc.storage_calls() == 1).await;

    let flushed = h.flush().await;
    assert!(
        flushed.data.is_empty(),
        "the fetch was still in flight, nothing to hand over"
    );

    gate.add_permits(64);
    assert_eq!(pending.recv_timeout(RECV).unwrap().unwrap(), U256::from(42));

    let after = h.flush().await;
    assert!(
        after.data.is_empty(),
        "a completion abandoned by a flush must not be cached, got {:?}",
        after.data
    );
}

/// The storage cache key is the (address, index) pair, so one slot is never
/// served another slot's value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn storage_cache_key_is_address_index_pair() {
    let rpc = MockRpc::new();
    rpc.set_storage(addr(1), U256::from(7), U256::from(42));
    rpc.set_storage(addr(1), U256::from(8), U256::from(43));
    let h = StorageHarness::spawn(Arc::clone(&rpc), cfg(4, 5_000));

    let slot7 = h.get(addr(1), U256::from(7), 100, hash(1));
    assert_eq!(slot7.recv_timeout(RECV).unwrap().unwrap(), U256::from(42));
    assert_eq!(h.rpc.storage_calls(), 1);

    let slot8 = h.get(addr(1), U256::from(8), 100, hash(1));
    assert_eq!(
        slot8.recv_timeout(RECV).unwrap().unwrap(),
        U256::from(43),
        "slot 8 must not be served slot 7's value"
    );
    assert_eq!(
        h.rpc.storage_calls(),
        2,
        "a different index under the same address must miss the cache"
    );

    let again7 = h.get(addr(1), U256::from(7), 100, hash(1));
    assert_eq!(again7.recv_timeout(RECV).unwrap().unwrap(), U256::from(42));
    let again8 = h.get(addr(1), U256::from(8), 100, hash(1));
    assert_eq!(again8.recv_timeout(RECV).unwrap().unwrap(), U256::from(43));
    assert_eq!(
        h.rpc.storage_calls(),
        2,
        "both slots must be cached under their own keys"
    );

    let cache = h.flush().await;
    assert_eq!(cache.block_hash, hash(1));
    assert_eq!(
        cache.data.get(&(addr(1), U256::from(7))),
        Some(&U256::from(42))
    );
    assert_eq!(
        cache.data.get(&(addr(1), U256::from(8))),
        Some(&U256::from(43))
    );
    assert_eq!(cache.data.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn block_hash_cache_hit_and_flush_returns_tagged_cache() {
    let rpc = MockRpc::new();
    rpc.set_block_hash(99, hash(0x99));
    let h = BlockHashHarness::spawn(Arc::clone(&rpc), cfg(4, 5_000));

    assert_eq!(
        h.get(99, hash(1)).recv_timeout(RECV).unwrap().unwrap(),
        hash(0x99)
    );
    assert_eq!(h.rpc.block_hash_calls(), 1);

    assert_eq!(
        h.get(99, hash(1)).recv_timeout(RECV).unwrap().unwrap(),
        hash(0x99)
    );
    assert_eq!(h.rpc.block_hash_calls(), 1);

    let started = std::time::Instant::now();
    let cache = h.flush().await;
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(cache.block_hash, hash(1));
    assert_eq!(cache.data.get(&99), Some(&hash(0x99)));
}

/// A tag change clears the cache outright, independent of the epoch guard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn block_hash_tag_change_clears_cache() {
    let rpc = MockRpc::new();
    rpc.set_block_hash(99, hash(0x99));
    let h = BlockHashHarness::spawn(Arc::clone(&rpc), cfg(4, 5_000));

    h.get(99, hash(1)).recv_timeout(RECV).unwrap().unwrap();
    assert_eq!(h.rpc.block_hash_calls(), 1);

    h.get(99, hash(2)).recv_timeout(RECV).unwrap().unwrap();
    assert_eq!(h.rpc.block_hash_calls(), 2);

    let cache = h.flush().await;
    assert_eq!(cache.block_hash, hash(2));
}
