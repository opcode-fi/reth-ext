pub mod artifacts;
pub mod changeset;
pub mod error;
pub mod fetch;
pub mod manager_core;
#[cfg(test)]
mod manager_tests;
pub mod managers;
pub mod rpc;
pub mod state;
pub mod transitions;
pub mod validation;

use std::sync::Arc;
use std::time::Duration;

use alloy_primitives::Log;
use rex_cons::ExExNotification;
use tokio::sync::{mpsc, oneshot, watch};

pub use error::{FetchKind, SimError};
pub use fetch::FetchConfig;
pub use state::{FetchChannels, StateSnapshot, StaticState};

fn clamp_concurrency(n: usize) -> usize {
    n.clamp(1, 1024)
}

pub type LogSink = crossbeam_channel::Sender<Vec<Log>>;

pub type StateUpdateReceiver = watch::Receiver<StateUpdate>;

pub type StateUpdateSender = watch::Sender<StateUpdate>;

#[derive(Clone, Debug, Default)]
pub struct StateUpdate {
    pub block: u64,
    pub block_hash: reth_revm::revm::primitives::B256,
    pub generation: u64,
}

use changeset::{FlushHandles, run_changeset_manager};
use fetch::{
    AccountFetchMessage, BlockHashFetchMessage, CodeFetchMessage, CodeRegistration,
    StorageFetchMessage,
};
use managers::{
    AccountFetchManager, BlockHashFetchManager, CodeFetchManager, CodeHints, SideChannel,
    StorageFetchManager,
};
use rpc::RpcClient;

pub type ReadySignal = oneshot::Receiver<u64>;

pub struct RexSimBuilder {
    rpc_endpoint: String,
    log_sink: Option<LogSink>,
    fetch: Option<FetchConfig>,
}

impl Default for RexSimBuilder {
    fn default() -> Self {
        Self {
            rpc_endpoint: "http://127.0.0.1:8545".to_string(),
            log_sink: None,
            fetch: None,
        }
    }
}

impl RexSimBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn rpc_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.rpc_endpoint = endpoint.into();
        self
    }

    pub fn log_sink(mut self, sink: LogSink) -> Self {
        self.log_sink = Some(sink);
        self
    }

    pub fn fetch(mut self, fetch: FetchConfig) -> Self {
        self.fetch = Some(fetch);
        self
    }

    pub fn build(self) -> RexSim {
        let (account_tx, account_rx) = mpsc::unbounded_channel();
        let (code_tx, code_rx) = mpsc::unbounded_channel();
        let (storage_tx, storage_rx) = mpsc::unbounded_channel();
        let (block_hash_tx, block_hash_rx) = mpsc::unbounded_channel();

        let channels = FetchChannels {
            account: account_tx,
            code: code_tx,
            storage: storage_tx,
            block_hash: block_hash_tx,
        };

        let mut fetch = self.fetch.unwrap_or_default();
        fetch.account = clamp_concurrency(fetch.account);
        fetch.code = clamp_concurrency(fetch.code);
        fetch.storage = clamp_concurrency(fetch.storage);
        fetch.block_hash = clamp_concurrency(fetch.block_hash);

        assert!(
            fetch.wait_timeout_ms > fetch.rpc_timeout_ms,
            "[fetch].wait_timeout_ms ({}) must exceed [fetch].rpc_timeout_ms ({})",
            fetch.wait_timeout_ms,
            fetch.rpc_timeout_ms
        );

        let state = Arc::new(StaticState::new(StateSnapshot::default(), channels, fetch));

        RexSim {
            state,
            rpc_endpoint: self.rpc_endpoint,
            log_sink: self.log_sink,
            fetch,
            account_fetch_rx: Some(account_rx),
            code_fetch_rx: Some(code_rx),
            storage_fetch_rx: Some(storage_rx),
            block_hash_fetch_rx: Some(block_hash_rx),
        }
    }
}

pub struct RexSim {
    state: Arc<StaticState>,
    rpc_endpoint: String,
    log_sink: Option<LogSink>,
    fetch: FetchConfig,
    account_fetch_rx: Option<mpsc::UnboundedReceiver<AccountFetchMessage>>,
    code_fetch_rx: Option<mpsc::UnboundedReceiver<CodeFetchMessage>>,
    storage_fetch_rx: Option<mpsc::UnboundedReceiver<StorageFetchMessage>>,
    block_hash_fetch_rx: Option<mpsc::UnboundedReceiver<BlockHashFetchMessage>>,
}

impl RexSim {
    pub fn builder() -> RexSimBuilder {
        RexSimBuilder::new()
    }

    pub fn block(&self) -> u64 {
        self.state.block()
    }

    pub fn spawn(
        &mut self,
        canon_rx: mpsc::Receiver<ExExNotification>,
    ) -> (Arc<StaticState>, ReadySignal, StateUpdateReceiver) {
        let rpc = RpcClient::new(
            &self.rpc_endpoint,
            Duration::from_millis(self.fetch.rpc_timeout_ms),
        );

        let account_fetch_rx = self.account_fetch_rx.take().expect("spawn called twice");
        let code_fetch_rx = self.code_fetch_rx.take().expect("spawn called twice");
        let storage_fetch_rx = self.storage_fetch_rx.take().expect("spawn called twice");
        let block_hash_fetch_rx = self.block_hash_fetch_rx.take().expect("spawn called twice");

        let (account_flush_tx, account_flush_rx) = mpsc::channel(1);
        let (code_flush_tx, code_flush_rx) = mpsc::channel(1);
        let (storage_flush_tx, storage_flush_rx) = mpsc::channel(1);
        let (block_hash_flush_tx, block_hash_flush_rx) = mpsc::channel(1);

        let (code_reg_tx, code_reg_rx) = mpsc::unbounded_channel::<CodeRegistration>();

        let (ready_tx, ready_rx) = oneshot::channel();

        let (update_tx, update_rx) = watch::channel(StateUpdate::default());

        let fetch = self.fetch;
        let account_manager = AccountFetchManager::new(
            account_fetch_rx,
            account_flush_rx,
            SideChannel::none(),
            code_reg_tx,
            rpc.clone(),
            fetch,
        );
        tokio::spawn(account_manager.run());

        let code_manager = CodeFetchManager::new(
            code_fetch_rx,
            code_flush_rx,
            SideChannel::new(code_reg_rx),
            CodeHints::default(),
            rpc.clone(),
            fetch,
        );
        tokio::spawn(code_manager.run());

        let storage_manager = StorageFetchManager::new(
            storage_fetch_rx,
            storage_flush_rx,
            SideChannel::none(),
            (),
            rpc.clone(),
            fetch,
        );
        tokio::spawn(storage_manager.run());

        let block_hash_manager = BlockHashFetchManager::new(
            block_hash_fetch_rx,
            block_hash_flush_rx,
            SideChannel::none(),
            (),
            rpc,
            fetch,
        );
        tokio::spawn(block_hash_manager.run());

        let flush_handles = FlushHandles {
            account: account_flush_tx,
            code: code_flush_tx,
            storage: storage_flush_tx,
            block_hash: block_hash_flush_tx,
        };

        let state = Arc::clone(&self.state);
        let state_for_manager = Arc::clone(&self.state);
        let log_sink = self.log_sink.take();

        tokio::spawn(async move {
            run_changeset_manager(
                canon_rx,
                state_for_manager,
                flush_handles,
                Some(ready_tx),
                Some(update_tx),
                log_sink,
            )
            .await;
        });

        (state, ready_rx, update_rx)
    }
}
