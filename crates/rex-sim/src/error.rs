use std::time::Duration;

use alloy_primitives::{Address, B256};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchKind {
    Account,
    Code,
    Storage,
    BlockHash,
}

#[derive(Debug, Error)]
pub enum SimError {
    #[error("fetcher channel closed")]
    FetcherClosed,
    #[error("fetch timeout {kind:?} after {elapsed:?}")]
    FetchTimeout { kind: FetchKind, elapsed: Duration },
    #[error("state is stale (waiting for valid chain state)")]
    StaleState,
    #[error("cannot fetch code by hash {code_hash} without an address hint")]
    MissingAddressHint { code_hash: B256 },
    #[error("code hash mismatch at {address}: expected {expected}, got {actual}")]
    CodeHashMismatch {
        address: Address,
        expected: B256,
        actual: B256,
    },
    #[error("block {0} not found")]
    BlockNotFound(u64),
    #[error("rpc transport error")]
    Rpc(#[from] alloy_provider::transport::TransportError),
}

impl reth_revm::revm::database_interface::DBErrorMarker for SimError {}
