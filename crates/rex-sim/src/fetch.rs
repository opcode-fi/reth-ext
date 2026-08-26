use crossbeam_channel::Sender;
use reth_revm::revm::primitives::{Address, B256, U256};
use reth_revm::revm::state::{AccountInfo, Bytecode};

use crate::error::SimError;

const fn default_rpc_timeout_ms() -> u64 {
    10_000
}

const fn default_wait_timeout_ms() -> u64 {
    30_000
}

const fn default_flush_timeout_ms() -> u64 {
    15_000
}

#[derive(Debug, Clone, Copy, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FetchConfig {
    /// In-flight account RPCs allowed at once.
    pub account: usize,
    /// In-flight code RPCs allowed at once.
    pub code: usize,
    /// In-flight storage RPCs allowed at once.
    pub storage: usize,
    /// In-flight block-hash RPCs allowed at once.
    pub block_hash: usize,
    /// Wall-clock bound on a single HTTP RPC round trip.
    #[serde(default = "default_rpc_timeout_ms")]
    pub rpc_timeout_ms: u64,
    /// Wall-clock bound on the sync revm bridge waiting for a fetch manager reply.
    #[serde(default = "default_wait_timeout_ms")]
    pub wait_timeout_ms: u64,
    /// Wall-clock bound on a fetch manager draining in-flight RPCs during a changeset flush.
    #[serde(default = "default_flush_timeout_ms")]
    pub flush_timeout_ms: u64,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self {
            account: 100,
            code: 100,
            storage: 100,
            block_hash: 100,
            rpc_timeout_ms: default_rpc_timeout_ms(),
            wait_timeout_ms: default_wait_timeout_ms(),
            flush_timeout_ms: default_flush_timeout_ms(),
        }
    }
}

impl FetchConfig {
    pub fn uniform(n: usize) -> Self {
        Self {
            account: n,
            code: n,
            storage: n,
            block_hash: n,
            ..Self::default()
        }
    }
}

pub struct FetchMessage<Req, Resp> {
    pub request: Req,
    pub respond_to: Sender<Result<Resp, SimError>>,
}

#[derive(Debug, Clone)]
pub struct AccountRequest {
    pub address: Address,
    pub block: u64,
    pub block_hash: B256,
}

pub type AccountFetchMessage = FetchMessage<AccountRequest, Option<AccountInfo>>;

#[derive(Debug, Clone)]
pub struct CodeRequest {
    pub code_hash: B256,
    pub block: u64,
    pub address_hint: Option<Address>,
}

pub type CodeFetchMessage = FetchMessage<CodeRequest, Bytecode>;

pub struct CodeRegistration {
    pub code_hash: B256,
    pub address: Address,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct StorageRequest {
    pub address: Address,
    pub index: U256,
    pub block: u64,
    pub block_hash: B256,
}

pub type StorageFetchMessage = FetchMessage<StorageRequest, U256>;

#[derive(Debug, Clone)]
pub struct BlockHashRequest {
    pub number: u64,
    pub at_block_hash: B256,
}

pub type BlockHashFetchMessage = FetchMessage<BlockHashRequest, B256>;

use hashbrown::HashMap;

#[derive(Default)]
pub struct TaggedCache<K, V> {
    pub block_hash: B256,
    pub data: HashMap<K, V>,
}

impl<K, V> TaggedCache<K, V> {
    pub fn is_valid_for(&self, block_hash: B256) -> bool {
        self.data.is_empty() || self.block_hash == block_hash
    }
}

pub type AccountCache = TaggedCache<Address, AccountInfo>;

pub type StorageCache = TaggedCache<(Address, U256), U256>;

pub type BlockHashCache = TaggedCache<u64, B256>;

#[derive(Default)]
pub struct CodeCache {
    pub data: HashMap<B256, Bytecode>,
}
