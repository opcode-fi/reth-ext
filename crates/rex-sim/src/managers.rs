use std::convert::Infallible;
use std::future::Future;

use hashbrown::HashMap;

use reth_revm::revm::primitives::{Address, B256, U256};
use reth_revm::revm::state::{AccountInfo, Bytecode};
use tokio::sync::mpsc;

use crate::error::SimError;
use crate::fetch::*;
use crate::manager_core::{FetchManager, FetchSpec};
use crate::rpc::RpcClient;

pub use crate::manager_core::SideChannel;

pub type AccountFetchManager = FetchManager<AccountSpec>;
pub type CodeFetchManager = FetchManager<CodeSpec>;
pub type StorageFetchManager = FetchManager<StorageSpec>;
pub type BlockHashFetchManager = FetchManager<BlockHashSpec>;

pub struct AccountSpec;

impl FetchSpec for AccountSpec {
    type Req = AccountRequest;
    type Key = Address;
    type Val = AccountInfo;
    type Resp = Option<AccountInfo>;
    type Cache = AccountCache;
    type Ctx = mpsc::UnboundedSender<CodeRegistration>;
    type Side = Infallible;

    const LABEL: &'static str = "Account";

    fn permits(fetch: &FetchConfig) -> usize {
        fetch.account
    }

    fn tag(req: &Self::Req) -> Option<B256> {
        Some(req.block_hash)
    }

    fn key(req: &Self::Req) -> Self::Key {
        req.address
    }

    fn hit(value: &Self::Val) -> Self::Resp {
        Some(value.clone())
    }

    fn store(resp: &Self::Resp) -> Option<Self::Val> {
        resp.clone()
    }

    fn on_success(ctx: &mut Self::Ctx, address: &Self::Key, resp: &Self::Resp) {
        if let Some(info) = resp {
            let _ = ctx.send(CodeRegistration {
                code_hash: info.code_hash,
                address: *address,
            });
        }
    }

    fn into_cache(tag: B256, data: HashMap<Self::Key, Self::Val>) -> Self::Cache {
        AccountCache {
            block_hash: tag,
            data,
        }
    }

    fn fetch(
        rpc: &RpcClient,
        _ctx: &Self::Ctx,
        req: &Self::Req,
    ) -> impl Future<Output = Result<Self::Resp, SimError>> + Send + 'static {
        let rpc = rpc.clone();
        let address = req.address;
        let block = req.block;
        async move { rpc.get_account(address, block).await }
    }
}

#[derive(Default)]
pub struct CodeHints {
    hash_to_address: HashMap<B256, Address>,
}

pub struct CodeSpec;

impl FetchSpec for CodeSpec {
    type Req = CodeRequest;
    type Key = B256;
    type Val = Bytecode;
    type Resp = Bytecode;
    type Cache = CodeCache;
    type Ctx = CodeHints;
    type Side = CodeRegistration;

    const LABEL: &'static str = "Code";

    const EPOCH_GUARDED: bool = false;

    fn permits(fetch: &FetchConfig) -> usize {
        fetch.code
    }

    fn tag(_req: &Self::Req) -> Option<B256> {
        None
    }

    fn key(req: &Self::Req) -> Self::Key {
        req.code_hash
    }

    fn hit(value: &Self::Val) -> Self::Resp {
        value.clone()
    }

    fn store(resp: &Self::Resp) -> Option<Self::Val> {
        Some(resp.clone())
    }

    fn on_side(ctx: &mut Self::Ctx, reg: Self::Side) {
        ctx.hash_to_address.insert(reg.code_hash, reg.address);
    }

    fn into_cache(_tag: B256, data: HashMap<Self::Key, Self::Val>) -> Self::Cache {
        CodeCache { data }
    }

    fn fetch(
        rpc: &RpcClient,
        ctx: &Self::Ctx,
        req: &Self::Req,
    ) -> impl Future<Output = Result<Self::Resp, SimError>> + Send + 'static {
        let rpc = rpc.clone();
        let code_hash = req.code_hash;
        let block = req.block;
        let address = req
            .address_hint
            .or_else(|| ctx.hash_to_address.get(&code_hash).copied());
        async move { rpc.get_code_by_hash(code_hash, address, block).await }
    }
}

pub struct StorageSpec;

impl FetchSpec for StorageSpec {
    type Req = StorageRequest;
    type Key = (Address, U256);
    type Val = U256;
    type Resp = U256;
    type Cache = StorageCache;
    type Ctx = ();
    type Side = Infallible;

    const LABEL: &'static str = "Storage";

    fn permits(fetch: &FetchConfig) -> usize {
        fetch.storage
    }

    fn tag(req: &Self::Req) -> Option<B256> {
        Some(req.block_hash)
    }

    fn key(req: &Self::Req) -> Self::Key {
        (req.address, req.index)
    }

    fn hit(value: &Self::Val) -> Self::Resp {
        *value
    }

    fn store(resp: &Self::Resp) -> Option<Self::Val> {
        Some(*resp)
    }

    fn into_cache(tag: B256, data: HashMap<Self::Key, Self::Val>) -> Self::Cache {
        StorageCache {
            block_hash: tag,
            data,
        }
    }

    fn fetch(
        rpc: &RpcClient,
        _ctx: &Self::Ctx,
        req: &Self::Req,
    ) -> impl Future<Output = Result<Self::Resp, SimError>> + Send + 'static {
        let rpc = rpc.clone();
        let address = req.address;
        let index = req.index;
        let block = req.block;
        async move { rpc.get_storage(address, index, block).await }
    }
}

pub struct BlockHashSpec;

impl FetchSpec for BlockHashSpec {
    type Req = BlockHashRequest;
    type Key = u64;
    type Val = B256;
    type Resp = B256;
    type Cache = BlockHashCache;
    type Ctx = ();
    type Side = Infallible;

    const LABEL: &'static str = "Block hash";

    fn permits(fetch: &FetchConfig) -> usize {
        fetch.block_hash
    }

    fn tag(req: &Self::Req) -> Option<B256> {
        Some(req.at_block_hash)
    }

    fn key(req: &Self::Req) -> Self::Key {
        req.number
    }

    fn hit(value: &Self::Val) -> Self::Resp {
        *value
    }

    fn store(resp: &Self::Resp) -> Option<Self::Val> {
        Some(*resp)
    }

    fn into_cache(tag: B256, data: HashMap<Self::Key, Self::Val>) -> Self::Cache {
        BlockHashCache {
            block_hash: tag,
            data,
        }
    }

    fn fetch(
        rpc: &RpcClient,
        _ctx: &Self::Ctx,
        req: &Self::Req,
    ) -> impl Future<Output = Result<Self::Resp, SimError>> + Send + 'static {
        let rpc = rpc.clone();
        let number = req.number;
        async move { rpc.get_block_hash(number).await }
    }
}
