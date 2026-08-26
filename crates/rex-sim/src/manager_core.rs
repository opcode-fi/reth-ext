use std::convert::Infallible;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

use hashbrown::HashMap;
use reth_revm::revm::primitives::B256;
use reth_tracing::tracing::warn;
use tokio::sync::{Semaphore, mpsc, oneshot};

use crate::error::SimError;
use crate::fetch::{FetchConfig, FetchMessage};
use crate::rpc::RpcClient;

const COMPLETION_CHANNEL_DEPTH: usize = 4096;

struct FetchDone<K, Resp> {
    key: K,
    result: Result<Resp, SimError>,
    /// Generation the request was issued under.
    epoch: u64,
    respond_to: crossbeam_channel::Sender<Result<Resp, SimError>>,
}

pub trait FetchSpec: Send + 'static {
    type Req: Send + 'static;
    type Key: Eq + Hash + Copy + Send + 'static;
    type Val: Clone + Send + 'static;
    type Resp: Send + 'static;
    type Cache: Send + 'static;
    type Ctx: Send + 'static;
    type Side: Send + 'static;

    const LABEL: &'static str;

    const EPOCH_GUARDED: bool = true;

    fn permits(fetch: &FetchConfig) -> usize;

    fn tag(req: &Self::Req) -> Option<B256>;

    fn key(req: &Self::Req) -> Self::Key;

    fn hit(value: &Self::Val) -> Self::Resp;

    fn store(resp: &Self::Resp) -> Option<Self::Val>;

    fn on_success(_ctx: &mut Self::Ctx, _key: &Self::Key, _resp: &Self::Resp) {}

    fn on_side(_ctx: &mut Self::Ctx, _msg: Self::Side) {}

    fn into_cache(tag: B256, data: HashMap<Self::Key, Self::Val>) -> Self::Cache;

    fn fetch(
        rpc: &RpcClient,
        ctx: &Self::Ctx,
        req: &Self::Req,
    ) -> impl Future<Output = Result<Self::Resp, SimError>> + Send + 'static;
}

pub struct SideChannel<S: FetchSpec>(mpsc::UnboundedReceiver<S::Side>);

impl<S: FetchSpec> SideChannel<S> {
    pub fn new(side_rx: mpsc::UnboundedReceiver<S::Side>) -> Self {
        Self(side_rx)
    }
}

impl<S: FetchSpec<Side = Infallible>> SideChannel<S> {
    pub fn none() -> Self {
        let (_closed, side_rx) = mpsc::unbounded_channel();
        Self(side_rx)
    }
}

pub struct FetchManager<S: FetchSpec> {
    fetch_rx: mpsc::UnboundedReceiver<FetchMessage<S::Req, S::Resp>>,
    flush_rx: mpsc::Receiver<oneshot::Sender<S::Cache>>,
    side_rx: mpsc::UnboundedReceiver<S::Side>,
    ctx: S::Ctx,
    rpc: RpcClient,
    cache_hash: B256,
    cache: HashMap<S::Key, S::Val>,
    permits: Arc<Semaphore>,
    completion_tx: mpsc::Sender<FetchDone<S::Key, S::Resp>>,
    completion_rx: mpsc::Receiver<FetchDone<S::Key, S::Resp>>,
    flush_timeout: Duration,
    epoch: u64,
    inflight: usize,
}

impl<S: FetchSpec> FetchManager<S> {
    pub fn new(
        fetch_rx: mpsc::UnboundedReceiver<FetchMessage<S::Req, S::Resp>>,
        flush_rx: mpsc::Receiver<oneshot::Sender<S::Cache>>,
        side: SideChannel<S>,
        ctx: S::Ctx,
        rpc: RpcClient,
        fetch: FetchConfig,
    ) -> Self {
        let (completion_tx, completion_rx) = mpsc::channel(COMPLETION_CHANNEL_DEPTH);
        Self {
            fetch_rx,
            flush_rx,
            side_rx: side.0,
            ctx,
            rpc,
            cache_hash: B256::ZERO,
            cache: HashMap::new(),
            permits: Arc::new(Semaphore::new(S::permits(&fetch))),
            completion_tx,
            completion_rx,
            flush_timeout: Duration::from_millis(fetch.flush_timeout_ms),
            epoch: 0,
            inflight: 0,
        }
    }

    fn drain_side(&mut self) {
        while let Ok(msg) = self.side_rx.try_recv() {
            S::on_side(&mut self.ctx, msg);
        }
    }

    fn apply_completion(&mut self, done: FetchDone<S::Key, S::Resp>) {
        self.inflight -= 1;
        let FetchDone {
            key,
            result,
            epoch,
            respond_to,
        } = done;

        if let Ok(resp) = &result {
            S::on_success(&mut self.ctx, &key, resp);
            if (!S::EPOCH_GUARDED || epoch == self.epoch)
                && let Some(value) = S::store(resp)
            {
                self.cache.insert(key, value);
            }
        }

        let _ = respond_to.send(result);
    }

    pub async fn run(mut self) {
        loop {
            tokio::select! {
                Some(tx) = self.flush_rx.recv() => {
                    let deadline = tokio::time::Instant::now() + self.flush_timeout;
                    while self.inflight > 0 {
                        match tokio::time::timeout_at(deadline, self.completion_rx.recv()).await {
                            Ok(Some(done)) => self.apply_completion(done),
                            Ok(None) => break,
                            Err(_) => {
                                warn!(target: "rex-sim", inflight = self.inflight, "{} flush drain timed out, abandoning in-flight fetches", S::LABEL);
                                break;
                            }
                        }
                    }
                    let cache = S::into_cache(
                        std::mem::take(&mut self.cache_hash),
                        std::mem::take(&mut self.cache),
                    );
                    let _ = tx.send(cache);

                    self.epoch += 1;
                }
                Some(done) = self.completion_rx.recv() => {
                    self.apply_completion(done);
                }
                Some(msg) = self.side_rx.recv() => {
                    S::on_side(&mut self.ctx, msg);
                    self.drain_side();
                }
                Some(mut msg) = self.fetch_rx.recv() => {
                    self.drain_side();
                    loop {
                        let req = msg.request;
                        if let Some(tag) = S::tag(&req)
                            && self.cache_hash != tag
                        {
                            self.cache.clear();
                            self.cache_hash = tag;
                            self.epoch += 1;
                        }
                        let key = S::key(&req);
                        match self.cache.get(&key) {

                            Some(value) => {
                                let _ = msg.respond_to.send(Ok(S::hit(value)));
                            }

                            None => {
                                let permit = Arc::clone(&self.permits)
                                    .acquire_owned()
                                    .await
                                    .expect("fetch semaphore never closed");
                                let fetch = S::fetch(&self.rpc, &self.ctx, &req);
                                let completion_tx = self.completion_tx.clone();
                                let epoch = self.epoch;
                                let respond_to = msg.respond_to;
                                self.inflight += 1;
                                tokio::spawn(async move {
                                    let result = fetch.await;
                                    drop(permit);
                                    let _ = completion_tx
                                        .send(FetchDone { key, result, epoch, respond_to })
                                        .await;
                                });
                            }
                        }
                        match self.fetch_rx.try_recv() { Ok(next) => msg = next, Err(_) => break }
                    }
                }
            }
        }
    }
}
