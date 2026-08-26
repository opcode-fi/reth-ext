use std::future::Future;
use std::hash::Hash;

use alloy_primitives::{Address, Bytes};
use alloy_provider::Provider;
use alloy_sol_types::SolCall;
use alloy_transport::TransportError;
use hashbrown::HashMap;
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Debug, Error)]
pub enum CallError {
    #[error("RPC transport error")]
    Transport(#[from] TransportError),
    #[error("ABI decode error")]
    Decode(#[from] alloy_sol_types::Error),
}

pub trait ProviderCallExt: Provider {
    fn call_sol<C: SolCall + Send>(
        &self,
        to: Address,
        call: C,
    ) -> impl std::future::Future<Output = Result<C::Return, CallError>> + Send;

    fn call_raw(
        &self,
        to: Address,
        input: Vec<u8>,
    ) -> impl std::future::Future<Output = Result<Bytes, CallError>> + Send;
}

impl<P: Provider> ProviderCallExt for P {
    async fn call_sol<C: SolCall + Send>(
        &self,
        to: Address,
        call: C,
    ) -> Result<C::Return, CallError> {
        let result = self
            .call(
                alloy_rpc_types::TransactionRequest::default()
                    .to(to)
                    .input(call.abi_encode().into()),
            )
            .await?;

        let decoded = C::abi_decode_returns(&result)?;
        Ok(decoded)
    }

    async fn call_raw(&self, to: Address, input: Vec<u8>) -> Result<Bytes, CallError> {
        let result = self
            .call(
                alloy_rpc_types::TransactionRequest::default()
                    .to(to)
                    .input(input.into()),
            )
            .await?;
        Ok(result)
    }
}

pub async fn cached_fetch<K, V, E, Fut>(
    cache: &RwLock<HashMap<K, V>>,
    key: K,
    fetch: impl FnOnce() -> Fut,
) -> Result<V, E>
where
    K: Eq + Hash,
    V: Clone,
    Fut: Future<Output = Result<V, E>>,
{
    {
        let cache = cache.read().await;
        if let Some(v) = cache.get(&key) {
            return Ok(v.clone());
        }
    }
    let v = fetch().await?;
    cache.write().await.insert(key, v.clone());
    Ok(v)
}

pub async fn cached_discover<K, V, E, Fut>(
    cache: &RwLock<HashMap<K, V>>,
    keys: &[K],
    mut fetch: impl FnMut(K) -> Fut,
) -> Result<Vec<V>, E>
where
    K: Eq + Hash + Copy,
    V: Clone,
    Fut: Future<Output = Result<V, E>>,
{
    let mut out = Vec::new();
    for &key in keys {
        {
            let cache = cache.read().await;
            if cache.contains_key(&key) {
                continue;
            }
        }
        let v = fetch(key).await?;
        cache.write().await.insert(key, v.clone());
        out.push(v);
    }
    Ok(out)
}
