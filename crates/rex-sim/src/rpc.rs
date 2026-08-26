use std::time::Duration;

use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types::BlockNumberOrTag;
use reth_revm::revm::primitives::{Address, B256, Bytes, KECCAK_EMPTY, U256, keccak256};
use reth_revm::revm::state::{AccountInfo, Bytecode};
use reth_tracing::tracing::{debug, warn};

use crate::error::SimError;

type FilledProvider = alloy_provider::fillers::FillProvider<
    alloy_provider::fillers::JoinFill<
        alloy_provider::Identity,
        alloy_provider::fillers::JoinFill<
            alloy_provider::fillers::GasFiller,
            alloy_provider::fillers::JoinFill<
                alloy_provider::fillers::BlobGasFiller,
                alloy_provider::fillers::JoinFill<
                    alloy_provider::fillers::NonceFiller,
                    alloy_provider::fillers::ChainIdFiller,
                >,
            >,
        >,
    >,
    alloy_provider::RootProvider,
>;

#[derive(Clone)]
pub struct RpcClient {
    provider: FilledProvider,
    #[cfg(test)]
    mock: Option<std::sync::Arc<mock::MockRpc>>,
}

impl RpcClient {
    pub fn new(endpoint: impl Into<String>, timeout: Duration) -> Self {
        let url: url::Url = endpoint.into().parse().expect("invalid RPC URL");
        let provider = ProviderBuilder::new().with_reqwest(url, |builder| {
            builder
                .timeout(timeout)
                .build()
                .expect("http client with timeout")
        });

        Self {
            provider,
            #[cfg(test)]
            mock: None,
        }
    }

    #[cfg(test)]
    pub fn mock(mock: std::sync::Arc<mock::MockRpc>) -> Self {
        Self {
            mock: Some(mock),
            ..Self::new("http://127.0.0.1:1", Duration::from_millis(1))
        }
    }

    pub async fn get_account(
        &self,
        address: Address,
        block: u64,
    ) -> Result<Option<AccountInfo>, SimError> {
        #[cfg(test)]
        if let Some(m) = &self.mock {
            return m.get_account(address, block).await;
        }
        let provider = &self.provider;

        let block_id = BlockNumberOrTag::Number(block);

        let (balance, nonce, code) = tokio::try_join!(
            async {
                provider
                    .get_balance(address)
                    .block_id(block_id.into())
                    .await
                    .map_err(|e| {
                        warn!(target: "rex-sim", %address, block, error = %e, "RPC error fetching balance");
                        SimError::Rpc(e)
                    })
            },
            async {
                provider
                    .get_transaction_count(address)
                    .block_id(block_id.into())
                    .await
                    .map_err(|e| {
                        warn!(target: "rex-sim", %address, block, error = %e, "RPC error fetching nonce");
                        SimError::Rpc(e)
                    })
            },
            async {
                provider
                    .get_code_at(address)
                    .block_id(block_id.into())
                    .await
                    .map_err(|e| {
                        warn!(target: "rex-sim", %address, block, error = %e, "RPC error fetching code");
                        SimError::Rpc(e)
                    })
            },
        )?;

        let code_hash = if code.is_empty() {
            KECCAK_EMPTY
        } else {
            keccak256(&code)
        };

        let info = AccountInfo {
            balance,
            nonce,
            code_hash,
            account_id: None,
            code: None,
        };

        debug!(
            target: "rex-sim",
            %address,
            block,
            balance = %info.balance,
            nonce = info.nonce,
            "Fetched account"
        );

        Ok(Some(info))
    }

    pub async fn get_code_by_hash(
        &self,
        code_hash: B256,
        address: Option<Address>,
        block: u64,
    ) -> Result<Bytecode, SimError> {
        #[cfg(test)]
        if let Some(m) = &self.mock {
            return m.get_code_by_hash(code_hash, address, block).await;
        }
        let provider = &self.provider;

        let address = match address {
            Some(addr) => addr,
            None => {
                warn!(target: "rex-sim", %code_hash, "get_code_by_hash called without address hint");
                return Err(SimError::MissingAddressHint { code_hash });
            }
        };

        let block_id = BlockNumberOrTag::Number(block);

        let code = provider
            .get_code_at(address)
            .block_id(block_id.into())
            .await
            .map_err(|e| {
                warn!(target: "rex-sim", %address, block, error = %e, "RPC error fetching code");
                SimError::Rpc(e)
            })?;

        let actual_hash = if code.is_empty() {
            KECCAK_EMPTY
        } else {
            keccak256(&code)
        };

        if actual_hash != code_hash {
            warn!(
                target: "rex-sim",
                %address,
                expected = %code_hash,
                actual = %actual_hash,
                "Code hash mismatch"
            );
            return Err(SimError::CodeHashMismatch {
                address,
                expected: code_hash,
                actual: actual_hash,
            });
        }

        debug!(
            target: "rex-sim",
            %code_hash,
            %address,
            block,
            code_len = code.len(),
            "Fetched code by hash"
        );

        Ok(Bytecode::new_raw(Bytes::from(code.to_vec())))
    }

    pub async fn get_storage(
        &self,
        address: Address,
        index: U256,
        block: u64,
    ) -> Result<U256, SimError> {
        #[cfg(test)]
        if let Some(m) = &self.mock {
            return m.get_storage(address, index, block).await;
        }
        let provider = &self.provider;

        let block_id = BlockNumberOrTag::Number(block);

        let value = provider
            .get_storage_at(address, index)
            .block_id(block_id.into())
            .await
            .map_err(|e| {
                warn!(target: "rex-sim", %address, %index, block, error = %e, "RPC error fetching storage");
                SimError::Rpc(e)
            })?;

        debug!(
            target: "rex-sim",
            %address,
            %index,
            block,
            %value,
            "Fetched storage"
        );

        Ok(value)
    }

    pub async fn get_block_hash(&self, number: u64) -> Result<B256, SimError> {
        #[cfg(test)]
        if let Some(m) = &self.mock {
            return m.get_block_hash(number).await;
        }
        let provider = &self.provider;

        let block_id = BlockNumberOrTag::Number(number);

        let block = provider.get_block_by_number(block_id).await.map_err(|e| {
            warn!(target: "rex-sim", block = number, error = %e, "RPC error fetching block");
            SimError::Rpc(e)
        })?;

        match block {
            Some(b) => {
                let hash = b.header.hash;
                debug!(target: "rex-sim", block = number, %hash, "Fetched block hash");
                Ok(hash)
            }
            None => {
                warn!(target: "rex-sim", block = number, "Block not found");
                Err(SimError::BlockNotFound(number))
            }
        }
    }

    pub async fn get_block_number(&self) -> Result<u64, SimError> {
        #[cfg(test)]
        if self.mock.is_some() {
            return Err(SimError::BlockNotFound(0));
        }
        let provider = &self.provider;

        let number = provider.get_block_number().await.map_err(|e| {
            warn!(target: "rex-sim", error = %e, "RPC error fetching block number");
            SimError::Rpc(e)
        })?;

        Ok(number)
    }
}

#[cfg(test)]
pub mod mock {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Semaphore;

    #[derive(Default)]
    pub struct MockRpc {
        pub accounts: Mutex<HashMap<Address, Option<AccountInfo>>>,
        pub codes: Mutex<HashMap<B256, Bytecode>>,
        pub storage: Mutex<HashMap<(Address, U256), U256>>,
        pub block_hashes: Mutex<HashMap<u64, B256>>,
        /// Address hints `get_code_by_hash` was invoked with, in call order.
        pub code_hints: Mutex<Vec<Option<Address>>>,
        pub account_calls: AtomicUsize,
        pub code_calls: AtomicUsize,
        pub storage_calls: AtomicUsize,
        pub block_hash_calls: AtomicUsize,
        gate: Option<Arc<Semaphore>>,
    }

    impl MockRpc {
        pub fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        pub fn gated() -> (Arc<Self>, Arc<Semaphore>) {
            let gate = Arc::new(Semaphore::new(0));
            let this = Arc::new(Self {
                gate: Some(Arc::clone(&gate)),
                ..Self::default()
            });
            (this, gate)
        }

        async fn wait(&self) {
            if let Some(gate) = &self.gate {
                gate.acquire().await.expect("gate never closed").forget();
            }
        }

        pub fn account_calls(&self) -> usize {
            self.account_calls.load(Ordering::SeqCst)
        }

        pub fn code_calls(&self) -> usize {
            self.code_calls.load(Ordering::SeqCst)
        }

        pub fn storage_calls(&self) -> usize {
            self.storage_calls.load(Ordering::SeqCst)
        }

        pub fn block_hash_calls(&self) -> usize {
            self.block_hash_calls.load(Ordering::SeqCst)
        }

        pub fn set_account(&self, address: Address, info: Option<AccountInfo>) {
            self.accounts.lock().unwrap().insert(address, info);
        }

        pub fn set_code(&self, code_hash: B256, code: Bytecode) {
            self.codes.lock().unwrap().insert(code_hash, code);
        }

        pub fn set_storage(&self, address: Address, index: U256, value: U256) {
            self.storage.lock().unwrap().insert((address, index), value);
        }

        pub fn set_block_hash(&self, number: u64, hash: B256) {
            self.block_hashes.lock().unwrap().insert(number, hash);
        }

        pub(super) async fn get_account(
            &self,
            address: Address,
            block: u64,
        ) -> Result<Option<AccountInfo>, SimError> {
            self.account_calls.fetch_add(1, Ordering::SeqCst);
            self.wait().await;
            let hit = self.accounts.lock().unwrap().get(&address).cloned();
            hit.ok_or(SimError::BlockNotFound(block))
        }

        pub(super) async fn get_code_by_hash(
            &self,
            code_hash: B256,
            address: Option<Address>,
            _block: u64,
        ) -> Result<Bytecode, SimError> {
            self.code_calls.fetch_add(1, Ordering::SeqCst);
            self.code_hints.lock().unwrap().push(address);
            self.wait().await;
            if address.is_none() {
                return Err(SimError::MissingAddressHint { code_hash });
            }
            let hit = self.codes.lock().unwrap().get(&code_hash).cloned();
            hit.ok_or(SimError::MissingAddressHint { code_hash })
        }

        pub(super) async fn get_storage(
            &self,
            address: Address,
            index: U256,
            block: u64,
        ) -> Result<U256, SimError> {
            self.storage_calls.fetch_add(1, Ordering::SeqCst);
            self.wait().await;
            let hit = self.storage.lock().unwrap().get(&(address, index)).copied();
            hit.ok_or(SimError::BlockNotFound(block))
        }

        pub(super) async fn get_block_hash(&self, number: u64) -> Result<B256, SimError> {
            self.block_hash_calls.fetch_add(1, Ordering::SeqCst);
            self.wait().await;
            let hit = self.block_hashes.lock().unwrap().get(&number).copied();
            hit.ok_or(SimError::BlockNotFound(number))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires running node"]
    async fn test_get_block_number() {
        let client = RpcClient::new("http://127.0.0.1:8545", Duration::from_secs(10));
        let number = client.get_block_number().await.unwrap();
        assert!(number > 0);
    }

    #[tokio::test]
    #[ignore = "requires running node"]
    async fn test_get_account() {
        let client = RpcClient::new("http://127.0.0.1:8545", Duration::from_secs(10));
        let block = client.get_block_number().await.unwrap();

        let weth: Address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
            .parse()
            .unwrap();
        let info = client.get_account(weth, block).await.unwrap();
        assert!(info.is_some());
    }

    #[tokio::test]
    #[ignore = "requires running node"]
    async fn test_get_storage() {
        let client = RpcClient::new("http://127.0.0.1:8545", Duration::from_secs(10));
        let block = client.get_block_number().await.unwrap();

        let weth: Address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
            .parse()
            .unwrap();
        let slot = U256::ZERO;
        let _value = client.get_storage(weth, slot, block).await.unwrap();
    }
}
