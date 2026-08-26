use hashbrown::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::Arc;

use alloy_consensus::Transaction;
use alloy_consensus::{TxReceipt, Typed2718, transaction::Recovered};
use alloy_eips::eip4895::Withdrawals;
use alloy_provider::{Provider, ProviderBuilder};
use alloy_rpc_types::{
    AccessList, BlockId, TransactionRequest,
    simulate::{SimBlock, SimulatePayload},
};
use parking_lot::RwLock;
use reth_ethereum_primitives::Receipt;
use reth_ethereum_primitives::TransactionSigned;
use reth_evm::{ConfigureEvm, NextBlockEnvAttributes};
use reth_evm_ethereum::EthEvmConfig;
use reth_primitives_traits::SealedHeader;
use reth_revm::revm::primitives::Bytes;
use reth_revm::revm::primitives::{Address, B256};
use reth_revm::{Context, ExecuteEvm, MainBuilder, MainContext};
use reth_tracing::tracing::{debug, warn};
use reth_tracing::{RethTracer, Tracer, tracing::info};
use rex_cons::ExExNotification;
use rex_sim::RexSim;

#[derive(Clone, Copy, Debug, PartialEq)]
enum ParityMode {
    Call,
    Simulate,
    Both,
}

impl ParityMode {
    fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "simulate" | "sim" => Self::Simulate,
            "both" => Self::Both,
            _ => Self::Call,
        }
    }
}

#[derive(Default)]
struct ParityReport {
    first_block: Option<u64>,
    last_block: Option<u64>,
    blocks_simulated: u64,
    total_txs: u64,
    total_contract_calls: u64,
    total_eip7702_txs: u64,
    total_parity_call: u64,
    total_parity_mismatch_call: u64,
    total_parity_simulate: u64,
    total_parity_mismatch_simulate: u64,
    total_gas: u64,
    total_simulation_time_ms: u128,
    parity_mode: Option<ParityMode>,
}

impl ParityReport {
    fn update_block_range(&mut self, block: u64) {
        if self.first_block.is_none() {
            self.first_block = Some(block);
        }
        self.last_block = Some(block);
        self.blocks_simulated += 1;
    }

    fn avg_simulation_time_ms(&self) -> f64 {
        if self.blocks_simulated == 0 {
            0.0
        } else {
            self.total_simulation_time_ms as f64 / self.blocks_simulated as f64
        }
    }

    fn parity_rate_call(&self) -> f64 {
        let total = self.total_parity_call + self.total_parity_mismatch_call;
        if total > 0 {
            self.total_parity_call as f64 / total as f64 * 100.0
        } else {
            0.0
        }
    }

    fn parity_rate_simulate(&self) -> f64 {
        let total = self.total_parity_simulate + self.total_parity_mismatch_simulate;
        if total > 0 {
            self.total_parity_simulate as f64 / total as f64 * 100.0
        } else {
            0.0
        }
    }

    fn write_report(&self, path: &str) -> std::io::Result<()> {
        let mut file = File::create(path)?;

        writeln!(
            file,
            "═══════════════════════════════════════════════════════════"
        )?;
        writeln!(file, "                    PARITY SIMULATION REPORT")?;
        writeln!(
            file,
            "═══════════════════════════════════════════════════════════"
        )?;
        writeln!(file)?;

        writeln!(file, "Configuration")?;
        writeln!(
            file,
            "───────────────────────────────────────────────────────────"
        )?;
        writeln!(
            file,
            "  Parity Mode:          {:?}",
            self.parity_mode.unwrap_or(ParityMode::Call)
        )?;
        writeln!(file)?;

        writeln!(file, "Block Range")?;
        writeln!(
            file,
            "───────────────────────────────────────────────────────────"
        )?;
        writeln!(
            file,
            "  First Block:          {}",
            self.first_block
                .map(|b| b.to_string())
                .unwrap_or_else(|| "N/A".to_string())
        )?;
        writeln!(
            file,
            "  Last Block:           {}",
            self.last_block
                .map(|b| b.to_string())
                .unwrap_or_else(|| "N/A".to_string())
        )?;
        writeln!(file, "  Blocks Simulated:     {}", self.blocks_simulated)?;
        writeln!(file)?;

        writeln!(file, "Transaction Statistics")?;
        writeln!(
            file,
            "───────────────────────────────────────────────────────────"
        )?;
        writeln!(file, "  Total Transactions:   {}", self.total_txs)?;
        writeln!(
            file,
            "  Contract Calls:       {}",
            self.total_contract_calls
        )?;
        writeln!(file, "  EIP-7702 Txs:         {}", self.total_eip7702_txs)?;
        writeln!(file)?;

        let mode = self.parity_mode.unwrap_or(ParityMode::Call);
        match mode {
            ParityMode::Call => {
                writeln!(file, "Parity Statistics (eth_call)")?;
                writeln!(
                    file,
                    "───────────────────────────────────────────────────────────"
                )?;
                writeln!(file, "  Parity (matched):     {}", self.total_parity_call)?;
                writeln!(
                    file,
                    "  Parity Mismatch:      {}",
                    self.total_parity_mismatch_call
                )?;
                writeln!(
                    file,
                    "  Parity Rate:          {:.2}%",
                    self.parity_rate_call()
                )?;
            }
            ParityMode::Simulate => {
                writeln!(file, "Parity Statistics (eth_simulateV1)")?;
                writeln!(
                    file,
                    "───────────────────────────────────────────────────────────"
                )?;
                writeln!(
                    file,
                    "  Parity (matched):     {}",
                    self.total_parity_simulate
                )?;
                writeln!(
                    file,
                    "  Parity Mismatch:      {}",
                    self.total_parity_mismatch_simulate
                )?;
                writeln!(
                    file,
                    "  Parity Rate:          {:.2}%",
                    self.parity_rate_simulate()
                )?;
            }
            ParityMode::Both => {
                writeln!(file, "Parity Statistics (eth_call)")?;
                writeln!(
                    file,
                    "───────────────────────────────────────────────────────────"
                )?;
                writeln!(file, "  Parity (matched):     {}", self.total_parity_call)?;
                writeln!(
                    file,
                    "  Parity Mismatch:      {}",
                    self.total_parity_mismatch_call
                )?;
                writeln!(
                    file,
                    "  Parity Rate:          {:.2}%",
                    self.parity_rate_call()
                )?;
                writeln!(file)?;
                writeln!(file, "Parity Statistics (eth_simulateV1)")?;
                writeln!(
                    file,
                    "───────────────────────────────────────────────────────────"
                )?;
                writeln!(
                    file,
                    "  Parity (matched):     {}",
                    self.total_parity_simulate
                )?;
                writeln!(
                    file,
                    "  Parity Mismatch:      {}",
                    self.total_parity_mismatch_simulate
                )?;
                writeln!(
                    file,
                    "  Parity Rate:          {:.2}%",
                    self.parity_rate_simulate()
                )?;
            }
        }
        writeln!(file)?;

        writeln!(file, "Performance")?;
        writeln!(
            file,
            "───────────────────────────────────────────────────────────"
        )?;
        writeln!(file, "  Total Gas Simulated:  {}", self.total_gas)?;
        writeln!(
            file,
            "  Total Gas (Mgas):     {:.2}",
            self.total_gas as f64 / 1_000_000.0
        )?;
        writeln!(
            file,
            "  Avg Sim Time/Block:   {:.2} ms",
            self.avg_simulation_time_ms()
        )?;
        writeln!(file)?;

        writeln!(
            file,
            "═══════════════════════════════════════════════════════════"
        )?;

        Ok(())
    }
}

const DEFAULT_REX_ENDPOINT: &str = "http://127.0.0.1:10000";
const DEFAULT_RPC_ENDPOINT: &str = "http://127.0.0.1:8545";

#[derive(Clone)]
struct BlockTxs {
    block: u64,
    txs: Vec<Recovered<TransactionSigned>>,
}

type LatestBlockTxs = Arc<RwLock<Option<BlockTxs>>>;

fn build_next_block_attributes(
    header: &SealedHeader,
    withdrawals: Option<Withdrawals>,
) -> NextBlockEnvAttributes {
    NextBlockEnvAttributes {
        timestamp: header.timestamp + 12,

        suggested_fee_recipient: Address::ZERO,

        prev_randao: header.mix_hash,

        gas_limit: header.gas_limit,

        parent_beacon_block_root: header.parent_beacon_block_root,

        withdrawals,

        extra_data: Bytes::new(),

        slot_number: None,
    }
}

fn decode_revert_message(data: &Bytes) -> Option<String> {
    if data.len() < 68 || data[0..4] != [0x08, 0xc3, 0x79, 0xa0] {
        return None;
    }

    let len_offset = 36;
    if data.len() < len_offset + 32 {
        return None;
    }
    let len = u64::from_be_bytes(data[len_offset + 24..len_offset + 32].try_into().ok()?) as usize;
    let str_start = len_offset + 32;
    if data.len() < str_start + len {
        return None;
    }
    String::from_utf8(data[str_start..str_start + len].to_vec()).ok()
}

fn get_selector(input: &Bytes) -> Option<String> {
    if input.len() >= 4 {
        Some(format!(
            "0x{:02x}{:02x}{:02x}{:02x}",
            input[0], input[1], input[2], input[3]
        ))
    } else {
        None
    }
}

#[tokio::main]
async fn main() {
    let _ = RethTracer::new().init();

    let args: Vec<String> = std::env::args().collect();
    let rex_endpoint = args
        .get(1)
        .map(|s| s.as_str())
        .unwrap_or(DEFAULT_REX_ENDPOINT);
    let rpc_endpoint = args
        .get(2)
        .map(|s| s.as_str())
        .unwrap_or(DEFAULT_RPC_ENDPOINT);
    let parity_mode = args
        .get(3)
        .map(|s| ParityMode::from_str(s))
        .unwrap_or(ParityMode::Call);

    info!(
        target: "parity",
        rex_endpoint,
        rpc_endpoint,
        parity_mode = ?parity_mode,
        "Starting parity check"
    );

    let provider =
        ProviderBuilder::new().connect_http(rpc_endpoint.parse().expect("Invalid RPC endpoint"));

    let handle = tokio::runtime::Handle::current();
    let canon_rx_sim = rex_cons::subscribe(rex_endpoint, &handle);
    let mut canon_rx_txs = rex_cons::subscribe(rex_endpoint, &handle);

    let latest_txs: LatestBlockTxs = Arc::new(RwLock::new(None));

    let latest_txs_writer = latest_txs.clone();
    tokio::spawn(async move {
        while let Some(notification) = canon_rx_txs.recv().await {
            if let ExExNotification::ChainCommitted { new } = notification
                && let Some(block) = new.blocks().values().last()
            {
                let block_num = block.header().number;

                let txs: Vec<Recovered<TransactionSigned>> =
                    block.clone_transactions_recovered().collect();

                *latest_txs_writer.write() = Some(BlockTxs {
                    block: block_num,
                    txs,
                });
            }
        }
    });

    let mut rex_sim = RexSim::builder().rpc_endpoint(rpc_endpoint).build();
    let (state, ready_rx, mut updates) = rex_sim.spawn(canon_rx_sim);
    let block = ready_rx.await.expect("Ready signal failed");
    info!(target: "parity", block = block, "RexSim ready");

    let evm_config = EthEvmConfig::mainnet();

    let mut report = ParityReport {
        parity_mode: Some(parity_mode),
        ..Default::default()
    };

    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => {
                info!(target: "parity", "Ctrl+C received, generating report...");
                break;
            }
            result = updates.changed() => {
                if result.is_err() {
                    break;
                }
            }
        }

        let update = updates.borrow().clone();

        if update.block_hash == B256::ZERO {
            info!(target: "parity", block = update.block, "Skipping stale state");
            continue;
        }

        let snapshot = state.snapshot();

        let Some(header) = snapshot.header.as_ref() else {
            info!(target: "parity", block = update.block, "No header available");
            continue;
        };

        let block_txs = loop {
            let maybe_txs = {
                let guard = latest_txs.read();
                guard.as_ref().filter(|b| b.block == update.block).cloned()
            };
            if let Some(txs) = maybe_txs {
                break txs;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        };

        let Some(receipts) = snapshot.receipts(update.block) else {
            info!(target: "parity", block = update.block, "Receipts not available");
            continue;
        };

        if block_txs.txs.len() != receipts.len() {
            info!(
                target: "parity",
                block = update.block,
                txs = block_txs.txs.len(),
                receipts = receipts.len(),
                "Transaction/receipt count mismatch"
            );
            continue;
        }

        let attributes = build_next_block_attributes(header, snapshot.withdrawals.clone());
        let evm_env = evm_config
            .next_evm_env(header.header(), &attributes)
            .expect("EthEvmConfig::next_evm_env is infallible");

        let mut success_count = 0;
        let mut revert_count = 0;
        let mut mismatch_count = 0;
        let mut skipped_count = 0;
        let mut contract_calls = 0;
        let mut eip7702_count = 0;
        let mut parity_call_count = 0;
        let mut parity_call_mismatch = 0;
        let mut parity_sim_count = 0;
        let mut parity_sim_mismatch = 0;
        let mut total_gas_simulated: u64 = 0;

        for tx in block_txs.txs.iter() {
            if tx.ty() == 4 {
                eip7702_count += 1;
            }
        }

        let basefee = evm_env.block_env.basefee;

        let block_id = BlockId::pending();

        let mut last_nonce: HashMap<Address, u64> = HashMap::new();
        for x in block_txs.txs.iter() {
            let e = last_nonce.entry(x.signer()).or_default();
            if x.nonce() > *e {
                *e = x.nonce()
            }

            if let Some(auth_list) = x.authorization_list() {
                for auth in auth_list {
                    if let Ok(authority) = auth.recover_authority() {
                        let e = last_nonce.entry(authority).or_default();

                        if auth.nonce() > *e {
                            *e = auth.nonce()
                        }
                    }
                }
            }
        }

        let sim_start = std::time::Instant::now();
        for (i, tx) in block_txs.txs.iter().enumerate() {
            let mut evm = Context::mainnet()
                .with_ref_db(&*state)
                .with_cfg(evm_env.cfg_env.clone())
                .with_block(evm_env.block_env.clone())
                .build_mainnet();

            let mut tx_env = evm_config.tx_env(tx);
            tx_env.nonce = last_nonce[&tx_env.caller] + 1;

            if tx_env.gas_price < basefee as u128 {
                tx_env.gas_price = basefee as u128;
            }

            let adjusted_gas_price = tx_env.gas_price;

            let result = evm.transact(tx_env);

            let expected_receipt: &Receipt = &receipts[i];
            let expected_success = expected_receipt.status();

            match result {
                Ok(result) => {
                    let simulated_success = result.result.is_success();
                    let local_output = result.result.output().cloned();
                    let gas_used = result.result.tx_gas_used();
                    total_gas_simulated += gas_used;

                    if simulated_success == expected_success {
                        if simulated_success {
                            success_count += 1;

                            if !tx.input().is_empty() {
                                contract_calls += 1;
                            }
                        } else {
                            revert_count += 1;
                        }
                    } else {
                        mismatch_count += 1;
                    }

                    let call_nonce = last_nonce[&tx.signer()] + 1;
                    let tx_type = tx.ty();

                    let (call_gas_price, call_max_fee, call_priority_fee) = if tx_type < 2 {
                        (Some(adjusted_gas_price), None, None)
                    } else {
                        let max_fee = tx.max_fee_per_gas().max(basefee as u128);
                        (None, Some(max_fee), tx.max_priority_fee_per_gas())
                    };

                    let call_request = TransactionRequest {
                        from: Some(tx.signer()),
                        to: tx.to().map(Into::into),
                        gas: Some(tx.gas_limit()),
                        gas_price: call_gas_price,
                        max_fee_per_gas: call_max_fee,
                        max_priority_fee_per_gas: call_priority_fee,
                        nonce: Some(call_nonce),
                        value: Some(tx.value()),
                        input: tx.input().clone().into(),
                        chain_id: tx.chain_id(),
                        access_list: tx.access_list().map(|al| {
                            AccessList(
                                al.iter()
                                    .map(|item| alloy_rpc_types::AccessListItem {
                                        address: item.address,
                                        storage_keys: item.storage_keys.clone(),
                                    })
                                    .collect(),
                            )
                        }),
                        transaction_type: Some(tx_type),
                        blob_versioned_hashes: if tx_type == 3 {
                            Some(tx.blob_versioned_hashes().unwrap_or_default().to_vec())
                        } else {
                            None
                        },
                        ..Default::default()
                    };

                    let selector = get_selector(tx.input());
                    let local_revert_msg = local_output
                        .as_ref()
                        .and_then(|o| decode_revert_message(&Bytes::copy_from_slice(o.as_ref())));

                    let sim_timestamp: u64 =
                        evm_env.block_env.timestamp.try_into().unwrap_or(u64::MAX);

                    if parity_mode == ParityMode::Call || parity_mode == ParityMode::Both {
                        match provider.call(call_request.clone()).block(block_id).await {
                            Ok(node_output) => {
                                let local_bytes: &[u8] =
                                    local_output.as_ref().map(|b| b.as_ref()).unwrap_or(&[]);
                                let node_bytes: &[u8] = node_output.as_ref();
                                if local_bytes == node_bytes {
                                    parity_call_count += 1;
                                } else {
                                    parity_call_mismatch += 1;
                                    let node_revert_msg = decode_revert_message(&node_output);
                                    warn!(
                                        target: "parity",
                                        block = update.block,
                                        tx_index = i,
                                        tx_hash = %tx.tx_hash(),
                                        from = %tx.signer(),
                                        to = ?tx.to(),
                                        selector = ?selector,
                                        input_len = tx.input().len(),
                                        local_output = ?local_output,
                                        local_revert_msg = ?local_revert_msg,
                                        node_output = %node_output,
                                        node_revert_msg = ?node_revert_msg,
                                        local_gas_used = gas_used,
                                        local_success = simulated_success,
                                        tx_type,
                                        call_nonce,
                                        sim_timestamp,
                                        sim_basefee = basefee,
                                        adjusted_gas_price,
                                        mode = "eth_call",
                                        "Output mismatch: local vs node"
                                    );
                                }
                            }
                            Err(e) => {
                                if !simulated_success {
                                    parity_call_count += 1;
                                } else {
                                    parity_call_mismatch += 1;
                                    warn!(
                                        target: "parity",
                                        block = update.block,
                                        tx_index = i,
                                        tx_hash = %tx.tx_hash(),
                                        from = %tx.signer(),
                                        to = ?tx.to(),
                                        selector = ?selector,
                                        input_len = tx.input().len(),
                                        local_output = ?local_output,
                                        local_revert_msg = ?local_revert_msg,
                                        node_error = %e,
                                        local_gas_used = gas_used,
                                        local_success = simulated_success,
                                        tx_type,
                                        call_nonce,
                                        sim_timestamp,
                                        sim_basefee = basefee,
                                        adjusted_gas_price,
                                        mode = "eth_call",
                                        "Parity mismatch: local success, node revert"
                                    );
                                }
                            }
                        }
                    }

                    if (parity_mode == ParityMode::Simulate || parity_mode == ParityMode::Both)
                        && tx_type != 3
                    {
                        let sim_block = SimBlock::default().call(call_request.clone());

                        let payload = SimulatePayload::default().extend(sim_block);

                        let sim_block_id = BlockId::pending();

                        match provider.simulate(&payload).block_id(sim_block_id).await {
                            Ok(results) => {
                                if let Some(block_result) = results.first() {
                                    if let Some(call_result) = block_result.calls.first() {
                                        let node_output = &call_result.return_data;

                                        let local_bytes: &[u8] = local_output
                                            .as_ref()
                                            .map(|b| b.as_ref())
                                            .unwrap_or(&[]);
                                        let node_bytes: &[u8] = node_output.as_ref();

                                        if local_bytes == node_bytes {
                                            parity_sim_count += 1;
                                        } else {
                                            parity_sim_mismatch += 1;
                                            let node_revert_msg =
                                                decode_revert_message(node_output);
                                            warn!(
                                                target: "parity",
                                                block = update.block,
                                                tx_index = i,
                                                tx_hash = %tx.tx_hash(),
                                                from = %tx.signer(),
                                                to = ?tx.to(),
                                                selector = ?selector,
                                                input_len = tx.input().len(),
                                                local_output = ?local_output,
                                                local_revert_msg = ?local_revert_msg,
                                                node_output = %node_output,
                                                node_revert_msg = ?node_revert_msg,
                                                node_gas_used = call_result.gas_used,
                                                node_status = call_result.status,
                                                node_error = ?call_result.error,
                                                local_gas_used = gas_used,
                                                local_success = simulated_success,
                                                tx_type,
                                                call_nonce,
                                                sim_timestamp,
                                                sim_basefee = basefee,
                                                adjusted_gas_price,
                                                mode = "eth_simulateV1",
                                                "Output mismatch: local vs node"
                                            );
                                        }
                                    } else {
                                        debug!(target: "parity", "eth_simulateV1 returned empty calls");
                                    }
                                } else {
                                    debug!(target: "parity", "eth_simulateV1 returned empty results");
                                }
                            }
                            Err(e) => {
                                if !simulated_success {
                                    parity_sim_count += 1;
                                } else {
                                    parity_sim_mismatch += 1;
                                    warn!(
                                        target: "parity",
                                        block = update.block,
                                        tx_index = i,
                                        tx_hash = %tx.tx_hash(),
                                        from = %tx.signer(),
                                        to = ?tx.to(),
                                        selector = ?selector,
                                        input_len = tx.input().len(),
                                        local_output = ?local_output,
                                        local_revert_msg = ?local_revert_msg,
                                        node_error = %e,
                                        local_gas_used = gas_used,
                                        local_success = simulated_success,
                                        tx_type,
                                        call_nonce,
                                        sim_timestamp,
                                        sim_basefee = basefee,
                                        adjusted_gas_price,
                                        mode = "eth_simulateV1",
                                        "Parity mismatch: local success, node error"
                                    );
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("lack of funds") {
                        skipped_count += 1;
                    } else {
                        mismatch_count += 1;
                        warn!(
                            target: "parity",
                            block = update.block,
                            tx_index = i,
                            tx_hash = %tx.hash(),
                            error = %e,
                            "Simulation error"
                        );
                    }
                }
            }
        }

        let elapsed_ms = sim_start.elapsed().as_millis();
        let total_mgas = total_gas_simulated as f64 / 1_000_000.0;

        match parity_mode {
            ParityMode::Call => {
                info!(
                    target: "parity",
                    block = update.block,
                    generation = update.generation,
                    txs = block_txs.txs.len(),
                    success = success_count,
                    reverts = revert_count,
                    mismatches = mismatch_count,
                    skipped = skipped_count,
                    contract_calls,
                    eip7702 = eip7702_count,
                    parity_call = parity_call_count,
                    parity_call_mismatch,
                    mgas = format!("{:.2}", total_mgas),
                    elapsed_ms,
                    "Block simulated"
                );
            }
            ParityMode::Simulate => {
                info!(
                    target: "parity",
                    block = update.block,
                    generation = update.generation,
                    txs = block_txs.txs.len(),
                    success = success_count,
                    reverts = revert_count,
                    mismatches = mismatch_count,
                    skipped = skipped_count,
                    contract_calls,
                    eip7702 = eip7702_count,
                    parity_sim = parity_sim_count,
                    parity_sim_mismatch,
                    mgas = format!("{:.2}", total_mgas),
                    elapsed_ms,
                    "Block simulated"
                );
            }
            ParityMode::Both => {
                info!(
                    target: "parity",
                    block = update.block,
                    generation = update.generation,
                    txs = block_txs.txs.len(),
                    success = success_count,
                    reverts = revert_count,
                    mismatches = mismatch_count,
                    skipped = skipped_count,
                    contract_calls,
                    eip7702 = eip7702_count,
                    parity_call = parity_call_count,
                    parity_call_mismatch,
                    parity_sim = parity_sim_count,
                    parity_sim_mismatch,
                    mgas = format!("{:.2}", total_mgas),
                    elapsed_ms,
                    "Block simulated"
                );
            }
        }

        report.update_block_range(update.block);
        report.total_txs += block_txs.txs.len() as u64;
        report.total_contract_calls += contract_calls as u64;
        report.total_eip7702_txs += eip7702_count as u64;
        report.total_parity_call += parity_call_count as u64;
        report.total_parity_mismatch_call += parity_call_mismatch as u64;
        report.total_parity_simulate += parity_sim_count as u64;
        report.total_parity_mismatch_simulate += parity_sim_mismatch as u64;
        report.total_gas += total_gas_simulated;
        report.total_simulation_time_ms += elapsed_ms;
    }

    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let report_path = format!("parity_report_{}.txt", timestamp);
    match report.write_report(&report_path) {
        Ok(()) => info!(target: "parity", path = %report_path, "Report written successfully"),
        Err(e) => warn!(target: "parity", error = %e, "Failed to write report"),
    }

    info!(target: "parity", "Shutting down");
}
