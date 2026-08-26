use hashbrown::HashMap;
use std::sync::Arc;
use std::time::Instant;

use alloy_primitives::Log;
use reth_execution_types::Chain;
use reth_revm::revm::primitives::U256;
use reth_revm::revm::state::Bytecode;
use reth_tracing::tracing::{error, info, warn};
use rex_cons::ExExNotification;
use tokio::sync::{mpsc, oneshot};

use crate::fetch::{AccountCache, BlockHashCache, CodeCache, StorageCache};
use crate::state::StaticState;
use crate::transitions::{
    AccountChange, BlockData, Caches, ChainTip, ExecutionChanges, build_commit_state,
    build_reorg_state, build_revert_state, mark_stale,
};
use crate::validation::{
    BlockInfo, CommitValidation, CurrentState, ReorgValidation, validate_chain_committed,
    validate_chain_reorged,
};
use crate::{LogSink, StateUpdate, StateUpdateSender};

pub struct FlushHandles {
    pub account: mpsc::Sender<oneshot::Sender<AccountCache>>,
    pub code: mpsc::Sender<oneshot::Sender<CodeCache>>,
    pub storage: mpsc::Sender<oneshot::Sender<StorageCache>>,
    pub block_hash: mpsc::Sender<oneshot::Sender<BlockHashCache>>,
}

pub async fn run_changeset_manager(
    mut notifications: mpsc::Receiver<ExExNotification>,
    state: Arc<StaticState>,
    flush_handles: FlushHandles,
    ready_signal: Option<oneshot::Sender<u64>>,
    update_sender: Option<StateUpdateSender>,
    log_sink: Option<LogSink>,
) {
    info!(target: "rex-sim", "Changeset manager starting, waiting for first notification");

    let mut ready_signal = ready_signal;

    while let Some(notification) = notifications.recv().await {
        let start = Instant::now();

        match notification {
            ExExNotification::ChainCommitted { new } => {
                let tip = new.tip();
                let tip_num = tip.header().number;
                let tip_hash = tip.hash();
                let first_block = new.first();
                let first_num = first_block.header().number;
                let first_parent_hash = first_block.header().parent_hash;

                let current_snapshot = state.snapshot();
                let current_block = current_snapshot.block;
                let current_hash = current_snapshot.block_hash;
                let is_stale = current_snapshot.stale;

                info!(
                    target: "rex-sim",
                    tip_block = tip_num,
                    tip_hash = %tip_hash,
                    first_block = first_num,
                    first_parent = %first_parent_hash,
                    current_block,
                    current_hash = %current_hash,
                    is_stale,
                    "Received chain commit"
                );

                let current_state = CurrentState {
                    block: current_block,
                    block_hash: current_hash,
                    stale: is_stale,
                };
                let first_block_info = BlockInfo {
                    number: first_num,
                    hash: first_block.hash(),
                    parent_hash: first_parent_hash,
                };
                let chain_blocks = new
                    .blocks_and_receipts()
                    .map(|(block, _)| (block.header().number, block.hash()));

                let validation =
                    validate_chain_committed(current_state, first_block_info, chain_blocks);

                match &validation {
                    CommitValidation::AcceptAny => {
                        let reason = if is_stale {
                            "state was stale"
                        } else if current_block == 0 {
                            "initial state (block 0)"
                        } else {
                            "initial state (zero hash)"
                        };
                        info!(
                            target: "rex-sim",
                            current_block,
                            current_hash = %current_hash,
                            is_stale,
                            reason,
                            "Accepting chain commit unconditionally"
                        );
                    }
                    CommitValidation::Valid => {}
                    CommitValidation::Gap { expected, got } => {
                        warn!(
                            target: "rex-sim",
                            expected,
                            got,
                            current_block = current_block,
                            current_hash = %current_hash,
                            "Chain gap detected - marking state as stale"
                        );
                    }
                    CommitValidation::OverlapMismatch => {
                        warn!(
                            target: "rex-sim",
                            first_block = first_num,
                            first_parent = %first_parent_hash,
                            current_block = current_block,
                            current_hash = %current_hash,
                            "Chain doesn't connect - overlap but no matching block"
                        );
                    }
                }

                if !validation.is_acceptable() {
                    let _ = flush_all_caches(&flush_handles).await;

                    let stale_state = mark_stale(&current_snapshot);
                    state.swap(stale_state);
                    error!(
                        target: "rex-sim",
                        current_block,
                        generation = state.generation(),
                        reason = "chain continuity gap",
                        "State marked as STALE - simulations will fail until valid notification"
                    );
                    continue;
                }

                let (account_cache, code_cache, storage_cache, block_hash_cache) =
                    flush_all_caches(&flush_handles).await;

                let caches =
                    caches_from_tuple(account_cache, code_cache, storage_cache, block_hash_cache);
                let changes = extract_execution_changes(&new);
                let blocks = extract_block_data(&new);
                let tip = ChainTip {
                    number: tip_num,
                    hash: tip_hash,
                };

                let (new_state, cache_stats) =
                    build_commit_state(&current_snapshot, caches, &changes, &blocks, tip);

                for (name, merged, discarded) in cache_stats.iter() {
                    if merged > 0 {
                        info!(target: "rex-sim", merged, name, "Merged lazy-loaded cache");
                    }
                    if discarded > 0 {
                        info!(target: "rex-sim", discarded, name, "Discarded stale cache");
                    }
                }

                let accounts_count = new_state.accounts.len();
                let code_count = new_state.code.len();
                let storage_count = new_state.storage.len();
                let block_hashes_count = new_state.block_hashes.len();
                let receipts_blocks = new_state.receipts_history.len();

                state.swap(new_state);

                let elapsed = start.elapsed();
                info!(
                    target: "rex-sim",
                    block = tip_num,
                    block_hash = %tip_hash,
                    generation = state.generation(),
                    elapsed_ms = elapsed.as_millis(),
                    accounts = accounts_count,
                    code = code_count,
                    storage = storage_count,
                    block_hashes = block_hashes_count,
                    receipts_blocks = receipts_blocks,
                    "Changeset applied"
                );

                if let Some(ref sender) = update_sender {
                    let _ = sender.send(StateUpdate {
                        block: tip_num,
                        block_hash: tip_hash,
                        generation: state.generation(),
                    });
                }

                if let Some(ref sink) = log_sink {
                    let logs = extract_logs(&new);
                    if !logs.is_empty() {
                        let _ = sink.send(logs);
                    }
                }

                if let Some(signal) = ready_signal.take() {
                    let _ = signal.send(tip_num);
                    info!(target: "rex-sim", block = tip_num, "Ready signal fired");
                }
            }

            ExExNotification::ChainReorged { old, new } => {
                let old_tip_num = old.tip().header().number;
                let old_tip_hash = old.tip().hash();
                let new_tip_num = new.tip().header().number;
                let new_tip_hash = new.tip().hash();
                let old_first_num = old.first().header().number;
                let reorg_depth = old.len();

                let current_snapshot = state.snapshot();
                let current_block = current_snapshot.block;
                let current_hash = current_snapshot.block_hash;

                info!(
                    target: "rex-sim",
                    old_tip = old_tip_num,
                    old_tip_hash = %old_tip_hash,
                    new_tip = new_tip_num,
                    new_tip_hash = %new_tip_hash,
                    reorg_depth,
                    current_block,
                    current_hash = %current_hash,
                    "Received chain reorg"
                );

                let current_state = CurrentState {
                    block: current_block,
                    block_hash: current_hash,
                    stale: false,
                };
                let old_tip_info = BlockInfo {
                    number: old_tip_num,
                    hash: old_tip_hash,
                    parent_hash: old.tip().header().parent_hash,
                };
                let old_chain_blocks = old
                    .blocks_and_receipts()
                    .map(|(block, _)| (block.header().number, block.hash()));

                let validation =
                    validate_chain_reorged(current_state, old_tip_info, old_chain_blocks);

                if matches!(validation, ReorgValidation::Mismatch) {
                    warn!(
                        target: "rex-sim",
                        old_tip_hash = %old_tip_hash,
                        current_hash = %current_hash,
                        "Reorg old chain doesn't match our state"
                    );
                }

                let _ = flush_all_caches(&flush_handles).await;

                if !validation.is_acceptable() {
                    let stale_state = mark_stale(&current_snapshot);
                    state.swap(stale_state);
                    error!(
                        target: "rex-sim",
                        current_block,
                        generation = state.generation(),
                        reason = "reorg chain mismatch",
                        "State marked as STALE - simulations will fail until valid notification"
                    );
                    continue;
                }

                let common_ancestor = old_first_num.saturating_sub(1);
                let changes = extract_execution_changes(&new);
                let blocks = extract_block_data(&new);
                let tip = ChainTip {
                    number: new_tip_num,
                    hash: new_tip_hash,
                };

                let new_state =
                    build_reorg_state(&current_snapshot, common_ancestor, &changes, &blocks, tip);

                let accounts_count = new_state.accounts.len();
                let code_count = new_state.code.len();
                let storage_count = new_state.storage.len();
                let block_hashes_count = new_state.block_hashes.len();
                let receipts_blocks = new_state.receipts_history.len();

                state.swap(new_state);

                let elapsed = start.elapsed();
                info!(
                    target: "rex-sim",
                    new_block = new_tip_num,
                    new_hash = %new_tip_hash,
                    generation = state.generation(),
                    elapsed_ms = elapsed.as_millis(),
                    accounts = accounts_count,
                    code = code_count,
                    storage = storage_count,
                    block_hashes = block_hashes_count,
                    receipts_blocks = receipts_blocks,
                    reorg_depth,
                    "Reorg state applied (caches discarded)"
                );

                if let Some(ref sender) = update_sender {
                    let _ = sender.send(StateUpdate {
                        block: new_tip_num,
                        block_hash: new_tip_hash,
                        generation: state.generation(),
                    });
                }

                if let Some(ref sink) = log_sink {
                    let logs = extract_logs(&new);
                    if !logs.is_empty() {
                        let _ = sink.send(logs);
                    }
                }

                if let Some(signal) = ready_signal.take() {
                    let _ = signal.send(new_tip_num);
                    info!(target: "rex-sim", block = new_tip_num, "Ready signal fired");
                }
            }

            ExExNotification::ChainReverted { old } => {
                let old_tip_num = old.tip().header().number;
                let old_tip_hash = old.tip().hash();
                let old_first_num = old.first().header().number;

                let current_snapshot = state.snapshot();
                let current_block = current_snapshot.block;
                let current_hash = current_snapshot.block_hash;

                warn!(
                    target: "rex-sim",
                    old_tip = old_tip_num,
                    old_tip_hash = %old_tip_hash,
                    current_block,
                    current_hash = %current_hash,
                    "Received chain revert (unusual - marking state as stale)"
                );

                let _ = flush_all_caches(&flush_handles).await;

                let common_ancestor = old_first_num.saturating_sub(1);
                let new_state = build_revert_state(&current_snapshot, common_ancestor);

                state.swap(new_state);

                if let Some(ref sender) = update_sender {
                    let _ = sender.send(StateUpdate {
                        block: common_ancestor,
                        block_hash: reth_revm::revm::primitives::B256::ZERO,
                        generation: state.generation(),
                    });
                }

                error!(
                    target: "rex-sim",
                    common_ancestor,
                    generation = state.generation(),
                    "State marked as STALE after revert - simulations will fail until valid notification received"
                );
            }
        }
    }

    info!(target: "rex-sim", "Changeset manager shutting down (channel closed)");
}

async fn flush_all_caches(
    flush_handles: &FlushHandles,
) -> (AccountCache, CodeCache, StorageCache, BlockHashCache) {
    let (account_tx, account_rx) = oneshot::channel();
    let (code_tx, code_rx) = oneshot::channel();
    let (storage_tx, storage_rx) = oneshot::channel();
    let (block_hash_tx, block_hash_rx) = oneshot::channel();

    let _ = flush_handles.account.send(account_tx).await;
    let _ = flush_handles.code.send(code_tx).await;
    let _ = flush_handles.storage.send(storage_tx).await;
    let _ = flush_handles.block_hash.send(block_hash_tx).await;

    let account = account_rx.await.unwrap_or_default();
    let code = code_rx.await.unwrap_or_default();
    let storage = storage_rx.await.unwrap_or_default();
    let block_hash = block_hash_rx.await.unwrap_or_default();

    (account, code, storage, block_hash)
}

fn caches_from_tuple(
    account: AccountCache,
    code: CodeCache,
    storage: StorageCache,
    block_hash: BlockHashCache,
) -> Caches {
    Caches {
        accounts: account,
        code,
        storage,
        block_hashes: block_hash,
    }
}

fn extract_execution_changes(chain: &Chain) -> ExecutionChanges {
    let execution_outcome = chain.execution_outcome();
    let bundle = execution_outcome.state();

    let mut accounts: HashMap<reth_revm::revm::primitives::Address, AccountChange> = HashMap::new();
    let mut contracts = HashMap::new();

    for (address, bundle_account) in bundle.state() {
        let mut storage = HashMap::new();
        for (slot, slot_value) in &bundle_account.storage {
            let slot_u256 = U256::from_be_bytes::<32>(slot.to_be_bytes::<32>());
            storage.insert(slot_u256, slot_value.present_value);
        }

        accounts.insert(
            *address,
            AccountChange {
                info: bundle_account.info.clone(),
                storage,
            },
        );
    }

    for (code_hash, bytecode) in &bundle.contracts {
        contracts.insert(*code_hash, Bytecode::new_raw(bytecode.original_bytes()));
    }

    ExecutionChanges {
        accounts,
        contracts,
    }
}

fn extract_block_data(chain: &Chain) -> Vec<BlockData> {
    chain
        .blocks_and_receipts()
        .map(|(block, receipts)| BlockData {
            number: block.header().number,
            hash: block.hash(),
            header: block.sealed_header().clone(),
            withdrawals: block.body().withdrawals.clone(),
            receipts: receipts.to_vec(),
            tx_hashes: block
                .body()
                .transactions
                .iter()
                .map(|tx| *tx.tx_hash())
                .collect(),
        })
        .collect()
}

fn extract_logs(chain: &Chain) -> Vec<Log> {
    let mut logs = Vec::new();
    for (_, receipts) in chain.blocks_and_receipts() {
        for receipt in receipts {
            for log in &receipt.logs {
                logs.push(log.clone());
            }
        }
    }
    logs
}
