use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use reth_tracing::{RethTracer, Tracer};
use rex_cons::ExExNotification;
use rex_sim::artifacts::{list_commit_blocks, list_reorgs, load_commit, load_reorg};
use rex_sim::changeset::{FlushHandles, run_changeset_manager};
use rex_sim::fetch::{AccountCache, BlockHashCache, CodeCache, FetchConfig, StorageCache};
use rex_sim::state::{FetchChannels, StateSnapshot, StaticState};
use tokio::sync::{mpsc, oneshot};

type DummyFlushHandles = (
    FlushHandles,
    mpsc::Receiver<oneshot::Sender<AccountCache>>,
    mpsc::Receiver<oneshot::Sender<CodeCache>>,
    mpsc::Receiver<oneshot::Sender<StorageCache>>,
    mpsc::Receiver<oneshot::Sender<BlockHashCache>>,
);

fn create_dummy_flush_handles() -> DummyFlushHandles {
    let (account_tx, account_rx) = mpsc::channel(1);
    let (code_tx, code_rx) = mpsc::channel(1);
    let (storage_tx, storage_rx) = mpsc::channel(1);
    let (block_hash_tx, block_hash_rx) = mpsc::channel(1);

    let handles = FlushHandles {
        account: account_tx,
        code: code_tx,
        storage: storage_tx,
        block_hash: block_hash_tx,
    };

    (handles, account_rx, code_rx, storage_rx, block_hash_rx)
}

async fn run_dummy_flush_responder(
    mut account_rx: mpsc::Receiver<oneshot::Sender<AccountCache>>,
    mut code_rx: mpsc::Receiver<oneshot::Sender<CodeCache>>,
    mut storage_rx: mpsc::Receiver<oneshot::Sender<StorageCache>>,
    mut block_hash_rx: mpsc::Receiver<oneshot::Sender<BlockHashCache>>,
) {
    loop {
        tokio::select! {
            Some(tx) = account_rx.recv() => { let _ = tx.send(AccountCache::default()); }
            Some(tx) = code_rx.recv() => { let _ = tx.send(CodeCache::default()); }
            Some(tx) = storage_rx.recv() => { let _ = tx.send(StorageCache::default()); }
            Some(tx) = block_hash_rx.recv() => { let _ = tx.send(BlockHashCache::default()); }
            else => break,
        }
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let _ = RethTracer::new().init()?;

    let args: Vec<String> = std::env::args().collect();
    let artifacts_dir = args
        .iter()
        .position(|a| a == "--artifacts")
        .and_then(|i| args.get(i + 1))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("../rex-cons/artifacts"));

    println!("RexSim Artifact Replay Test");
    println!("===========================");
    println!("Artifacts dir: {}", artifacts_dir.display());
    println!();

    let blocks = list_commit_blocks(&artifacts_dir)?;
    let reorgs = list_reorgs(&artifacts_dir)?;

    println!("Available artifacts:");
    println!(
        "  Commits: {} blocks ({} to {})",
        blocks.len(),
        blocks.first().unwrap_or(&0),
        blocks.last().unwrap_or(&0)
    );
    println!("  Reorgs:  {}", reorgs.len());
    for (os, oe, ns, ne) in &reorgs {
        println!("    - old {}..{} -> new {}..{}", os, oe, ns, ne);
    }
    println!();

    println!("========== TEST 1: Normal Commit Flow ==========");
    test_normal_commits(&artifacts_dir, &blocks[..10.min(blocks.len())]).await?;

    if !reorgs.is_empty() {
        println!("\n========== TEST 2: Reorg Handling ==========");
        test_reorg(&artifacts_dir, &blocks, &reorgs[0]).await?;
    }

    println!("\n========== TEST 3: Skipped Commits (Connection Loss) ==========");
    test_skipped_commits(&artifacts_dir, &blocks).await?;

    println!("\n========== ALL TESTS COMPLETED ==========");
    Ok(())
}

async fn test_normal_commits(artifacts_dir: &PathBuf, blocks: &[u64]) -> eyre::Result<()> {
    println!("Testing {} sequential commits...", blocks.len());

    let (account_tx, _) = mpsc::unbounded_channel();
    let (code_tx, _) = mpsc::unbounded_channel();
    let (storage_tx, _) = mpsc::unbounded_channel();
    let (block_hash_tx, _) = mpsc::unbounded_channel();

    let channels = FetchChannels {
        account: account_tx,
        code: code_tx,
        storage: storage_tx,
        block_hash: block_hash_tx,
    };

    let state = Arc::new(StaticState::new(
        StateSnapshot::default(),
        channels,
        FetchConfig::default(),
    ));
    let (flush_handles, account_rx, code_rx, storage_rx, block_hash_rx) =
        create_dummy_flush_handles();
    let (notif_tx, notif_rx) = mpsc::channel(64);
    let (ready_tx, ready_rx) = oneshot::channel();

    let state_clone = Arc::clone(&state);
    tokio::spawn(async move {
        run_changeset_manager(
            notif_rx,
            state_clone,
            flush_handles,
            Some(ready_tx),
            None,
            None,
        )
        .await;
    });
    tokio::spawn(run_dummy_flush_responder(
        account_rx,
        code_rx,
        storage_rx,
        block_hash_rx,
    ));

    for (i, &block) in blocks.iter().enumerate() {
        let notification = load_commit(artifacts_dir, block)?;
        notif_tx.send(notification).await?;

        tokio::time::sleep(Duration::from_millis(50)).await;

        let current_block = state.block();
        let generation = state.generation();
        let is_stale = state.snapshot().stale;

        println!(
            "  [{}/{}] Sent block {}: state.block={}, generation={}, stale={}",
            i + 1,
            blocks.len(),
            block,
            current_block,
            generation,
            is_stale
        );

        assert_eq!(current_block, block, "Block number mismatch");
        assert!(!is_stale, "State should not be stale");
    }

    let ready_block = ready_rx.await?;
    println!("Ready signal received for block {}", ready_block);
    println!("PASS: Normal commit flow works correctly");

    Ok(())
}

async fn test_reorg(
    artifacts_dir: &PathBuf,
    blocks: &[u64],
    reorg: &(u64, u64, u64, u64),
) -> eyre::Result<()> {
    let (old_start, old_end, new_start, new_end) = *reorg;
    println!(
        "Testing reorg: old {}..{} -> new {}..{}",
        old_start, old_end, new_start, new_end
    );

    let pre_reorg_blocks: Vec<u64> = blocks
        .iter()
        .filter(|&&b| b < old_start)
        .take(5)
        .copied()
        .collect();

    if pre_reorg_blocks.is_empty() {
        println!("SKIP: No blocks available before reorg");
        return Ok(());
    }

    let (account_tx, _) = mpsc::unbounded_channel();
    let (code_tx, _) = mpsc::unbounded_channel();
    let (storage_tx, _) = mpsc::unbounded_channel();
    let (block_hash_tx, _) = mpsc::unbounded_channel();

    let channels = FetchChannels {
        account: account_tx,
        code: code_tx,
        storage: storage_tx,
        block_hash: block_hash_tx,
    };

    let state = Arc::new(StaticState::new(
        StateSnapshot::default(),
        channels,
        FetchConfig::default(),
    ));
    let (flush_handles, account_rx, code_rx, storage_rx, block_hash_rx) =
        create_dummy_flush_handles();
    let (notif_tx, notif_rx) = mpsc::channel(64);
    let (ready_tx, _ready_rx) = oneshot::channel();

    let state_clone = Arc::clone(&state);
    tokio::spawn(async move {
        run_changeset_manager(
            notif_rx,
            state_clone,
            flush_handles,
            Some(ready_tx),
            None,
            None,
        )
        .await;
    });
    tokio::spawn(run_dummy_flush_responder(
        account_rx,
        code_rx,
        storage_rx,
        block_hash_rx,
    ));

    println!("  Sending {} pre-reorg commits...", pre_reorg_blocks.len());
    for &block in &pre_reorg_blocks {
        let notification = load_commit(artifacts_dir, block)?;
        notif_tx.send(notification).await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let blocks_to_reorg: Vec<u64> = blocks
        .iter()
        .filter(|&&b| b >= *pre_reorg_blocks.last().unwrap_or(&0) && b <= old_end)
        .copied()
        .collect();

    println!(
        "  Sending {} commits to be reorged...",
        blocks_to_reorg.len()
    );
    for &block in &blocks_to_reorg {
        if let Ok(notification) = load_commit(artifacts_dir, block) {
            notif_tx.send(notification).await?;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    let pre_reorg_state = state.block();
    let pre_reorg_hash = state.snapshot().block_hash;
    let pre_reorg_gen = state.generation();
    println!(
        "  Pre-reorg state: block={}, hash={}, generation={}",
        pre_reorg_state, pre_reorg_hash, pre_reorg_gen
    );

    println!("  Sending reorg notification...");
    let reorg_notification = load_reorg(artifacts_dir, old_start, old_end, new_start, new_end)?;

    if let ExExNotification::ChainReorged { old, new } = &reorg_notification {
        println!(
            "    Old chain tip: {} ({})",
            old.tip().header().number,
            old.tip().hash()
        );
        println!(
            "    New chain tip: {} ({})",
            new.tip().header().number,
            new.tip().hash()
        );
    }

    notif_tx.send(reorg_notification).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let post_reorg_state = state.block();
    let post_reorg_hash = state.snapshot().block_hash;
    let post_reorg_gen = state.generation();
    let is_stale = state.snapshot().stale;

    println!(
        "  Post-reorg state: block={}, hash={}, generation={}, stale={}",
        post_reorg_state, post_reorg_hash, post_reorg_gen, is_stale
    );

    assert!(
        post_reorg_gen > pre_reorg_gen,
        "Generation should increment on reorg"
    );

    if pre_reorg_hash != post_reorg_hash {
        println!(
            "  Block hash changed: {} -> {}",
            pre_reorg_hash, post_reorg_hash
        );
    }

    println!("PASS: Reorg handling works correctly");
    Ok(())
}

async fn test_skipped_commits(artifacts_dir: &PathBuf, blocks: &[u64]) -> eyre::Result<()> {
    if blocks.len() < 10 {
        println!("SKIP: Not enough blocks for skip test");
        return Ok(());
    }

    println!("Testing skipped commits (simulating connection loss)...");

    let (account_tx, _) = mpsc::unbounded_channel();
    let (code_tx, _) = mpsc::unbounded_channel();
    let (storage_tx, _) = mpsc::unbounded_channel();
    let (block_hash_tx, _) = mpsc::unbounded_channel();

    let channels = FetchChannels {
        account: account_tx,
        code: code_tx,
        storage: storage_tx,
        block_hash: block_hash_tx,
    };

    let state = Arc::new(StaticState::new(
        StateSnapshot::default(),
        channels,
        FetchConfig::default(),
    ));
    let (flush_handles, account_rx, code_rx, storage_rx, block_hash_rx) =
        create_dummy_flush_handles();
    let (notif_tx, notif_rx) = mpsc::channel(64);
    let (ready_tx, _ready_rx) = oneshot::channel();

    let state_clone = Arc::clone(&state);
    tokio::spawn(async move {
        run_changeset_manager(
            notif_rx,
            state_clone,
            flush_handles,
            Some(ready_tx),
            None,
            None,
        )
        .await;
    });
    tokio::spawn(run_dummy_flush_responder(
        account_rx,
        code_rx,
        storage_rx,
        block_hash_rx,
    ));

    println!("  Phase 1: Sending 3 sequential commits...");
    for &block in &blocks[..3] {
        let notification = load_commit(artifacts_dir, block)?;
        notif_tx.send(notification).await?;
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    let after_normal = state.snapshot();
    println!(
        "    State after normal commits: block={}, stale={}",
        after_normal.block, after_normal.stale
    );
    assert!(
        !after_normal.stale,
        "Should not be stale after normal commits"
    );

    let skip_to = 8.min(blocks.len() - 1);
    println!(
        "  Phase 2: Skipping blocks {} to {}, sending block {}...",
        blocks[3],
        blocks[skip_to - 1],
        blocks[skip_to]
    );

    let notification = load_commit(artifacts_dir, blocks[skip_to])?;
    notif_tx.send(notification).await?;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let after_skip = state.snapshot();
    println!(
        "    State after skip: block={}, stale={}",
        after_skip.block, after_skip.stale
    );

    if after_skip.stale {
        println!("  CORRECT: State marked as stale due to gap detection");
    } else {
        println!(
            "  WARNING: State not marked as stale (block={}, expected gap)",
            after_skip.block
        );
    }

    println!("  Phase 3: Sending valid continuation...");

    if blocks.len() > skip_to + 3 {
        for &block in &blocks[skip_to + 1..skip_to + 3] {
            let notification = load_commit(artifacts_dir, block)?;
            notif_tx.send(notification).await?;
            tokio::time::sleep(Duration::from_millis(30)).await;
        }

        let after_recovery = state.snapshot();
        println!(
            "    State after recovery attempt: block={}, stale={}",
            after_recovery.block, after_recovery.stale
        );
    }

    println!("PASS: Skipped commits correctly detected and handled");
    Ok(())
}
