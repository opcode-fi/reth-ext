use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use reth_revm::DatabaseRef;
use reth_revm::revm::primitives::{Address, U256};
use reth_tracing::{RethTracer, Tracer};
use rex_sim::RexSim;

const KNOWN_CONTRACTS: &[&str] = &[
    "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
    "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    "0x6B175474E89094C44Da98b954EedeAC495271d0F",
    "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984",
    "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D",
    "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45",
    "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9",
    "0x514910771AF9Ca656af840dff83E8264EcF986CA",
];

#[derive(Default)]
struct Stats {
    account_lookups: AtomicU64,
    account_hits: AtomicU64,
    account_errors: AtomicU64,
    storage_lookups: AtomicU64,
    storage_hits: AtomicU64,
    storage_errors: AtomicU64,
    block_hash_lookups: AtomicU64,
    block_hash_hits: AtomicU64,
    block_hash_errors: AtomicU64,
    latencies_us: parking_lot::Mutex<Vec<u64>>,
}

impl Stats {
    fn record_account_lookup(&self, success: bool, latency: Duration) {
        self.account_lookups.fetch_add(1, Ordering::Relaxed);
        if success {
            self.account_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.account_errors.fetch_add(1, Ordering::Relaxed);
        }
        self.latencies_us.lock().push(latency.as_micros() as u64);
    }

    fn record_storage_lookup(&self, success: bool, latency: Duration) {
        self.storage_lookups.fetch_add(1, Ordering::Relaxed);
        if success {
            self.storage_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.storage_errors.fetch_add(1, Ordering::Relaxed);
        }
        self.latencies_us.lock().push(latency.as_micros() as u64);
    }

    fn record_block_hash_lookup(&self, success: bool, latency: Duration) {
        self.block_hash_lookups.fetch_add(1, Ordering::Relaxed);
        if success {
            self.block_hash_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.block_hash_errors.fetch_add(1, Ordering::Relaxed);
        }
        self.latencies_us.lock().push(latency.as_micros() as u64);
    }

    fn print_report(&self) {
        let account_total = self.account_lookups.load(Ordering::Relaxed);
        let account_hits = self.account_hits.load(Ordering::Relaxed);
        let account_errors = self.account_errors.load(Ordering::Relaxed);

        let storage_total = self.storage_lookups.load(Ordering::Relaxed);
        let storage_hits = self.storage_hits.load(Ordering::Relaxed);
        let storage_errors = self.storage_errors.load(Ordering::Relaxed);

        let block_hash_total = self.block_hash_lookups.load(Ordering::Relaxed);
        let block_hash_hits = self.block_hash_hits.load(Ordering::Relaxed);
        let block_hash_errors = self.block_hash_errors.load(Ordering::Relaxed);

        let mut latencies = self.latencies_us.lock().clone();
        latencies.sort_unstable();

        let p50 = latencies.get(latencies.len() / 2).copied().unwrap_or(0);
        let p95 = latencies
            .get(latencies.len() * 95 / 100)
            .copied()
            .unwrap_or(0);
        let p99 = latencies
            .get(latencies.len() * 99 / 100)
            .copied()
            .unwrap_or(0);
        let avg = if !latencies.is_empty() {
            latencies.iter().sum::<u64>() / latencies.len() as u64
        } else {
            0
        };

        println!("\n========== BENCHMARK RESULTS ==========\n");

        println!("Account Lookups:");
        println!("  Total:  {}", account_total);
        println!(
            "  Hits:   {} ({:.1}%)",
            account_hits,
            if account_total > 0 {
                account_hits as f64 / account_total as f64 * 100.0
            } else {
                0.0
            }
        );
        println!("  Errors: {}", account_errors);

        println!("\nStorage Lookups:");
        println!("  Total:  {}", storage_total);
        println!(
            "  Hits:   {} ({:.1}%)",
            storage_hits,
            if storage_total > 0 {
                storage_hits as f64 / storage_total as f64 * 100.0
            } else {
                0.0
            }
        );
        println!("  Errors: {}", storage_errors);

        println!("\nBlock Hash Lookups:");
        println!("  Total:  {}", block_hash_total);
        println!(
            "  Hits:   {} ({:.1}%)",
            block_hash_hits,
            if block_hash_total > 0 {
                block_hash_hits as f64 / block_hash_total as f64 * 100.0
            } else {
                0.0
            }
        );
        println!("  Errors: {}", block_hash_errors);

        println!("\nLatency (microseconds):");
        println!("  Avg:    {} us", avg);
        println!("  P50:    {} us", p50);
        println!("  P95:    {} us", p95);
        println!("  P99:    {} us", p99);

        let total_ops = account_total + storage_total + block_hash_total;
        println!("\nTotal Operations: {}", total_ops);

        println!("\n========================================\n");
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let _ = RethTracer::new().init()?;

    let args: Vec<String> = std::env::args().collect();
    let rpc_endpoint = args
        .iter()
        .position(|a| a == "--rpc")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
        .unwrap_or("http://127.0.0.1:8545");
    let rex_endpoint = args
        .iter()
        .position(|a| a == "--rex")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
        .unwrap_or("http://127.0.0.1:10000");

    let num_operations: u64 = args
        .iter()
        .position(|a| a == "--ops")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(100000);

    let num_threads: usize = args
        .iter()
        .position(|a| a == "--threads")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);

    println!("RexSim Database Benchmark");
    println!("=========================");
    println!("RPC endpoint: {}", rpc_endpoint);
    println!("Rex endpoint: {}", rex_endpoint);
    println!("Operations:   {}", num_operations);
    println!("Threads:      {}", num_threads);
    println!();

    println!("Subscribing to canon state...");
    let handle = tokio::runtime::Handle::current();
    let canon_rx = rex_cons::subscribe(rex_endpoint, &handle);

    println!("Building RexSim...");
    let mut rex_sim = RexSim::builder().rpc_endpoint(rpc_endpoint).build();

    let (state, ready_rx, _updates) = rex_sim.spawn(canon_rx);
    println!("Waiting for first block...");

    let block = ready_rx.await?;
    println!("Ready at block {}", block);

    let contracts: Vec<Address> = KNOWN_CONTRACTS
        .iter()
        .filter_map(|s| s.parse().ok())
        .collect();

    let stats = Arc::new(Stats::default());

    println!("\nStarting benchmark with {} threads...", num_threads);
    let start = Instant::now();

    let mut handles = Vec::new();
    let ops_per_thread = num_operations / num_threads as u64;

    for thread_id in 0..num_threads {
        let state = Arc::clone(&state);
        let stats = Arc::clone(&stats);
        let contracts = contracts.clone();

        let handle = std::thread::spawn(move || {
            let mut rng = SmallRng::seed_from_u64(thread_id as u64);

            for _ in 0..ops_per_thread {
                let op_type = rng.gen_range(0..3);

                match op_type {
                    0 => {
                        let addr = if rng.gen_bool(0.7) && !contracts.is_empty() {
                            contracts[rng.gen_range(0..contracts.len())]
                        } else {
                            Address::random()
                        };

                        let start = Instant::now();
                        let result = state.basic_ref(addr);
                        let latency = start.elapsed();
                        stats.record_account_lookup(result.is_ok(), latency);
                    }
                    1 => {
                        let addr = if rng.gen_bool(0.8) && !contracts.is_empty() {
                            contracts[rng.gen_range(0..contracts.len())]
                        } else {
                            Address::random()
                        };
                        let slot = U256::from(rng.gen_range(0u64..100));

                        let start = Instant::now();
                        let result = state.storage_ref(addr, slot);
                        let latency = start.elapsed();
                        stats.record_storage_lookup(result.is_ok(), latency);
                    }
                    2 => {
                        let current_block = state.block();
                        let block_num = if current_block > 256 {
                            rng.gen_range(current_block - 256..current_block)
                        } else {
                            rng.gen_range(1..current_block.max(2))
                        };

                        let start = Instant::now();
                        let result = state.block_hash_ref(block_num);
                        let latency = start.elapsed();
                        stats.record_block_hash_lookup(result.is_ok(), latency);
                    }
                    _ => unreachable!(),
                }
            }
        });

        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    let elapsed = start.elapsed();
    println!("Benchmark completed in {:.2?}", elapsed);

    stats.print_report();

    let total_ops = num_operations;
    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
    println!("Throughput: {:.0} ops/sec", ops_per_sec);

    Ok(())
}
