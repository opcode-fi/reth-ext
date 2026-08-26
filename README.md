# reth-ext

Simulating against the head of the chain usually costs an RPC round trip for
every account, slot and contract you touch, and hands back an answer that was
stale before it arrived. **reth-ext moves the state into your process instead:**
an ExEx streams every canonical-state notification out of a
[reth](https://github.com/paradigmxyz/reth) node over gRPC, and a
`revm::DatabaseRef` applies the diffs in memory — so `basic_ref` and
`storage_ref` become hash-map reads at the current block, with no network on the
hot path. You see the tip move the instant the node commits it, reorgs revert and
re-apply cleanly, and when the sim cannot prove which block it is standing on it
errors instead of handing you a plausible wrong number.

Nothing here is chain-strategy code — it is the plumbing under it.

```
               reth node                                     your process
  ┌───────────────────────────────┐               ┌────────────────────────────────┐
  │ EthereumNode::default()       │               │ rex-cons                       │
  │   └ ExEx "reth-rex-ext"       │               │   reconnect + bincode decode   │
  │       canonical_state_stream  │───── :10000 ─►│              │                 │
  │                               │               │              ▼                 │
  │                               │               │ rex-sim                        │
  │ http rpc                      │               │   StateSnapshot @ tip          │
  │                               │◄──── :8545 ───│   impl revm::DatabaseRef       │
  │                               │               │              │                 │
  └───────────────────────────────┘               │              ▼                 │
                                                  │            revm                │
                                                  └────────────────────────────────┘
```

The `:8545` arrow is the cold path. It fires once per account, slot or bytecode
the sim has never seen, and never again while that entry stays live — everything
after that comes from the stream.

## The crates

| crate | what it is |
| --- | --- |
| [`reth-rex`](crates/reth-rex) | A reth node binary — stock `EthereumNode`, plus one ExEx that forwards every `CanonStateNotification` (commit and reorg) as a bincode blob over a gRPC stream on `:10000`. 136 lines. |
| [`exex-proto`](crates/exex-proto) | The protobuf service both sides speak. One rpc, one message, one `bytes` field. |
| [`rex-cons`](crates/rex-cons) | The client. `subscribe(endpoint, handle) -> mpsc::Receiver<ExExNotification>` on a dedicated thread, with exponential-backoff reconnect and mid-stream decode-error recovery. |
| [`rex-sim`](crates/rex-sim) | The state layer. Keeps a `StateSnapshot` at chain tip by applying each notification's `ExecutionOutcome`, hydrates whatever it has never seen from the node's RPC, and exposes it as a `revm::DatabaseRef`. |
| [`provider-ext`](crates/provider-ext) | Small alloy conveniences used alongside the above: `call_sol` / `call_raw` on any `Provider`, plus two async cache helpers. Independent of the rest. |

`reth-rex` and `rex-cons` are deliberately tiny and boring. The substance is in
`rex-sim` — see [docs/DESIGN.md](docs/DESIGN.md).

## Why not just use the RPC

You can, and `rex-sim` still does for cold state. The difference is what happens
per simulation:

- **State is local.** Once an account/slot has been touched it stays in the
  snapshot and is updated from execution outcomes, not re-fetched. A hot
  simulation does zero network I/O.
- **You know when the ground moved.** Every applied notification bumps a
  generation counter. `StaticState::guard()` / `check_guard()` let you detect
  that the block changed underneath a simulation that was already in flight, so
  you can discard the result rather than act on a torn read.
- **Reorgs are events, not surprises.** `ChainReorged` reverts the old chain's
  changes and applies the new one's, including truncating receipt history.
  A gap or a hash mismatch marks the snapshot `stale` and every read errors with
  `SimError::StaleState` until a clean commit re-establishes the tip — it never
  serves state it cannot prove.

## Quick start

### 1. Run the node

You need a synced execution client. `deploy/` builds reth-rex plus a lighthouse
beacon node sharing one engine-API JWT:

```bash
./deploy/setup.sh                                     # data dirs + JWT + deploy/.env
docker compose -f deploy/docker-compose.yml up -d --build
```

Exposed on loopback: `8545` (http rpc), `8546` (ws), `10000` (the ExEx stream).
`30303` and the lighthouse p2p ports are the only public ones. Initial sync is
the usual multi-day affair. `deploy/reth.toml` prunes to what `rex-sim` actually
reads: sender recovery, transaction lookup, account/storage/bodies history all
keep a 10,064-block window, and receipts are dropped below the beacon deposit
contract's deployment block (11,052,984), keeping that contract's own logs.

To point the ExEx at an existing node instead, `reth-rex` takes the same CLI as
`reth` — see `deploy/reth.sh`.

### 2. Consume the stream

```bash
cd crates/rex-cons
cargo run --release --example subscribe -- http://127.0.0.1:10000
```

Writes each notification to `artifacts/` as bincode. Those files are the input
to `rex-sim`'s offline replay tests, so capture a few hundred blocks once and you
can iterate on state-transition logic with no node running.

### 3. Simulate against tip

```rust
use rex_sim::RexSim;
use rex_sim::state::StateValidity;
use reth_revm::{Context, DatabaseRef, MainBuilder, MainContext};

let handle = tokio::runtime::Handle::current();
let canon_rx = rex_cons::subscribe("http://127.0.0.1:10000", &handle);

let mut sim = RexSim::builder()
    .rpc_endpoint("http://127.0.0.1:8545")
    .build();

// `ready` fires on the first notification applied; `updates` is a watch channel
// carrying (block, block_hash, generation) after every applied notification.
let (state, ready, mut updates) = sim.spawn(canon_rx);
let first_block = ready.await?;

loop {
    updates.changed().await?;
    let guard = state.guard();

    // `state` is a revm DatabaseRef. Read it directly, or hand it to revm.
    // Cold entries are fetched from the node's RPC transparently.
    let info = state.basic_ref(some_address)?;

    let mut evm = Context::mainnet().with_ref_db(&*state).build_mainnet();
    // …simulate…

    if state.check_guard(&guard) != StateValidity::Valid {
        continue; // tip moved mid-simulation; the result is stale
    }
}
```

Live version: [`crates/rex-sim/examples/live_update.rs`](crates/rex-sim/examples/live_update.rs).

## Examples

Run from inside the crate directory.

**rex-sim**

| example | what it does |
| --- | --- |
| `live_update` | Minimal loop: subscribe, spawn, print each tip advance. Start here. |
| `verify_changed_accounts` | Cross-checks the accounts the notification says changed against what the snapshot holds. |
| `parity` | **The correctness harness.** Re-executes every transaction of each incoming block against the snapshot with revm and diffs the outputs against the node's own `eth_call` and/or `eth_simulateV1` at that block. Writes a report with per-mode parity rates. `cargo run --release --example parity -- <rex> <rpc> [call\|simulate\|both]`. |
| `bench_db` | Randomised read load over known mainnet contracts; reports hit rates and latency for the account/storage/code paths. |
| `test_artifacts` | Replays captured `artifacts/` notifications — commits and reorgs — through the changeset manager with no node attached. |

**rex-cons**

| example | what it does |
| --- | --- |
| `subscribe` | Connects and writes every notification to `artifacts/` as bincode. |

## Building

Each crate is a **standalone cargo project with its own `Cargo.lock`**, not a
workspace member. That is deliberate: `reth-rex` pulls the entire reth node dep
tree and pins `alloy-rpc-types` exactly, while `rex-sim` needs only revm and the
primitives. Keeping them separate stops one from dragging the other's resolution
around, and lets the Docker build copy two directories instead of the repo.

```bash
cd crates/rex-sim && cargo build --release --locked
```

`rex-cons` and `rex-sim` declare `alloy-rpc-types` and `alloy-rpc-types-engine`
at `=2.0.5` without using them directly. **That is deliberate and load-bearing.**
A git dependency does not carry its `Cargo.lock`, so a consumer resolving these
crates fresh picks up a newer alloy and reth v2.3.0 stops compiling
(`missing field target_gas_limit in EthPayloadAttributes`). The pin is the only
thing that travels with the dependency. Do not remove it as an unused dep.

The locks are committed and `--locked` is the intended way to build: every reth
dependency is a **git tag pin (`v2.3.0`)**, so the lock is the only thing making
the build reproducible. Bumping reth means bumping the tag in all three manifests
that depend on it — `reth-rex`, `rex-cons`, `rex-sim` — and regenerating each lock
together.

Tests are offline and fast:

```bash
cd crates/rex-sim && cargo test          # 63 tests, no node required
```

Requirements: Rust 1.85+ (edition 2024; the Docker build uses 1.93),
`protobuf-compiler` for `exex-proto`'s build script, and `libclang-dev` +
`pkg-config` + `libssl-dev` for reth's native deps.

Dependency edges are the only thing tying the crates together:

```
exex-proto ──┬──► reth-rex     (node binary)
             └──► rex-cons ──► rex-sim
provider-ext (independent)
```

## Other chains

The bridge is only `CanonStateSubscriptions` plus bincode, so the node side ports
to a reth fork by installing the same ExEx there — we have run it against a BSC
node this way and the **consumer side needed no changes at all**, because
`ExecutionOutcome`, the payload `rex-sim` actually consumes, is the same type
regardless of the chain's primitives. Only the Ethereum node binary ships here;
the fork does not.

What does need attention on a faster chain is the tuning. `RECEIPTS_HISTORY_DEPTH`
and the node-side broadcast channel are sized in *blocks*, so a 3-second block
time cuts the wall-clock slack they buy you by 4×. See
[docs/DESIGN.md](docs/DESIGN.md#7-tuning).

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your
option.
