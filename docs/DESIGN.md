# Design notes

How the pieces actually work, and the invariants they hold. Read
[the README](../README.md) first for what they are.

## 1. The wire

`reth-rex` is a stock `EthereumNode` with one ExEx installed. The ExEx does two
separate things, and the split matters:

- **Payload source: `ctx.provider().canonical_state_stream()`.** Every
  `CanonStateNotification::{Commit, Reorg}` is converted to an
  `ExExNotification`, bincode-encoded through
  `reth_exex_types::serde_bincode_compat`, and pushed to a
  `broadcast::channel(32)` that the gRPC service subscribes to per client.
- **Watermark: `ctx.notifications`.** The ExEx's own notification stream is
  consumed only to call `send_finished_height`, which lets reth prune the ExEx
  WAL. Nothing is forwarded from it.

Using the canonical stream rather than the ExEx stream is what keeps latency at
"the moment the node committed" instead of "after the WAL round trip". The cost
is that a slow client can fall off the back of the broadcast channel — 32 blocks
of slack on mainnet, ~6 minutes.

The proto is one `bytes` field ([`exex.proto`](../crates/exex-proto/proto/exex.proto)).
Both sides set `max_encoding_message_size`/`max_decoding_message_size` to
`usize::MAX`: a busy block's `ExecutionOutcome` runs to tens of megabytes and the
default 4 MB tonic cap would drop it. HTTP/2 keepalive is 300s with a 60s
timeout on both ends.

`rex-cons` owns the reconnect logic so callers never see the seam:

- `subscribe()` spawns a dedicated `rex-cons` thread that owns the async
  connection and hands `ExExNotification`s out over an `mpsc::channel(64)`.
- Connection failures retry with exponential backoff, 1s doubling to 30s.
- A **decode** failure logs and skips that one message rather than tearing down
  the stream.
- Backoff resets on the first successfully decoded notification, not on connect.

There is no replay: a client that reconnects resumes at the live tip and has a
hole. `rex-sim` detects that hole rather than papering over it — see §4.

## 2. rex-sim: the read path

`StaticState` is an `ArcSwap<StateSnapshot>` plus an `AtomicU64` generation
counter, and it implements `revm::DatabaseRef`. Every read is:

```
basic_ref(addr)
  ├── snapshot.stale?          → Err(SimError::StaleState)
  ├── snapshot.accounts hit?   → clone, return          ← the common case, no I/O
  └── miss
        ├── crossbeam::bounded(1) reply channel
        ├── unbounded mpsc → AccountFetchManager, tagged with (block, block_hash)
        └── rx.recv_timeout(wait_timeout_ms)            ← blocks this thread
```

`DatabaseRef` is synchronous and revm calls it from wherever it is running, so
the miss path parks the calling thread on a crossbeam channel while an async
fetch manager does the RPC. That is the reason for the builder's assertion that
`wait_timeout_ms > rpc_timeout_ms`: the waiter must outlive the request it is
waiting on, or it reports a timeout for a fetch that was going to succeed.

Requests carry the **block hash the snapshot was at when the read happened**, not
just a number. Everything downstream keys off that hash.

## 3. rex-sim: the fetch managers

Four managers — account, code, storage, block hash — are one generic
`FetchManager<S: FetchSpec>` ([`manager_core.rs`](../crates/rex-sim/src/manager_core.rs))
instantiated four ways ([`managers.rs`](../crates/rex-sim/src/managers.rs)). Each
one owns a `HashMap` cache, a `Semaphore` bounding in-flight RPCs, and an epoch
counter.

**Tag invalidation.** A request whose `tag` (the block hash) differs from the
cache's current tag clears the cache and bumps the epoch. The cache only ever
holds values read at one block.

**Epoch guarding.** A fetch spawned under epoch *N* that returns after the epoch
moved on still **answers its caller** — that caller asked at block *N* and the
value is correct for it — but is **not stored** in the cache. This is the
subtle one: without it, a fetch issued before a reorg would poison the
post-reorg cache with pre-reorg state.

**Code is exempt** (`EPOCH_GUARDED = false`, `tag() = None`). Bytecode is keyed
by its own hash and is immutable, so it is valid at every block and the code
cache is never invalidated.

**Code hints.** `code_by_hash_ref` gives you a hash, but Ethereum's RPC has no
`eth_getCodeByHash` — you need an address. When the account manager resolves an
account it forwards `(code_hash → address)` to the code manager over a side
channel, which the code manager accumulates in `CodeHints`. A hash with no hint
and no explicit `address_hint` fails with `SimError::MissingAddressHint`.
Fetched code is verified: a `keccak256(code) != code_hash` mismatch is
`SimError::CodeHashMismatch`, never a silent wrong answer.

## 4. rex-sim: the write path

`run_changeset_manager` ([`changeset.rs`](../crates/rex-sim/src/changeset.rs)) is
the only writer. Per notification:

1. **Validate continuity** against the current snapshot
   ([`validation.rs`](../crates/rex-sim/src/validation.rs)):

   | verdict | meaning |
   | --- | --- |
   | `Valid` | the chain connects to what we hold |
   | `AcceptAny` | we hold nothing (block 0 / zero hash) or we are stale — adopt unconditionally |
   | `Gap` | first block ≠ current + 1 |
   | `OverlapMismatch` | ranges overlap but no block matches our hash |

2. **Flush the four caches.** Each manager is asked for its cache over a
   `oneshot`; it drains in-flight fetches up to `flush_timeout_ms`, hands the map
   over, and bumps its epoch. This is the barrier that makes step 3 a consistent
   merge instead of a race.

3. **Build the new snapshot** ([`transitions.rs`](../crates/rex-sim/src/transitions.rs)):
   merge the flushed caches (entries tagged with a hash that is no longer the
   pre-state hash are discarded and counted), apply the chain's
   `ExecutionOutcome` — account/storage/code diffs, receipts, changed accounts,
   the sealed header and withdrawals — then swap it in and bump the generation.

4. **Notify.** `StateUpdate { block, block_hash, generation }` on a
   `watch::Sender`; an optional `LogSink` receives the block's logs; the first
   applied notification fires the `ready` oneshot.

Reorgs run the same shape via `build_reorg_state`: revert the old chain's
changes, apply the new chain's, and truncate receipt and changed-account history
from the reorg point. Reverts (`ChainReverted`) revert without applying.

**Staleness is a latch.** A `Gap` or `OverlapMismatch` flushes the caches, marks
the snapshot stale, and swaps it in. From then on *every* `DatabaseRef` read
returns `SimError::StaleState` — the sim refuses to serve state it cannot prove
— until a commit arrives that validates as `AcceptAny` and re-establishes a tip
from scratch. A dropped connection, a missed block, a client that fell off the
broadcast channel: all of them land here rather than producing quietly wrong
simulations.

## 5. Detecting a torn read

The generation counter is exposed for callers whose simulation spans a tip
advance:

```rust
let guard = state.guard();          // (generation, block_hash, stale)
// … a simulation that takes longer than a block …
match state.check_guard(&guard) {
    StateValidity::Valid          => { /* result is good */ }
    StateValidity::Changed { .. } => { /* tip moved — discard */ }
    StateValidity::WasStale
    | StateValidity::BecameStale  => { /* no usable state */ }
}
```

`StaticState::snapshot()` returns an `Arc<StateSnapshot>`, so a caller that wants
a genuinely frozen view can hold one and read it directly — at the cost of losing
the lazy-fetch path, which only exists on the `DatabaseRef` impl.

## 6. History depth

`RECEIPTS_HISTORY_DEPTH = 64` bounds both the receipt history and the
changed-accounts history. On mainnet that is ~12.8 minutes of lookback for
`state.receipts(block)` and `state.changed_accounts(block)`. Beyond it, go to the
node.

## 7. Tuning

`FetchConfig`, passed to `RexSimBuilder::fetch()`, is `serde::Deserialize` with
`deny_unknown_fields`, so it drops straight into a config file.

| field | default | what it bounds |
| --- | --- | --- |
| `account` / `code` / `storage` / `block_hash` | 100 each | in-flight RPCs per manager (semaphore permits, clamped to 1..=1024) |
| `rpc_timeout_ms` | 10_000 | one HTTP round trip |
| `wait_timeout_ms` | 30_000 | the sync `DatabaseRef` waiter — **must exceed `rpc_timeout_ms`** |
| `flush_timeout_ms` | 15_000 | draining in-flight fetches during a changeset flush |

Two more constants are sized in **blocks**, so they are the ones to revisit when
porting to a faster chain:

| constant | where | mainnet (12s) | BSC (3s) |
| --- | --- | --- | --- |
| `RECEIPTS_HISTORY_DEPTH = 64` | `rex-sim/src/state.rs` | 12.8 min | 3.2 min |
| `broadcast::channel(32)` | `reth-rex/src/main.rs` | 6.4 min of client slack | 1.6 min |

## 8. Testing

```bash
cd crates/rex-sim && cargo test              # 63 offline tests
cd crates/rex-sim && cargo test -- --ignored # 3 more, needs a node on :8545
```

Coverage is concentrated where the bugs are:

- `transitions.rs` (28) — commit/reorg/revert snapshot construction and cache merging.
- `validation.rs` (17) — the continuity verdicts, including every gap and overlap shape.
- `manager_tests.rs` (13) — the fetch managers against a `MockRpc`: tag
  invalidation, epoch guarding, flush drain, code-hint resolution, hash mismatch.
- `state.rs` (4) — history ring-buffer bounds.

Above the unit tests there are two integration levers:

- **`examples/test_artifacts`** replays captured notifications (commits *and*
  reorgs) through the real changeset manager with no node attached. Capture them
  with `rex-cons`'s `subscribe` example.
- **`examples/parity`** is the end-to-end oracle. For each live block it
  re-executes every transaction against the snapshot with revm and diffs the
  result against the node's own `eth_call` and/or `eth_simulateV1` at that block,
  reporting a per-mode parity rate. If a state-transition change is wrong, parity
  is where it shows up.
