use reth_tracing::{RethTracer, Tracer, tracing::info};
use rex_sim::RexSim;

const DEFAULT_REX_ENDPOINT: &str = "http://127.0.0.1:10000";
const DEFAULT_RPC_ENDPOINT: &str = "http://127.0.0.1:8545";

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

    info!(
        target: "live_update",
        rex_endpoint = %rex_endpoint,
        rpc_endpoint = %rpc_endpoint,
        "Starting RexSim"
    );

    let handle = tokio::runtime::Handle::current();
    let canon_rx = rex_cons::subscribe(rex_endpoint, &handle);

    let mut rex_sim = RexSim::builder().rpc_endpoint(rpc_endpoint).build();

    let (_state, ready_rx, mut updates) = rex_sim.spawn(canon_rx);

    let block = ready_rx.await.expect("Ready signal failed");
    info!(target: "live_update", block = block, "Ready");

    while updates.changed().await.is_ok() {
        let update = updates.borrow();
        info!(
            target: "live_update",
            block = update.block,
            generation = update.generation,
            "Block updated"
        );
    }

    info!(target: "live_update", "Update channel closed");
}
