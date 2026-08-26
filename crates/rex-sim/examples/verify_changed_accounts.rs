use reth_revm::DatabaseRef;
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

    info!(target: "verify_changed_accounts", %rex_endpoint, %rpc_endpoint, "starting");

    let handle = tokio::runtime::Handle::current();
    let canon_rx = rex_cons::subscribe(rex_endpoint, &handle);

    let mut rex_sim = RexSim::builder().rpc_endpoint(rpc_endpoint).build();
    let (state, ready_rx, mut updates) = rex_sim.spawn(canon_rx);

    let ready_block = ready_rx.await.expect("ready signal failed");
    info!(target: "verify_changed_accounts", block = ready_block, "ready");

    while updates.changed().await.is_ok() {
        let block = updates.borrow().block;
        match state.changed_accounts(block) {
            Some(accounts) => {
                info!(
                    target: "verify_changed_accounts",
                    block,
                    changed = accounts.len(),
                    "changed accounts retained for block"
                );
                for a in accounts {
                    let balance = state.basic_ref(a).ok().flatten().map(|i| i.balance);
                    info!(
                        target: "verify_changed_accounts",
                        block,
                        address = %a,
                        ?balance,
                        "  changed account + current native balance"
                    );
                }
            }
            None => info!(
                target: "verify_changed_accounts",
                block,
                "NO changed-accounts entry retained (unexpected for a fresh commit)"
            ),
        }
    }

    info!(target: "verify_changed_accounts", "update channel closed");
}
