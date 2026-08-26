use reth_tracing::{RethTracer, Tracer, tracing::info};
use rex_cons::{EthBincodeNotification, ExExNotification, NotificationStream};
use std::fs;
use std::path::Path;

const ARTIFACTS_DIR: &str = "artifacts";

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let _ = RethTracer::new().init()?;

    let artifacts_path = Path::new(ARTIFACTS_DIR);
    if !artifacts_path.exists() {
        fs::create_dir_all(artifacts_path)?;
        info!("Created artifacts directory");
    }

    let endpoint = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:10000".to_string());

    let mut stream = NotificationStream::new(endpoint);

    loop {
        let notification = stream.next().await;

        let filename = match &notification {
            ExExNotification::ChainCommitted { new } => {
                let range = new.range();
                info!(committed_chain = ?range, "Received commit");
                format!("commit_{}_{}.bincode", range.start(), range.end())
            }
            ExExNotification::ChainReorged { old, new } => {
                let old_range = old.range();
                let new_range = new.range();
                info!(from_chain = ?old_range, to_chain = ?new_range, "Received reorg");
                format!(
                    "reorg_{}_{}_to_{}_{}.bincode",
                    old_range.start(),
                    old_range.end(),
                    new_range.start(),
                    new_range.end()
                )
            }
            ExExNotification::ChainReverted { old } => {
                let range = old.range();
                info!(reverted_chain = ?range, "Received revert");
                format!("revert_{}_{}.bincode", range.start(), range.end())
            }
        };

        let filepath = artifacts_path.join(&filename);
        let bincode_notification: EthBincodeNotification<'_> = (&notification).into();
        let enc = bincode::serialize(&bincode_notification).expect("failed to serialize");

        if let Err(e) = fs::write(&filepath, enc) {
            info!(%e, "Failed to write notification to file");
        } else {
            info!(file = %filepath.display(), "Saved notification");
        }
    }
}
