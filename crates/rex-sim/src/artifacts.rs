use std::fs;
use std::path::Path;

use reth_ethereum_primitives::EthPrimitives;
use reth_exex_types::serde_bincode_compat::ExExNotification as BincodeExExNotification;
use rex_cons::ExExNotification;

pub type EthBincodeNotification<'a> = BincodeExExNotification<'a, EthPrimitives>;

pub fn load_notification(path: impl AsRef<Path>) -> eyre::Result<ExExNotification> {
    let data = fs::read(path.as_ref())?;
    let bincode_notification: EthBincodeNotification = bincode::deserialize(&data)?;
    Ok(bincode_notification.into())
}

pub fn load_commit(artifacts_dir: impl AsRef<Path>, block: u64) -> eyre::Result<ExExNotification> {
    let filename = format!("commit_{}_{}.bincode", block, block);
    let path = artifacts_dir.as_ref().join(filename);
    load_notification(path)
}

pub fn load_reorg(
    artifacts_dir: impl AsRef<Path>,
    old_start: u64,
    old_end: u64,
    new_start: u64,
    new_end: u64,
) -> eyre::Result<ExExNotification> {
    let filename = format!(
        "reorg_{}_{}_to_{}_{}.bincode",
        old_start, old_end, new_start, new_end
    );
    let path = artifacts_dir.as_ref().join(filename);
    load_notification(path)
}

pub fn load_revert(
    artifacts_dir: impl AsRef<Path>,
    start: u64,
    end: u64,
) -> eyre::Result<ExExNotification> {
    let filename = format!("revert_{}_{}.bincode", start, end);
    let path = artifacts_dir.as_ref().join(filename);
    load_notification(path)
}

pub fn list_commit_blocks(artifacts_dir: impl AsRef<Path>) -> eyre::Result<Vec<u64>> {
    let mut blocks = Vec::new();

    for entry in fs::read_dir(artifacts_dir)? {
        let entry = entry?;
        let filename = entry.file_name();
        let filename = filename.to_string_lossy();

        if filename.starts_with("commit_")
            && filename.ends_with(".bincode")
            && let Some(block_str) = filename
                .strip_prefix("commit_")
                .and_then(|s| s.split('_').next())
            && let Ok(block) = block_str.parse::<u64>()
        {
            blocks.push(block);
        }
    }

    blocks.sort();
    Ok(blocks)
}

pub fn list_reorgs(artifacts_dir: impl AsRef<Path>) -> eyre::Result<Vec<(u64, u64, u64, u64)>> {
    let mut reorgs = Vec::new();

    for entry in fs::read_dir(artifacts_dir)? {
        let entry = entry?;
        let filename = entry.file_name();
        let filename = filename.to_string_lossy();

        if filename.starts_with("reorg_") && filename.ends_with(".bincode") {
            let parts: Vec<&str> = filename
                .strip_prefix("reorg_")
                .unwrap_or("")
                .strip_suffix(".bincode")
                .unwrap_or("")
                .split("_to_")
                .collect();

            if parts.len() == 2 {
                let old_parts: Vec<&str> = parts[0].split('_').collect();
                let new_parts: Vec<&str> = parts[1].split('_').collect();

                if old_parts.len() == 2
                    && new_parts.len() == 2
                    && let (Ok(os), Ok(oe), Ok(ns), Ok(ne)) = (
                        old_parts[0].parse::<u64>(),
                        old_parts[1].parse::<u64>(),
                        new_parts[0].parse::<u64>(),
                        new_parts[1].parse::<u64>(),
                    )
                {
                    reorgs.push((os, oe, ns, ne));
                }
            }
        }
    }

    reorgs.sort();
    Ok(reorgs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_commit_blocks() {
        let artifacts_dir = "../rex-cons/artifacts";
        if Path::new(artifacts_dir).exists() {
            let blocks = list_commit_blocks(artifacts_dir).unwrap();
            assert!(!blocks.is_empty());

            for window in blocks.windows(2) {
                assert!(window[0] < window[1]);
            }
        }
    }
}
