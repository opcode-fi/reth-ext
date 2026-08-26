use reth_revm::revm::primitives::B256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockInfo {
    pub number: u64,
    pub hash: B256,
    pub parent_hash: B256,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CurrentState {
    pub block: u64,
    pub block_hash: B256,
    pub stale: bool,
}

impl CurrentState {
    pub fn initial() -> Self {
        Self {
            block: 0,
            block_hash: B256::ZERO,
            stale: false,
        }
    }

    pub fn is_initial(&self) -> bool {
        self.block == 0 || self.block_hash.is_zero()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitValidation {
    Valid,
    AcceptAny,
    Gap { expected: u64, got: u64 },
    OverlapMismatch,
}

impl CommitValidation {
    pub fn is_acceptable(&self) -> bool {
        matches!(self, CommitValidation::Valid | CommitValidation::AcceptAny)
    }
}

pub fn validate_chain_committed<I>(
    current: CurrentState,
    first_block: BlockInfo,
    chain_blocks: I,
) -> CommitValidation
where
    I: IntoIterator<Item = (u64, B256)>,
{
    if current.is_initial() || current.stale {
        return CommitValidation::AcceptAny;
    }

    if first_block.number == current.block + 1 && first_block.parent_hash == current.block_hash {
        return CommitValidation::Valid;
    }

    if first_block.number <= current.block {
        for (num, hash) in chain_blocks {
            if num == current.block && hash == current.block_hash {
                return CommitValidation::Valid;
            }
        }

        return CommitValidation::OverlapMismatch;
    }

    CommitValidation::Gap {
        expected: current.block + 1,
        got: first_block.number,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReorgValidation {
    Valid,
    AcceptAny,
    Mismatch,
}

impl ReorgValidation {
    pub fn is_acceptable(&self) -> bool {
        matches!(self, ReorgValidation::Valid | ReorgValidation::AcceptAny)
    }
}

pub fn validate_chain_reorged<I>(
    current: CurrentState,
    old_tip: BlockInfo,
    old_chain_blocks: I,
) -> ReorgValidation
where
    I: IntoIterator<Item = (u64, B256)>,
{
    if current.is_initial() {
        return ReorgValidation::AcceptAny;
    }

    if old_tip.hash == current.block_hash {
        return ReorgValidation::Valid;
    }

    for (num, hash) in old_chain_blocks {
        if num == current.block && hash == current.block_hash {
            return ReorgValidation::Valid;
        }
    }

    ReorgValidation::Mismatch
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CacheValidation {
    Valid,
    Empty,
    Stale,
}

pub fn validate_cache(
    cache_block_hash: B256,
    cache_is_empty: bool,
    current_block_hash: B256,
) -> CacheValidation {
    if cache_is_empty {
        return CacheValidation::Empty;
    }
    if cache_block_hash == current_block_hash {
        return CacheValidation::Valid;
    }
    CacheValidation::Stale
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(n: u64) -> B256 {
        let mut bytes = [0u8; 32];
        bytes[24..32].copy_from_slice(&n.to_be_bytes());
        B256::from(bytes)
    }

    mod chain_committed {
        use super::*;

        #[test]
        fn initial_state_accepts_any() {
            let current = CurrentState::initial();
            let first = BlockInfo {
                number: 100,
                hash: hash(100),
                parent_hash: hash(99),
            };

            let result = validate_chain_committed(current, first, std::iter::empty());
            assert_eq!(result, CommitValidation::AcceptAny);
            assert!(result.is_acceptable());
        }

        #[test]
        fn stale_state_accepts_any() {
            let current = CurrentState {
                block: 50,
                block_hash: hash(50),
                stale: true,
            };
            let first = BlockInfo {
                number: 100,
                hash: hash(100),
                parent_hash: hash(99),
            };

            let result = validate_chain_committed(current, first, std::iter::empty());
            assert_eq!(result, CommitValidation::AcceptAny);
            assert!(result.is_acceptable());
        }

        #[test]
        fn perfect_continuity_is_valid() {
            let current = CurrentState {
                block: 100,
                block_hash: hash(100),
                stale: false,
            };

            let first = BlockInfo {
                number: 101,
                hash: hash(101),
                parent_hash: hash(100),
            };

            let result = validate_chain_committed(current, first, std::iter::empty());
            assert_eq!(result, CommitValidation::Valid);
            assert!(result.is_acceptable());
        }

        #[test]
        fn perfect_continuity_wrong_parent_hash() {
            let current = CurrentState {
                block: 100,
                block_hash: hash(100),
                stale: false,
            };

            let first = BlockInfo {
                number: 101,
                hash: hash(101),
                parent_hash: hash(999),
            };

            let result = validate_chain_committed(current, first, std::iter::empty());

            assert_eq!(
                result,
                CommitValidation::Gap {
                    expected: 101,
                    got: 101
                }
            );
            assert!(!result.is_acceptable());
        }

        #[test]
        fn overlap_with_match_is_valid() {
            let current = CurrentState {
                block: 100,
                block_hash: hash(100),
                stale: false,
            };

            let first = BlockInfo {
                number: 98,
                hash: hash(98),
                parent_hash: hash(97),
            };
            let chain_blocks = vec![
                (98, hash(98)),
                (99, hash(99)),
                (100, hash(100)),
                (101, hash(101)),
            ];

            let result = validate_chain_committed(current, first, chain_blocks);
            assert_eq!(result, CommitValidation::Valid);
            assert!(result.is_acceptable());
        }

        #[test]
        fn overlap_without_match_is_invalid() {
            let current = CurrentState {
                block: 100,
                block_hash: hash(100),
                stale: false,
            };

            let first = BlockInfo {
                number: 98,
                hash: hash(98),
                parent_hash: hash(97),
            };
            let chain_blocks = vec![
                (98, hash(98)),
                (99, hash(99)),
                (100, hash(9999)),
                (101, hash(101)),
            ];

            let result = validate_chain_committed(current, first, chain_blocks);
            assert_eq!(result, CommitValidation::OverlapMismatch);
            assert!(!result.is_acceptable());
        }

        #[test]
        fn gap_is_detected() {
            let current = CurrentState {
                block: 100,
                block_hash: hash(100),
                stale: false,
            };

            let first = BlockInfo {
                number: 105,
                hash: hash(105),
                parent_hash: hash(104),
            };

            let result = validate_chain_committed(current, first, std::iter::empty());
            assert_eq!(
                result,
                CommitValidation::Gap {
                    expected: 101,
                    got: 105
                }
            );
            assert!(!result.is_acceptable());
        }

        #[test]
        fn same_block_different_hash_detected() {
            let current = CurrentState {
                block: 100,
                block_hash: hash(100),
                stale: false,
            };

            let first = BlockInfo {
                number: 100,
                hash: hash(9999),
                parent_hash: hash(99),
            };
            let chain_blocks = vec![(100, hash(9999))];

            let result = validate_chain_committed(current, first, chain_blocks);
            assert_eq!(result, CommitValidation::OverlapMismatch);
            assert!(!result.is_acceptable());
        }
    }

    mod chain_reorged {
        use super::*;

        #[test]
        fn initial_state_accepts_any() {
            let current = CurrentState::initial();
            let old_tip = BlockInfo {
                number: 100,
                hash: hash(100),
                parent_hash: hash(99),
            };

            let result = validate_chain_reorged(current, old_tip, std::iter::empty());
            assert_eq!(result, ReorgValidation::AcceptAny);
            assert!(result.is_acceptable());
        }

        #[test]
        fn perfect_match_is_valid() {
            let current = CurrentState {
                block: 100,
                block_hash: hash(100),
                stale: false,
            };

            let old_tip = BlockInfo {
                number: 100,
                hash: hash(100),
                parent_hash: hash(99),
            };

            let result = validate_chain_reorged(current, old_tip, std::iter::empty());
            assert_eq!(result, ReorgValidation::Valid);
            assert!(result.is_acceptable());
        }

        #[test]
        fn our_state_in_old_chain_is_valid() {
            let current = CurrentState {
                block: 98,
                block_hash: hash(98),
                stale: false,
            };

            let old_tip = BlockInfo {
                number: 100,
                hash: hash(100),
                parent_hash: hash(99),
            };
            let old_chain = vec![(98, hash(98)), (99, hash(99)), (100, hash(100))];

            let result = validate_chain_reorged(current, old_tip, old_chain);
            assert_eq!(result, ReorgValidation::Valid);
            assert!(result.is_acceptable());
        }

        #[test]
        fn mismatch_is_detected() {
            let current = CurrentState {
                block: 100,
                block_hash: hash(100),
                stale: false,
            };

            let old_tip = BlockInfo {
                number: 98,
                hash: hash(98),
                parent_hash: hash(97),
            };
            let old_chain = vec![
                (95, hash(95)),
                (96, hash(96)),
                (97, hash(97)),
                (98, hash(98)),
            ];

            let result = validate_chain_reorged(current, old_tip, old_chain);
            assert_eq!(result, ReorgValidation::Mismatch);
            assert!(!result.is_acceptable());
        }

        #[test]
        fn same_block_different_hash_is_mismatch() {
            let current = CurrentState {
                block: 100,
                block_hash: hash(100),
                stale: false,
            };

            let old_tip = BlockInfo {
                number: 100,
                hash: hash(9999),
                parent_hash: hash(99),
            };
            let old_chain = vec![(100, hash(9999))];

            let result = validate_chain_reorged(current, old_tip, old_chain);
            assert_eq!(result, ReorgValidation::Mismatch);
            assert!(!result.is_acceptable());
        }
    }

    mod cache_validation {
        use super::*;

        #[test]
        fn empty_cache_is_valid() {
            let result = validate_cache(hash(100), true, hash(200));
            assert_eq!(result, CacheValidation::Empty);
        }

        #[test]
        fn matching_hash_is_valid() {
            let result = validate_cache(hash(100), false, hash(100));
            assert_eq!(result, CacheValidation::Valid);
        }

        #[test]
        fn mismatched_hash_is_stale() {
            let result = validate_cache(hash(100), false, hash(200));
            assert_eq!(result, CacheValidation::Stale);
        }

        #[test]
        fn zero_hash_cache_with_entries_is_stale() {
            let result = validate_cache(B256::ZERO, false, hash(100));
            assert_eq!(result, CacheValidation::Stale);
        }
    }
}
