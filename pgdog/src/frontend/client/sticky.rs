//! Sticky settings for clients that override
//! default routing behavior determined by the query parser.

use rand::{rng, Rng};

#[derive(Debug, Clone, Copy)]
pub struct Sticky {
    /// Which shard to use for omnisharded queries, making them
    /// stick to only one database.
    pub omni_index: usize,
}

impl Default for Sticky {
    fn default() -> Self {
        Self::new()
    }
}

impl Sticky {
    pub fn new() -> Self {
        Self {
            omni_index: rng().random_range(1..usize::MAX),
        }
    }

    #[cfg(test)]
    pub fn new_test() -> Self {
        Self { omni_index: 1 }
    }
}
