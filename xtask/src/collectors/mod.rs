use anyhow::Result;
use std::path::Path;

use crate::report::CategoryMap;

pub mod structural;

/// A collector extracts metric violations from one source.
///
/// Each collector returns a [`CategoryMap`] of `category → entity → excess`.
/// The ratchet engine is agnostic about which collector produced which entry.
pub trait Collector {
    #[allow(dead_code)] // used once a second collector lands; keeps the trait shape stable.
    fn name(&self) -> &str;
    fn collect(&self, workspace_root: &Path) -> Result<CategoryMap>;
}
