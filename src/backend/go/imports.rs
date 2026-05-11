//! Import tracking for the Go backend's emit pipeline.
//!
//! [`GoImports`] collects standard-library and extra imports needed by an
//! emitted file and renders them as a single `import (...)` block, with the
//! standard library and external imports separated by a blank line.

use std::collections::BTreeSet;

#[derive(Default)]
pub(super) struct GoImports {
    pub(super) context: bool,
    pub(super) database_sql: bool,
    pub(super) encoding_json: bool,
    pub(super) fmt: bool,
    pub(super) strings: bool,
    pub(super) time: bool,
    pub(super) extra: BTreeSet<String>,
}

impl GoImports {
    /// Add an import path. Standard-library paths recognised here are promoted
    /// to dedicated bool flags; everything else goes into `extra`.
    pub(super) fn add_import(&mut self, imp: Option<String>) {
        match imp.as_deref() {
            Some("\"time\"") => self.time = true,
            Some("\"database/sql\"") => self.database_sql = true,
            Some(s) => {
                self.extra.insert(s.to_string());
            },
            None => {},
        }
    }

    /// Return true if any import is needed.
    pub(super) fn has_any(&self) -> bool {
        self.context || self.database_sql || self.encoding_json || self.fmt || self.strings || self.time || !self.extra.is_empty()
    }

    /// Render the collected imports as a Go `import (...)` block appended to `src`.
    pub(super) fn write(&self, src: &mut String) {
        let mut std_imports: Vec<&str> = Vec::new();
        if self.context {
            std_imports.push("\"context\"");
        }
        if self.database_sql {
            std_imports.push("\"database/sql\"");
        }
        if self.encoding_json {
            std_imports.push("\"encoding/json\"");
        }
        if self.fmt {
            std_imports.push("\"fmt\"");
        }
        if self.strings {
            std_imports.push("\"strings\"");
        }
        if self.time {
            std_imports.push("\"time\"");
        }

        let extra: Vec<&str> = self.extra.iter().map(|s| s.as_str()).collect();

        if std_imports.is_empty() && extra.is_empty() {
            return;
        }

        src.push_str("import (\n");
        for imp in &std_imports {
            src.push_str(&format!("\t{imp}\n"));
        }
        if !extra.is_empty() {
            if !std_imports.is_empty() {
                src.push('\n');
            }
            for imp in &extra {
                src.push_str(&format!("\t{imp}\n"));
            }
        }
        src.push_str(")\n");
    }
}
