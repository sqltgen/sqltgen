/// Shared test utilities for backend tests.
///
/// Provides common fixture factories and lookup helpers used across all
/// backend test modules.
use crate::backend::GeneratedFile;
use crate::config::OutputConfig;
use crate::ir::{Column, SqlType, Table};

/// Default output config: `out = "out"`, no package, no list param strategy.
pub fn cfg() -> OutputConfig {
    OutputConfig { out: "out".to_string(), package: String::new(), list_params: None, ..Default::default() }
}

/// Find a generated file by filename and return its content.
///
/// Panics if no file with the given name exists in the output.
pub fn get_file<'a>(files: &'a [GeneratedFile], name: &str) -> &'a str {
    files.iter().find(|f| f.path.file_name().is_some_and(|n| n == name)).unwrap_or_else(|| panic!("file {name:?} not found")).content.as_str()
}

/// A minimal `users` table with `id` (bigint PK), `name` (text), `bio` (text, nullable).
pub fn user_table() -> Table {
    Table::new("user", vec![Column::new_primary_key("id", SqlType::BigInt), Column::new_not_nullable("name", SqlType::Text), Column::new("bio", SqlType::Text)])
}
