/// Shared test utilities for backend tests.
///
/// Provides common fixture factories and lookup helpers used across all
/// backend test modules.
use crate::backend::GeneratedFile;
use crate::config::OutputConfig;
use crate::ir::{Column, SqlType, Table};
use std::path::PathBuf;

/// Default output config: `out = "out"`, no package, no list param strategy.
pub fn cfg() -> OutputConfig {
    OutputConfig { out: "out".to_string(), package: String::new(), list_params: None, ..Default::default() }
}

/// Output config with a Java/Kotlin package set (`com.example.db`).
pub fn cfg_pkg() -> OutputConfig {
    OutputConfig { out: "out".to_string(), package: "com.example.db".to_string(), list_params: None, ..Default::default() }
}

/// Find a generated file by filename and return its content.
///
/// Panics if no file with the given name exists in the output.
pub fn get_file<'a>(files: &'a [GeneratedFile], name: &str) -> &'a str {
    files.iter().find(|f| f.path.file_name().is_some_and(|n| n == name)).unwrap_or_else(|| panic!("file {name:?} not found")).content.as_str()
}

/// Find a generated file by path suffix and return its content.
///
/// Matches if the file's path ends with the given suffix (using path-component
/// matching, not string matching). Use this when multiple files share the same
/// filename (e.g. `mod.rs` at different directory depths).
///
/// Panics if no match is found.
pub fn get_file_by_path<'a>(files: &'a [GeneratedFile], suffix: &str) -> &'a str {
    let suffix_path = PathBuf::from(suffix);
    files.iter().find(|f| f.path.ends_with(&suffix_path)).unwrap_or_else(|| panic!("file with suffix {suffix:?} not found")).content.as_str()
}

/// A minimal `users` table with `id` (bigint PK), `name` (text), `bio` (text, nullable).
pub fn user_table() -> Table {
    Table::new("user", vec![Column::new_primary_key("id", SqlType::BigInt), Column::new_not_nullable("name", SqlType::Text), Column::new("bio", SqlType::Text)])
}

/// A minimal read-only `user_summary` view with id and display_name columns.
pub fn user_summary_view() -> Table {
    Table::view("user_summary", vec![Column::new_not_nullable("id", SqlType::BigInt), Column::new_not_nullable("display_name", SqlType::Text)])
}
