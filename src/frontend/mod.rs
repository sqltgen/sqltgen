pub(crate) mod common;
pub mod mysql;
pub mod postgres;
pub mod sqlite;

use std::path::PathBuf;

use crate::ir::{Query, Schema};

/// One input file (or inline source) presented to the schema loader.
///
/// `path` is used only for error reporting — it appears verbatim in collision
/// error messages. For inline test inputs, use `SchemaFile::inline`.
#[derive(Debug, Clone)]
pub struct SchemaFile {
    pub path: PathBuf,
    pub content: String,
}

impl SchemaFile {
    /// Wrap an in-memory DDL string with a synthetic `<input>` path label.
    /// Suitable for tests and any caller that has DDL but no source path.
    pub fn inline(ddl: impl Into<String>) -> Self {
        Self { path: PathBuf::from("<input>"), content: ddl.into() }
    }
}

pub trait DialectParser {
    /// Parse one or more DDL files into a single [`Schema`].
    ///
    /// Files are processed in the given order. Cross-file `ALTER TABLE` /
    /// `DROP TABLE` mutations are honored. A bare `CREATE TABLE` for a
    /// table that already exists is reported as a collision error.
    fn parse_schema_files(&self, files: &[SchemaFile], default_schema: Option<&str>) -> anyhow::Result<Schema>;

    /// Convenience wrapper for callers (especially tests) that have a single
    /// in-memory DDL string. Delegates to [`parse_schema_files`] using
    /// [`SchemaFile::inline`].
    fn parse_schema(&self, ddl: &str, default_schema: Option<&str>) -> anyhow::Result<Schema> {
        self.parse_schema_files(&[SchemaFile::inline(ddl)], default_schema)
    }

    fn parse_queries(&self, sql: &str, schema: &Schema, default_schema: Option<&str>) -> anyhow::Result<Vec<Query>>;
}
