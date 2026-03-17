use crate::backend::{Codegen, GeneratedFile};
use crate::config::OutputConfig;
use crate::ir::{Query, Schema};

mod adapter;
mod core;

/// Database engine target for Java/JDBC output.
pub use crate::backend::jdbc::JdbcTarget;

/// Code generator for Java/JDBC output.
pub struct JavaCodegen {
    /// Selected JDBC engine target.
    pub target: JdbcTarget,
}

impl Codegen for JavaCodegen {
    fn generate(&self, schema: &Schema, queries: &[Query], config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
        let contract = adapter::resolve_java_contract();
        core::generate_core_files(schema, queries, &contract, config)
    }
}

#[cfg(test)]
fn java_type(sql_type: &crate::ir::SqlType, nullable: bool) -> String {
    core::java_type_pub(sql_type, nullable)
}

#[cfg(test)]
fn resultset_read_expr(sql_type: &crate::ir::SqlType, nullable: bool, idx: usize) -> String {
    core::resultset_read_expr_pub(sql_type, nullable, idx)
}

#[cfg(test)]
mod tests;
