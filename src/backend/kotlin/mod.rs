use crate::backend::{Codegen, GeneratedFile};
use crate::config::OutputConfig;
use crate::ir::{Query, Schema};

mod adapter;
mod core;

/// Database engine target for Kotlin/JDBC output.
pub use crate::backend::jdbc::JdbcTarget;

/// Code generator for Kotlin/JDBC output.
pub struct KotlinCodegen {
    /// Selected JDBC engine target.
    pub target: JdbcTarget,
}

impl Codegen for KotlinCodegen {
    fn generate(&self, schema: &Schema, queries: &[Query], config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
        let contract = adapter::resolve_kotlin_contract(self.target);
        core::generate_core_files(schema, queries, &contract, config)
    }
}

#[cfg(test)]
fn kotlin_type(sql_type: &crate::ir::SqlType, nullable: bool) -> String {
    core::kotlin_type_pub(sql_type, nullable)
}

#[cfg(test)]
fn resultset_read_expr(sql_type: &crate::ir::SqlType, nullable: bool, idx: usize) -> String {
    core::resultset_read_expr_pub(sql_type, nullable, idx)
}

#[cfg(test)]
mod tests;
