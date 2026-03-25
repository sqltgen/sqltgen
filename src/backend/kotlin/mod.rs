use crate::backend::manifest::build_manifest_file;
use crate::backend::naming::to_camel_case;
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
        let mut files = core::generate_core_files(schema, queries, &contract, config)?;

        if let Some(manifest) = build_manifest_file(
            "kotlin",
            self.target.engine_str(),
            config,
            schema,
            queries,
            &to_camel_case,
            &|st, nullable| core::kotlin_field_type(st, nullable, config),
            &|p| core::kotlin_param_type_resolved(p, config),
        ) {
            files.push(manifest);
        }

        Ok(files)
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
