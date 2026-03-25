use crate::backend::manifest::build_manifest_file;
use crate::backend::naming::to_camel_case;
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
        let contract = adapter::resolve_java_contract(self.target);
        let mut files = core::generate_core_files(schema, queries, &contract, config)?;

        if let Some(manifest) = build_manifest_file(
            "java",
            self.target.engine_str(),
            config,
            schema,
            queries,
            &to_camel_case,
            &|st, nullable| core::java_field_type(st, nullable, config),
            &|p| core::java_param_type_resolved(p, config),
        ) {
            files.push(manifest);
        }

        Ok(files)
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
