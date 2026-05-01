use crate::backend::manifest::build_manifest_file;
use crate::backend::naming::to_camel_case;
use crate::backend::{Codegen, GeneratedFile};
use crate::config::OutputConfig;
use crate::ir::{Query, Schema};

mod adapter;
mod core;
mod typemap;

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
        let type_map = typemap::build_kotlin_type_map(config);
        let strategy = config.list_params.clone().unwrap_or_default();
        let ctx = core::GenerationContext { schema, queries, config, contract: &contract, type_map: &type_map, strategy };
        let mut files = core::generate_core_files(&ctx)?;

        if let Some(manifest) = build_manifest_file(
            "kotlin",
            self.target.engine_str(),
            config,
            schema,
            queries,
            &to_camel_case,
            &|st, nullable| type_map.kotlin_type(st, nullable),
            &|p| core::kotlin_param_type(p, &type_map),
        ) {
            files.push(manifest);
        }

        Ok(files)
    }
}

#[cfg(test)]
fn kotlin_type(sql_type: &crate::ir::SqlType, nullable: bool) -> String {
    typemap::build_kotlin_type_map(&crate::config::OutputConfig::default()).kotlin_type(sql_type, nullable)
}

#[cfg(test)]
fn resultset_read_expr(sql_type: &crate::ir::SqlType, nullable: bool, idx: usize, config: &crate::config::OutputConfig) -> String {
    let type_map = typemap::build_kotlin_type_map(config);
    core::resultset_read_expr_pub(sql_type, nullable, idx, &type_map)
}

#[cfg(test)]
mod tests;
