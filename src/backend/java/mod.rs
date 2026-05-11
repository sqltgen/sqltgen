use crate::backend::manifest::build_manifest_file;
use crate::backend::naming::to_camel_case;
use crate::backend::{Codegen, GeneratedFile};
use crate::config::OutputConfig;
use crate::ir::{Query, Schema};

mod adapter;
mod core;
mod typemap;

/// Database engine target for Java/JDBC output.
pub use crate::backend::jdbc::JdbcTarget;

/// Code generator for Java/JDBC output.
pub struct JavaCodegen {
    /// Selected JDBC engine target.
    pub target: JdbcTarget,
}

impl Codegen for JavaCodegen {
    fn generate(&self, schema: &Schema, queries: &[Query], config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
        let json_bind = adapter::json_bind_for(self.target);
        let type_map = typemap::build_java_type_map(config);
        let strategy = config.list_params.clone().unwrap_or_default();
        let ctx = core::GenerationContext { schema, queries, config, json_bind, type_map: &type_map, strategy };
        let mut files = core::generate_core_files(&ctx)?;

        if let Some(manifest) = build_manifest_file(
            "java",
            self.target.engine_str(),
            config,
            schema,
            queries,
            &to_camel_case,
            &|st, nullable| type_map.java_type(st, nullable),
            &|p| type_map.java_param_type(p),
        ) {
            files.push(manifest);
        }

        Ok(files)
    }
}

#[cfg(test)]
fn java_type(sql_type: &crate::ir::SqlType, nullable: bool) -> String {
    typemap::build_java_type_map(&crate::config::OutputConfig::default()).java_type(sql_type, nullable)
}

#[cfg(test)]
fn resultset_read_expr(sql_type: &crate::ir::SqlType, nullable: bool, idx: usize) -> String {
    typemap::build_java_type_map(&crate::config::OutputConfig::default()).read_expr(sql_type, nullable, idx)
}

#[cfg(test)]
mod tests;
