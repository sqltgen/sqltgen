use crate::backend::manifest::build_manifest_file;
use crate::backend::naming::to_snake_case;
use crate::backend::{Codegen, GeneratedFile};
use crate::config::OutputConfig;
use crate::ir::{Query, Schema};

#[cfg(test)]
use crate::ir::SqlType;

mod adapter;
mod core;

pub enum PythonTarget {
    Postgres,
    Sqlite,
    Mysql,
}

impl From<crate::config::Engine> for PythonTarget {
    fn from(engine: crate::config::Engine) -> Self {
        match engine {
            crate::config::Engine::Postgresql => PythonTarget::Postgres,
            crate::config::Engine::Sqlite => PythonTarget::Sqlite,
            crate::config::Engine::Mysql => PythonTarget::Mysql,
        }
    }
}

impl PythonTarget {
    fn engine_str(&self) -> &'static str {
        match self {
            PythonTarget::Postgres => "postgresql",
            PythonTarget::Sqlite => "sqlite",
            PythonTarget::Mysql => "mysql",
        }
    }
}

pub struct PythonCodegen {
    pub target: PythonTarget,
}

impl Codegen for PythonCodegen {
    fn generate(&self, schema: &Schema, queries: &[Query], config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
        let contract = adapter::resolve_python_contract(&self.target);

        let mut files = Vec::new();
        files.push(adapter::emit_helper_file(&contract, config));
        files.extend(core::generate_core_files(schema, queries, &contract, config)?);

        if let Some(manifest) = build_manifest_file(
            "python",
            self.target.engine_str(),
            config,
            schema,
            queries,
            &to_snake_case,
            &|st, nullable| core::python_field_type(st, nullable, &contract, config),
            &|p| core::python_param_type_resolved(&p.sql_type, p.nullable, &contract, config),
        ) {
            files.push(manifest);
        }

        Ok(files)
    }
}

#[cfg(test)]
fn python_type(sql_type: &SqlType, nullable: bool, target: &PythonTarget) -> String {
    core::python_type_for_target(sql_type, nullable, target)
}

#[cfg(test)]
mod tests;
