use crate::backend::{Codegen, GeneratedFile};
use crate::config::{Engine, OutputConfig};
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

impl From<Engine> for PythonTarget {
    fn from(engine: Engine) -> Self {
        match engine {
            Engine::Postgresql => PythonTarget::Postgres,
            Engine::Sqlite => PythonTarget::Sqlite,
            Engine::Mysql => PythonTarget::Mysql,
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

        Ok(files)
    }
}

#[cfg(test)]
fn python_type(sql_type: &SqlType, nullable: bool, target: &PythonTarget) -> String {
    core::python_type_for_target(sql_type, nullable, target)
}

#[cfg(test)]
mod tests;
