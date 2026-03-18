use crate::backend::{Codegen, GeneratedFile};
use crate::config::{Engine, OutputConfig};
use crate::ir::{Query, Schema, SqlType};

mod adapter;
mod core;

pub enum CppTarget {
    Postgres,
    Sqlite,
    Mysql,
}

impl From<Engine> for CppTarget {
    fn from(engine: Engine) -> Self {
        match engine {
            Engine::Postgresql => CppTarget::Postgres,
            Engine::Sqlite => CppTarget::Sqlite,
            Engine::Mysql => CppTarget::Mysql,
        }
    }
}

pub struct CppCodegen {
    pub target: CppTarget,
}

impl Codegen for CppCodegen {
    fn generate(&self, schema: &Schema, queries: &[Query], config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
        let contract = adapter::resolve_contract(&self.target);
        let mut files = core::generate_table_files(schema, config)?;
        files.extend(core::generate_query_files(schema, queries, &contract, config)?);
        Ok(files)
    }
}

fn cpp_type(sql_type: &SqlType, nullable: bool, _target: &CppTarget) -> String {
    core::cpp_type(sql_type, nullable)
}

#[cfg(test)]
mod tests;
