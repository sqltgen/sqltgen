use crate::backend::{Codegen, GeneratedFile};
use crate::config::{Engine, OutputConfig};
use crate::ir::{Query, Schema, SqlType};

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
        Ok(Vec::new()) // placeholder
    }
}

fn cpp_type(sql_type: &SqlType, nullable: bool, target: &CppTarget) -> String {
    return "int".to_string(); //placeholder
}