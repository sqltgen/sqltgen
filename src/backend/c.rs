use crate::backend::{Codegen, GeneratedFile};
use crate::config::{Engine, OutputConfig};
use crate::ir::{Query, Schema};

pub enum CTarget {
    Postgres,
    Sqlite,
    Mysql,
}

impl From<Engine> for CTarget {
    fn from(engine: Engine) -> Self {
        match engine {
            Engine::Postgresql => CTarget::Postgres,
            Engine::Sqlite => CTarget::Sqlite,
            Engine::Mysql => CTarget::Mysql,
        }
    }
}

pub struct CCodegen {
    pub target: CTarget,
}

impl Codegen for CCodegen {
    fn generate(&self, schema: &Schema, queries: &[Query], config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
        Ok(Vec::new()) // placeholder
    }
}