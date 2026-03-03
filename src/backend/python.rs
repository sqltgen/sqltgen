use crate::backend::{Codegen, GeneratedFile};
use crate::config::OutputConfig;
use crate::ir::{Query, Schema};

pub struct PythonCodegen;

impl Codegen for PythonCodegen {
    fn generate(
        &self,
        _schema: &Schema,
        _queries: &[Query],
        _config: &OutputConfig,
    ) -> anyhow::Result<Vec<GeneratedFile>> {
        unimplemented!("Python codegen not yet implemented")
    }
}
