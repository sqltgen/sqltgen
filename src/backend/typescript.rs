use crate::backend::{Codegen, GeneratedFile};
use crate::config::OutputConfig;
use crate::ir::{Query, Schema};

pub struct TypeScriptCodegen;

impl Codegen for TypeScriptCodegen {
    fn generate(
        &self,
        _schema: &Schema,
        _queries: &[Query],
        _config: &OutputConfig,
    ) -> anyhow::Result<Vec<GeneratedFile>> {
        unimplemented!("TypeScript codegen not yet implemented")
    }
}
