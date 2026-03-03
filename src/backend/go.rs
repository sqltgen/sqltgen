use crate::backend::{Codegen, GeneratedFile};
use crate::config::OutputConfig;
use crate::ir::{Query, Schema};

pub struct GoCodegen;

impl Codegen for GoCodegen {
    fn generate(
        &self,
        _schema: &Schema,
        _queries: &[Query],
        _config: &OutputConfig,
    ) -> anyhow::Result<Vec<GeneratedFile>> {
        unimplemented!("Go codegen not yet implemented")
    }
}
