use crate::backend::{Codegen, GeneratedFile};
use crate::config::OutputConfig;
use crate::ir::{Query, Schema};

pub struct RustCodegen;

impl Codegen for RustCodegen {
    fn generate(
        &self,
        _schema: &Schema,
        _queries: &[Query],
        _config: &OutputConfig,
    ) -> anyhow::Result<Vec<GeneratedFile>> {
        unimplemented!("Rust codegen not yet implemented")
    }
}
