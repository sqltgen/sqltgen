pub mod common;
pub mod go;
pub mod java;
pub mod kotlin;
pub mod python;
pub mod rust;
pub mod typescript;

use std::path::PathBuf;

use crate::config::OutputConfig;
use crate::ir::{Query, Schema};

pub struct GeneratedFile {
    pub path: PathBuf,
    pub content: String,
}

pub trait Codegen {
    fn generate(&self, schema: &Schema, queries: &[Query], config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>>;
}
