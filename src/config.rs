use std::collections::HashMap;
use std::path::Path;

use anyhow::Context;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct SqltgenConfig {
    pub version: String,
    pub engine: Engine,
    /// Path to the DDL schema file.
    pub schema: String,
    /// Path to the annotated query file.
    pub queries: String,
    /// Map from language name (e.g. "java", "kotlin") to output config.
    pub gen: HashMap<String, OutputConfig>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Engine {
    Postgresql,
    Sqlite,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OutputConfig {
    /// Output root directory, e.g. "src/main/java".
    pub out: String,
    /// Package / module name, e.g. "com.example.db".
    pub package: String,
}

impl SqltgenConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file: {}", path.display()))?;
        Self::from_str(&text)
    }

    pub fn from_str(text: &str) -> anyhow::Result<Self> {
        serde_json::from_str(text).context("parsing sqltgen config JSON")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"{
        "version": "1",
        "engine": "postgresql",
        "schema": "schema.sql",
        "queries": "queries.sql",
        "gen": {
            "java":   { "out": "src/main/java",   "package": "com.example.db" },
            "kotlin": { "out": "src/main/kotlin", "package": "com.example.db" }
        }
    }"#;

    #[test]
    fn parses_sample_config() {
        let cfg = SqltgenConfig::from_str(SAMPLE).unwrap();
        assert_eq!(cfg.version, "1");
        assert_eq!(cfg.engine, Engine::Postgresql);
        assert_eq!(cfg.schema, "schema.sql");
        assert_eq!(cfg.queries, "queries.sql");
        assert_eq!(cfg.gen.len(), 2);

        let java = cfg.gen.get("java").unwrap();
        assert_eq!(java.out, "src/main/java");
        assert_eq!(java.package, "com.example.db");

        let kotlin = cfg.gen.get("kotlin").unwrap();
        assert_eq!(kotlin.out, "src/main/kotlin");
        assert_eq!(kotlin.package, "com.example.db");
    }
}
