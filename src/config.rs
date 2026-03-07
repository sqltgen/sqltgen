use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct SqltgenConfig {
    pub version: String,
    pub engine: Engine,
    /// Path to the DDL schema file.
    pub schema: String,
    /// Path(s) to annotated query files or glob patterns.
    pub queries: QueryPaths,
    /// Map from language name (e.g. "java", "kotlin") to output config.
    pub gen: HashMap<String, OutputConfig>,
}

/// Query file paths or glob patterns from config.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum QueryPaths {
    Single(String),
    Many(Vec<String>),
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Engine {
    Postgresql,
    Sqlite,
    Mysql,
}

/// How variable-length list parameters (`-- @ids bigint[] not null`) are transmitted
/// to the database in generated code.
///
/// `native` (default) uses a single bind per list param with an engine-specific SQL
/// function to unpack it (`= ANY($N)` on PostgreSQL, `json_each` on SQLite,
/// `JSON_TABLE` on MySQL).  `dynamic` builds `IN (?,?,…)` at call time with one bind
/// per element.
#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ListParamStrategy {
    /// Engine-idiomatic single-bind approach (default).
    #[default]
    Native,
    /// Runtime-expanded `IN (?,?,…)` with one bind per element.
    Dynamic,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OutputConfig {
    /// Output root directory, e.g. "src/main/java".
    pub out: String,
    /// Package / module name, e.g. "com.example.db".
    pub package: String,
    /// Strategy for list parameters (`-- @ids bigint[] not null`).
    /// Defaults to `native` when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list_params: Option<ListParamStrategy>,
}

impl SqltgenConfig {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path).with_context(|| format!("reading config file: {}", path.display()))?;
        Self::from_str(&text)
    }

    pub fn from_str(text: &str) -> anyhow::Result<Self> {
        serde_json::from_str(text).context("parsing sqltgen config JSON")
    }

    /// Resolve query file globs relative to the config directory.
    pub fn expand_queries(&self, base_dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
        let mut out = Vec::new();
        for entry in self.queries.iter() {
            let pattern_path = base_dir.join(entry);
            let pattern = pattern_path.to_string_lossy().to_string();
            let mut matches = Vec::new();
            for item in glob::glob(&pattern).with_context(|| format!("expanding glob pattern: {pattern}"))? {
                matches.push(item.with_context(|| format!("reading glob entry: {pattern}"))?);
            }
            if matches.is_empty() {
                bail!("queries pattern matched no files: {pattern}");
            }
            matches.sort();
            for path in matches {
                if path.is_dir() {
                    bail!("queries path is a directory: {} (use a glob like **/*.sql)", path.display());
                }
                out.push(path);
            }
        }
        Ok(out)
    }
}

impl QueryPaths {
    fn iter(&self) -> Box<dyn Iterator<Item = &str> + '_> {
        match self {
            QueryPaths::Single(path) => Box::new(std::iter::once(path.as_str())),
            QueryPaths::Many(paths) => Box::new(paths.iter().map(|p| p.as_str())),
        }
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

    const MULTI_QUERIES: &str = r#"{
        "version": "1",
        "engine": "postgresql",
        "schema": "schema.sql",
        "queries": ["queries/*.sql", "more.sql"],
        "gen": {
            "java": { "out": "src/main/java", "package": "com.example.db" }
        }
    }"#;

    #[test]
    fn parses_sample_config() {
        let cfg = SqltgenConfig::from_str(SAMPLE).unwrap();
        assert_eq!(cfg.version, "1");
        assert_eq!(cfg.engine, Engine::Postgresql);
        assert_eq!(cfg.schema, "schema.sql");
        match cfg.queries {
            QueryPaths::Single(ref path) => assert_eq!(path, "queries.sql"),
            QueryPaths::Many(_) => panic!("expected single queries path"),
        }
        assert_eq!(cfg.gen.len(), 2);

        let java = cfg.gen.get("java").unwrap();
        assert_eq!(java.out, "src/main/java");
        assert_eq!(java.package, "com.example.db");

        let kotlin = cfg.gen.get("kotlin").unwrap();
        assert_eq!(kotlin.out, "src/main/kotlin");
        assert_eq!(kotlin.package, "com.example.db");
    }

    #[test]
    fn parses_multi_query_paths() {
        let cfg = SqltgenConfig::from_str(MULTI_QUERIES).unwrap();
        match cfg.queries {
            QueryPaths::Single(_) => panic!("expected multiple queries paths"),
            QueryPaths::Many(paths) => {
                assert_eq!(paths, vec!["queries/*.sql", "more.sql"]);
            },
        }
    }

    #[test]
    fn expands_query_globs() {
        let root = std::env::temp_dir().join(format!(
            "sqltgen_test_queries_{}_{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(root.join("queries")).unwrap();
        std::fs::write(root.join("queries/a.sql"), "-- name: A :one\nSELECT 1;").unwrap();
        std::fs::write(root.join("queries/b.sql"), "-- name: B :one\nSELECT 2;").unwrap();
        std::fs::write(root.join("more.sql"), "-- name: C :one\nSELECT 3;").unwrap();

        let cfg = SqltgenConfig {
            version: "1".to_string(),
            engine: Engine::Postgresql,
            schema: "schema.sql".to_string(),
            queries: QueryPaths::Many(vec!["queries/*.sql".to_string(), "more.sql".to_string()]),
            gen: HashMap::new(),
        };

        let paths = cfg.expand_queries(&root).unwrap();
        let expected = vec![root.join("queries/a.sql"), root.join("queries/b.sql"), root.join("more.sql")];
        assert_eq!(paths, expected);
        std::fs::remove_dir_all(root).unwrap();
    }
}
