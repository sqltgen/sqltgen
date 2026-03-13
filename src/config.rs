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
    /// Map from target language to output config.
    pub gen: HashMap<Language, OutputConfig>,
}

/// One or more glob patterns for a single named query group.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum GroupPaths {
    /// A single glob pattern.
    Single(String),
    /// Multiple glob patterns merged into one group.
    Many(Vec<String>),
}

/// Query file paths or glob patterns from config.
///
/// Three forms are supported:
/// - **Single** — one file; all queries land in the default `"queries"` group.
/// - **Many** — multiple files/globs; each resolved file's stem becomes its group name.
///   Files with the same stem are merged into one output file.
/// - **Grouped** — explicit map of group name → paths; collision-free.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum QueryPaths {
    /// A single file or glob pattern. All queries use the default group.
    Single(String),
    /// Multiple files or glob patterns. Group is derived from each file's stem.
    Many(Vec<String>),
    /// Explicit map of group name to one or more glob patterns.
    Grouped(HashMap<String, GroupPaths>),
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Engine {
    Postgresql,
    Sqlite,
    Mysql,
}

/// Target language for code generation.
///
/// Used as the key in `gen` — invalid values are rejected at deserialization time.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Language {
    Java,
    Kotlin,
    Rust,
    Go,
    Python,
    TypeScript,
    JavaScript,
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
        Self::from_json(&text)
    }

    pub fn from_json(text: &str) -> anyhow::Result<Self> {
        let cfg: Self = serde_json::from_str(text).context("parsing sqltgen config JSON")?;
        if cfg.version != "1" {
            bail!("unsupported config version {:?} (expected \"1\")", cfg.version);
        }
        Ok(cfg)
    }

    /// Resolve query file globs relative to the config directory.
    ///
    /// Returns a list of `(path, group)` pairs. The group is:
    /// - `""` (empty) for a `Single` config — backends treat this as the default group.
    /// - The file stem for each resolved path in a `Many` config.
    /// - The explicit map key for a `Grouped` config.
    pub fn expand_queries(&self, base_dir: &Path) -> anyhow::Result<Vec<(PathBuf, String)>> {
        match &self.queries {
            QueryPaths::Single(pattern) => {
                let files = expand_glob(base_dir, pattern)?;
                Ok(files.into_iter().map(|p| (p, String::new())).collect())
            },
            QueryPaths::Many(patterns) => {
                let mut out = Vec::new();
                for pattern in patterns {
                    for path in expand_glob(base_dir, pattern)? {
                        let group = path.file_stem().and_then(|s| s.to_str()).unwrap_or("queries").to_string();
                        out.push((path, group));
                    }
                }
                Ok(out)
            },
            QueryPaths::Grouped(map) => {
                let mut out = Vec::new();
                for (group, group_paths) in map {
                    let patterns: Vec<&str> = match group_paths {
                        GroupPaths::Single(p) => vec![p.as_str()],
                        GroupPaths::Many(ps) => ps.iter().map(|p| p.as_str()).collect(),
                    };
                    for pattern in patterns {
                        for path in expand_glob(base_dir, pattern)? {
                            out.push((path, group.clone()));
                        }
                    }
                }
                Ok(out)
            },
        }
    }
}

/// Expand a single glob pattern relative to `base_dir`, returning sorted file paths.
fn expand_glob(base_dir: &Path, pattern: &str) -> anyhow::Result<Vec<PathBuf>> {
    let pattern_path = base_dir.join(pattern);
    let pattern_str = pattern_path.to_string_lossy().to_string();
    let mut matches = Vec::new();
    for item in glob::glob(&pattern_str).with_context(|| format!("expanding glob pattern: {pattern_str}"))? {
        matches.push(item.with_context(|| format!("reading glob entry: {pattern_str}"))?);
    }
    if matches.is_empty() {
        bail!("queries pattern matched no files: {pattern_str}");
    }
    matches.sort();
    for path in &matches {
        if path.is_dir() {
            bail!("queries path is a directory: {} (use a glob like **/*.sql)", path.display());
        }
    }
    Ok(matches)
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
        let cfg = SqltgenConfig::from_json(SAMPLE).unwrap();
        assert_eq!(cfg.version, "1");
        assert_eq!(cfg.engine, Engine::Postgresql);
        assert_eq!(cfg.schema, "schema.sql");
        match cfg.queries {
            QueryPaths::Single(ref path) => assert_eq!(path, "queries.sql"),
            _ => panic!("expected single queries path"),
        }
        assert_eq!(cfg.gen.len(), 2);

        let java = cfg.gen.get(&Language::Java).unwrap();
        assert_eq!(java.out, "src/main/java");
        assert_eq!(java.package, "com.example.db");

        let kotlin = cfg.gen.get(&Language::Kotlin).unwrap();
        assert_eq!(kotlin.out, "src/main/kotlin");
        assert_eq!(kotlin.package, "com.example.db");
    }

    #[test]
    fn parses_multi_query_paths() {
        let cfg = SqltgenConfig::from_json(MULTI_QUERIES).unwrap();
        match cfg.queries {
            QueryPaths::Single(_) | QueryPaths::Grouped(_) => panic!("expected multiple queries paths"),
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

        let paths: Vec<PathBuf> = cfg.expand_queries(&root).unwrap().into_iter().map(|(p, _)| p).collect();
        let expected = vec![root.join("queries/a.sql"), root.join("queries/b.sql"), root.join("more.sql")];
        assert_eq!(paths, expected);
        std::fs::remove_dir_all(root).unwrap();
    }

    const GROUPED_QUERIES: &str = r#"{
        "version": "1",
        "engine": "postgresql",
        "schema": "schema.sql",
        "queries": {
            "users": "queries/users.sql",
            "posts": ["queries/posts.sql", "queries/extra.sql"]
        },
        "gen": {
            "java": { "out": "src/main/java", "package": "com.example.db" }
        }
    }"#;

    #[test]
    fn parses_grouped_query_paths() {
        let cfg = SqltgenConfig::from_json(GROUPED_QUERIES).unwrap();
        match cfg.queries {
            QueryPaths::Grouped(ref map) => {
                assert_eq!(map.len(), 2);
                assert!(map.contains_key("users"));
                assert!(map.contains_key("posts"));
                match map.get("users").unwrap() {
                    GroupPaths::Single(p) => assert_eq!(p, "queries/users.sql"),
                    GroupPaths::Many(_) => panic!("expected single path for users"),
                }
                match map.get("posts").unwrap() {
                    GroupPaths::Single(_) => panic!("expected many paths for posts"),
                    GroupPaths::Many(ps) => assert_eq!(ps.len(), 2),
                }
            },
            _ => panic!("expected grouped queries"),
        }
    }

    #[test]
    fn expand_many_assigns_stem_as_group() {
        let root = std::env::temp_dir().join(format!(
            "sqltgen_test_stems_{}_{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("users.sql"), "-- name: A :one\nSELECT 1;").unwrap();
        std::fs::write(root.join("posts.sql"), "-- name: B :one\nSELECT 2;").unwrap();

        let cfg = SqltgenConfig {
            version: "1".to_string(),
            engine: Engine::Postgresql,
            schema: "schema.sql".to_string(),
            queries: QueryPaths::Many(vec!["users.sql".to_string(), "posts.sql".to_string()]),
            gen: HashMap::new(),
        };
        let pairs = cfg.expand_queries(&root).unwrap();
        let groups: Vec<&str> = pairs.iter().map(|(_, g)| g.as_str()).collect();
        assert!(groups.contains(&"users"));
        assert!(groups.contains(&"posts"));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn expand_single_assigns_empty_group() {
        let root = std::env::temp_dir().join(format!(
            "sqltgen_test_single_{}_{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("queries.sql"), "-- name: A :one\nSELECT 1;").unwrap();

        let cfg = SqltgenConfig {
            version: "1".to_string(),
            engine: Engine::Postgresql,
            schema: "schema.sql".to_string(),
            queries: QueryPaths::Single("queries.sql".to_string()),
            gen: HashMap::new(),
        };
        let pairs = cfg.expand_queries(&root).unwrap();
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].1, "");
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rejects_invalid_version() {
        let json = r#"{
            "version": "2",
            "engine": "postgresql",
            "schema": "schema.sql",
            "queries": "queries.sql",
            "gen": { "java": { "out": "gen", "package": "com.example" } }
        }"#;
        let err = SqltgenConfig::from_json(json).unwrap_err();
        assert!(err.to_string().contains("unsupported config version"));
    }

    #[test]
    fn rejects_invalid_language_key() {
        let json = r#"{
            "version": "1",
            "engine": "postgresql",
            "schema": "schema.sql",
            "queries": "queries.sql",
            "gen": {
                "jaava": { "out": "src/main/java", "package": "com.example.db" }
            }
        }"#;
        assert!(SqltgenConfig::from_json(json).is_err());
    }
}
