use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use serde::{Deserialize, Serialize};

use crate::ir::SqlType;

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

/// How a single SQL type maps to a host-language type, supporting split field/param forms.
///
/// Use `Same` when the same type is appropriate for both result columns and query parameters.
/// Use `Split` when the field type (used in generated structs) differs from the param type
/// (used in query function signatures).
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum TypeOverride {
    /// Use the same TypeRef for both result columns (field) and query parameters (param).
    Same(TypeRef),
    /// Use different TypeRefs for result columns (field) and query parameters (param).
    Split {
        field: TypeRef,
        #[serde(default)]
        param: Option<TypeRef>,
    },
}

/// A reference to a target-language type — a preset name, FQN, plain name, or full explicit
/// specification with optional import and read/write conversion expressions.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub enum TypeRef {
    /// A string: preset name (e.g. `"jackson"`), FQN (contains `.`), or plain type name.
    String(String),
    /// Full explicit specification.
    Explicit {
        #[serde(rename = "type")]
        name: String,
        import: Option<String>,
        /// Wraps the raw driver read expression. `{raw}` is replaced with the driver
        /// read call (e.g. `rs.getString(1)`). Only applied by backends that emit
        /// per-column read code (Java, Kotlin).
        read_expr: Option<String>,
        /// Wraps the application value before param binding. `{value}` is replaced
        /// with the parameter name. Only applied by backends that emit explicit binds.
        write_expr: Option<String>,
    },
}

/// Which side of a field/param split to resolve.
#[derive(Debug, Clone, Copy)]
pub enum TypeVariant {
    /// Resolve for a result column (struct field or record component).
    Field,
    /// Resolve for a query parameter (function argument).
    Param,
}

/// A fully resolved type reference, ready to emit into generated code.
#[derive(Debug, Clone)]
pub struct ResolvedType {
    /// The type name to emit (e.g. `"JsonNode"`, `"LocalDate"`, `"serde_json::Value"`).
    pub name: String,
    /// Import path to add (language-specific, without the `import`/`use` keyword).
    pub import: Option<String>,
    /// Wraps the raw driver read expression; `{raw}` placeholder is substituted at use.
    pub read_expr: Option<String>,
    /// Wraps the application value for param binding; `{value}` placeholder is substituted.
    pub write_expr: Option<String>,
    /// Additional static fields to emit in the generated query class (Java/Kotlin only).
    pub extra_fields: Vec<ExtraField>,
}

/// A static field declaration to emit in the generated query class (e.g. an `ObjectMapper`
/// for Jackson). Used to avoid re-creating expensive objects per call.
#[derive(Debug, Clone)]
pub struct ExtraField {
    /// The full field declaration line.
    pub declaration: String,
    /// Import needed for this field (if any).
    pub import: Option<String>,
}

/// Maps a [`SqlType`] variant to its lowercase string key used in `type_overrides` config.
pub fn sql_type_key(sql_type: &SqlType) -> &'static str {
    match sql_type {
        SqlType::Boolean => "boolean",
        SqlType::SmallInt => "smallint",
        SqlType::Integer => "integer",
        SqlType::BigInt => "bigint",
        SqlType::Real => "real",
        SqlType::Double => "double",
        SqlType::Decimal => "decimal",
        SqlType::Text => "text",
        SqlType::Char(_) => "char",
        SqlType::VarChar(_) => "varchar",
        SqlType::Bytes => "bytes",
        SqlType::Date => "date",
        SqlType::Time => "time",
        SqlType::Timestamp => "timestamp",
        SqlType::TimestampTz => "timestamptz",
        SqlType::Interval => "interval",
        SqlType::Uuid => "uuid",
        SqlType::Json => "json",
        SqlType::Jsonb => "jsonb",
        SqlType::Array(_) => "array",
        SqlType::Custom(_) => "custom",
    }
}

/// Resolve a [`TypeRef`] to a [`ResolvedType`].
///
/// Returns `None` for known preset names (e.g. `"jackson"`, `"serde_json"`) that must be
/// handled by the backend's own `try_preset` function. For FQNs (containing `.`), the last
/// segment becomes the type name and the full string the import. For plain names (no `.`),
/// the name is used as-is with no import.
pub fn resolve_type_ref(type_ref: &TypeRef) -> Option<ResolvedType> {
    const KNOWN_PRESETS: &[&str] = &["jackson", "gson", "serde_json", "object"];
    match type_ref {
        TypeRef::Explicit { name, import, read_expr, write_expr } => Some(ResolvedType {
            name: name.clone(),
            import: import.clone(),
            read_expr: read_expr.clone(),
            write_expr: write_expr.clone(),
            extra_fields: vec![],
        }),
        TypeRef::String(s) => {
            if KNOWN_PRESETS.contains(&s.as_str()) {
                return None;
            }
            if s.contains('.') {
                let name = s.split('.').next_back().unwrap_or(s.as_str()).to_string();
                Some(ResolvedType { name, import: Some(s.clone()), read_expr: None, write_expr: None, extra_fields: vec![] })
            } else {
                Some(ResolvedType { name: s.clone(), import: None, read_expr: None, write_expr: None, extra_fields: vec![] })
            }
        },
    }
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

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct OutputConfig {
    /// Output root directory, e.g. "src/main/java".
    pub out: String,
    /// Package / module name, e.g. "com.example.db".
    pub package: String,
    /// Strategy for list parameters (`-- @ids bigint[] not null`).
    /// Defaults to `native` when omitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list_params: Option<ListParamStrategy>,
    /// Per-type overrides mapping SQL type keys (e.g. `"json"`, `"uuid"`) to target-language
    /// type specifications. When present, overrides the backend's default type mapping.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub type_overrides: HashMap<String, TypeOverride>,
}

impl OutputConfig {
    /// Returns the configured [`TypeRef`] for the given SQL type and use-site variant,
    /// or `None` if no override is configured.
    ///
    /// Does not resolve preset names — call [`resolve_type_ref`] and your backend's own
    /// `try_preset` on the returned ref to get a [`ResolvedType`].
    pub fn get_type_ref(&self, sql_type: &SqlType, variant: TypeVariant) -> Option<&TypeRef> {
        let key = sql_type_key(sql_type);
        let override_ = self.type_overrides.get(key)?;
        match override_ {
            TypeOverride::Same(type_ref) => Some(type_ref),
            TypeOverride::Split { field, param } => match variant {
                TypeVariant::Field => Some(field),
                TypeVariant::Param => Some(param.as_ref().unwrap_or(field)),
            },
        }
    }
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
    fn expand_grouped_assigns_map_key_as_group() {
        let root = std::env::temp_dir().join(format!(
            "sqltgen_test_grouped_{}_{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("users.sql"), "-- name: A :one\nSELECT 1;").unwrap();
        std::fs::write(root.join("posts.sql"), "-- name: B :one\nSELECT 2;").unwrap();

        let mut map = HashMap::new();
        map.insert("users".to_string(), GroupPaths::Single("users.sql".to_string()));
        map.insert("posts".to_string(), GroupPaths::Single("posts.sql".to_string()));
        let cfg = SqltgenConfig {
            version: "1".to_string(),
            engine: Engine::Postgresql,
            schema: "schema.sql".to_string(),
            queries: QueryPaths::Grouped(map),
            gen: HashMap::new(),
        };
        let pairs = cfg.expand_queries(&root).unwrap();
        assert_eq!(pairs.len(), 2);
        let groups: Vec<&str> = pairs.iter().map(|(_, g)| g.as_str()).collect();
        assert!(groups.contains(&"users"), "Grouped variant must assign map key as group");
        assert!(groups.contains(&"posts"), "Grouped variant must assign map key as group");
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

    // ─── TypeRef / TypeOverride deserialization ──────────────────────────────

    #[test]
    fn test_type_ref_string_deserializes() {
        let v: TypeRef = serde_json::from_str(r#""jackson""#).unwrap();
        assert!(matches!(v, TypeRef::String(s) if s == "jackson"));
    }

    #[test]
    fn test_type_ref_fqn_deserializes() {
        let v: TypeRef = serde_json::from_str(r#""java.time.LocalDate""#).unwrap();
        assert!(matches!(v, TypeRef::String(s) if s == "java.time.LocalDate"));
    }

    #[test]
    fn test_type_ref_explicit_deserializes() {
        let v: TypeRef = serde_json::from_str(r#"{"type":"JsonNode","import":"com.fasterxml.jackson.databind.JsonNode","read_expr":"objectMapper.readValue({raw}, JsonNode.class)","write_expr":"objectMapper.writeValueAsString({value})"}"#).unwrap();
        match v {
            TypeRef::Explicit { name, import, read_expr, write_expr } => {
                assert_eq!(name, "JsonNode");
                assert_eq!(import.unwrap(), "com.fasterxml.jackson.databind.JsonNode");
                assert!(read_expr.unwrap().contains("{raw}"));
                assert!(write_expr.unwrap().contains("{value}"));
            },
            _ => panic!("expected Explicit"),
        }
    }

    #[test]
    fn test_type_override_same_deserializes() {
        let v: TypeOverride = serde_json::from_str(r#""jackson""#).unwrap();
        assert!(matches!(v, TypeOverride::Same(TypeRef::String(s)) if s == "jackson"));
    }

    #[test]
    fn test_type_override_split_deserializes() {
        let v: TypeOverride = serde_json::from_str(r#"{"field":"java.time.LocalDate","param":"String"}"#).unwrap();
        match v {
            TypeOverride::Split { field: TypeRef::String(f), param: Some(TypeRef::String(p)) } => {
                assert_eq!(f, "java.time.LocalDate");
                assert_eq!(p, "String");
            },
            _ => panic!("expected Split"),
        }
    }

    #[test]
    fn test_type_override_split_no_param_deserializes() {
        let v: TypeOverride = serde_json::from_str(r#"{"field":"LocalDate"}"#).unwrap();
        assert!(matches!(v, TypeOverride::Split { field: TypeRef::String(_), param: None }));
    }

    // ─── resolve_type_ref ───────────────────────────────────────────────────

    #[test]
    fn test_resolve_type_ref_fqn_splits_on_dot() {
        let tr = TypeRef::String("java.time.LocalDate".to_string());
        let r = resolve_type_ref(&tr).unwrap();
        assert_eq!(r.name, "LocalDate");
        assert_eq!(r.import.unwrap(), "java.time.LocalDate");
    }

    #[test]
    fn test_resolve_type_ref_plain_no_import() {
        let tr = TypeRef::String("String".to_string());
        let r = resolve_type_ref(&tr).unwrap();
        assert_eq!(r.name, "String");
        assert!(r.import.is_none());
    }

    #[test]
    fn test_resolve_type_ref_preset_returns_none() {
        for preset in &["jackson", "gson", "serde_json", "object"] {
            let tr = TypeRef::String(preset.to_string());
            assert!(resolve_type_ref(&tr).is_none(), "preset {preset} should return None");
        }
    }

    #[test]
    fn test_resolve_type_ref_explicit() {
        let tr = TypeRef::Explicit {
            name: "MyType".to_string(),
            import: Some("com.example.MyType".to_string()),
            read_expr: Some("parse({raw})".to_string()),
            write_expr: Some("serialize({value})".to_string()),
        };
        let r = resolve_type_ref(&tr).unwrap();
        assert_eq!(r.name, "MyType");
        assert_eq!(r.import.unwrap(), "com.example.MyType");
        assert_eq!(r.read_expr.unwrap(), "parse({raw})");
        assert_eq!(r.write_expr.unwrap(), "serialize({value})");
    }

    // ─── get_type_ref ───────────────────────────────────────────────────────

    #[test]
    fn test_get_type_ref_same_returns_same_for_both_variants() {
        let mut cfg = OutputConfig::default();
        cfg.type_overrides.insert("json".to_string(), TypeOverride::Same(TypeRef::String("jackson".to_string())));
        let field_ref = cfg.get_type_ref(&SqlType::Json, TypeVariant::Field).unwrap();
        let param_ref = cfg.get_type_ref(&SqlType::Json, TypeVariant::Param).unwrap();
        assert!(matches!(field_ref, TypeRef::String(s) if s == "jackson"));
        assert!(matches!(param_ref, TypeRef::String(s) if s == "jackson"));
    }

    #[test]
    fn test_get_type_ref_split_returns_correct_side() {
        let mut cfg = OutputConfig::default();
        cfg.type_overrides.insert(
            "date".to_string(),
            TypeOverride::Split {
                field: TypeRef::String("java.time.LocalDate".to_string()),
                param: Some(TypeRef::String("String".to_string())),
            },
        );
        let field_ref = cfg.get_type_ref(&SqlType::Date, TypeVariant::Field).unwrap();
        let param_ref = cfg.get_type_ref(&SqlType::Date, TypeVariant::Param).unwrap();
        assert!(matches!(field_ref, TypeRef::String(s) if s == "java.time.LocalDate"));
        assert!(matches!(param_ref, TypeRef::String(s) if s == "String"));
    }

    #[test]
    fn test_get_type_ref_split_no_param_falls_back_to_field() {
        let mut cfg = OutputConfig::default();
        cfg.type_overrides.insert(
            "uuid".to_string(),
            TypeOverride::Split { field: TypeRef::String("java.util.UUID".to_string()), param: None },
        );
        let field_ref = cfg.get_type_ref(&SqlType::Uuid, TypeVariant::Field).unwrap();
        let param_ref = cfg.get_type_ref(&SqlType::Uuid, TypeVariant::Param).unwrap();
        assert!(matches!(field_ref, TypeRef::String(s) if s == "java.util.UUID"));
        assert!(matches!(param_ref, TypeRef::String(s) if s == "java.util.UUID"));
    }

    #[test]
    fn test_get_type_ref_absent_returns_none() {
        let cfg = OutputConfig::default();
        assert!(cfg.get_type_ref(&SqlType::Json, TypeVariant::Field).is_none());
    }

    #[test]
    fn test_output_config_type_overrides_deserializes_from_json() {
        let json = r#"{
            "version": "1",
            "engine": "postgresql",
            "schema": "schema.sql",
            "queries": "queries.sql",
            "gen": {
                "java": {
                    "out": "src/main/java",
                    "package": "com.example.db",
                    "type_overrides": { "json": "jackson", "date": { "field": "java.time.LocalDate" } }
                }
            }
        }"#;
        let cfg = SqltgenConfig::from_json(json).unwrap();
        let java = cfg.gen.get(&Language::Java).unwrap();
        assert!(java.type_overrides.contains_key("json"));
        assert!(java.type_overrides.contains_key("date"));
    }
}
