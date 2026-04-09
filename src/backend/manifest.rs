use std::path::PathBuf;

use serde::Serialize;

use crate::backend::common::infer_row_type_name;
use crate::backend::GeneratedFile;
use crate::config::{sql_type_key, OutputConfig};
use crate::ir::{Parameter, Query, QueryCmd, Schema, SqlType};

/// JSON manifest describing the generated API surface.
///
/// Emitted when `OutputConfig::manifest` is set. Contains resolved,
/// language-specific types for all models and query functions, enabling
/// downstream tooling (e.g. test generators) to work without parsing
/// the generated source code.
#[derive(Debug, Serialize)]
pub struct Manifest {
    pub language: String,
    pub engine: String,
    pub package: String,
    pub models: Vec<ManifestModel>,
    pub functions: Vec<ManifestFunction>,
}

/// A generated model (struct, dataclass, interface, record).
#[derive(Debug, Serialize)]
pub struct ManifestModel {
    pub name: String,
    pub fields: Vec<ManifestField>,
}

/// A field in a model or a parameter in a function.
#[derive(Debug, Serialize)]
pub struct ManifestField {
    pub name: String,
    pub lang_type: String,
    pub sql_type: String,
    pub nullable: bool,
}

/// A generated query function.
#[derive(Debug, Serialize)]
pub struct ManifestFunction {
    pub name: String,
    pub command: String,
    pub params: Vec<ManifestField>,
    /// The model name returned by this function, or `null` for `:exec`/`:execrows`.
    pub returns: Option<String>,
}

/// Render the SQL type for manifest output.
///
/// More descriptive than `sql_type_key` for parameterised types.
fn manifest_sql_type(sql_type: &SqlType) -> String {
    match sql_type {
        SqlType::Char(Some(n)) => format!("char({n})"),
        SqlType::VarChar(Some(n)) => format!("varchar({n})"),
        SqlType::Array(inner) => format!("{}[]", manifest_sql_type(inner)),
        SqlType::Enum(name) | SqlType::Custom(name) => name.clone(),
        other => sql_type_key(other).to_string(),
    }
}

/// Command string for the manifest.
fn manifest_command(cmd: &QueryCmd) -> &'static str {
    match cmd {
        QueryCmd::One => "one",
        QueryCmd::Many => "many",
        QueryCmd::Exec => "exec",
        QueryCmd::ExecRows => "execrows",
    }
}

/// Build a manifest file from the generated API surface.
///
/// Returns `None` when `config.manifest` is not set. Each backend calls
/// this from its `generate` method, supplying closures that resolve types
/// using the backend's own type-mapping logic.
///
/// - `to_func_name` converts a query name (PascalCase) to the target language's
///   function naming convention (e.g. `to_snake_case` for Python/Rust).
/// - `resolve_field_type` maps `(SqlType, nullable)` to the language type string
///   used in generated model fields.
/// - `resolve_param_type` maps a `Parameter` to the language type string used in
///   generated function signatures (handles list params, nullable wrapping, etc.).
#[allow(clippy::too_many_arguments)]
pub fn build_manifest_file(
    language: &str,
    engine: &str,
    config: &OutputConfig,
    schema: &Schema,
    queries: &[Query],
    to_func_name: &dyn Fn(&str) -> String,
    resolve_field_type: &dyn Fn(&SqlType, bool) -> String,
    resolve_param_type: &dyn Fn(&Parameter) -> String,
) -> Option<GeneratedFile> {
    let manifest_path = config.manifest.as_ref()?;

    let models = build_models(schema, resolve_field_type);
    let inline_models = build_inline_models(queries, schema, resolve_field_type);
    let functions = build_functions(queries, schema, to_func_name, resolve_param_type);

    let mut all_models = models;
    all_models.extend(inline_models);

    let manifest = Manifest { language: language.to_string(), engine: engine.to_string(), package: config.package.clone(), models: all_models, functions };

    let json = serde_json::to_string_pretty(&manifest).expect("manifest serialization should not fail");

    Some(GeneratedFile { path: PathBuf::from(manifest_path), content: json })
}

/// Build models from schema tables.
fn build_models(schema: &Schema, resolve_field_type: &dyn Fn(&SqlType, bool) -> String) -> Vec<ManifestModel> {
    schema
        .tables
        .iter()
        .map(|table| ManifestModel {
            name: crate::backend::common::model_name(table, schema.default_schema.as_deref()),
            fields: table
                .columns
                .iter()
                .map(|col| ManifestField {
                    name: col.name.clone(),
                    lang_type: resolve_field_type(&col.sql_type, col.nullable),
                    sql_type: manifest_sql_type(&col.sql_type),
                    nullable: col.nullable,
                })
                .collect(),
        })
        .collect()
}

/// Build inline row-type models for queries that don't match a schema table.
fn build_inline_models(queries: &[Query], schema: &Schema, resolve_field_type: &dyn Fn(&SqlType, bool) -> String) -> Vec<ManifestModel> {
    queries
        .iter()
        .filter(|q| crate::backend::common::has_inline_rows(q, schema))
        .map(|q| ManifestModel {
            name: crate::backend::common::row_type_name(&q.name),
            fields: q
                .result_columns
                .iter()
                .map(|col| ManifestField {
                    name: col.name.clone(),
                    lang_type: resolve_field_type(&col.sql_type, col.nullable),
                    sql_type: manifest_sql_type(&col.sql_type),
                    nullable: col.nullable,
                })
                .collect(),
        })
        .collect()
}

/// Build function entries from queries.
fn build_functions(
    queries: &[Query],
    schema: &Schema,
    to_func_name: &dyn Fn(&str) -> String,
    resolve_param_type: &dyn Fn(&Parameter) -> String,
) -> Vec<ManifestFunction> {
    queries
        .iter()
        .map(|q| ManifestFunction {
            name: to_func_name(&q.name),
            command: manifest_command(&q.cmd).to_string(),
            params: q
                .params
                .iter()
                .map(|p| ManifestField {
                    name: p.name.clone(),
                    lang_type: resolve_param_type(p),
                    sql_type: manifest_sql_type(&p.sql_type),
                    nullable: p.nullable,
                })
                .collect(),
            returns: infer_row_type_name(q, schema),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{Column, ResultColumn, Table};

    #[test]
    fn test_manifest_sql_type_simple() {
        assert_eq!(manifest_sql_type(&SqlType::BigInt), "bigint");
        assert_eq!(manifest_sql_type(&SqlType::Text), "text");
        assert_eq!(manifest_sql_type(&SqlType::Uuid), "uuid");
        assert_eq!(manifest_sql_type(&SqlType::TimestampTz), "timestamptz");
    }

    #[test]
    fn test_manifest_sql_type_parameterised() {
        assert_eq!(manifest_sql_type(&SqlType::Char(Some(10))), "char(10)");
        assert_eq!(manifest_sql_type(&SqlType::Char(None)), "char");
        assert_eq!(manifest_sql_type(&SqlType::VarChar(Some(255))), "varchar(255)");
    }

    #[test]
    fn test_manifest_sql_type_array() {
        assert_eq!(manifest_sql_type(&SqlType::Array(Box::new(SqlType::BigInt))), "bigint[]");
    }

    #[test]
    fn test_manifest_sql_type_custom() {
        assert_eq!(manifest_sql_type(&SqlType::Custom("citext".to_string())), "citext");
    }

    #[test]
    fn test_build_manifest_file_returns_none_when_not_configured() {
        let config = OutputConfig::default();
        let schema = Schema::default();
        let result = build_manifest_file("python", "postgresql", &config, &schema, &[], &|n| n.to_string(), &|_, _| "str".to_string(), &|_| "str".to_string());
        assert!(result.is_none());
    }

    #[test]
    fn test_build_manifest_file_produces_valid_json() {
        let config = OutputConfig { manifest: Some("gen/manifest.json".to_string()), package: "db".to_string(), ..Default::default() };

        let schema = Schema::with_tables(vec![Table::new(
            "users",
            vec![Column::new_primary_key("id", SqlType::BigInt), Column::new_not_nullable("name", SqlType::Text)],
        )]);

        let queries = vec![Query::one(
            "GetUser",
            "SELECT id, name FROM users WHERE id = $1",
            vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
            vec![ResultColumn::not_nullable("id", SqlType::BigInt), ResultColumn::not_nullable("name", SqlType::Text)],
        )];

        let result =
            build_manifest_file("python", "postgresql", &config, &schema, &queries, &|n| n.to_lowercase(), &|_, _| "str".to_string(), &|_| "int".to_string());

        let file = result.expect("should produce manifest");
        assert_eq!(file.path, PathBuf::from("gen/manifest.json"));

        let manifest: serde_json::Value = serde_json::from_str(&file.content).expect("should be valid JSON");
        assert_eq!(manifest["language"], "python");
        assert_eq!(manifest["engine"], "postgresql");
        assert_eq!(manifest["package"], "db");
        assert_eq!(manifest["models"].as_array().unwrap().len(), 1);
        assert_eq!(manifest["models"][0]["name"], "Users");
        assert_eq!(manifest["functions"].as_array().unwrap().len(), 1);
        assert_eq!(manifest["functions"][0]["name"], "getuser");
        assert_eq!(manifest["functions"][0]["command"], "one");
        assert_eq!(manifest["functions"][0]["returns"], "Users");
    }

    #[test]
    fn test_manifest_includes_inline_row_models() {
        let config = OutputConfig { manifest: Some("manifest.json".to_string()), ..Default::default() };

        let schema = Schema::default();
        let queries = vec![Query::one(
            "GetStats",
            "SELECT count, avg FROM ...",
            vec![],
            vec![ResultColumn::not_nullable("count", SqlType::BigInt), ResultColumn::not_nullable("avg", SqlType::Double)],
        )];

        let result =
            build_manifest_file("python", "postgresql", &config, &schema, &queries, &|n| n.to_string(), &|_, _| "int".to_string(), &|_| "int".to_string());

        let file = result.unwrap();
        let manifest: serde_json::Value = serde_json::from_str(&file.content).unwrap();
        assert_eq!(manifest["models"][0]["name"], "GetStatsRow");
        assert_eq!(manifest["functions"][0]["returns"], "GetStatsRow");
    }

    #[test]
    fn test_manifest_exec_returns_null() {
        let config = OutputConfig { manifest: Some("manifest.json".to_string()), ..Default::default() };

        let schema = Schema::default();
        let queries = vec![Query::exec("DeleteUser", "DELETE FROM users WHERE id = $1", vec![Parameter::scalar(1, "id", SqlType::BigInt, false)])];

        let result =
            build_manifest_file("python", "postgresql", &config, &schema, &queries, &|n| n.to_string(), &|_, _| "int".to_string(), &|_| "int".to_string());

        let file = result.unwrap();
        let manifest: serde_json::Value = serde_json::from_str(&file.content).unwrap();
        assert!(manifest["functions"][0]["returns"].is_null());
    }
}
