use std::collections::BTreeSet;
use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::common::{group_queries, has_inline_rows, infer_row_type_name, infer_table, querier_class_name, queries_file_stem, sql_const_name};
use crate::backend::naming::{to_pascal_case, to_snake_case};
use crate::backend::sql_rewrite::rewrite_to_anon_params;
use crate::backend::GeneratedFile;
use crate::config::OutputConfig;
use crate::ir::{Query, QueryCmd, Schema, SqlType, Table};

use super::adapter::{CppBodyEmitter, CppEngineContract, CppParamStyle};

// ─── Type mappings ─────────────────────────────────────────────────────────────

pub(super) fn cpp_type(sql_type: &SqlType, nullable: bool) -> String {
    let base = match sql_type {
        SqlType::Boolean => "bool".to_string(),
        SqlType::SmallInt => "std::int16_t".to_string(),
        SqlType::Integer => "std::int32_t".to_string(),
        SqlType::BigInt => "std::int64_t".to_string(),
        SqlType::Real => "float".to_string(),
        SqlType::Double => "double".to_string(),
        SqlType::Decimal => "std::string".to_string(), // double loses precision
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => "std::string".to_string(),
        SqlType::Bytes => "std::vector<std::uint8_t>".to_string(),
        SqlType::Date | SqlType::Time | SqlType::Timestamp | SqlType::TimestampTz | SqlType::Interval => "std::string".to_string(),
        SqlType::Uuid => "std::string".to_string(),
        SqlType::Json | SqlType::Jsonb => "std::string".to_string(),
        SqlType::Array(inner) => format!("std::vector<{}>", cpp_type(inner, false)),
        SqlType::Custom(_) => "std::string".to_string(),
    };

    if nullable {
        format!("std::optional<{base}>")
    } else {
        base
    }
}

// Helpers for collecting `#include`s needed

#[derive(Default)]
struct CppIncludes {
    set: BTreeSet<&'static str>,
}

impl CppIncludes {
    fn scan(&mut self, sql_type: &SqlType, nullable: bool) {
        if nullable {
            self.set.insert("<optional>");
        }

        match sql_type {
            SqlType::Boolean | SqlType::Real | SqlType::Double => {},
            SqlType::SmallInt | SqlType::Integer | SqlType::BigInt => {
                self.set.insert("<cstdint>");
            },
            SqlType::Decimal | 
            SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) | 
            SqlType::Date | SqlType::Time | SqlType::Timestamp | SqlType::TimestampTz | SqlType::Interval |
            SqlType::Uuid | SqlType::Json | SqlType::Jsonb | SqlType::Custom(_) => {
                self.set.insert("<string>");
            },
            SqlType::Bytes => {
                self.set.insert("<cstdint>");
                self.set.insert("<vector>");
            },
            SqlType::Array(inner) => {
                self.set.insert("<vector>");
                self.scan(inner, false);
            },
        }
    }
}

/// Collect all standard-library includes needed by a table's column types.
fn scan_table_includes(table: &Table) -> CppIncludes {
    let mut includes = CppIncludes::default();
    for col in &table.columns {
        includes.scan(&col.sql_type, col.nullable);
    }
    includes
}

/// Collect all standard-library includes needed by a query file.
/// Function signatures, inline row structs, SQL string constants, and other types.
fn scan_query_includes(queries: &[Query], schema: &Schema) -> CppIncludes {
    let mut includes = CppIncludes::default();
    // Always need <string> for SQL string constants.
    includes.set.insert("<string>");

    for query in queries {
        // Scan parameter types.
        for p in &query.params {
            if p.is_list {
                includes.set.insert("<vector>");
                includes.scan(&p.sql_type, false);
            } else {
                includes.scan(&p.sql_type, p.nullable);
            }
        }
        // Scan return type.
        match query.cmd {
            QueryCmd::Exec => {},
            QueryCmd::ExecRows => {
                includes.set.insert("<cstdint>");
            },
            QueryCmd::One => {
                includes.set.insert("<optional>");
                // Scan result column types for inline row structs.
                if has_inline_rows(query, schema) {
                    for col in &query.result_columns {
                        includes.scan(&col.sql_type, col.nullable);
                    }
                }
            },
            QueryCmd::Many => {
                includes.set.insert("<vector>");
                if has_inline_rows(query, schema) {
                    for col in &query.result_columns {
                        includes.scan(&col.sql_type, col.nullable);
                    }
                }
            },
        }
    }
    includes
}

/// Return the C++ return type string for a query.
fn query_return_type(query: &Query, schema: &Schema) -> String {
    match query.cmd {
        QueryCmd::Exec => "void".to_string(),
        QueryCmd::ExecRows => "std::int64_t".to_string(),
        QueryCmd::One => format!("std::optional<{}>", result_row_type(query, schema)),
        QueryCmd::Many => format!("std::vector<{}>", result_row_type(query, schema)),
    }
}

// ─── Table header generation ─────────────────────────────────────────────────────────────

fn emit_table_header(table: &Table, namespace: &str) -> anyhow::Result<String> {
    let mut src = String::new();
    let struct_name = to_pascal_case(&table.name);
    let includes = scan_table_includes(table);

    writeln!(src, "#pragma once")?;
    writeln!(src, "// Generated by sqltgen. Do not edit.")?;
    writeln!(src)?;

    for include in &includes.set {
        writeln!(src, "#include {include}")?;
    }

    if !includes.set.is_empty() {
        writeln!(src)?;
    }

    if !namespace.is_empty() {
        writeln!(src, "namespace {namespace} {{")?;
        writeln!(src)?;
    }

    writeln!(src, "struct {struct_name} {{")?;
    for col in &table.columns {
        writeln!(src, "    {} {};", cpp_type(&col.sql_type, col.nullable), col.name)?;
    }
    writeln!(src, "}};")?;

    if !namespace.is_empty() {
        writeln!(src)?;
        writeln!(src, "}} // namespace {namespace}")?;
    }

    Ok(src)
}

pub(super) fn generate_table_files(schema: &Schema, config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
    let mut files = Vec::new();

    for table in &schema.tables {
        files.push(GeneratedFile {
            path: PathBuf::from(&config.out).join(format!("{}.hpp", table.name)),
            content: emit_table_header(table, &config.package)?,
        });
    }

    Ok(files)
}

// ─── Query generation - .hpp and .cpp ──────────────────────────────────────────────────

/// Normalize SQL placeholders for the target engine's client library.
fn normalize_sql(sql: &str, style: CppParamStyle) -> String {
    match style {
        // libpqxx uses $1, $2, … — keep as-is from the IR.
        CppParamStyle::Dollar => sql.to_string(),
        // sqlite3 uses ?1, ?2, … — already in this form for SQLite-frontend queries,
        // but Postgres-style $N needs rewriting. For now we keep as-is since the
        // frontend already emits the right style per engine.
        CppParamStyle::QuestionNumbered => sql.to_string(),
        // libmysqlclient uses anonymous ? — rewrite $N/? N to ?.
        CppParamStyle::QuestionAnon => rewrite_to_anon_params(sql),
    }
}

/// Derive the row type name for a query's result columns.
fn result_row_type(query: &Query, schema: &Schema) -> String {
    infer_row_type_name(query, schema).unwrap_or_else(|| "std::string".to_string())
}

/// Build the C++ parameter list string for a query function signature.
fn params_signature(query: &Query, conn_type: &str) -> String {
    let mut parts = vec![format!("{conn_type} db")];
    for p in &query.params {
        let ty = if p.is_list {
            format!("std::vector<{}>", cpp_type(&p.sql_type, false))
        } else {
            cpp_type(&p.sql_type, p.nullable)
        };
        parts.push(format!("const {ty}& {}", to_snake_case(&p.name)));
    }
    parts.join(", ")
}

/// Emit a per-query inline row struct for queries whose result columns don't match
/// a known table.
fn emit_inline_row_struct(src: &mut String, query: &Query) -> anyhow::Result<()> {
    let name = format!("{}Row", to_pascal_case(&query.name));
    writeln!(src, "struct {name} {{")?;
    for col in &query.result_columns {
        writeln!(src, "    {} {};", cpp_type(&col.sql_type, col.nullable), col.name)?;
    }
    writeln!(src, "}};")?;
    Ok(())
}

/// Emit a SQL string constant: `inline constexpr const char* SQL_GET_USER = "...";`
fn emit_sql_constant(src: &mut String, query: &Query, param_style: CppParamStyle) -> anyhow::Result<()> {
    let const_name = sql_const_name(&query.name);
    let raw_sql = query.params.iter()
        .find(|p| p.is_list)
        .and_then(|p| p.native_list_sql.as_deref())
        .unwrap_or(&query.sql);
    let sql = normalize_sql(raw_sql, param_style);
    let sql = sql.trim_end().trim_end_matches(';');
    // Use raw string literal R"sql(...)sql" to avoid escaping issues.
    writeln!(src, "inline const std::string {const_name} = R\"sql({sql})sql\";")?;
    Ok(())
}

/// Emit a single function declaration.
fn emit_function_decl(src: &mut String, query: &Query, schema: &Schema, conn_type: &str) -> anyhow::Result<()> {
    let fn_name = to_snake_case(&query.name);
    let ret = query_return_type(query, schema);
    let params = params_signature(query, conn_type);
    writeln!(src, "{ret} {fn_name}({params});")?;
    Ok(())
}

/// Emit a single function definition with body.
fn emit_function_def(src: &mut String, query: &Query, schema: &Schema, contract: &CppEngineContract) -> anyhow::Result<()> {
    let fn_name = to_snake_case(&query.name);
    let ret = query_return_type(query, schema);
    let params = params_signature(query, contract.conn_type);
    writeln!(src, "{ret} {fn_name}({params}) {{")?;
    match contract.body_emitter {
        CppBodyEmitter::Pqxx => emit_pqxx_body(src, query, schema)?,
        CppBodyEmitter::Sqlite3 => emit_sqlite3_body(src, query, schema)?,
        CppBodyEmitter::Mysql => emit_mysql_body(src, query, schema)?,
    }
    writeln!(src, "}}")?;
    Ok(())
}

// ─── Engine-specific body emitters ────────────────────────────────────────────

/// Emit the function body for a libpqxx (PostgreSQL) query.
fn emit_pqxx_body(src: &mut String, query: &Query, schema: &Schema) -> anyhow::Result<()> {
    let const_name = sql_const_name(&query.name);
    let param_names: Vec<String> = query.params.iter().map(|p| to_snake_case(&p.name)).collect();

    // Emit the common execute preamble: txn + exec/exec_params.
    let emit_exec = |src: &mut String, capture: &str| -> anyhow::Result<()> {
        writeln!(src, "    pqxx::work txn(db);")?;
        if param_names.is_empty() {
            writeln!(src, "    {capture}txn.exec({const_name});")?;
        } else {
            writeln!(src, "    {capture}txn.exec_params({const_name}, {});", param_names.join(", "))?;
        }
        writeln!(src, "    txn.commit();")?;
        Ok(())
    };

    match query.cmd {
        QueryCmd::Exec => {
            emit_exec(src, "")?;
        },
        QueryCmd::ExecRows => {
            emit_exec(src, "pqxx::result r = ")?;
            writeln!(src, "    return r.affected_rows();")?;
        },
        QueryCmd::One => {
            let row_type = result_row_type(query, schema);
            emit_exec(src, "pqxx::result r = ")?;
            writeln!(src, "    if (r.empty()) return std::nullopt;")?;
            writeln!(src, "    const auto& row = r[0];")?;
            emit_pqxx_row_construction(src, &row_type, &query.result_columns, "    ")?;
            writeln!(src, "    return result;")?;
        },
        QueryCmd::Many => {
            let row_type = result_row_type(query, schema);
            emit_exec(src, "pqxx::result r = ")?;
            writeln!(src, "    std::vector<{row_type}> rows;")?;
            writeln!(src, "    rows.reserve(r.size());")?;
            writeln!(src, "    for (const auto& row : r) {{")?;
            emit_pqxx_row_construction(src, &row_type, &query.result_columns, "        ")?;
            writeln!(src, "        rows.push_back(std::move(result));")?;
            writeln!(src, "    }}")?;
            writeln!(src, "    return rows;")?;
        },
    }
    Ok(())
}

/// Emit a row construction: `auto result = RowType{ row[0].as<T>(), ... };`
fn emit_pqxx_row_construction(src: &mut String, row_type: &str, columns: &[crate::ir::ResultColumn], indent: &str) -> anyhow::Result<()> {
    writeln!(src, "{indent}auto result = {row_type}{{")?;
    for (i, col) in columns.iter().enumerate() {
        let base_type = cpp_type(&col.sql_type, false);
        let expr = if col.nullable {
            format!("row[{i}].is_null() ? std::nullopt : std::optional<{base_type}>(row[{i}].as<{base_type}>())")
        } else {
            format!("row[{i}].as<{base_type}>()")
        };
        let comma = if i + 1 < columns.len() { "," } else { "" };
        writeln!(src, "{indent}    {expr}{comma}")?;
    }
    writeln!(src, "{indent}}};")?;
    Ok(())
}

/// Emit the function body for a sqlite3 query.
fn emit_sqlite3_body(src: &mut String, _query: &Query, _schema: &Schema) -> anyhow::Result<()> {
    writeln!(src, "    // TODO: not yet implemented")?;
    Ok(())
}

/// Emit the function body for a libmysqlclient (MySQL) query.
fn emit_mysql_body(src: &mut String, _query: &Query, _schema: &Schema) -> anyhow::Result<()> {
    writeln!(src, "    // TODO: not yet implemented")?;
    Ok(())
}

/// Emit a Querier class declaration that wraps a connection and delegates to free functions.
fn emit_querier_decl(src: &mut String, group: &str, queries: &[Query], schema: &Schema, conn_type: &str) -> anyhow::Result<()> {
    let class_name = querier_class_name(group);
    writeln!(src, "class {class_name} {{")?;
    writeln!(src, "    {conn_type} db_;")?;
    writeln!(src, "public:")?;
    writeln!(src, "    explicit {class_name}({conn_type} db) : db_(db) {{}}")?;

    for query in queries {
        writeln!(src)?;
        let fn_name = to_snake_case(&query.name);
        let ret = query_return_type(query, schema);
        let params_no_db = querier_method_params(query);
        if params_no_db.is_empty() {
            writeln!(src, "    {ret} {fn_name}();")?;
        } else {
            writeln!(src, "    {ret} {fn_name}({params_no_db});")?;
        }
    }

    writeln!(src, "}};")?;
    Ok(())
}

/// Build the parameter list for a Querier method (same as free function, minus the db param).
fn querier_method_params(query: &Query) -> String {
    let parts: Vec<String> = query.params.iter().map(|p| {
        let ty = if p.is_list {
            format!("std::vector<{}>", cpp_type(&p.sql_type, false))
        } else {
            cpp_type(&p.sql_type, p.nullable)
        };
        format!("const {ty}& {}", to_snake_case(&p.name))
    }).collect();
    parts.join(", ")
}

/// Generate query files (one `.hpp` + one `.cpp` per query group).
pub(super) fn generate_query_files(
    schema: &Schema,
    queries: &[Query],
    contract: &CppEngineContract,
    config: &OutputConfig,
) -> anyhow::Result<Vec<GeneratedFile>> {
    let mut files = Vec::new();
    let groups = group_queries(queries);

    for (group, group_queries) in &groups {
        let stem = queries_file_stem(group);
        let header = emit_queries_header(group, group_queries, schema, contract, config)?;
        files.push(GeneratedFile {
            path: PathBuf::from(&config.out).join(format!("{stem}.hpp")),
            content: header,
        });
        let source = emit_queries_source(stem, group_queries, schema, contract, config)?;
        files.push(GeneratedFile {
            path: PathBuf::from(&config.out).join(format!("{stem}.cpp")),
            content: source,
        });
    }

    Ok(files)
}

/// Emit the full content of a queries header file.
fn emit_queries_header(
    group: &str,
    queries: &[Query],
    schema: &Schema,
    contract: &CppEngineContract,
    config: &OutputConfig,
) -> anyhow::Result<String> {
    let mut src = String::new();

    writeln!(src, "#pragma once")?;
    writeln!(src, "// Generated by sqltgen. Do not edit.")?;
    writeln!(src)?;

    // Standard-library includes from types used in signatures.
    let includes = scan_query_includes(queries, schema);
    for inc in &includes.set {
        writeln!(src, "#include {inc}")?;
    }
    if !includes.set.is_empty() {
        writeln!(src)?;
    }

    // Database client include.
    writeln!(src, "#include {}", contract.db_include)?;
    writeln!(src)?;

    // Include table headers that are used as return types.
    let mut needed_tables: BTreeSet<&str> = BTreeSet::new();
    for query in queries {
        if let Some(table_name) = infer_table(query, schema) {
            needed_tables.insert(table_name);
        }
    }
    for table_name in &needed_tables {
        writeln!(src, "#include \"{table_name}.hpp\"")?;
    }
    if !needed_tables.is_empty() {
        writeln!(src)?;
    }

    // Open namespace.
    if !config.package.is_empty() {
        writeln!(src, "namespace {} {{", config.package)?;
        writeln!(src)?;
    }

    // Inline row structs for queries that don't map to a table.
    for query in queries {
        if has_inline_rows(query, schema) {
            emit_inline_row_struct(&mut src, query)?;
            writeln!(src)?;
        }
    }

    // SQL string constants.
    for query in queries {
        emit_sql_constant(&mut src, query, contract.param_style)?;
    }
    if !queries.is_empty() {
        writeln!(src)?;
    }

    // Function declarations.
    for (i, query) in queries.iter().enumerate() {
        if i > 0 {
            writeln!(src)?;
        }
        emit_function_decl(&mut src, query, schema, contract.conn_type)?;
    }

    // Querier class.
    if !queries.is_empty() {
        writeln!(src)?;
        emit_querier_decl(&mut src, group, queries, schema, contract.conn_type)?;
    }

    // Close namespace.
    if !config.package.is_empty() {
        writeln!(src)?;
        writeln!(src, "}} // namespace {}", config.package)?;
    }

    Ok(src)
}

/// Emit the full content of a queries source (.cpp) file.
fn emit_queries_source(
    header_stem: &str,
    queries: &[Query],
    schema: &Schema,
    contract: &CppEngineContract,
    config: &OutputConfig,
) -> anyhow::Result<String> {
    let mut src = String::new();

    writeln!(src, "// Generated by sqltgen. Do not edit.")?;
    writeln!(src, "#include \"{header_stem}.hpp\"")?;
    writeln!(src)?;

    // Open namespace.
    if !config.package.is_empty() {
        writeln!(src, "namespace {} {{", config.package)?;
        writeln!(src)?;
    }

    // Function definitions.
    for (i, query) in queries.iter().enumerate() {
        if i > 0 {
            writeln!(src)?;
        }
        emit_function_def(&mut src, query, schema, contract)?;
    }

    // Close namespace.
    if !config.package.is_empty() {
        writeln!(src)?;
        writeln!(src, "}} // namespace {}", config.package)?;
    }

    Ok(src)
}