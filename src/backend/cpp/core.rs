use std::collections::BTreeSet;
use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::common::{group_queries, has_inline_rows, infer_row_type_name, infer_table, querier_class_name, queries_file_stem, sql_const_name};
use crate::backend::naming::{to_pascal_case, to_snake_case};
use crate::backend::sql_rewrite::{parse_placeholder_indices, rewrite_to_anon_params};
use crate::backend::GeneratedFile;
use crate::config::OutputConfig;
use crate::ir::{Parameter, Query, QueryCmd, ResultColumn, Schema, SqlType, Table};

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
    // Always need <stdexcept> for std::runtime_error in query bodies.
    includes.set.insert("<stdexcept>");

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
fn emit_sqlite3_body(src: &mut String, query: &Query, schema: &Schema) -> anyhow::Result<()> {
    let const_name = sql_const_name(&query.name);
    match query.cmd {
        QueryCmd::Exec => {
            emit_sqlite3_prepare(src, &const_name)?;
            emit_sqlite3_bind_params(src, &query.params)?;
            writeln!(src, "    int rc = sqlite3_step(stmt);")?;
            writeln!(src, "    sqlite3_finalize(stmt);")?;
            writeln!(src, "    if (rc != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(db));")?;
        },
        QueryCmd::ExecRows => {
            emit_sqlite3_prepare(src, &const_name)?;
            emit_sqlite3_bind_params(src, &query.params)?;
            writeln!(src, "    int rc = sqlite3_step(stmt);")?;
            writeln!(src, "    sqlite3_finalize(stmt);")?;
            writeln!(src, "    if (rc != SQLITE_DONE) throw std::runtime_error(sqlite3_errmsg(db));")?;
            writeln!(src, "    return sqlite3_changes(db);")?;
        },
        QueryCmd::One => {
            let row_type = result_row_type(query, schema);
            emit_sqlite3_prepare(src, &const_name)?;
            emit_sqlite3_bind_params(src, &query.params)?;
            writeln!(src, "    int rc = sqlite3_step(stmt);")?;
            writeln!(src, "    if (rc == SQLITE_DONE) {{")?;
            writeln!(src, "        sqlite3_finalize(stmt);")?;
            writeln!(src, "        return std::nullopt;")?;
            writeln!(src, "    }}")?;
            writeln!(src, "    if (rc != SQLITE_ROW) {{")?;
            writeln!(src, "        sqlite3_finalize(stmt);")?;
            writeln!(src, "        throw std::runtime_error(sqlite3_errmsg(db));")?;
            writeln!(src, "    }}")?;
            emit_sqlite3_row_construction(src, &row_type, &query.result_columns, "    ")?;
            writeln!(src, "    sqlite3_finalize(stmt);")?;
            writeln!(src, "    return result;")?;
        },
        QueryCmd::Many => {
            let row_type = result_row_type(query, schema);
            emit_sqlite3_prepare(src, &const_name)?;
            emit_sqlite3_bind_params(src, &query.params)?;
            writeln!(src, "    std::vector<{row_type}> rows;")?;
            writeln!(src, "    while (sqlite3_step(stmt) == SQLITE_ROW) {{")?;
            emit_sqlite3_row_construction(src, &row_type, &query.result_columns, "        ")?;
            writeln!(src, "        rows.push_back(std::move(result));")?;
            writeln!(src, "    }}")?;
            writeln!(src, "    sqlite3_finalize(stmt);")?;
            writeln!(src, "    return rows;")?;
        },
    }
    Ok(())
}

/// Emit `sqlite3_prepare_v2` + error check.
fn emit_sqlite3_prepare(src: &mut String, const_name: &str) -> anyhow::Result<()> {
    writeln!(src, "    sqlite3_stmt* stmt;")?;
    writeln!(src, "    if (sqlite3_prepare_v2(db, {const_name}.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {{")?;
    writeln!(src, "        throw std::runtime_error(sqlite3_errmsg(db));")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit `sqlite3_bind_*` calls for each parameter.
fn emit_sqlite3_bind_params(src: &mut String, params: &[Parameter]) -> anyhow::Result<()> {
    for param in params {
        let idx = param.index;
        let name = to_snake_case(&param.name);
        if param.is_list {
            emit_sqlite3_bind_list(src, &param.sql_type, idx, &name)?;
        } else {
            let bind_call = sqlite3_bind_call(&param.sql_type, idx, &name, param.nullable);
            writeln!(src, "    {bind_call};")?;
        }
    }
    Ok(())
}

/// Emit code to serialize a `std::vector<T>` to a JSON array string and bind as text.
fn emit_sqlite3_bind_list(src: &mut String, sql_type: &SqlType, idx: usize, name: &str) -> anyhow::Result<()> {
    let inner_type = match sql_type {
        SqlType::Array(inner) => inner.as_ref(),
        _ => sql_type,
    };
    let to_str = match inner_type {
        SqlType::Boolean | SqlType::SmallInt | SqlType::Integer | SqlType::BigInt |
        SqlType::Real | SqlType::Double =>
            format!("{name}_json += std::to_string({name}[i]);"),
        // String-like types: wrap each element in double quotes.
        _ =>
            format!("{name}_json += \"\\\"\" + {name}[i] + \"\\\"\";"),
    };
    writeln!(src, "    std::string {name}_json = \"[\";")?;
    writeln!(src, "    for (size_t i = 0; i < {name}.size(); ++i) {{")?;
    writeln!(src, "        if (i > 0) {name}_json += \",\";")?;
    writeln!(src, "        {to_str}")?;
    writeln!(src, "    }}")?;
    writeln!(src, "    {name}_json += \"]\";")?;
    writeln!(src, "    sqlite3_bind_text(stmt, {idx}, {name}_json.c_str(), -1, SQLITE_TRANSIENT);")?;
    Ok(())
}

/// Return the appropriate `sqlite3_bind_*` expression for a parameter.
fn sqlite3_bind_call(sql_type: &SqlType, idx: usize, name: &str, nullable: bool) -> String {
    if nullable {
        let inner = sqlite3_bind_call(sql_type, idx, &format!("{name}.value()"), false);
        return format!("{name}.has_value() ? {inner} : sqlite3_bind_null(stmt, {idx})");
    }
    match sql_type {
        SqlType::Boolean | SqlType::SmallInt | SqlType::Integer =>
            format!("sqlite3_bind_int(stmt, {idx}, {name})"),
        SqlType::BigInt =>
            format!("sqlite3_bind_int64(stmt, {idx}, {name})"),
        SqlType::Real | SqlType::Double =>
            format!("sqlite3_bind_double(stmt, {idx}, {name})"),
        SqlType::Bytes =>
            format!("sqlite3_bind_blob(stmt, {idx}, {name}.data(), static_cast<int>({name}.size()), SQLITE_TRANSIENT)"),
        // Everything else is text (string types, dates, decimal, uuid, json, etc.)
        _ =>
            format!("sqlite3_bind_text(stmt, {idx}, {name}.c_str(), -1, SQLITE_TRANSIENT)"),
    }
}

/// Emit a row construction from sqlite3 column accessors.
fn emit_sqlite3_row_construction(src: &mut String, row_type: &str, columns: &[ResultColumn], indent: &str) -> anyhow::Result<()> {
    writeln!(src, "{indent}auto result = {row_type}{{")?;
    for (i, col) in columns.iter().enumerate() {
        let expr = sqlite3_column_expr(&col.sql_type, i, col.nullable);
        let comma = if i + 1 < columns.len() { "," } else { "" };
        writeln!(src, "{indent}    {expr}{comma}")?;
    }
    writeln!(src, "{indent}}};")?;
    Ok(())
}

/// Return the C++ expression to read a column value from a sqlite3_stmt.
fn sqlite3_column_expr(sql_type: &SqlType, idx: usize, nullable: bool) -> String {
    if nullable {
        let base_type = cpp_type(sql_type, false);
        let inner = sqlite3_column_expr(sql_type, idx, false);
        return format!("sqlite3_column_type(stmt, {idx}) == SQLITE_NULL ? std::nullopt : std::optional<{base_type}>({inner})");
    }
    match sql_type {
        SqlType::Boolean =>
            format!("static_cast<bool>(sqlite3_column_int(stmt, {idx}))"),
        SqlType::SmallInt =>
            format!("static_cast<std::int16_t>(sqlite3_column_int(stmt, {idx}))"),
        SqlType::Integer =>
            format!("sqlite3_column_int(stmt, {idx})"),
        SqlType::BigInt =>
            format!("sqlite3_column_int64(stmt, {idx})"),
        SqlType::Real =>
            format!("static_cast<float>(sqlite3_column_double(stmt, {idx}))"),
        SqlType::Double =>
            format!("sqlite3_column_double(stmt, {idx})"),
        SqlType::Bytes =>
            format!("std::vector<std::uint8_t>(reinterpret_cast<const std::uint8_t*>(sqlite3_column_blob(stmt, {idx})), reinterpret_cast<const std::uint8_t*>(sqlite3_column_blob(stmt, {idx})) + sqlite3_column_bytes(stmt, {idx}))"),
        // Everything else is text
        _ =>
            format!("std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, {idx})))"),
    }
}

/// Emit the function body for a libmysqlclient (MySQL) query.
fn emit_mysql_body(src: &mut String, query: &Query, schema: &Schema) -> anyhow::Result<()> {
    let const_name = sql_const_name(&query.name);

    match query.cmd {
        QueryCmd::Exec => {
            emit_mysql_prepare(src, &const_name)?;
            emit_mysql_bind_params(src, query)?;
            emit_mysql_execute(src)?;
            writeln!(src, "    mysql_stmt_close(stmt);")?;
        },
        QueryCmd::ExecRows => {
            emit_mysql_prepare(src, &const_name)?;
            emit_mysql_bind_params(src, query)?;
            emit_mysql_execute(src)?;
            writeln!(src, "    my_ulonglong affected = mysql_stmt_affected_rows(stmt);")?;
            writeln!(src, "    mysql_stmt_close(stmt);")?;
            writeln!(src, "    return static_cast<std::int64_t>(affected);")?;
        },
        QueryCmd::One => {
            let row_type = result_row_type(query, schema);
            emit_mysql_prepare(src, &const_name)?;
            emit_mysql_bind_params(src, query)?;
            emit_mysql_execute(src)?;
            emit_mysql_bind_result_columns(src, &query.result_columns)?;
            // First fetch — populates lengths for variable-length columns
            writeln!(src, "    int rc = mysql_stmt_fetch(stmt);")?;
            writeln!(src, "    if (rc == MYSQL_NO_DATA) {{")?;
            writeln!(src, "        mysql_stmt_close(stmt);")?;
            writeln!(src, "        return std::nullopt;")?;
            writeln!(src, "    }}")?;
            writeln!(src, "    if (rc != 0 && rc != MYSQL_DATA_TRUNCATED) {{")?;
            writeln!(src, "        std::string err = mysql_stmt_error(stmt);")?;
            writeln!(src, "        mysql_stmt_close(stmt);")?;
            writeln!(src, "        throw std::runtime_error(err);")?;
            writeln!(src, "    }}")?;
            // Second pass: fetch actual data for variable-length columns
            emit_mysql_fetch_varlen_columns(src, &query.result_columns)?;
            emit_mysql_row_construction(src, &row_type, &query.result_columns, "    ")?;
            writeln!(src, "    mysql_stmt_close(stmt);")?;
            writeln!(src, "    return result;")?;
        },
        QueryCmd::Many => {
            let row_type = result_row_type(query, schema);
            emit_mysql_prepare(src, &const_name)?;
            emit_mysql_bind_params(src, query)?;
            emit_mysql_execute(src)?;
            emit_mysql_bind_result_columns(src, &query.result_columns)?;
            writeln!(src, "    std::vector<{row_type}> rows;")?;
            writeln!(src, "    while (true) {{")?;
            writeln!(src, "        int rc = mysql_stmt_fetch(stmt);")?;
            writeln!(src, "        if (rc == MYSQL_NO_DATA) break;")?;
            writeln!(src, "        if (rc != 0 && rc != MYSQL_DATA_TRUNCATED) {{")?;
            writeln!(src, "            std::string err = mysql_stmt_error(stmt);")?;
            writeln!(src, "            mysql_stmt_close(stmt);")?;
            writeln!(src, "            throw std::runtime_error(err);")?;
            writeln!(src, "        }}")?;
            emit_mysql_fetch_varlen_columns_indented(src, &query.result_columns, "        ")?;
            emit_mysql_row_construction(src, &row_type, &query.result_columns, "        ")?;
            writeln!(src, "        rows.push_back(std::move(result));")?;
            writeln!(src, "    }}")?;
            writeln!(src, "    mysql_stmt_close(stmt);")?;
            writeln!(src, "    return rows;")?;
        },
    }
    Ok(())
}

/// Emit `mysql_stmt_init` + `mysql_stmt_prepare` + error checks.
fn emit_mysql_prepare(src: &mut String, const_name: &str) -> anyhow::Result<()> {
    writeln!(src, "    MYSQL_STMT* stmt = mysql_stmt_init(db);")?;
    writeln!(src, "    if (!stmt) throw std::runtime_error(mysql_error(db));")?;
    writeln!(src, "    if (mysql_stmt_prepare(stmt, {const_name}.c_str(), {const_name}.size()) != 0) {{")?;
    writeln!(src, "        std::string err = mysql_stmt_error(stmt);")?;
    writeln!(src, "        mysql_stmt_close(stmt);")?;
    writeln!(src, "        throw std::runtime_error(err);")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit `MYSQL_BIND` array setup and `mysql_stmt_bind_param` for all parameters.
///
/// MySQL uses anonymous `?` placeholders, so when a parameter appears more than once
/// in the SQL (e.g. `WHERE ? = 'all' OR genre = ?` for a single `@genre` param)
/// we must emit one bind entry per `?` occurrence.
fn emit_mysql_bind_params(src: &mut String, query: &Query) -> anyhow::Result<()> {
    if query.params.is_empty() {
        return Ok(());
    }

    // Build index → Parameter lookup.
    let by_idx: std::collections::HashMap<usize, &Parameter> =
        query.params.iter().map(|p| (p.index, p)).collect();

    // One entry per `?` in the rewritten SQL, in occurrence order.
    let bind_plan = parse_placeholder_indices(&query.sql);
    let n = bind_plan.len();
    writeln!(src, "    MYSQL_BIND bind[{n}];")?;
    writeln!(src, "    memset(bind, 0, sizeof(bind));")?;

    // Emit variable declarations once per unique parameter.
    for param in &query.params {
        let name = to_snake_case(&param.name);
        writeln!(src)?;
        if param.is_list {
            emit_mysql_bind_list_vars(src, &param.sql_type, &name)?;
        } else if param.nullable {
            let flag = format!("{name}_is_null");
            writeln!(src, "    my_bool {flag} = !{name}.has_value();")?;
            emit_mysql_bind_field_vars(src, &param.sql_type, &format!("{name}.value()"), true)?;
        } else {
            emit_mysql_bind_field_vars(src, &param.sql_type, &name, false)?;
        }
    }

    // Emit bind assignments in SQL occurrence order.
    for (slot, &param_idx) in bind_plan.iter().enumerate() {
        let param = by_idx[&param_idx];
        let name = to_snake_case(&param.name);
        writeln!(src)?;
        if param.is_list {
            emit_mysql_bind_list_assign(src, slot, &name)?;
        } else if param.nullable {
            let flag = format!("{name}_is_null");
            writeln!(src, "    bind[{slot}].is_null = &{flag};")?;
            emit_mysql_bind_field_assign(src, &param.sql_type, slot, &format!("{name}.value()"), true)?;
        } else {
            emit_mysql_bind_field_assign(src, &param.sql_type, slot, &name, false)?;
        }
    }

    writeln!(src)?;
    writeln!(src, "    if (mysql_stmt_bind_param(stmt, bind) != 0) {{")?;
    writeln!(src, "        std::string err = mysql_stmt_error(stmt);")?;
    writeln!(src, "        mysql_stmt_close(stmt);")?;
    writeln!(src, "        throw std::runtime_error(err);")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit variable declarations for serializing a list param to a JSON array string.
/// Called once per unique list parameter.
fn emit_mysql_bind_list_vars(src: &mut String, sql_type: &SqlType, name: &str) -> anyhow::Result<()> {
    let inner_type = match sql_type {
        SqlType::Array(inner) => inner.as_ref(),
        _ => sql_type,
    };
    let to_str = match inner_type {
        SqlType::Boolean | SqlType::SmallInt | SqlType::Integer | SqlType::BigInt |
        SqlType::Real | SqlType::Double =>
            format!("p_{name}_json += std::to_string({name}[i]);"),
        // String-like types: wrap each element in double quotes.
        _ =>
            format!("p_{name}_json += \"\\\"\" + {name}[i] + \"\\\"\";"),
    };
    writeln!(src, "    std::string p_{name}_json = \"[\";")?;
    writeln!(src, "    for (size_t i = 0; i < {name}.size(); ++i) {{")?;
    writeln!(src, "        if (i > 0) p_{name}_json += \",\";")?;
    writeln!(src, "        {to_str}")?;
    writeln!(src, "    }}")?;
    writeln!(src, "    p_{name}_json += \"]\";")?;
    writeln!(src, "    unsigned long p_{name}_json_len = p_{name}_json.size();")?;
    Ok(())
}

/// Emit bind assignment for a list param at a given bind slot.
/// Called once per `?` occurrence that references this list param.
fn emit_mysql_bind_list_assign(src: &mut String, slot: usize, name: &str) -> anyhow::Result<()> {
    writeln!(src, "    bind[{slot}].buffer_type = MYSQL_TYPE_STRING;")?;
    writeln!(src, "    bind[{slot}].buffer = const_cast<char*>(p_{name}_json.c_str());")?;
    writeln!(src, "    bind[{slot}].buffer_length = p_{name}_json.size();")?;
    writeln!(src, "    bind[{slot}].length = &p_{name}_json_len;")?;
    Ok(())
}

/// Emit variable declarations for a scalar parameter bind.
/// Called once per unique parameter. Only emits for types that need a length variable.
/// If `guarded` is true, the declaration is wrapped in an `if (name.has_value())` guard.
fn emit_mysql_bind_field_vars(src: &mut String, sql_type: &SqlType, name: &str, guarded: bool) -> anyhow::Result<()> {
    let (_, _, needs_length) = mysql_bind_info(sql_type, name);
    if needs_length {
        let indent = if guarded { "        " } else { "    " };
        let len_var = format!("p_{}_len", name.replace('.', "_").replace("()", ""));
        if guarded {
            writeln!(src, "    unsigned long {len_var} = 0;")?;
            writeln!(src, "    if ({name_root}.has_value()) {{", name_root = name.trim_end_matches(".value()"))?;
            writeln!(src, "{indent}{len_var} = {name}.size();")?;
            writeln!(src, "    }}")?;
        } else {
            writeln!(src, "    unsigned long {len_var} = {name}.size();")?;
        }
    }
    Ok(())
}

/// Emit bind slot assignments for a scalar parameter.
/// Called once per `?` occurrence that references this parameter.
/// If `guarded` is true, buffer assignment is wrapped in an `if (name.has_value())` guard.
fn emit_mysql_bind_field_assign(src: &mut String, sql_type: &SqlType, slot: usize, name: &str, guarded: bool) -> anyhow::Result<()> {
    let (mysql_type, buf_expr, needs_length) = mysql_bind_info(sql_type, name);
    let indent = if guarded { "        " } else { "    " };

    writeln!(src, "    bind[{slot}].buffer_type = {mysql_type};")?;

    if guarded {
        writeln!(src, "    if ({name_root}.has_value()) {{", name_root = name.trim_end_matches(".value()"))?;
    }

    writeln!(src, "{indent}bind[{slot}].buffer = {buf_expr};")?;
    if needs_length {
        let len_var = format!("p_{}_len", name.replace('.', "_").replace("()", ""));
        writeln!(src, "{indent}bind[{slot}].buffer_length = {name}.size();")?;
        writeln!(src, "{indent}bind[{slot}].length = &{len_var};")?;
    }

    if guarded {
        writeln!(src, "    }}")?;
    }
    Ok(())
}

/// Return (MYSQL_TYPE_*, buffer expression, needs_length) for a given SqlType.
fn mysql_bind_info(sql_type: &SqlType, name: &str) -> (&'static str, String, bool) {
    match sql_type {
        SqlType::Boolean =>
            ("MYSQL_TYPE_TINY", format!("const_cast<bool*>(&{name})"), false),
        SqlType::SmallInt =>
            ("MYSQL_TYPE_SHORT", format!("const_cast<std::int16_t*>(&{name})"), false),
        SqlType::Integer =>
            ("MYSQL_TYPE_LONG", format!("const_cast<std::int32_t*>(&{name})"), false),
        SqlType::BigInt =>
            ("MYSQL_TYPE_LONGLONG", format!("const_cast<std::int64_t*>(&{name})"), false),
        SqlType::Real =>
            ("MYSQL_TYPE_FLOAT", format!("const_cast<float*>(&{name})"), false),
        SqlType::Double =>
            ("MYSQL_TYPE_DOUBLE", format!("const_cast<double*>(&{name})"), false),
        SqlType::Bytes =>
            ("MYSQL_TYPE_BLOB", format!("const_cast<char*>(reinterpret_cast<const char*>({name}.data()))"), true),
        // Everything else is a string
        _ =>
            ("MYSQL_TYPE_STRING", format!("const_cast<char*>({name}.c_str())"), true),
    }
}

/// Emit `mysql_stmt_execute` with error handling.
fn emit_mysql_execute(src: &mut String) -> anyhow::Result<()> {
    writeln!(src, "    if (mysql_stmt_execute(stmt) != 0) {{")?;
    writeln!(src, "        std::string err = mysql_stmt_error(stmt);")?;
    writeln!(src, "        mysql_stmt_close(stmt);")?;
    writeln!(src, "        throw std::runtime_error(err);")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Return true if a SqlType is variable-length (string, blob, etc.).
fn mysql_is_varlen(sql_type: &SqlType) -> bool {
    matches!(mysql_bind_info(sql_type, "x"), (_, _, true))
}

/// Return the MYSQL_TYPE_* constant for a SqlType.
fn mysql_type_const(sql_type: &SqlType) -> &'static str {
    mysql_bind_info(sql_type, "x").0
}

/// Emit `MYSQL_BIND result_bind[]` setup and `mysql_stmt_bind_result` for output columns.
fn emit_mysql_bind_result_columns(src: &mut String, columns: &[ResultColumn]) -> anyhow::Result<()> {
    let n = columns.len();
    writeln!(src, "    MYSQL_BIND result_bind[{n}];")?;
    writeln!(src, "    memset(result_bind, 0, sizeof(result_bind));")?;

    // Declare local variables for each column
    for (i, col) in columns.iter().enumerate() {
        let col_name = to_snake_case(&col.name);
        let mysql_type = mysql_type_const(&col.sql_type);
        writeln!(src)?;

        if mysql_is_varlen(&col.sql_type) {
            // Variable-length: bind with null buffer, just capture length
            writeln!(src, "    unsigned long {col_name}_len = 0;")?;
            writeln!(src, "    my_bool {col_name}_is_null = 0;")?;
            writeln!(src, "    result_bind[{i}].buffer_type = {mysql_type};")?;
            writeln!(src, "    result_bind[{i}].buffer = nullptr;")?;
            writeln!(src, "    result_bind[{i}].buffer_length = 0;")?;
            writeln!(src, "    result_bind[{i}].length = &{col_name}_len;")?;
            writeln!(src, "    result_bind[{i}].is_null = &{col_name}_is_null;")?;
        } else {
            // Fixed-size: bind directly to a local variable
            let cpp_ty = cpp_type(&col.sql_type, false);
            writeln!(src, "    {cpp_ty} {col_name}_val{{}};")?;
            writeln!(src, "    my_bool {col_name}_is_null = 0;")?;
            writeln!(src, "    result_bind[{i}].buffer_type = {mysql_type};")?;
            writeln!(src, "    result_bind[{i}].buffer = &{col_name}_val;")?;
            writeln!(src, "    result_bind[{i}].is_null = &{col_name}_is_null;")?;
        }
    }

    writeln!(src)?;
    writeln!(src, "    if (mysql_stmt_bind_result(stmt, result_bind) != 0) {{")?;
    writeln!(src, "        std::string err = mysql_stmt_error(stmt);")?;
    writeln!(src, "        mysql_stmt_close(stmt);")?;
    writeln!(src, "        throw std::runtime_error(err);")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit `mysql_stmt_fetch_column` calls for variable-length columns after a fetch.
fn emit_mysql_fetch_varlen_columns(src: &mut String, columns: &[ResultColumn]) -> anyhow::Result<()> {
    emit_mysql_fetch_varlen_columns_indented(src, columns, "    ")
}

/// Emit `mysql_stmt_fetch_column` calls with a custom indent.
fn emit_mysql_fetch_varlen_columns_indented(src: &mut String, columns: &[ResultColumn], indent: &str) -> anyhow::Result<()> {
    for (i, col) in columns.iter().enumerate() {
        if !mysql_is_varlen(&col.sql_type) {
            continue;
        }
        let col_name = to_snake_case(&col.name);
        let is_blob = matches!(col.sql_type, SqlType::Bytes);

        if is_blob {
            writeln!(src, "{indent}std::vector<std::uint8_t> {col_name}_val({col_name}_len);")?;
            writeln!(src, "{indent}result_bind[{i}].buffer = {col_name}_val.data();")?;
        } else {
            writeln!(src, "{indent}std::string {col_name}_val({col_name}_len, '\\0');")?;
            writeln!(src, "{indent}result_bind[{i}].buffer = {col_name}_val.data();")?;
        }
        writeln!(src, "{indent}result_bind[{i}].buffer_length = {col_name}_len;")?;
        writeln!(src, "{indent}mysql_stmt_fetch_column(stmt, &result_bind[{i}], {i}, 0);")?;
    }
    Ok(())
}

/// Emit row construction from the local variables populated by mysql fetch.
fn emit_mysql_row_construction(src: &mut String, row_type: &str, columns: &[ResultColumn], indent: &str) -> anyhow::Result<()> {
    writeln!(src, "{indent}auto result = {row_type}{{")?;
    for (i, col) in columns.iter().enumerate() {
        let col_name = to_snake_case(&col.name);
        let val = format!("{col_name}_val");
        let expr = if col.nullable {
            let base_type = cpp_type(&col.sql_type, false);
            format!("{col_name}_is_null ? std::nullopt : std::optional<{base_type}>({val})")
        } else {
            val
        };
        let comma = if i + 1 < columns.len() { "," } else { "" };
        writeln!(src, "{indent}    {expr}{comma}")?;
    }
    writeln!(src, "{indent}}};")?;
    Ok(())
}

// ─── Querier ────────────────────────────────────────────

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

// ─── Emit Query files ────────────────────────────────────────────

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
    if matches!(contract.body_emitter, CppBodyEmitter::Mysql) {
        writeln!(src, "#include <cstring>")?;
    }
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