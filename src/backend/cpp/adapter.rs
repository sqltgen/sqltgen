use std::fmt::Write;

use crate::backend::naming::to_snake_case;
use crate::backend::sql_rewrite::parse_placeholder_indices;
use crate::ir::{Parameter, Query, QueryCmd, ResultColumn, Schema, SqlType};

use super::core::{cpp_type, field_bindings, move_fields, result_row_type};

/// How SQL placeholders should be rewritten for the target engine's client.
#[derive(Clone, Copy)]
pub(super) enum CppParamStyle {
    /// PostgreSQL libpqxx: uses `$1`, `$2`, … (kept as-is from the IR).
    Dollar,
    /// SQLite sqlite3: uses `?1`, `?2, …` (already in IR for SQLite frontend,
    /// but Postgres-originated SQL needs rewriting).
    QuestionNumbered,
    /// libmysqlclient uses `?` (anonymous positional).
    QuestionAnon,
}

/// Resolved engine-specific contract consumed by `core.rs` emitters.
pub(super) struct CppCoreContract {
    /// Primary `#include` for the database client (e.g. `<pqxx/pqxx>`).
    pub(super) db_include: &'static str,
    /// The C++ type used for a database connection parameter
    /// (e.g. `pqxx::connection&`, `sqlite3*`, `MYSQL*`).
    pub(super) conn_type: &'static str,
    /// Placeholder style used by this engine's client library.
    pub(super) param_style: CppParamStyle,
    /// Extra `#include`s needed in generated `.cpp` source files.
    pub(super) source_includes: &'static [&'static str],
    /// Engine-specific query body emitter.
    pub(super) emit_query_body: for<'a> fn(&mut String, &CppQueryContext<'a>) -> anyhow::Result<()>,
}

/// Per-query context forwarded from the generic core to the adapter-specific emitter.
pub(super) struct CppQueryContext<'a> {
    pub(super) query: &'a Query,
    pub(super) schema: &'a Schema,
}

pub(super) fn resolve_contract(target: &super::CppTarget) -> CppCoreContract {
    match target {
        super::CppTarget::Libpqxx => CppCoreContract {
            db_include: "<pqxx/pqxx>",
            conn_type: "pqxx::connection&",
            param_style: CppParamStyle::Dollar,
            source_includes: &[],
            emit_query_body: emit_pqxx_body,
        },
        super::CppTarget::Sqlite3 => CppCoreContract {
            db_include: "<sqlite3.h>",
            conn_type: "sqlite3*",
            param_style: CppParamStyle::QuestionNumbered,
            source_includes: &[],
            emit_query_body: emit_sqlite3_body,
        },
        super::CppTarget::Libmysqlclient => CppCoreContract {
            db_include: "<mysql/mysql.h>",
            conn_type: "MYSQL*",
            param_style: CppParamStyle::QuestionAnon,
            source_includes: &["<cstring>"],
            emit_query_body: emit_mysql_body,
        },
    }
}

/// Build the `pqxx::params{...}` expression, or `None` if the query has no parameters.
fn pqxx_params_expr(query: &Query) -> Option<String> {
    if query.params.is_empty() {
        None
    } else {
        let names: Vec<String> = query.params.iter().map(|p| to_snake_case(&p.name)).collect();
        Some(format!("pqxx::params{{{}}}", names.join(", ")))
    }
}

/// Build the `<T1, T2, ...>` template type argument list from result columns.
fn pqxx_query_type_args(columns: &[ResultColumn]) -> String {
    columns.iter().map(|col| cpp_type(&col.sql_type, col.nullable)).collect::<Vec<_>>().join(", ")
}

/// Emit the function body for a libpqxx (PostgreSQL) query.
///
/// Uses the idiomatic libpqxx 8 API:
/// - `exec(sql, pqxx::params{...})` for commands (Exec, ExecRows)
/// - `query01<T...>(sql, pqxx::params{...})` for single-row queries (One)
/// - `query<T...>(sql, pqxx::params{...})` for multi-row queries (Many)
fn emit_pqxx_body(src: &mut String, ctx: &CppQueryContext<'_>) -> anyhow::Result<()> {
    let query = ctx.query;
    let const_name = crate::backend::common::sql_const_name(&query.name);
    let params_expr = pqxx_params_expr(query);
    let call_args = match &params_expr {
        Some(p) => format!("{const_name}, {p}"),
        None => const_name,
    };

    writeln!(src, "    pqxx::work txn(db);")?;

    match query.cmd {
        QueryCmd::Exec => {
            writeln!(src, "    txn.exec({call_args}).no_rows();")?;
            writeln!(src, "    txn.commit();")?;
        },
        QueryCmd::ExecRows => {
            writeln!(src, "    auto affected = txn.exec({call_args}).affected_rows();")?;
            writeln!(src, "    txn.commit();")?;
            writeln!(src, "    return static_cast<std::int64_t>(affected);")?;
        },
        QueryCmd::One => {
            let row_type = result_row_type(query, ctx.schema);
            let type_args = pqxx_query_type_args(&query.result_columns);
            writeln!(src, "    auto opt = txn.query01<{type_args}>({call_args});")?;
            writeln!(src, "    txn.commit();")?;
            writeln!(src, "    if (!opt) return std::nullopt;")?;
            let bindings = field_bindings(query);
            writeln!(src, "    auto& [{bindings}] = *opt;")?;
            let moved = move_fields(query);
            writeln!(src, "    return {row_type}{{{moved}}};")?;
        },
        QueryCmd::Many => {
            let row_type = result_row_type(query, ctx.schema);
            let type_args = pqxx_query_type_args(&query.result_columns);
            let bindings = field_bindings(query);
            writeln!(src, "    std::vector<{row_type}> rows;")?;
            writeln!(src, "    for (auto [{bindings}] : txn.query<{type_args}>({call_args})) {{")?;
            let moved = move_fields(query);
            writeln!(src, "        rows.push_back({row_type}{{{moved}}});")?;
            writeln!(src, "    }}")?;
            writeln!(src, "    txn.commit();")?;
            writeln!(src, "    return rows;")?;
        },
    }
    Ok(())
}

/// Emit the function body for a sqlite3 query.
fn emit_sqlite3_body(src: &mut String, ctx: &CppQueryContext<'_>) -> anyhow::Result<()> {
    let query = ctx.query;
    let const_name = crate::backend::common::sql_const_name(&query.name);
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
            let row_type = result_row_type(query, ctx.schema);
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
            let row_type = result_row_type(query, ctx.schema);
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
        SqlType::Boolean | SqlType::SmallInt | SqlType::Integer | SqlType::BigInt | SqlType::Real | SqlType::Double => {
            format!("{name}_json += std::to_string({name}[i]);")
        },
        _ => format!("{name}_json += \"\\\"\" + {name}[i] + \"\\\"\";"),
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
        SqlType::Boolean | SqlType::SmallInt | SqlType::Integer => format!("sqlite3_bind_int(stmt, {idx}, {name})"),
        SqlType::BigInt => format!("sqlite3_bind_int64(stmt, {idx}, {name})"),
        SqlType::Real | SqlType::Double => format!("sqlite3_bind_double(stmt, {idx}, {name})"),
        SqlType::Bytes => {
            format!("sqlite3_bind_blob(stmt, {idx}, {name}.data(), static_cast<int>({name}.size()), SQLITE_TRANSIENT)")
        },
        _ => format!("sqlite3_bind_text(stmt, {idx}, {name}.c_str(), -1, SQLITE_TRANSIENT)"),
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
        SqlType::Boolean => format!("static_cast<bool>(sqlite3_column_int(stmt, {idx}))"),
        SqlType::SmallInt => format!("static_cast<std::int16_t>(sqlite3_column_int(stmt, {idx}))"),
        SqlType::Integer => format!("sqlite3_column_int(stmt, {idx})"),
        SqlType::BigInt => format!("sqlite3_column_int64(stmt, {idx})"),
        SqlType::Real => format!("static_cast<float>(sqlite3_column_double(stmt, {idx}))"),
        SqlType::Double => format!("sqlite3_column_double(stmt, {idx})"),
        SqlType::Bytes => format!("std::vector<std::uint8_t>(reinterpret_cast<const std::uint8_t*>(sqlite3_column_blob(stmt, {idx})), reinterpret_cast<const std::uint8_t*>(sqlite3_column_blob(stmt, {idx})) + sqlite3_column_bytes(stmt, {idx}))"),
        _ => format!("std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt, {idx})))"),
    }
}

/// Emit the function body for a libmysqlclient (MySQL) query.
fn emit_mysql_body(src: &mut String, ctx: &CppQueryContext<'_>) -> anyhow::Result<()> {
    let query = ctx.query;
    let const_name = crate::backend::common::sql_const_name(&query.name);

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
            let row_type = result_row_type(query, ctx.schema);
            emit_mysql_prepare(src, &const_name)?;
            emit_mysql_bind_params(src, query)?;
            emit_mysql_execute(src)?;
            emit_mysql_bind_result_columns(src, &query.result_columns)?;
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
            emit_mysql_fetch_varlen_columns(src, &query.result_columns)?;
            emit_mysql_row_construction(src, &row_type, &query.result_columns, "    ")?;
            writeln!(src, "    mysql_stmt_close(stmt);")?;
            writeln!(src, "    return result;")?;
        },
        QueryCmd::Many => {
            let row_type = result_row_type(query, ctx.schema);
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
fn emit_mysql_bind_params(src: &mut String, query: &Query) -> anyhow::Result<()> {
    if query.params.is_empty() {
        return Ok(());
    }

    let by_idx: std::collections::HashMap<usize, &Parameter> = query.params.iter().map(|p| (p.index, p)).collect();
    let bind_plan = parse_placeholder_indices(&query.sql);
    let n = bind_plan.len();
    writeln!(src, "    MYSQL_BIND bind[{n}];")?;
    writeln!(src, "    memset(bind, 0, sizeof(bind));")?;

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

fn emit_mysql_bind_list_vars(src: &mut String, sql_type: &SqlType, name: &str) -> anyhow::Result<()> {
    let inner_type = match sql_type {
        SqlType::Array(inner) => inner.as_ref(),
        _ => sql_type,
    };
    let to_str = match inner_type {
        SqlType::Boolean | SqlType::SmallInt | SqlType::Integer | SqlType::BigInt | SqlType::Real | SqlType::Double => {
            format!("p_{name}_json += std::to_string({name}[i]);")
        },
        _ => format!("p_{name}_json += \"\\\"\" + {name}[i] + \"\\\"\";"),
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

fn emit_mysql_bind_list_assign(src: &mut String, slot: usize, name: &str) -> anyhow::Result<()> {
    writeln!(src, "    bind[{slot}].buffer_type = MYSQL_TYPE_STRING;")?;
    writeln!(src, "    bind[{slot}].buffer = const_cast<char*>(p_{name}_json.c_str());")?;
    writeln!(src, "    bind[{slot}].buffer_length = p_{name}_json.size();")?;
    writeln!(src, "    bind[{slot}].length = &p_{name}_json_len;")?;
    Ok(())
}

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

fn mysql_bind_info(sql_type: &SqlType, name: &str) -> (&'static str, String, bool) {
    match sql_type {
        SqlType::Boolean => ("MYSQL_TYPE_TINY", format!("const_cast<bool*>(&{name})"), false),
        SqlType::SmallInt => ("MYSQL_TYPE_SHORT", format!("const_cast<std::int16_t*>(&{name})"), false),
        SqlType::Integer => ("MYSQL_TYPE_LONG", format!("const_cast<std::int32_t*>(&{name})"), false),
        SqlType::BigInt => ("MYSQL_TYPE_LONGLONG", format!("const_cast<std::int64_t*>(&{name})"), false),
        SqlType::Real => ("MYSQL_TYPE_FLOAT", format!("const_cast<float*>(&{name})"), false),
        SqlType::Double => ("MYSQL_TYPE_DOUBLE", format!("const_cast<double*>(&{name})"), false),
        SqlType::Bytes => ("MYSQL_TYPE_BLOB", format!("const_cast<char*>(reinterpret_cast<const char*>({name}.data()))"), true),
        _ => ("MYSQL_TYPE_STRING", format!("const_cast<char*>({name}.c_str())"), true),
    }
}

fn emit_mysql_execute(src: &mut String) -> anyhow::Result<()> {
    writeln!(src, "    if (mysql_stmt_execute(stmt) != 0) {{")?;
    writeln!(src, "        std::string err = mysql_stmt_error(stmt);")?;
    writeln!(src, "        mysql_stmt_close(stmt);")?;
    writeln!(src, "        throw std::runtime_error(err);")?;
    writeln!(src, "    }}")?;
    Ok(())
}

fn mysql_is_varlen(sql_type: &SqlType) -> bool {
    matches!(mysql_bind_info(sql_type, "x"), (_, _, true))
}

fn mysql_type_const(sql_type: &SqlType) -> &'static str {
    mysql_bind_info(sql_type, "x").0
}

fn emit_mysql_bind_result_columns(src: &mut String, columns: &[ResultColumn]) -> anyhow::Result<()> {
    let n = columns.len();
    writeln!(src, "    MYSQL_BIND result_bind[{n}];")?;
    writeln!(src, "    memset(result_bind, 0, sizeof(result_bind));")?;

    for (i, col) in columns.iter().enumerate() {
        let col_name = to_snake_case(&col.name);
        let mysql_type = mysql_type_const(&col.sql_type);
        writeln!(src)?;

        if mysql_is_varlen(&col.sql_type) {
            writeln!(src, "    unsigned long {col_name}_len = 0;")?;
            writeln!(src, "    my_bool {col_name}_is_null = 0;")?;
            writeln!(src, "    result_bind[{i}].buffer_type = {mysql_type};")?;
            writeln!(src, "    result_bind[{i}].buffer = nullptr;")?;
            writeln!(src, "    result_bind[{i}].buffer_length = 0;")?;
            writeln!(src, "    result_bind[{i}].length = &{col_name}_len;")?;
            writeln!(src, "    result_bind[{i}].is_null = &{col_name}_is_null;")?;
        } else {
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

fn emit_mysql_fetch_varlen_columns(src: &mut String, columns: &[ResultColumn]) -> anyhow::Result<()> {
    emit_mysql_fetch_varlen_columns_indented(src, columns, "    ")
}

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
