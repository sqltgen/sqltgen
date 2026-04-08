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

/// C++ helper: escapes a string for safe embedding in a JSON array.
/// Used by SQLite and MySQL list-param serialization (json_each).
const JSON_ESCAPE: &str = r#"static std::string json_escape(const std::string& s) {
    std::string out = "\"";
    for (char c : s) {
        if (c == '\\' || c == '"') out += '\\';
        out += c;
    }
    out += '"';
    return out;
}
"#;

/// C++ helper: parses a JSON string using simdjson.
/// Not wired up yet — will be used when simdjson support is added.
#[allow(dead_code)]
const _PARSE_JSON: &str = r#"static simdjson::dom::element parse_json(simdjson::dom::parser& parser, const std::string& s) {
    return parser.parse(simdjson::padded_string(s));
}
"#;

/// C++ RAII wrapper for `MYSQL_STMT*`.
/// Handles init+prepare in the constructor and close in the destructor,
/// so generated query functions never need manual `mysql_stmt_close` on
/// error paths.
const MYSQL_STMT_HELPER: &str = r#"class MysqlStmt {
    MYSQL_STMT* stmt_;
public:
    MysqlStmt(MYSQL* db, const std::string& sql) : stmt_(mysql_stmt_init(db)) {
        if (!stmt_) throw std::runtime_error(mysql_error(db));
        if (mysql_stmt_prepare(stmt_, sql.c_str(), sql.size()) != 0) {
            std::string err = mysql_stmt_error(stmt_);
            mysql_stmt_close(stmt_);
            throw std::runtime_error(err);
        }
    }
    ~MysqlStmt() { if (stmt_) mysql_stmt_close(stmt_); }
    MysqlStmt(const MysqlStmt&) = delete;
    MysqlStmt& operator=(const MysqlStmt&) = delete;
    void bind_param(MYSQL_BIND* bind) {
        if (mysql_stmt_bind_param(stmt_, bind) != 0)
            throw std::runtime_error(mysql_stmt_error(stmt_));
    }
    void execute() {
        if (mysql_stmt_execute(stmt_) != 0)
            throw std::runtime_error(mysql_stmt_error(stmt_));
    }
    void bind_result(MYSQL_BIND* bind) {
        if (mysql_stmt_bind_result(stmt_, bind) != 0)
            throw std::runtime_error(mysql_stmt_error(stmt_));
    }
    bool fetch_row() {
        int rc = mysql_stmt_fetch(stmt_);
        if (rc == MYSQL_NO_DATA) return false;
        if (rc != 0 && rc != MYSQL_DATA_TRUNCATED)
            throw std::runtime_error(mysql_stmt_error(stmt_));
        return true;
    }
    void fetch_column(MYSQL_BIND* bind, unsigned col) {
        mysql_stmt_fetch_column(stmt_, bind, col, 0);
    }
    my_ulonglong affected_rows() { return mysql_stmt_affected_rows(stmt_); }
};
"#;

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
    /// Static C++ helper functions emitted at the top of each `.cpp` file.
    pub(super) source_helpers: &'static [&'static str],
    /// Engine-specific query body emitter.
    pub(super) emit_query_body: for<'a> fn(&mut String, &CppQueryContext<'a>) -> anyhow::Result<()>,
}

/// Per-query context forwarded from the generic core to the adapter-specific emitter.
pub(super) struct CppQueryContext<'a> {
    pub(super) query: &'a Query,
    pub(super) schema: &'a Schema,
}

/// Resolve the engine-specific C++ generation contract for the selected backend.
pub(super) fn resolve_contract(target: &super::CppTarget) -> CppCoreContract {
    match target {
        super::CppTarget::Libpqxx => CppCoreContract {
            db_include: "<pqxx/pqxx>",
            conn_type: "pqxx::connection&",
            param_style: CppParamStyle::Dollar,
            source_includes: &[],
            source_helpers: &[],
            emit_query_body: emit_pqxx_body,
        },
        super::CppTarget::Sqlite3 => CppCoreContract {
            db_include: "<sqlite3.h>",
            conn_type: "sqlite3*",
            param_style: CppParamStyle::QuestionNumbered,
            source_includes: &[],
            source_helpers: &[JSON_ESCAPE],
            emit_query_body: emit_sqlite3_body,
        },
        super::CppTarget::Libmysqlclient => CppCoreContract {
            db_include: "<mysql/mysql.h>",
            conn_type: "MYSQL*",
            param_style: CppParamStyle::QuestionAnon,
            source_includes: &["<cstring>"],
            source_helpers: &[JSON_ESCAPE, MYSQL_STMT_HELPER],
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
///
/// Two groups share structure:
/// - Exec/ExecRows: single step expecting SQLITE_DONE, no row reading.
/// - One/Many: row-collecting loop with identical prepare→bind→loop→finalize→check,
///   diverging only at the tail (One asserts at-most-one, Many returns the vector).
fn emit_sqlite3_body(src: &mut String, ctx: &CppQueryContext<'_>) -> anyhow::Result<()> {
    let query = ctx.query;
    let const_name = crate::backend::common::sql_const_name(&query.name);

    // Phase 1: prepare (all 4 cases)
    emit_sqlite3_prepare(src, &const_name)?;

    // Phase 2: bind params (all 4 cases)
    emit_sqlite3_bind_params(src, &query.params)?;

    // Phase 3: execute + read
    match query.cmd {
        QueryCmd::Exec | QueryCmd::ExecRows => {
            writeln!(src, "    int rc = sqlite3_step(stmt);")?;
            if query.cmd == QueryCmd::ExecRows {
                writeln!(src, "    std::int64_t affected = sqlite3_changes64(db);")?;
            }
            emit_sqlite3_finalize_and_check(src)?;
            if query.cmd == QueryCmd::ExecRows {
                writeln!(src, "    return affected;")?;
            }
        },
        QueryCmd::One | QueryCmd::Many => {
            let row_type = result_row_type(query, ctx.schema);
            // Row-collecting loop (identical for One and Many).
            writeln!(src, "    std::vector<{row_type}> rows;")?;
            writeln!(src, "    int rc;")?;
            writeln!(src, "    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {{")?;
            emit_sqlite3_row_push(src, &row_type, &query.result_columns, "        ")?;
            writeln!(src, "    }}")?;
            emit_sqlite3_finalize_and_check(src)?;
            // Tail: only divergence between One and Many.
            if query.cmd == QueryCmd::One {
                writeln!(src, "    if (rows.size() > 1) {{")?;
                writeln!(src, "        throw std::runtime_error(\"query returned more than one row\");")?;
                writeln!(src, "    }}")?;
                writeln!(src, "    return rows.empty() ? std::nullopt : std::optional<{row_type}>(std::move(rows[0]));")?;
            } else {
                writeln!(src, "    return rows;")?;
            }
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
        _ => format!("{name}_json += json_escape({name}[i]);"),
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

/// Emit `sqlite3_finalize(stmt)` + `SQLITE_DONE` check. Shared by all 4 cases.
fn emit_sqlite3_finalize_and_check(src: &mut String) -> anyhow::Result<()> {
    writeln!(src, "    sqlite3_finalize(stmt);")?;
    writeln!(src, "    if (rc != SQLITE_DONE) {{")?;
    writeln!(src, "        throw std::runtime_error(sqlite3_errmsg(db));")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit `rows.push_back(RowType{ col0, col1, ... });`
fn emit_sqlite3_row_push(src: &mut String, row_type: &str, columns: &[ResultColumn], indent: &str) -> anyhow::Result<()> {
    writeln!(src, "{indent}rows.push_back({row_type}{{")?;
    for (i, col) in columns.iter().enumerate() {
        let expr = sqlite3_column_expr(&col.sql_type, i, col.nullable);
        let comma = if i + 1 < columns.len() { "," } else { "" };
        writeln!(src, "{indent}    {expr}{comma}")?;
    }
    writeln!(src, "{indent}}});")?;
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
///
/// Uses `MysqlStmt` (a generated RAII wrapper) for the statement lifecycle.
/// `MysqlStmt`'s constructor does init+prepare; its destructor closes the
/// statement, so no manual `mysql_stmt_close` is needed on any path.
fn emit_mysql_body(src: &mut String, ctx: &CppQueryContext<'_>) -> anyhow::Result<()> {
    let query = ctx.query;
    let const_name = crate::backend::common::sql_const_name(&query.name);

    writeln!(src, "    MysqlStmt stmt(db, {const_name});")?;
    if !query.params.is_empty() {
        writeln!(src)?;
    }
    emit_mysql_bind_params(src, query)?;
    if !query.params.is_empty() {
        writeln!(src)?;
    }

    match query.cmd {
        QueryCmd::Exec => {
            writeln!(src, "    stmt.execute();")?;
        },
        QueryCmd::ExecRows => {
            writeln!(src, "    stmt.execute();")?;
            writeln!(src, "    return static_cast<std::int64_t>(stmt.affected_rows());")?;
        },
        QueryCmd::One => {
            let row_type = result_row_type(query, ctx.schema);
            writeln!(src, "    stmt.execute();")?;
            writeln!(src)?;
            emit_mysql_bind_result_columns(src, &query.result_columns)?;
            writeln!(src)?;
            writeln!(src, "    if (!stmt.fetch_row()) return std::nullopt;")?;
            let has_varlen = query.result_columns.iter().any(|c| mysql_is_varlen(&c.sql_type));
            if has_varlen {
                writeln!(src)?;
                emit_mysql_fetch_varlen_columns(src, &query.result_columns)?;
                writeln!(src)?;
            }
            emit_mysql_row_construction(src, &row_type, &query.result_columns, "    ", "return ", ";")?;
        },
        QueryCmd::Many => {
            let row_type = result_row_type(query, ctx.schema);
            writeln!(src, "    stmt.execute();")?;
            writeln!(src)?;
            emit_mysql_bind_result_columns(src, &query.result_columns)?;
            writeln!(src)?;
            writeln!(src, "    std::vector<{row_type}> rows;")?;
            writeln!(src, "    while (stmt.fetch_row()) {{")?;
            let has_varlen = query.result_columns.iter().any(|c| mysql_is_varlen(&c.sql_type));
            if has_varlen {
                writeln!(src)?;
                emit_mysql_fetch_varlen_columns_indented(src, &query.result_columns, "        ")?;
                writeln!(src)?;
            }
            emit_mysql_row_construction(src, &row_type, &query.result_columns, "        ", "rows.push_back(", ");")?;
            writeln!(src, "    }}")?;
            writeln!(src, "    return rows;")?;
        },
    }
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

    writeln!(src, "    stmt.bind_param(bind);")?;
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
        _ => format!("p_{name}_json += json_escape({name}[i]);"),
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
            writeln!(src, "    my_bool {col_name}_is_null = false;")?;
            writeln!(src, "    result_bind[{i}].buffer_type   = {mysql_type};")?;
            writeln!(src, "    result_bind[{i}].buffer        = nullptr;")?;
            writeln!(src, "    result_bind[{i}].buffer_length = 0;")?;
            writeln!(src, "    result_bind[{i}].length        = &{col_name}_len;")?;
            writeln!(src, "    result_bind[{i}].is_null       = &{col_name}_is_null;")?;
        } else {
            let cpp_ty = cpp_type(&col.sql_type, false);
            writeln!(src, "    {cpp_ty} {col_name}_val{{}};")?;
            writeln!(src, "    my_bool {col_name}_is_null = false;")?;
            writeln!(src, "    result_bind[{i}].buffer_type = {mysql_type};")?;
            writeln!(src, "    result_bind[{i}].buffer      = &{col_name}_val;")?;
            writeln!(src, "    result_bind[{i}].is_null     = &{col_name}_is_null;")?;
        }
    }

    writeln!(src, "    stmt.bind_result(result_bind);")?;
    Ok(())
}

fn emit_mysql_fetch_varlen_columns(src: &mut String, columns: &[ResultColumn]) -> anyhow::Result<()> {
    emit_mysql_fetch_varlen_columns_indented(src, columns, "    ")
}

fn emit_mysql_fetch_varlen_columns_indented(src: &mut String, columns: &[ResultColumn], indent: &str) -> anyhow::Result<()> {
    let mut first = true;
    for (i, col) in columns.iter().enumerate() {
        if !mysql_is_varlen(&col.sql_type) {
            continue;
        }
        if !first {
            writeln!(src)?;
        }
        first = false;
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
        writeln!(src, "{indent}stmt.fetch_column(&result_bind[{i}], {i});")?;
    }
    Ok(())
}

fn emit_mysql_row_construction(src: &mut String, row_type: &str, columns: &[ResultColumn], indent: &str, open: &str, close: &str) -> anyhow::Result<()> {
    writeln!(src, "{indent}{open}{row_type}{{")?;
    for (i, col) in columns.iter().enumerate() {
        let col_name = to_snake_case(&col.name);
        let val = format!("{col_name}_val");
        let is_varlen = mysql_is_varlen(&col.sql_type);
        let expr = if col.nullable {
            let base_type = cpp_type(&col.sql_type, false);
            if is_varlen {
                format!("{col_name}_is_null ? std::nullopt : std::optional<{base_type}>(std::move({val}))")
            } else {
                format!("{col_name}_is_null ? std::nullopt : std::optional<{base_type}>({val})")
            }
        } else if is_varlen {
            format!("std::move({val})")
        } else {
            val
        };
        let comma = if i + 1 < columns.len() { "," } else { "" };
        writeln!(src, "{indent}    {expr}{comma}")?;
    }
    writeln!(src, "{indent}}}{close}")?;
    Ok(())
}
