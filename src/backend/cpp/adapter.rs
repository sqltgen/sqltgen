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

/// C++ RAII wrapper for `sqlite3_stmt*`.
///
/// Wraps the sqlite3 statement lifecycle so generated query functions don't
/// have to track `sqlite3_finalize` on every error path. The class is
/// intentionally thin — every method maps 1:1 to a single `sqlite3_*` call:
///
/// - constructor           → `sqlite3_prepare_v2`
/// - destructor            → `sqlite3_finalize`
/// - `bind_*`              → `sqlite3_bind_*`  (return code checked)
/// - `step`                → `sqlite3_step`   (SQLITE_ROW → true, SQLITE_DONE → false, else throw)
/// - `execute`             → `step()` once, require SQLITE_DONE (for non-row queries)
/// - `changes`             → `sqlite3_changes64` on the stored connection
/// - `is_null` / `column_*`→ `sqlite3_column_type` / `sqlite3_column_*`
///
/// The helper holds `sqlite3*` alongside the statement because
/// `sqlite3_changes64` and `sqlite3_errmsg` are connection-scoped.
const SQLITE_STMT_HELPER: &str = r#"class SqliteStmt {
    sqlite3* db_;
    sqlite3_stmt* stmt_ = nullptr;
public:
    SqliteStmt(sqlite3* db, const std::string& sql) : db_(db) {
        if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt_, nullptr) != SQLITE_OK)
            throw std::runtime_error(sqlite3_errmsg(db_));
    }
    ~SqliteStmt() { if (stmt_) sqlite3_finalize(stmt_); }
    SqliteStmt(const SqliteStmt&) = delete;
    SqliteStmt& operator=(const SqliteStmt&) = delete;

    // Bind a parameter. `i` is 1-based, matching sqlite3's placeholder numbering.
    void bind_int(int i, int v) { check(sqlite3_bind_int(stmt_, i, v)); }
    void bind_int64(int i, std::int64_t v) { check(sqlite3_bind_int64(stmt_, i, v)); }
    void bind_double(int i, double v) { check(sqlite3_bind_double(stmt_, i, v)); }
    void bind_text(int i, const std::string& v) {
        // SQLITE_TRANSIENT: sqlite copies the bytes, so `v` may be destroyed immediately.
        check(sqlite3_bind_text(stmt_, i, v.c_str(), -1, SQLITE_TRANSIENT));
    }
    void bind_blob(int i, const std::vector<std::uint8_t>& v) {
        check(sqlite3_bind_blob(stmt_, i, v.data(), static_cast<int>(v.size()), SQLITE_TRANSIENT));
    }
    void bind_null(int i) { check(sqlite3_bind_null(stmt_, i)); }

    // Advance the cursor. Returns true on SQLITE_ROW, false on SQLITE_DONE.
    // Throws on any other result code.
    bool step() {
        int rc = sqlite3_step(stmt_);
        if (rc == SQLITE_ROW)  return true;
        if (rc == SQLITE_DONE) return false;
        throw std::runtime_error(sqlite3_errmsg(db_));
    }
    // Run a non-row-returning statement. Steps once and requires SQLITE_DONE.
    void execute() {
        if (step()) throw std::runtime_error("execute: unexpected row");
    }
    // Rows modified by the most recent write on this connection.
    std::int64_t changes() const { return sqlite3_changes64(db_); }

    // Column readers. `i` is 0-based, matching sqlite3's column numbering.
    bool is_null(int i) const { return sqlite3_column_type(stmt_, i) == SQLITE_NULL; }
    int column_int(int i) const { return sqlite3_column_int(stmt_, i); }
    std::int64_t column_int64(int i) const { return sqlite3_column_int64(stmt_, i); }
    double column_double(int i) const { return sqlite3_column_double(stmt_, i); }
    std::string column_text(int i) const {
        // sqlite3_column_text points to storage owned by the statement and is valid
        // until the next step/reset/finalize — copying into std::string is deliberate.
        return std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt_, i)));
    }
    std::vector<std::uint8_t> column_blob(int i) const {
        auto* p = reinterpret_cast<const std::uint8_t*>(sqlite3_column_blob(stmt_, i));
        return std::vector<std::uint8_t>(p, p + sqlite3_column_bytes(stmt_, i));
    }

private:
    void check(int rc) {
        if (rc != SQLITE_OK) throw std::runtime_error(sqlite3_errmsg(db_));
    }
};
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
            source_helpers: &[JSON_ESCAPE, SQLITE_STMT_HELPER],
            emit_query_body: emit_sqlite3_body,
        },
        super::CppTarget::Libmysqlclient => CppCoreContract {
            db_include: "<mysql.h>",
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
/// Uses `SqliteStmt` (a generated RAII wrapper) for the statement lifecycle.
/// The wrapper's constructor prepares the statement, its destructor finalizes
/// it, and `step()` translates the result codes into `bool`/throw — so the
/// generated bodies don't manage `sqlite3_finalize` or the `SQLITE_DONE` check
/// on any path.
fn emit_sqlite3_body(src: &mut String, ctx: &CppQueryContext<'_>) -> anyhow::Result<()> {
    let query = ctx.query;
    let const_name = crate::backend::common::sql_const_name(&query.name);

    writeln!(src, "    SqliteStmt stmt(db, {const_name});")?;
    emit_sqlite3_bind_params(src, &query.params)?;

    match query.cmd {
        QueryCmd::Exec => {
            writeln!(src, "    stmt.execute();")?;
        },
        QueryCmd::ExecRows => {
            writeln!(src, "    stmt.execute();")?;
            writeln!(src, "    return stmt.changes();")?;
        },
        QueryCmd::One | QueryCmd::Many => {
            let row_type = result_row_type(query, ctx.schema);
            writeln!(src, "    std::vector<{row_type}> rows;")?;
            writeln!(src, "    while (stmt.step()) {{")?;
            emit_sqlite3_row_push(src, &row_type, &query.result_columns, "        ")?;
            writeln!(src, "    }}")?;
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

/// Emit `stmt.bind_*` calls for each parameter.
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
    writeln!(src, "    stmt.bind_text({idx}, {name}_json);")?;
    Ok(())
}

/// Return the appropriate `stmt.bind_*` expression for a parameter.
fn sqlite3_bind_call(sql_type: &SqlType, idx: usize, name: &str, nullable: bool) -> String {
    if nullable {
        let inner = sqlite3_bind_call(sql_type, idx, &format!("{name}.value()"), false);
        return format!("{name}.has_value() ? {inner} : stmt.bind_null({idx})");
    }
    match sql_type {
        SqlType::Boolean => format!("stmt.bind_int({idx}, static_cast<int>({name}))"),
        SqlType::SmallInt | SqlType::Integer => format!("stmt.bind_int({idx}, {name})"),
        SqlType::BigInt => format!("stmt.bind_int64({idx}, {name})"),
        SqlType::Real | SqlType::Double => format!("stmt.bind_double({idx}, {name})"),
        SqlType::Bytes => format!("stmt.bind_blob({idx}, {name})"),
        _ => format!("stmt.bind_text({idx}, {name})"),
    }
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

/// Return the C++ expression to read a column value via the `SqliteStmt` helper.
fn sqlite3_column_expr(sql_type: &SqlType, idx: usize, nullable: bool) -> String {
    if nullable {
        let base_type = cpp_type(sql_type, false);
        let inner = sqlite3_column_expr(sql_type, idx, false);
        return format!("stmt.is_null({idx}) ? std::nullopt : std::optional<{base_type}>({inner})");
    }
    match sql_type {
        SqlType::Boolean => format!("static_cast<bool>(stmt.column_int({idx}))"),
        SqlType::SmallInt => format!("static_cast<std::int16_t>(stmt.column_int({idx}))"),
        SqlType::Integer => format!("stmt.column_int({idx})"),
        SqlType::BigInt => format!("stmt.column_int64({idx})"),
        SqlType::Real => format!("static_cast<float>(stmt.column_double({idx}))"),
        SqlType::Double => format!("stmt.column_double({idx})"),
        SqlType::Bytes => format!("stmt.column_blob({idx})"),
        _ => format!("stmt.column_text({idx})"),
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

    let mut declared: std::collections::HashSet<String> = std::collections::HashSet::new();

    for (slot, &param_idx) in bind_plan.iter().enumerate() {
        let param = by_idx[&param_idx];
        let name = to_snake_case(&param.name);
        let first_time = declared.insert(name.clone());
        writeln!(src)?;
        emit_mysql_bind_param_block(src, param, slot, &name, first_time)?;
    }

    writeln!(src, "    stmt.bind_param(bind);")?;
    Ok(())
}

/// Emit a single `bind[slot]` block in the canonical field order:
/// `buffer`, `buffer_type`, (`length` preceded by its `p_*_len` local), `buffer_length`,
/// (`is_null` preceded by its `*_is_null` flag). Helper locals (`p_*_len`, `*_is_null`,
/// `*_val`, `p_*_json`) are emitted only the first time a given parameter name is seen,
/// so the same param referenced in multiple slots doesn't redeclare them.
fn emit_mysql_bind_param_block(src: &mut String, param: &Parameter, slot: usize, name: &str, first_time: bool) -> anyhow::Result<()> {
    writeln!(src, "    // {name} — {}", mysql_param_shape_label(param))?;

    // List params are encoded as a JSON blob string.
    if param.is_list {
        if first_time {
            emit_mysql_bind_list_json(src, &param.sql_type, name)?;
        }
        writeln!(src, "    bind[{slot}].buffer = const_cast<char*>(p_{name}_json.c_str());")?;
        writeln!(src, "    bind[{slot}].buffer_type = MYSQL_TYPE_STRING;")?;
        if first_time {
            writeln!(src, "    unsigned long p_{name}_json_len = p_{name}_json.size();")?;
        }
        writeln!(src, "    bind[{slot}].length = &p_{name}_json_len;")?;
        writeln!(src, "    bind[{slot}].buffer_length = p_{name}_json_len;")?;
        return Ok(());
    }

    let mysql_type = mysql_type_const(&param.sql_type);
    let is_varlen = mysql_is_varlen(&param.sql_type);

    // Nullable fixed-width scalar: can't take address of `value_or(0)` directly,
    // so materialize into a named local first.
    if param.nullable && !is_varlen {
        let val_var = format!("{name}_val");
        let cpp_ty = cpp_type(&param.sql_type, false);
        if first_time {
            writeln!(src, "    {cpp_ty} {val_var} = {name}.value_or({cpp_ty}{{}});")?;
        }
        writeln!(src, "    bind[{slot}].buffer = &{val_var};")?;
        writeln!(src, "    bind[{slot}].buffer_type = {mysql_type};")?;
        let flag = format!("{name}_is_null");
        if first_time {
            writeln!(src, "    my_bool {flag} = !{name}.has_value();")?;
        }
        writeln!(src, "    bind[{slot}].is_null = &{flag};")?;
        return Ok(());
    }

    // Remaining cases: non-null scalar, non-null varlen, nullable varlen.
    // For nullable varlen we inline a ternary so libmysql gets a valid (but ignored)
    // pointer when the optional is empty — libmysql reads `is_null` first and skips
    // the buffer side entirely when the flag is set.
    let buf_expr: String = match (&param.sql_type, param.nullable) {
        (SqlType::Bytes, true) => format!("const_cast<char*>({name}.has_value() ? reinterpret_cast<const char*>({name}.value().data()) : nullptr)"),
        (_, true) => format!("const_cast<char*>({name}.has_value() ? {name}.value().c_str() : \"\")"),
        _ => mysql_bind_info(&param.sql_type, name).1,
    };

    writeln!(src, "    bind[{slot}].buffer = {buf_expr};")?;
    writeln!(src, "    bind[{slot}].buffer_type = {mysql_type};")?;

    if is_varlen {
        let len_var = format!("p_{name}_len");
        if first_time {
            if param.nullable {
                writeln!(src, "    unsigned long {len_var} = {name}.has_value() ? {name}.value().size() : 0;")?;
            } else {
                writeln!(src, "    unsigned long {len_var} = {name}.size();")?;
            }
        }
        writeln!(src, "    bind[{slot}].length = &{len_var};")?;
        writeln!(src, "    bind[{slot}].buffer_length = {len_var};")?;
    }

    if param.nullable {
        let flag = format!("{name}_is_null");
        if first_time {
            writeln!(src, "    my_bool {flag} = !{name}.has_value();")?;
        }
        writeln!(src, "    bind[{slot}].is_null = &{flag};")?;
    }

    Ok(())
}

/// Build the `p_{name}_json` blob local for a list parameter (without its length decl,
/// which the caller emits at the canonical position right before `bind[i].length`).
fn emit_mysql_bind_list_json(src: &mut String, sql_type: &SqlType, name: &str) -> anyhow::Result<()> {
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
    Ok(())
}

/// Terse, type-like tag for a bind-block header comment. Examples:
/// `int`, `int?`, `string`, `string?`, `int[] (JSON)`. Encodes only the
/// dimensions that change the block's shape (nullability, varlen-ness,
/// list-ness); the exact MySQL/C++ type is already visible on the next line.
fn mysql_param_shape_label(param: &Parameter) -> String {
    if param.is_list {
        let inner = match &param.sql_type {
            SqlType::Array(inner) => inner.as_ref(),
            other => other,
        };
        return format!("{}[] (JSON)", sql_type_short_label(inner));
    }
    let base = sql_type_short_label(&param.sql_type);
    if param.nullable {
        format!("{base}?")
    } else {
        base
    }
}

fn sql_type_short_label(sql_type: &SqlType) -> String {
    match sql_type {
        SqlType::Boolean => "bool".to_string(),
        SqlType::SmallInt => "smallint".to_string(),
        SqlType::Integer => "int".to_string(),
        SqlType::BigInt => "bigint".to_string(),
        SqlType::Real => "float".to_string(),
        SqlType::Double => "double".to_string(),
        SqlType::Decimal => "decimal".to_string(),
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => "string".to_string(),
        SqlType::Bytes => "bytes".to_string(),
        SqlType::Date => "date".to_string(),
        SqlType::Time => "time".to_string(),
        SqlType::Timestamp => "timestamp".to_string(),
        SqlType::TimestampTz => "timestamptz".to_string(),
        SqlType::Interval => "interval".to_string(),
        SqlType::Uuid => "uuid".to_string(),
        SqlType::Json | SqlType::Jsonb => "json".to_string(),
        SqlType::Array(inner) => format!("{}[]", sql_type_short_label(inner)),
        SqlType::Enum(n) => n.clone(),
        SqlType::Custom(n) => n.clone(),
    }
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
        let label = {
            let mut s = sql_type_short_label(&col.sql_type);
            if col.nullable {
                s.push('?');
            }
            if mysql_is_varlen(&col.sql_type) {
                s.push_str(" (two-phase)");
            }
            s
        };
        writeln!(src)?;
        writeln!(src, "    // {col_name} — {label}")?;
        if mysql_is_varlen(&col.sql_type) {
            writeln!(src, "    result_bind[{i}].buffer = nullptr; // filled below")?;
            writeln!(src, "    result_bind[{i}].buffer_type = {mysql_type};")?;
            writeln!(src, "    unsigned long {col_name}_len = 0;")?;
            writeln!(src, "    result_bind[{i}].length = &{col_name}_len;")?;
            writeln!(src, "    result_bind[{i}].buffer_length = 0;")?;
            writeln!(src, "    my_bool {col_name}_is_null = false;")?;
            writeln!(src, "    result_bind[{i}].is_null = &{col_name}_is_null;")?;
        } else {
            let cpp_ty = cpp_type(&col.sql_type, false);
            writeln!(src, "    {cpp_ty} {col_name}_val{{}};")?;
            writeln!(src, "    result_bind[{i}].buffer = &{col_name}_val;")?;
            writeln!(src, "    result_bind[{i}].buffer_type = {mysql_type};")?;
            writeln!(src, "    my_bool {col_name}_is_null = false;")?;
            writeln!(src, "    result_bind[{i}].is_null = &{col_name}_is_null;")?;
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
