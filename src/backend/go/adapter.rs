use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::GeneratedFile;
use crate::config::OutputConfig;

use super::GoTarget;

/// How SQL placeholders are formatted for this engine.
#[derive(Clone, Copy, PartialEq)]
pub(super) enum GoPlaceholderMode {
    /// PostgreSQL-style `$N` numbered placeholders.
    NumberedDollar,
    /// Anonymous `?` placeholders (SQLite, MySQL).
    QuestionMark,
}

/// How scalar query parameters are bound for this engine.
#[derive(Clone, Copy)]
pub(super) enum GoBindMode {
    /// Bind once per unique parameter index (PostgreSQL `$N` resolves by number).
    UniqueParams,
    /// Bind once per SQL placeholder occurrence (SQLite, MySQL positional).
    Positional,
}

/// How JSON columns are represented in Go.
#[derive(Clone, Copy)]
pub(super) enum GoJsonMode {
    /// `[]byte` — PostgreSQL returns raw JSON bytes.
    Bytes,
    /// `string` — SQLite and MySQL return JSON as a text string.
    String,
}

/// Compile-time adapter contract consumed by the Go core emitter.
///
/// Captures all engine/driver differences so core.rs can emit clean,
/// branch-free code.
pub(super) struct GoCoreContract {
    /// One-line comment naming the recommended driver, emitted in `sqltgen.go`.
    pub(super) driver_comment: &'static str,
    /// Placeholder format used when rewriting SQL.
    pub(super) placeholder_mode: GoPlaceholderMode,
    /// Whether to bind params by unique index or by SQL occurrence order.
    pub(super) bind_mode: GoBindMode,
    /// JSON column representation.
    pub(super) json_mode: GoJsonMode,
    /// Expression template for binding array parameters. `{name}` is replaced
    /// with the Go variable name (e.g. `"pq.Array({name})"` or `"{name}"`).
    pub(super) array_param_expr: &'static str,
    /// Import required by `array_param_expr`, if any.
    pub(super) array_param_import: Option<&'static str>,

    // ── DB interface abstraction ──────────────────────────────────────────────
    /// Go type used in function signatures for the DB handle (e.g. `"*sql.DB"`).
    pub(super) db_type: &'static str,
    /// Method name for exec statements (e.g. `"ExecContext"`).
    pub(super) exec_method: &'static str,
    /// Method name for multi-row queries (e.g. `"QueryContext"`).
    pub(super) query_method: &'static str,
    /// Method name for single-row queries (e.g. `"QueryRowContext"`).
    pub(super) query_row_method: &'static str,
    /// Expression for the "no rows" error sentinel (e.g. `"sql.ErrNoRows"`).
    pub(super) no_rows_expr: &'static str,
    /// Import required by `no_rows_expr` if not already covered by other imports.
    pub(super) no_rows_import: Option<&'static str>,
    /// Expression template for scanning array result columns. `{dest}` is
    /// replaced with the destination expression (e.g. `"scanArray({dest})"` or
    /// `"{dest}"` when the driver handles arrays natively).
    pub(super) array_scan_expr: &'static str,
    /// Whether the queries file needs `"database/sql"` imported unconditionally.
    /// False when the driver provides its own DB interface (nullable types that
    /// need `database/sql` are handled separately by the type map).
    pub(super) needs_database_sql_import: bool,
}

/// Resolve the Go adapter contract for the selected engine target.
pub(super) fn resolve_go_contract(target: &GoTarget) -> GoCoreContract {
    match target {
        GoTarget::Postgres => GoCoreContract {
            driver_comment: "// Driver: github.com/jackc/pgx/v5 (native)",
            placeholder_mode: GoPlaceholderMode::NumberedDollar,
            bind_mode: GoBindMode::UniqueParams,
            json_mode: GoJsonMode::Bytes,
            array_param_expr: "{name}",
            array_param_import: None,
            db_type: "DBTX",
            exec_method: "Exec",
            query_method: "Query",
            query_row_method: "QueryRow",
            no_rows_expr: "pgx.ErrNoRows",
            no_rows_import: Some("\"github.com/jackc/pgx/v5\""),
            array_scan_expr: "{dest}",
            needs_database_sql_import: false,
        },
        GoTarget::Sqlite => GoCoreContract {
            driver_comment: "// Driver: modernc.org/sqlite",
            placeholder_mode: GoPlaceholderMode::QuestionMark,
            bind_mode: GoBindMode::Positional,
            json_mode: GoJsonMode::String,
            array_param_expr: "{name}",
            array_param_import: None,
            db_type: "*sql.DB",
            exec_method: "ExecContext",
            query_method: "QueryContext",
            query_row_method: "QueryRowContext",
            no_rows_expr: "sql.ErrNoRows",
            no_rows_import: None,
            array_scan_expr: "scanArray({dest})",
            needs_database_sql_import: true,
        },
        GoTarget::Mysql => GoCoreContract {
            driver_comment: "// Driver: github.com/go-sql-driver/mysql",
            placeholder_mode: GoPlaceholderMode::QuestionMark,
            bind_mode: GoBindMode::Positional,
            json_mode: GoJsonMode::String,
            array_param_expr: "{name}",
            array_param_import: None,
            db_type: "*sql.DB",
            exec_method: "ExecContext",
            query_method: "QueryContext",
            query_row_method: "QueryRowContext",
            no_rows_expr: "sql.ErrNoRows",
            no_rows_import: None,
            array_scan_expr: "scanArray({dest})",
            needs_database_sql_import: true,
        },
    }
}

/// Emit the static `_sqltgen.go` helper file.
pub(super) fn emit_helper_file(contract: &GoCoreContract, package_name: &str, config: &OutputConfig) -> GeneratedFile {
    let content = build_helper_content(contract, package_name);
    GeneratedFile { path: PathBuf::from(&config.out).join("sqltgen.go"), content }
}

fn build_helper_content(contract: &GoCoreContract, package_name: &str) -> String {
    if contract.db_type == "DBTX" {
        build_pgx_helper(contract, package_name)
    } else {
        build_database_sql_helper(contract, package_name)
    }
}

/// Helper file for drivers using `database/sql` (SQLite, MySQL, and any future
/// database/sql-based driver).
fn build_database_sql_helper(contract: &GoCoreContract, package_name: &str) -> String {
    let dollar = contract.placeholder_mode == GoPlaceholderMode::NumberedDollar;
    let mut src = String::new();
    _ = writeln!(src, "// Code generated by sqltgen. Do not edit.");
    _ = writeln!(src, "{}", contract.driver_comment);
    _ = writeln!(src);
    _ = writeln!(src, "package {package_name}");
    _ = writeln!(src);
    _ = writeln!(src, "import (");
    _ = writeln!(src, "\t\"context\"");
    _ = writeln!(src, "\t\"database/sql\"");
    _ = writeln!(src, "\t\"fmt\"");
    _ = writeln!(src, "\t\"strings\"");
    if let Some(arr_import) = contract.array_param_import {
        _ = writeln!(src, "\t{arr_import}");
    }
    _ = writeln!(src, ")");
    _ = writeln!(src);
    emit_exec_rows_database_sql(&mut src, contract);
    _ = writeln!(src);
    emit_build_in_clause(&mut src, dollar);
    _ = writeln!(src);
    _ = writeln!(src, "// scanArray returns a scan destination for SQL ARRAY columns.");
    _ = writeln!(src, "func scanArray(dest any) any {{");
    if let Some(arr_import) = contract.array_param_import {
        // If we have an array import, we have pq.Array
        let _ = arr_import;
        _ = writeln!(src, "\treturn pq.Array(dest)");
    } else {
        _ = writeln!(src, "\treturn dest");
    }
    _ = writeln!(src, "}}");
    src
}

/// Helper file for pgx native driver (PostgreSQL).
fn build_pgx_helper(contract: &GoCoreContract, package_name: &str) -> String {
    let mut src = String::new();
    _ = writeln!(src, "// Code generated by sqltgen. Do not edit.");
    _ = writeln!(src, "{}", contract.driver_comment);
    _ = writeln!(src);
    _ = writeln!(src, "package {package_name}");
    _ = writeln!(src);
    _ = writeln!(src, "import (");
    _ = writeln!(src, "\t\"context\"");
    _ = writeln!(src, "\t\"fmt\"");
    _ = writeln!(src, "\t\"strings\"");
    _ = writeln!(src);
    _ = writeln!(src, "\t\"github.com/jackc/pgx/v5\"");
    _ = writeln!(src, "\t\"github.com/jackc/pgx/v5/pgconn\"");
    _ = writeln!(src, ")");
    _ = writeln!(src);
    _ = writeln!(src, "// DBTX is the interface satisfied by *pgxpool.Pool, *pgx.Conn, and pgx.Tx.");
    _ = writeln!(src, "type DBTX interface {{");
    _ = writeln!(src, "\tExec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)");
    _ = writeln!(src, "\tQuery(ctx context.Context, sql string, args ...any) (pgx.Rows, error)");
    _ = writeln!(src, "\tQueryRow(ctx context.Context, sql string, args ...any) pgx.Row");
    _ = writeln!(src, "}}");
    _ = writeln!(src);
    _ = writeln!(src, "// execRows runs a statement and returns the number of affected rows.");
    _ = writeln!(src, "func execRows(ctx context.Context, db DBTX, query string, args ...any) (int64, error) {{");
    _ = writeln!(src, "\ttag, err := db.Exec(ctx, query, args...)");
    _ = writeln!(src, "\tif err != nil {{");
    _ = writeln!(src, "\t\treturn 0, err");
    _ = writeln!(src, "\t}}");
    _ = writeln!(src, "\treturn tag.RowsAffected(), nil");
    _ = writeln!(src, "}}");
    _ = writeln!(src);
    emit_build_in_clause(&mut src, true);
    src
}

/// Emit `execRows` for `database/sql` drivers.
fn emit_exec_rows_database_sql(src: &mut String, contract: &GoCoreContract) {
    let db_type = contract.db_type;
    let exec_method = contract.exec_method;
    _ = writeln!(src, "// execRows runs a statement and returns the number of affected rows.");
    _ = writeln!(src, "func execRows(ctx context.Context, db {db_type}, query string, args ...any) (int64, error) {{");
    _ = writeln!(src, "\tresult, err := db.{exec_method}(ctx, query, args...)");
    _ = writeln!(src, "\tif err != nil {{");
    _ = writeln!(src, "\t\treturn 0, err");
    _ = writeln!(src, "\t}}");
    _ = writeln!(src, "\treturn result.RowsAffected()");
    _ = writeln!(src, "}}");
}

/// Emit `buildInClause` — shared by both pgx and database/sql helpers.
fn emit_build_in_clause(src: &mut String, dollar: bool) {
    _ = writeln!(src, "// buildInClause constructs an IN clause with n positional placeholders.");
    _ = writeln!(src, "func buildInClause(prefix, suffix string, startIdx int, count int) string {{");
    _ = writeln!(src, "\tplaceholders := make([]string, count)");
    _ = writeln!(src, "\tfor i := range count {{");
    if dollar {
        _ = writeln!(src, "\t\tplaceholders[i] = fmt.Sprintf(\"$%d\", startIdx+i)");
    } else {
        _ = writeln!(src, "\t\tplaceholders[i] = \"?\"");
        _ = writeln!(src, "\t\t_ = fmt.Sprintf // suppress unused import");
    }
    _ = writeln!(src, "\t}}");
    _ = writeln!(src, "\treturn prefix + \"IN (\" + strings.Join(placeholders, \", \") + \")\" + suffix");
    _ = writeln!(src, "}}");
}
