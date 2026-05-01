use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::sql_rewrite::{positional_bind_names, rewrite_to_anon_params};
use crate::backend::GeneratedFile;
use crate::config::OutputConfig;
use crate::ir::Query;

use super::GoTarget;

/// How JSON columns are represented in Go for a given driver.
#[derive(Clone, Copy)]
pub(super) enum GoJsonMode {
    /// `[]byte` — pgx returns raw JSON bytes.
    Bytes,
    /// `string` — `database/sql` drivers return JSON as text.
    String,
}

/// Driver-specific behavior consumed by the engine-agnostic core.
///
/// Each method encapsulates one place where the generated Go code differs
/// between drivers (pgx for Postgres, `database/sql` for SQLite/MySQL). The
/// core never branches on the target — it only calls these methods.
pub(super) trait GoDriverAdapter {
    // ── Driver characteristics consumed by core/typemap ──────────────────────

    /// JSON column representation used by the typemap.
    fn json_mode(&self) -> GoJsonMode;

    /// Go type used in function signatures for the DB handle.
    fn db_type(&self) -> &'static str;

    /// Method name for exec statements (e.g. `"Exec"` or `"ExecContext"`).
    fn exec_method(&self) -> &'static str;

    /// Method name for multi-row queries.
    fn query_method(&self) -> &'static str;

    /// Method name for single-row queries.
    fn query_row_method(&self) -> &'static str;

    /// Expression for the "no rows" sentinel error (e.g. `"sql.ErrNoRows"`).
    fn no_rows_expr(&self) -> &'static str;

    /// Import that provides `no_rows_expr`, if not already implied by other imports.
    fn no_rows_import(&self) -> Option<&'static str> {
        None
    }

    /// Template for binding array parameters; `{name}` substituted by the Go variable.
    fn array_param_expr(&self) -> &'static str {
        "{name}"
    }

    /// Import required by `array_param_expr`, if any.
    fn array_param_import(&self) -> Option<&'static str> {
        None
    }

    /// Template for scanning array columns; `{dest}` substituted by the destination.
    fn array_scan_expr(&self) -> &'static str;

    /// Whether queries files need `database/sql` imported unconditionally.
    fn needs_database_sql_import(&self) -> bool;

    // ── Behavior driven by placeholder/bind style ────────────────────────────

    /// Normalize SQL placeholders for this driver.
    fn normalize_sql(&self, sql: &str) -> String;

    /// Bind names for a scalar query — one entry per bound argument.
    fn scalar_bind_names<'a>(&self, query: &'a Query) -> Vec<&'a str>;

    /// Whether a dynamic-list query needs `fmt` imported.
    /// Numbered placeholder drivers use `fmt.Sprintf`; `?` drivers don't.
    fn dynamic_list_needs_fmt(&self) -> bool;

    /// Emit the `placeholders := ...` lines plus the `sql := ...` line for a
    /// dynamic-list query. `scalars_before` is the count of scalar parameters
    /// that appear before the list parameter (used for numbered placeholder
    /// offset calculation).
    fn emit_dynamic_sql(&self, src: &mut String, before_sql: &str, after_sql: &str, lp_name: &str, scalars_before: usize) -> anyhow::Result<()>;

    // ── Helper file ──────────────────────────────────────────────────────────

    /// Build the contents of the generated `sqltgen.go` helper file.
    fn helper_content(&self, package_name: &str) -> String;
}

// ── pgx (Postgres) ───────────────────────────────────────────────────────────

pub(super) struct PgxAdapter;

impl GoDriverAdapter for PgxAdapter {
    fn json_mode(&self) -> GoJsonMode {
        GoJsonMode::Bytes
    }
    fn db_type(&self) -> &'static str {
        "DBTX"
    }
    fn exec_method(&self) -> &'static str {
        "Exec"
    }
    fn query_method(&self) -> &'static str {
        "Query"
    }
    fn query_row_method(&self) -> &'static str {
        "QueryRow"
    }
    fn no_rows_expr(&self) -> &'static str {
        "pgx.ErrNoRows"
    }
    fn no_rows_import(&self) -> Option<&'static str> {
        Some("\"github.com/jackc/pgx/v5\"")
    }
    fn array_scan_expr(&self) -> &'static str {
        "{dest}"
    }
    fn needs_database_sql_import(&self) -> bool {
        false
    }

    fn normalize_sql(&self, sql: &str) -> String {
        sql.to_string()
    }

    fn scalar_bind_names<'a>(&self, query: &'a Query) -> Vec<&'a str> {
        query.params.iter().map(|p| p.name.as_str()).collect()
    }

    fn dynamic_list_needs_fmt(&self) -> bool {
        true
    }

    fn emit_dynamic_sql(&self, src: &mut String, before_sql: &str, after_sql: &str, lp_name: &str, scalars_before: usize) -> anyhow::Result<()> {
        writeln!(src, "\tplaceholders := make([]string, len({lp_name}))")?;
        writeln!(src, "\tfor i := range {lp_name} {{")?;
        writeln!(src, "\t\tplaceholders[i] = fmt.Sprintf(\"${{}}\", {start}+i)", start = scalars_before + 1)?;
        writeln!(src, "\t}}")?;
        writeln!(src, "\tsql := `{before_sql}` + \"IN (\" + strings.Join(placeholders, \", \") + \")\" + `{after_sql}`")?;
        Ok(())
    }

    fn helper_content(&self, package_name: &str) -> String {
        let mut src = String::new();
        let _ = writeln!(src, "// Code generated by sqltgen. Do not edit.");
        let _ = writeln!(src, "// Driver: github.com/jackc/pgx/v5 (native)");
        let _ = writeln!(src);
        let _ = writeln!(src, "package {package_name}");
        let _ = writeln!(src);
        let _ = writeln!(src, "import (");
        let _ = writeln!(src, "\t\"context\"");
        let _ = writeln!(src, "\t\"fmt\"");
        let _ = writeln!(src, "\t\"strings\"");
        let _ = writeln!(src);
        let _ = writeln!(src, "\t\"github.com/jackc/pgx/v5\"");
        let _ = writeln!(src, "\t\"github.com/jackc/pgx/v5/pgconn\"");
        let _ = writeln!(src, ")");
        let _ = writeln!(src);
        let _ = writeln!(src, "// DBTX is the interface satisfied by *pgxpool.Pool, *pgx.Conn, and pgx.Tx.");
        let _ = writeln!(src, "type DBTX interface {{");
        let _ = writeln!(src, "\tExec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)");
        let _ = writeln!(src, "\tQuery(ctx context.Context, sql string, args ...any) (pgx.Rows, error)");
        let _ = writeln!(src, "\tQueryRow(ctx context.Context, sql string, args ...any) pgx.Row");
        let _ = writeln!(src, "}}");
        let _ = writeln!(src);
        let _ = writeln!(src, "// execRows runs a statement and returns the number of affected rows.");
        let _ = writeln!(src, "func execRows(ctx context.Context, db DBTX, query string, args ...any) (int64, error) {{");
        let _ = writeln!(src, "\ttag, err := db.Exec(ctx, query, args...)");
        let _ = writeln!(src, "\tif err != nil {{");
        let _ = writeln!(src, "\t\treturn 0, err");
        let _ = writeln!(src, "\t}}");
        let _ = writeln!(src, "\treturn tag.RowsAffected(), nil");
        let _ = writeln!(src, "}}");
        let _ = writeln!(src);
        emit_build_in_clause(&mut src, true);
        src
    }
}

// ── database/sql (SQLite, MySQL) ─────────────────────────────────────────────

pub(super) struct DatabaseSqlAdapter {
    /// One-line `// Driver: …` comment emitted in the helper file header.
    driver_comment: &'static str,
}

impl GoDriverAdapter for DatabaseSqlAdapter {
    fn json_mode(&self) -> GoJsonMode {
        GoJsonMode::String
    }
    fn db_type(&self) -> &'static str {
        "*sql.DB"
    }
    fn exec_method(&self) -> &'static str {
        "ExecContext"
    }
    fn query_method(&self) -> &'static str {
        "QueryContext"
    }
    fn query_row_method(&self) -> &'static str {
        "QueryRowContext"
    }
    fn no_rows_expr(&self) -> &'static str {
        "sql.ErrNoRows"
    }
    fn array_scan_expr(&self) -> &'static str {
        "scanArray({dest})"
    }
    fn needs_database_sql_import(&self) -> bool {
        true
    }

    fn normalize_sql(&self, sql: &str) -> String {
        rewrite_to_anon_params(sql)
    }

    fn scalar_bind_names<'a>(&self, query: &'a Query) -> Vec<&'a str> {
        positional_bind_names(query)
    }

    fn dynamic_list_needs_fmt(&self) -> bool {
        false
    }

    fn emit_dynamic_sql(&self, src: &mut String, before_sql: &str, after_sql: &str, lp_name: &str, _scalars_before: usize) -> anyhow::Result<()> {
        writeln!(src, "\tplaceholders := strings.Repeat(\"?, \", len({lp_name}))")?;
        writeln!(src, "\tif len(placeholders) > 0 {{")?;
        writeln!(src, "\t\tplaceholders = placeholders[:len(placeholders)-2]")?;
        writeln!(src, "\t}}")?;
        writeln!(src, "\tsql := `{before_sql}IN (\" + placeholders + \"){after_sql}`")?;
        Ok(())
    }

    fn helper_content(&self, package_name: &str) -> String {
        let mut src = String::new();
        let _ = writeln!(src, "// Code generated by sqltgen. Do not edit.");
        let _ = writeln!(src, "{}", self.driver_comment);
        let _ = writeln!(src);
        let _ = writeln!(src, "package {package_name}");
        let _ = writeln!(src);
        let _ = writeln!(src, "import (");
        let _ = writeln!(src, "\t\"context\"");
        let _ = writeln!(src, "\t\"database/sql\"");
        let _ = writeln!(src, "\t\"fmt\"");
        let _ = writeln!(src, "\t\"strings\"");
        if let Some(arr_import) = self.array_param_import() {
            let _ = writeln!(src, "\t{arr_import}");
        }
        let _ = writeln!(src, ")");
        let _ = writeln!(src);
        let _ = writeln!(src, "// execRows runs a statement and returns the number of affected rows.");
        let _ = writeln!(src, "func execRows(ctx context.Context, db {db_type}, query string, args ...any) (int64, error) {{", db_type = self.db_type());
        let _ = writeln!(src, "\tresult, err := db.{}(ctx, query, args...)", self.exec_method());
        let _ = writeln!(src, "\tif err != nil {{");
        let _ = writeln!(src, "\t\treturn 0, err");
        let _ = writeln!(src, "\t}}");
        let _ = writeln!(src, "\treturn result.RowsAffected()");
        let _ = writeln!(src, "}}");
        let _ = writeln!(src);
        emit_build_in_clause(&mut src, false);
        let _ = writeln!(src);
        let _ = writeln!(src, "// scanArray returns a scan destination for SQL ARRAY columns.");
        let _ = writeln!(src, "func scanArray(dest any) any {{");
        if self.array_param_import().is_some() {
            let _ = writeln!(src, "\treturn pq.Array(dest)");
        } else {
            let _ = writeln!(src, "\treturn dest");
        }
        let _ = writeln!(src, "}}");
        src
    }
}

/// Build the adapter for the selected target.
pub(super) fn build_adapter(target: &GoTarget) -> Box<dyn GoDriverAdapter> {
    match target {
        GoTarget::Postgres => Box::new(PgxAdapter),
        GoTarget::Sqlite => Box::new(DatabaseSqlAdapter { driver_comment: "// Driver: modernc.org/sqlite" }),
        GoTarget::Mysql => Box::new(DatabaseSqlAdapter { driver_comment: "// Driver: github.com/go-sql-driver/mysql" }),
    }
}

/// Emit the static `sqltgen.go` helper file for the selected adapter.
pub(super) fn emit_helper_file(adapter: &dyn GoDriverAdapter, package_name: &str, config: &OutputConfig) -> GeneratedFile {
    GeneratedFile { path: PathBuf::from(&config.out).join("sqltgen.go"), content: adapter.helper_content(package_name) }
}

/// Emit the shared `buildInClause` helper. `dollar` is true for numbered-placeholder drivers.
fn emit_build_in_clause(src: &mut String, dollar: bool) {
    let _ = writeln!(src, "// buildInClause constructs an IN clause with n positional placeholders.");
    let _ = writeln!(src, "func buildInClause(prefix, suffix string, startIdx int, count int) string {{");
    let _ = writeln!(src, "\tplaceholders := make([]string, count)");
    let _ = writeln!(src, "\tfor i := range count {{");
    if dollar {
        let _ = writeln!(src, "\t\tplaceholders[i] = fmt.Sprintf(\"$%d\", startIdx+i)");
    } else {
        let _ = writeln!(src, "\t\tplaceholders[i] = \"?\"");
        let _ = writeln!(src, "\t\t_ = fmt.Sprintf // suppress unused import");
    }
    let _ = writeln!(src, "\t}}");
    let _ = writeln!(src, "\treturn prefix + \"IN (\" + strings.Join(placeholders, \", \") + \")\" + suffix");
    let _ = writeln!(src, "}}");
}
