use crate::backend::GeneratedFile;
use crate::config::OutputConfig;
use crate::ir::Query;
use database_sql_adapter::DatabaseSqlAdapter;
use pgx_adapter::PgxAdapter;
use std::fmt::Write;
use std::path::PathBuf;

use super::GoTarget;

mod database_sql_adapter;
mod pgx_adapter;

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

// ── database/sql (SQLite, MySQL) ─────────────────────────────────────────────

/// Build the adapter for the selected target.
pub(super) fn build_adapter(target: &GoTarget) -> Box<dyn GoDriverAdapter> {
    match target {
        GoTarget::Postgres => Box::new(PgxAdapter),
        GoTarget::Sqlite => Box::new(DatabaseSqlAdapter::new("// Driver: modernc.org/sqlite")),
        GoTarget::Mysql => Box::new(DatabaseSqlAdapter::new("// Driver: github.com/go-sql-driver/mysql")),
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
