use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::GeneratedFile;
use crate::config::{ListParamStrategy, OutputConfig, ResolvedType};
use crate::ir::{Parameter, SqlType};

use super::core::{emit_mysql_query, emit_pg_query, emit_sqlite_query, QueryContext};
use super::{JsOutput, JsTarget};

/// Driver-specific configuration and behavior for the TypeScript/JavaScript backend.
///
/// Each implementation provides concrete behaviors — type expressions, SQL transformations,
/// query emission — rather than flags for generic code to branch on.
pub(super) trait JsDriverAdapter {
    fn output(&self) -> JsOutput;
    fn runtime_hint(&self) -> &'static str;
    /// JS/TS type name for `DATE` columns (e.g. `"Date"` or `"string"`).
    fn date_field_type(&self) -> &'static str;
    /// Write expression template for BigInt scalar params, if this driver cannot
    /// serialize JS `bigint` directly (e.g. `"String({value})"`).
    fn bigint_write_expr(&self) -> Option<&'static str>;
    /// Resolve a named type preset (e.g. `"object"` for JSON columns) to a
    /// [`ResolvedType`], incorporating any driver-specific read/write transforms.
    fn resolve_preset(&self, name: &str) -> Option<ResolvedType>;
    /// Rewrite SQL placeholders for the target driver.
    ///
    /// PostgreSQL keeps `$N` unchanged; SQLite and MySQL rewrite to anonymous `?`.
    fn normalize_sql(&self, sql: &str) -> String;
    /// Prepare SQL for a native-list constant: normalize placeholders and add any
    /// driver-specific array type casts.
    ///
    /// For pg: adds `::bigint[]`, `::numeric[]`, or `::uuid[]` casts on `= ANY($N)` when
    /// the element type would otherwise be inferred as `text[]`.
    /// For SQLite/MySQL: just normalizes placeholders.
    fn apply_native_list_sql(&self, sql: &str, lp: &Parameter) -> String;
    /// JS expression for a list parameter value.
    ///
    /// For BigInt on pg: returns `name.map(String)` — elements become `string[]` which,
    /// combined with a `::bigint[]` SQL cast, is precision-safe and type-correct.
    /// For all other cases: returns `name` unchanged.
    fn list_arg(&self, sql_type: &SqlType, name: &str) -> String;
    /// Content of the generated `sqltgen.{ts,js}` runtime helper module.
    fn helper_content(&self) -> String;
    /// Emit the complete query function into `src`.
    fn emit_query(&self, src: &mut String, ctx: &QueryContext, strategy: &ListParamStrategy) -> anyhow::Result<()>;
}

pub(super) struct PgAdapter {
    pub(super) output: JsOutput,
}

pub(super) struct BetterSqlite3Adapter {
    pub(super) output: JsOutput,
}

pub(super) struct Mysql2Adapter {
    pub(super) output: JsOutput,
}

impl JsDriverAdapter for PgAdapter {
    fn output(&self) -> JsOutput {
        self.output
    }

    fn runtime_hint(&self) -> &'static str {
        "pg (node-postgres) — npm install pg"
    }

    fn date_field_type(&self) -> &'static str {
        "Date"
    }

    fn bigint_write_expr(&self) -> Option<&'static str> {
        Some("String({value})")
    }

    fn resolve_preset(&self, name: &str) -> Option<ResolvedType> {
        match name {
            // pg driver auto-deserializes JSON/JSONB to JS objects; no parse needed on read.
            "object" => Some(ResolvedType {
                name: "unknown".to_string(),
                import: None,
                read_expr: None,
                write_expr: Some("JSON.stringify({value})".to_string()),
                extra_fields: vec![],
            }),
            _ => None,
        }
    }

    fn normalize_sql(&self, sql: &str) -> String {
        sql.to_string()
    }

    fn apply_native_list_sql(&self, sql: &str, lp: &Parameter) -> String {
        if let Some(cast) = pg_array_cast_type(&lp.sql_type) {
            let pattern = format!("ANY(${})", lp.index);
            let replacement = format!("ANY(${0}::{1}[])", lp.index, cast);
            sql.replace(&pattern, &replacement)
        } else {
            sql.to_string()
        }
    }

    fn list_arg(&self, sql_type: &SqlType, name: &str) -> String {
        if matches!(sql_type, SqlType::BigInt) {
            format!("{name}.map(String)")
        } else {
            name.to_string()
        }
    }

    fn helper_content(&self) -> String {
        build_helper_file(JsTarget::Pg, self.output)
    }

    fn emit_query(&self, src: &mut String, ctx: &QueryContext, strategy: &ListParamStrategy) -> anyhow::Result<()> {
        emit_pg_query(src, ctx, strategy)
    }
}

impl JsDriverAdapter for BetterSqlite3Adapter {
    fn output(&self) -> JsOutput {
        self.output
    }

    fn runtime_hint(&self) -> &'static str {
        "better-sqlite3 — npm install better-sqlite3"
    }

    fn date_field_type(&self) -> &'static str {
        "Date"
    }

    fn bigint_write_expr(&self) -> Option<&'static str> {
        None
    }

    fn resolve_preset(&self, name: &str) -> Option<ResolvedType> {
        match name {
            // better-sqlite3 returns JSON columns as raw text strings; parse on read.
            "object" => {
                let read_expr = match self.output {
                    JsOutput::TypeScript => "JSON.parse({raw} as string)".to_string(),
                    JsOutput::JavaScript => "JSON.parse({raw})".to_string(),
                };
                Some(ResolvedType {
                    name: "unknown".to_string(),
                    import: None,
                    read_expr: Some(read_expr),
                    write_expr: Some("JSON.stringify({value})".to_string()),
                    extra_fields: vec![],
                })
            },
            _ => None,
        }
    }

    fn normalize_sql(&self, sql: &str) -> String {
        crate::backend::sql_rewrite::rewrite_to_anon_params(sql)
    }

    fn apply_native_list_sql(&self, sql: &str, _lp: &Parameter) -> String {
        self.normalize_sql(sql)
    }

    fn list_arg(&self, _sql_type: &SqlType, name: &str) -> String {
        name.to_string()
    }

    fn helper_content(&self) -> String {
        build_helper_file(JsTarget::BetterSqlite3, self.output)
    }

    fn emit_query(&self, src: &mut String, ctx: &QueryContext, strategy: &ListParamStrategy) -> anyhow::Result<()> {
        emit_sqlite_query(src, ctx, strategy)
    }
}

impl JsDriverAdapter for Mysql2Adapter {
    fn output(&self) -> JsOutput {
        self.output
    }

    fn runtime_hint(&self) -> &'static str {
        "mysql2 — npm install mysql2"
    }

    fn date_field_type(&self) -> &'static str {
        "string"
    }

    fn bigint_write_expr(&self) -> Option<&'static str> {
        Some("String({value})")
    }

    fn resolve_preset(&self, name: &str) -> Option<ResolvedType> {
        match name {
            // mysql2 auto-parses JSON columns; no JSON.parse needed on read.
            "object" => Some(ResolvedType {
                name: "unknown".to_string(),
                import: None,
                read_expr: None,
                write_expr: Some("JSON.stringify({value})".to_string()),
                extra_fields: vec![],
            }),
            _ => None,
        }
    }

    fn normalize_sql(&self, sql: &str) -> String {
        crate::backend::sql_rewrite::rewrite_to_anon_params(sql)
    }

    fn apply_native_list_sql(&self, sql: &str, _lp: &Parameter) -> String {
        self.normalize_sql(sql)
    }

    fn list_arg(&self, _sql_type: &SqlType, name: &str) -> String {
        // mysql2 text protocol calls .toString() on each param value, so bigint[] spreads correctly.
        name.to_string()
    }

    fn helper_content(&self) -> String {
        build_helper_file(JsTarget::Mysql2, self.output)
    }

    fn emit_query(&self, src: &mut String, ctx: &QueryContext, strategy: &ListParamStrategy) -> anyhow::Result<()> {
        emit_mysql_query(src, ctx, strategy)
    }
}

/// PostgreSQL array cast type for `= ANY($N::type[])`.
///
/// Returns `None` when no cast is needed (e.g., `integer` column with `number[]` param).
/// A cast is required when the JS representation is `string` — pg would otherwise infer
/// `text[]`, causing a type mismatch against the actual column type.
fn pg_array_cast_type(sql_type: &SqlType) -> Option<&'static str> {
    match sql_type {
        SqlType::BigInt => Some("bigint"),
        SqlType::Decimal => Some("numeric"),
        SqlType::Uuid => Some("uuid"),
        _ => None,
    }
}

/// Build a boxed driver adapter for the given target and output format.
pub(super) fn build_adapter(target: JsTarget, output: JsOutput) -> Box<dyn JsDriverAdapter> {
    match target {
        JsTarget::Pg => Box::new(PgAdapter { output }),
        JsTarget::BetterSqlite3 => Box::new(BetterSqlite3Adapter { output }),
        JsTarget::Mysql2 => Box::new(Mysql2Adapter { output }),
    }
}

/// Build the generated `sqltgen.{ext}` helper file.
pub(super) fn emit_helper_file(adapter: &dyn JsDriverAdapter, config: &OutputConfig, ext: &str) -> GeneratedFile {
    GeneratedFile { path: PathBuf::from(&config.out).join(format!("sqltgen.{ext}")), content: adapter.helper_content() }
}

fn build_helper_file(target: JsTarget, output: JsOutput) -> String {
    let mut src = String::new();
    _ = writeln!(src, "// Generated by sqltgen. Do not edit.");
    _ = writeln!(src);
    match output {
        JsOutput::TypeScript => {
            match target {
                JsTarget::Pg => {
                    _ = writeln!(src, "import type {{ ClientBase }} from 'pg';");
                    _ = writeln!(src, "export type Db = ClientBase;");
                },
                JsTarget::BetterSqlite3 => {
                    _ = writeln!(src, "import type {{ Database }} from 'better-sqlite3';");
                    _ = writeln!(src, "export type Db = Database;");
                },
                JsTarget::Mysql2 => {
                    _ = writeln!(src, "import type {{ Connection }} from 'mysql2/promise';");
                    _ = writeln!(src, "export type Db = Connection;");
                },
            }
            _ = writeln!(src, "export type ConnectFn = () => Db | Promise<Db>;");
            _ = writeln!(src, "type ClosableDb = Db & {{ release?: () => void; close?: () => void }};");
            _ = writeln!(src);
            _ = writeln!(src, "export async function releaseDb(db: Db): Promise<void> {{");
            _ = writeln!(src, "  const closable = db as ClosableDb;");
            _ = writeln!(src, "  if (typeof closable.release === 'function') {{");
            _ = writeln!(src, "    closable.release();");
            _ = writeln!(src, "    return;");
            _ = writeln!(src, "  }}");
            _ = writeln!(src, "  if (typeof closable.close === 'function') {{");
            _ = writeln!(src, "    closable.close();");
            _ = writeln!(src, "  }}");
            _ = writeln!(src, "}}");
        },
        JsOutput::JavaScript => {
            match target {
                JsTarget::Pg => {
                    _ = writeln!(src, "/** @typedef {{import('pg').ClientBase}} Db */");
                },
                JsTarget::BetterSqlite3 => {
                    _ = writeln!(src, "/** @typedef {{import('better-sqlite3').Database}} Db */");
                },
                JsTarget::Mysql2 => {
                    _ = writeln!(src, "/** @typedef {{import('mysql2/promise').Connection}} Db */");
                },
            }
            _ = writeln!(src, "/** @typedef {{() => Db | Promise<Db>}} ConnectFn */");
            _ = writeln!(src);
            _ = writeln!(src, "/** @param {{Db}} db */");
            _ = writeln!(src, "export async function releaseDb(db) {{");
            _ = writeln!(src, "  if (typeof db.release === 'function') {{");
            _ = writeln!(src, "    db.release();");
            _ = writeln!(src, "    return;");
            _ = writeln!(src, "  }}");
            _ = writeln!(src, "  if (typeof db.close === 'function') {{");
            _ = writeln!(src, "    db.close();");
            _ = writeln!(src, "  }}");
            _ = writeln!(src, "}}");
        },
    }
    src
}
