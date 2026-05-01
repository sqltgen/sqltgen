use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::naming::to_camel_case;
use crate::backend::sql_rewrite::positional_bind_names;
use crate::backend::GeneratedFile;
use crate::config::{OutputConfig, ResolvedType};
use crate::ir::{Parameter, Query, SqlType};

use super::core::{ts_row_type, GenerationContext};
use super::typemap::JsTypeMap;
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
    /// Build the args expression for a query in the engine's expected syntax
    /// (JS array `[a, b]` for pg/mysql, bare comma list `a, b` for sqlite;
    /// unique-by-index for pg, positional-with-repetition for sqlite/mysql).
    ///
    /// `list_slot` substitutes the given expression in the list-param's slot —
    /// pass the JSON-stringified var name for native lists, or `...spread` for
    /// dynamic lists. `None` for queries without a list parameter.
    fn build_args(&self, query: &Query, type_map: &JsTypeMap, list_slot: Option<(&Parameter, &str)>) -> String;

    /// Emit any setup lines that must precede a native-list query body and
    /// return the JS expression that substitutes for the list-param value in
    /// the args. For pg this is just the list itself (or its BigInt-coerced
    /// form); for sqlite/mysql it is a JSON-stringified local variable.
    fn emit_native_list_prelude(&self, src: &mut String, lp: &Parameter, type_map: &JsTypeMap) -> anyhow::Result<String>;

    /// JS expression that builds the placeholder string for a dynamic IN
    /// expansion (`$N`-numbered for pg, anonymous `?` for sqlite/mysql).
    /// `scalars_before` counts scalar params with lower index than the list
    /// param — pg uses it to start numbering at the right offset; engines that
    /// use anonymous placeholders ignore it.
    fn dynamic_placeholders_expr(&self, lp_name: &str, scalars_before: usize) -> String;

    // ── result-extraction primitives ──────────────────────────────────────────
    /// Emit the statement that fires a query and discards the result (Exec cmd).
    fn emit_exec(&self, src: &mut String, sql_expr: &str, args: &str) -> anyhow::Result<()>;
    /// Emit lines that bind the affected-rows count to `var` (ExecRows cmd).
    fn emit_bind_count(&self, src: &mut String, sql_expr: &str, args: &str, var: &str) -> anyhow::Result<()>;
    /// Emit lines that bind the first result row to `var` (One cmd). The bound
    /// expression is typed `Row | undefined` (or its loose-typed pg equivalent).
    fn emit_bind_one(&self, src: &mut String, ctx: &GenerationContext, query: &Query, sql_expr: &str, args: &str, var: &str) -> anyhow::Result<()>;
    /// Emit lines that bind the rows array to `var` (Many cmd). The bound
    /// expression is typed `Row[]`.
    fn emit_bind_many(&self, src: &mut String, ctx: &GenerationContext, query: &Query, sql_expr: &str, args: &str, var: &str) -> anyhow::Result<()>;
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

    fn build_args(&self, query: &Query, type_map: &JsTypeMap, list_slot: Option<(&Parameter, &str)>) -> String {
        // pg uses $N reference-by-number, so each unique param index appears
        // exactly once in the bound array regardless of SQL repetition.
        let mut params: Vec<&Parameter> = query.params.iter().collect();
        params.sort_by_key(|p| p.index);
        let exprs: Vec<String> = params
            .iter()
            .map(|p| match list_slot {
                Some((lp, expr)) if p.index == lp.index => expr.to_string(),
                _ => type_map.write_expr(p),
            })
            .collect();
        format!("[{}]", exprs.join(", "))
    }

    fn emit_native_list_prelude(&self, _src: &mut String, lp: &Parameter, _type_map: &JsTypeMap) -> anyhow::Result<String> {
        // pg accepts JS arrays directly; no JSON setup needed. The substituted
        // expression is just the (possibly BigInt-coerced) list value.
        Ok(self.list_arg(&lp.sql_type, &to_camel_case(&lp.name)))
    }

    fn dynamic_placeholders_expr(&self, lp_name: &str, scalars_before: usize) -> String {
        let start = scalars_before + 1;
        format!("{lp_name}.map((_, i) => '$' + ({start} + i)).join(', ')")
    }

    fn emit_exec(&self, src: &mut String, sql_expr: &str, args: &str) -> anyhow::Result<()> {
        writeln!(src, "  await db.query({sql_expr}, {args});")?;
        Ok(())
    }

    fn emit_bind_count(&self, src: &mut String, sql_expr: &str, args: &str, var: &str) -> anyhow::Result<()> {
        writeln!(src, "  const result = await db.query({sql_expr}, {args});")?;
        writeln!(src, "  const {var} = result.rowCount ?? 0;")?;
        Ok(())
    }

    fn emit_bind_one(&self, src: &mut String, ctx: &GenerationContext, query: &Query, sql_expr: &str, args: &str, var: &str) -> anyhow::Result<()> {
        let row = ts_row_type(query, ctx.schema);
        let generic = ts_generic(self.output, &row);
        writeln!(src, "  const result = await db.query{generic}({sql_expr}, {args});")?;
        writeln!(src, "  const {var} = result.rows[0];")?;
        Ok(())
    }

    fn emit_bind_many(&self, src: &mut String, ctx: &GenerationContext, query: &Query, sql_expr: &str, args: &str, var: &str) -> anyhow::Result<()> {
        let row = ts_row_type(query, ctx.schema);
        let generic = ts_generic(self.output, &row);
        writeln!(src, "  const result = await db.query{generic}({sql_expr}, {args});")?;
        writeln!(src, "  const {var} = result.rows;")?;
        Ok(())
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

    fn build_args(&self, query: &Query, type_map: &JsTypeMap, list_slot: Option<(&Parameter, &str)>) -> String {
        // better-sqlite3 takes spread args; the arg list follows SQL occurrence order with repetition.
        build_positional_args(query, type_map, list_slot)
    }

    fn emit_native_list_prelude(&self, src: &mut String, lp: &Parameter, type_map: &JsTypeMap) -> anyhow::Result<String> {
        let lp_name = to_camel_case(&lp.name);
        let stringify = type_map.list_json_stringify(&lp.sql_type, &lp_name);
        writeln!(src, "  const {lp_name}Json = {stringify};")?;
        Ok(format!("{lp_name}Json"))
    }

    fn dynamic_placeholders_expr(&self, lp_name: &str, _scalars_before: usize) -> String {
        format!(r#"{lp_name}.map(() => "?").join(", ")"#)
    }

    fn emit_exec(&self, src: &mut String, sql_expr: &str, args: &str) -> anyhow::Result<()> {
        writeln!(src, "  db.prepare({sql_expr}).run({args});")?;
        Ok(())
    }

    fn emit_bind_count(&self, src: &mut String, sql_expr: &str, args: &str, var: &str) -> anyhow::Result<()> {
        writeln!(src, "  const {var} = db.prepare({sql_expr}).run({args}).changes;")?;
        Ok(())
    }

    fn emit_bind_one(&self, src: &mut String, ctx: &GenerationContext, query: &Query, sql_expr: &str, args: &str, var: &str) -> anyhow::Result<()> {
        let row = ts_row_type(query, ctx.schema);
        let cast = ts_cast(&format!("{row} | undefined"), self.output);
        writeln!(src, "  const {var} = db.prepare({sql_expr}).get({args}){cast};")?;
        Ok(())
    }

    fn emit_bind_many(&self, src: &mut String, ctx: &GenerationContext, query: &Query, sql_expr: &str, args: &str, var: &str) -> anyhow::Result<()> {
        let row = ts_row_type(query, ctx.schema);
        let cast = ts_cast(&format!("{row}[]"), self.output);
        writeln!(src, "  const {var} = db.prepare({sql_expr}).all({args}){cast};")?;
        Ok(())
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
        // mysql2 (via sql-escaper) serializes bigint as `value + ''` — exact decimal,
        // no float rounding — then inlines it as an unquoted numeric literal in SQL.
        // Wrapping in String() first would produce a quoted string, which MySQL rejects
        // for LIMIT/OFFSET even though it accepts it for integer column comparisons.
        None
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

    fn build_args(&self, query: &Query, type_map: &JsTypeMap, list_slot: Option<(&Parameter, &str)>) -> String {
        // mysql2 takes a JS array; placeholders are anonymous `?` so args follow SQL occurrence order.
        format!("[{}]", build_positional_args(query, type_map, list_slot))
    }

    fn emit_native_list_prelude(&self, src: &mut String, lp: &Parameter, type_map: &JsTypeMap) -> anyhow::Result<String> {
        let lp_name = to_camel_case(&lp.name);
        let stringify = type_map.list_json_stringify(&lp.sql_type, &lp_name);
        writeln!(src, "  const {lp_name}Json = {stringify};")?;
        Ok(format!("{lp_name}Json"))
    }

    fn dynamic_placeholders_expr(&self, lp_name: &str, _scalars_before: usize) -> String {
        format!(r#"{lp_name}.map(() => "?").join(", ")"#)
    }

    fn emit_exec(&self, src: &mut String, sql_expr: &str, args: &str) -> anyhow::Result<()> {
        writeln!(src, "  await db.query({sql_expr}, {args});")?;
        Ok(())
    }

    fn emit_bind_count(&self, src: &mut String, sql_expr: &str, args: &str, var: &str) -> anyhow::Result<()> {
        let rsh = mysql_type_param(self.output, "ResultSetHeader");
        writeln!(src, "  const [result] = await db.query{rsh}({sql_expr}, {args});")?;
        writeln!(src, "  const {var} = result.affectedRows;")?;
        Ok(())
    }

    fn emit_bind_one(&self, src: &mut String, ctx: &GenerationContext, query: &Query, sql_expr: &str, args: &str, var: &str) -> anyhow::Result<()> {
        let row = ts_row_type(query, ctx.schema);
        let generic = mysql_type_param(self.output, "RowDataPacket[]");
        let cast = ts_cast(&format!("{row} | undefined"), self.output);
        writeln!(src, "  const [rdp] = await db.query{generic}({sql_expr}, {args});")?;
        writeln!(src, "  const {var} = rdp[0]{cast};")?;
        Ok(())
    }

    fn emit_bind_many(&self, src: &mut String, ctx: &GenerationContext, query: &Query, sql_expr: &str, args: &str, var: &str) -> anyhow::Result<()> {
        let row = ts_row_type(query, ctx.schema);
        let generic = mysql_type_param(self.output, "RowDataPacket[]");
        let cast = ts_cast(&format!("{row}[]"), self.output);
        writeln!(src, "  const [rdp] = await db.query{generic}({sql_expr}, {args});")?;
        writeln!(src, "  const {var} = rdp{cast};")?;
        Ok(())
    }
}

/// Build a comma-separated args list following SQL occurrence order
/// (with repetition), substituting `list_slot.1` wherever the list param's
/// name would appear. Used by sqlite (bare) and mysql (wrapped).
fn build_positional_args(query: &Query, type_map: &JsTypeMap, list_slot: Option<(&Parameter, &str)>) -> String {
    positional_bind_names(query)
        .iter()
        .map(|&n| match list_slot {
            Some((lp, expr)) if n == lp.name => expr.to_string(),
            _ => query.params.iter().find(|p| p.name == n).map(|p| type_map.write_expr(p)).unwrap_or_else(|| to_camel_case(n)),
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Return ` as <ts_type>` for TypeScript or empty string for JavaScript.
fn ts_cast(ts_type: &str, output: JsOutput) -> String {
    match output {
        JsOutput::TypeScript => format!(" as {ts_type}"),
        JsOutput::JavaScript => String::new(),
    }
}

/// Return `<ts_type>` (TypeScript generic argument) or empty string (JavaScript).
fn mysql_type_param(output: JsOutput, ts_type: &str) -> String {
    match output {
        JsOutput::TypeScript => format!("<{ts_type}>"),
        JsOutput::JavaScript => String::new(),
    }
}

/// Return `<row>` for TypeScript or empty string for JavaScript.
fn ts_generic(output: JsOutput, row: &str) -> String {
    match output {
        JsOutput::TypeScript => format!("<{row}>"),
        JsOutput::JavaScript => String::new(),
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
