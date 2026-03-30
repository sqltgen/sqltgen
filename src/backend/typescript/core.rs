use std::collections::BTreeSet;
use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::common::{
    flat_row_type_name, group_queries, has_inline_rows, infer_row_type_name, infer_table, nested_type_name, querier_class_name, queries_file_stem,
    row_type_name as inline_row_type_name, sql_const_name,
};
use crate::backend::naming::{to_camel_case, to_pascal_case};
use crate::backend::sql_rewrite::{positional_bind_names, rewrite_to_anon_params, split_at_in_clause};
use crate::backend::GeneratedFile;
use crate::config::{resolve_type_override, Language, ListParamStrategy, OutputConfig, ResolvedType, TypeVariant};
use crate::ir::{NestedGroup, Parameter, Query, QueryCmd, Schema, SqlType, Table};

use super::adapter::TsCoreContract;
use super::{JsOutput, TypeScriptCodegen};

#[cfg(test)]
use super::JsTarget;

/// Resolve a known TypeScript/JavaScript preset name to a [`ResolvedType`].
///
/// `json_needs_parse` comes from the adapter contract: PostgreSQL's `pg` driver
/// auto-deserializes jsonb columns, so no `JSON.parse` is needed on read.
fn try_preset_ts(name: &str, output: &JsOutput, json_needs_parse: bool) -> Option<ResolvedType> {
    match name {
        "object" => {
            let read_expr = if json_needs_parse {
                match output {
                    JsOutput::TypeScript => Some("JSON.parse({raw} as string)".to_string()),
                    JsOutput::JavaScript => Some("JSON.parse({raw})".to_string()),
                }
            } else {
                None
            };
            Some(ResolvedType {
                name: "unknown".to_string(),
                import: None,
                read_expr,
                write_expr: Some("JSON.stringify({value})".to_string()),
                extra_fields: vec![],
            })
        },
        _ => None,
    }
}

fn get_type_override_ts(sql_type: &SqlType, variant: TypeVariant, config: &OutputConfig, contract: &TsCoreContract) -> Option<ResolvedType> {
    let language = match contract.output {
        JsOutput::TypeScript => Language::TypeScript,
        JsOutput::JavaScript => Language::JavaScript,
    };
    resolve_type_override(sql_type, variant, config, language, |s| try_preset_ts(s, &contract.output, contract.json_needs_parse))
}

/// Map a SQL type to its JavaScript/TypeScript type string, applying any configured override.
pub(super) fn js_type_resolved(sql_type: &SqlType, nullable: bool, contract: &TsCoreContract, config: &OutputConfig) -> String {
    if let Some(resolved) = get_type_override_ts(sql_type, TypeVariant::Field, config, contract) {
        return if nullable { format!("{} | null", resolved.name) } else { resolved.name };
    }
    js_type_with_contract(sql_type, nullable, contract)
}

/// Return the JS/TS expression used to bind a parameter, applying any configured write_expr.
///
/// Normally this is just the camelCase param name. When a write_expr is configured,
/// the param name is wrapped (e.g. `JSON.stringify(payload)`).
fn ts_write_expr(p: &Parameter, config: &OutputConfig, contract: &TsCoreContract) -> String {
    let name = to_camel_case(&p.name);
    if let Some(resolved) = get_type_override_ts(&p.sql_type, TypeVariant::Param, config, contract) {
        if let Some(expr) = &resolved.write_expr {
            return expr.replace("{value}", &name);
        }
    }
    name
}

/// Build a row-transformation expression if any result column has a read_expr override.
///
/// Returns `None` when no columns need transformation (the raw driver row can be returned
/// directly). Returns `Some(expr)` where `expr` spreads `raw_var` and overrides the
/// transformed columns: `{ ...raw, col: JSON.parse(raw.col as string), ... }`.
fn row_transform_expr(query: &Query, config: &OutputConfig, contract: &TsCoreContract, raw_var: &str) -> Option<String> {
    let transforms: Vec<String> = query
        .result_columns
        .iter()
        .filter_map(|col| {
            let resolved = get_type_override_ts(&col.sql_type, TypeVariant::Field, config, contract)?;
            let expr = resolved.read_expr?;
            let raw_access = format!("{raw_var}.{}", col.name);
            Some(format!("{}: {}", col.name, expr.replace("{raw}", &raw_access)))
        })
        .collect();
    if transforms.is_empty() {
        return None;
    }
    Some(format!("{{ ...{raw_var}, {} }}", transforms.join(", ")))
}

/// Per-query context computed once in the target-specific emitter and forwarded to body helpers.
pub(super) struct QueryContext<'a> {
    query: &'a Query,
    schema: &'a Schema,
    contract: &'a TsCoreContract,
    config: &'a OutputConfig,
    fn_name: String,
    ret: String,
    conn_type: &'static str,
}

impl<'a> QueryContext<'a> {
    fn new(query: &'a Query, schema: &'a Schema, contract: &'a TsCoreContract, config: &'a OutputConfig, conn_type: &'static str) -> Self {
        Self { fn_name: to_camel_case(&query.name), ret: return_type(query, schema), query, schema, contract, config, conn_type }
    }

    fn params(&self) -> Vec<&'a Parameter> {
        self.query.params.iter().collect()
    }
}

pub(super) fn generate_core_files(
    codegen: &TypeScriptCodegen,
    schema: &Schema,
    queries: &[Query],
    contract: &TsCoreContract,
    config: &OutputConfig,
) -> anyhow::Result<Vec<GeneratedFile>> {
    let ext = codegen.ext();
    let mut files = Vec::new();
    for table in &schema.tables {
        let content = codegen.emit_model_file(table, config)?;
        files.push(GeneratedFile { path: PathBuf::from(&config.out).join(format!("{}.{ext}", table.name)), content });
    }
    let groups = group_queries(queries);
    let mut group_stems: Vec<String> = Vec::new();
    for (group, group_queries) in &groups {
        let stem = queries_file_stem(group).to_string();
        group_stems.push(stem.clone());
        let content = build_queries_file_with_contract(group, group_queries, schema, contract, config)?;
        files.push(GeneratedFile { path: PathBuf::from(&config.out).join(format!("{stem}.{ext}")), content });
    }
    let index_content = codegen.emit_index_file(schema, &group_stems)?;
    files.push(GeneratedFile { path: PathBuf::from(&config.out).join(format!("index.{ext}")), content: index_content });
    Ok(files)
}

impl TypeScriptCodegen {
    /// Returns the file extension: `"ts"` for TypeScript, `"js"` for JavaScript.
    pub(crate) fn ext(&self) -> &'static str {
        match self.output {
            JsOutput::TypeScript => "ts",
            JsOutput::JavaScript => "js",
        }
    }

    pub(crate) fn emit_model_file(&self, table: &Table, config: &OutputConfig) -> anyhow::Result<String> {
        let contract = super::adapter::resolve_ts_contract(self.target, self.output);
        let mut src = String::new();
        writeln!(src, "// Generated by sqltgen. Do not edit.")?;
        writeln!(src)?;
        let name = to_pascal_case(&table.name);
        let fields: Vec<(&str, &SqlType, bool)> = table.columns.iter().map(|c| (c.name.as_str(), &c.sql_type, c.nullable)).collect();
        match self.output {
            JsOutput::TypeScript => emit_ts_interface(&mut src, &name, &fields, &contract, config)?,
            JsOutput::JavaScript => emit_js_typedef(&mut src, &name, &fields, &contract, config)?,
        }
        Ok(src)
    }

    pub(crate) fn emit_index_file(&self, schema: &Schema, group_stems: &[String]) -> anyhow::Result<String> {
        // TypeScript imports must not use .ts extensions; JS (ESM) requires .js.
        let import_ext = match self.output {
            JsOutput::TypeScript => "",
            JsOutput::JavaScript => ".js",
        };
        let mut src = String::new();
        writeln!(src, "// Generated by sqltgen. Do not edit.")?;
        writeln!(src)?;
        for table in &schema.tables {
            writeln!(src, "export * from './{}{import_ext}';", table.name)?;
        }
        for stem in group_stems {
            writeln!(src, "export * from './{stem}{import_ext}';")?;
        }
        Ok(src)
    }
}

// ─── Type emission ────────────────────────────────────────────────────────────

/// Emit a TypeScript `interface` block for the given fields.
fn emit_ts_interface(src: &mut String, name: &str, fields: &[(&str, &SqlType, bool)], contract: &TsCoreContract, config: &OutputConfig) -> anyhow::Result<()> {
    writeln!(src, "export interface {name} {{")?;
    for (fname, ftype, nullable) in fields {
        writeln!(src, "  {fname}: {};", js_type_resolved(ftype, *nullable, contract, config))?;
    }
    writeln!(src, "}}")?;
    Ok(())
}

/// Emit a JSDoc `@typedef` block for the given fields.
fn emit_js_typedef(src: &mut String, name: &str, fields: &[(&str, &SqlType, bool)], contract: &TsCoreContract, config: &OutputConfig) -> anyhow::Result<()> {
    writeln!(src, "/**")?;
    writeln!(src, " * @typedef {{Object}} {name}")?;
    for (fname, ftype, nullable) in fields {
        let ty = js_type_resolved(ftype, *nullable, contract, config);
        writeln!(src, " * @property {{{ty}}} {fname}")?;
    }
    writeln!(src, " */")?;
    Ok(())
}

/// Emit the inline row type for a query whose result doesn't match any schema table.
fn emit_inline_row_type_with_contract(src: &mut String, query: &Query, contract: &TsCoreContract, config: &OutputConfig) -> anyhow::Result<()> {
    let name = inline_row_type_name(&query.name);
    let fields: Vec<(&str, &SqlType, bool)> = query.result_columns.iter().map(|c| (c.name.as_str(), &c.sql_type, c.nullable)).collect();
    match contract.output {
        JsOutput::TypeScript => emit_ts_interface(src, &name, &fields, contract, config),
        JsOutput::JavaScript => emit_js_typedef(src, &name, &fields, contract, config),
    }
}

// ─── Nested result types ──────────────────────────────────────────────────────

/// Emit all type declarations for a nested-result query:
///
/// 1. A private flat row interface (raw SQL result shape before aggregation).
/// 2. A public child interface per nested group.
/// 3. A public parent interface with nested array fields.
fn emit_nested_types(src: &mut String, query: &Query, contract: &TsCoreContract, config: &OutputConfig) -> anyhow::Result<()> {
    let parent_cols = query.parent_columns();

    match contract.output {
        JsOutput::TypeScript => {
            // 1. Private flat row type
            let flat_name = flat_row_type_name(&query.name);
            let flat_fields: Vec<(&str, &SqlType, bool)> = query.result_columns.iter().map(|c| (c.name.as_str(), &c.sql_type, c.nullable)).collect();
            writeln!(src, "interface {flat_name} {{")?;
            for (fname, ftype, nullable) in &flat_fields {
                writeln!(src, "  {fname}: {};", js_type_resolved(ftype, *nullable, contract, config))?;
            }
            writeln!(src, "}}")?;
            writeln!(src)?;

            // 2. Child types
            for group in &query.nested_groups {
                let child_name = nested_type_name(&query.name, &group.field_name);
                writeln!(src, "export interface {child_name} {{")?;
                for nc in &group.columns {
                    writeln!(src, "  {}: {};", nc.target_name, js_type_resolved(&nc.sql_type, nc.nullable, contract, config))?;
                }
                writeln!(src, "}}")?;
                writeln!(src)?;
            }

            // 3. Parent type
            let parent_name = inline_row_type_name(&query.name);
            writeln!(src, "export interface {parent_name} {{")?;
            for pc in &parent_cols {
                writeln!(src, "  {}: {};", pc.name, js_type_resolved(&pc.sql_type, pc.nullable, contract, config))?;
            }
            for group in &query.nested_groups {
                let child_name = nested_type_name(&query.name, &group.field_name);
                writeln!(src, "  {}: {child_name}[];", group.field_name)?;
            }
            writeln!(src, "}}")?;
        },
        JsOutput::JavaScript => {
            // 2. Child @typedefs
            for group in &query.nested_groups {
                let child_name = nested_type_name(&query.name, &group.field_name);
                writeln!(src, "/**")?;
                writeln!(src, " * @typedef {{Object}} {child_name}")?;
                for nc in &group.columns {
                    let ty = js_type_resolved(&nc.sql_type, nc.nullable, contract, config);
                    writeln!(src, " * @property {{{ty}}} {}", nc.target_name)?;
                }
                writeln!(src, " */")?;
                writeln!(src)?;
            }

            // 3. Parent @typedef
            let parent_name = inline_row_type_name(&query.name);
            writeln!(src, "/**")?;
            writeln!(src, " * @typedef {{Object}} {parent_name}")?;
            for pc in &parent_cols {
                let ty = js_type_resolved(&pc.sql_type, pc.nullable, contract, config);
                writeln!(src, " * @property {{{ty}}} {}", pc.name)?;
            }
            for group in &query.nested_groups {
                let child_name = nested_type_name(&query.name, &group.field_name);
                writeln!(src, " * @property {{{child_name}[]}} {}", group.field_name)?;
            }
            writeln!(src, " */")?;
        },
    }
    Ok(())
}

/// Emit the row-aggregation body for a nested `:many` query (PostgreSQL target).
///
/// Generates a `Map`-based grouping loop that collapses flat rows into parent
/// objects with nested arrays, then returns `Array.from(map.values())`.
fn emit_pg_nested_many_body(src: &mut String, ctx: &QueryContext, sql_expr: &str, args: &str) -> anyhow::Result<()> {
    let flat_name = flat_row_type_name(&ctx.query.name);
    let parent_name = inline_row_type_name(&ctx.query.name);
    let parent_cols = ctx.query.parent_columns();
    let call = pg_query_call(sql_expr, &flat_name, args, &ctx.contract.output);
    let generic = map_generic(&parent_name, &ctx.contract.output);
    writeln!(src, "  const result = await {call};")?;
    writeln!(src, "  const grouped = new Map{generic}();")?;
    emit_seen_sets(src, ctx.query, &ctx.contract.output)?;
    writeln!(src, "  for (const row of result.rows) {{")?;
    emit_aggregation_loop_body(src, ctx.query, &parent_cols)?;
    writeln!(src, "  }}")?;
    writeln!(src, "  return Array.from(grouped.values());")?;
    Ok(())
}

fn ensure_no_nested_list_combo(query: &Query) -> anyhow::Result<()> {
    if query.has_nested_groups() && query.params.iter().any(|p| p.is_list) {
        anyhow::bail!(
            "query '{}' combines nested results with list params; this combination is not supported for TypeScript/JavaScript yet",
            query.name
        );
    }
    Ok(())
}

/// Emit the row-aggregation body for a nested `:one` query (PostgreSQL target).
///
/// All flat rows belong to a single parent. Returns the parent with nested
/// arrays, or `null` if no rows were returned.
fn emit_pg_nested_one_body(src: &mut String, ctx: &QueryContext, sql_expr: &str, args: &str) -> anyhow::Result<()> {
    let flat_name = flat_row_type_name(&ctx.query.name);
    let parent_name = inline_row_type_name(&ctx.query.name);
    let parent_cols = ctx.query.parent_columns();
    let call = pg_query_call(sql_expr, &flat_name, args, &ctx.contract.output);
    writeln!(src, "  const result = await {call};")?;
    writeln!(src, "  if (result.rows.length === 0) return null;")?;
    let parent_init = build_parent_initializer(&parent_cols, &ctx.query.nested_groups, "result.rows[0]");
    let ann = ts_type_annotation(&parent_name, &ctx.contract.output);
    writeln!(src, "  const parent{ann} = {{ {parent_init} }};")?;
    emit_seen_sets(src, ctx.query, &ctx.contract.output)?;
    writeln!(src, "  for (const row of result.rows) {{")?;
    emit_one_nested_push_body(src, &ctx.query.nested_groups)?;
    writeln!(src, "  }}")?;
    writeln!(src, "  return parent;")?;
    Ok(())
}

/// Emit the row-aggregation body for a nested `:many` query (SQLite target).
fn emit_sqlite_nested_many_body(src: &mut String, ctx: &QueryContext, sql_expr: &str, args: &str) -> anyhow::Result<()> {
    let parent_name = inline_row_type_name(&ctx.query.name);
    let parent_cols = ctx.query.parent_columns();

    match ctx.contract.output {
        JsOutput::TypeScript => {
            let flat_name = flat_row_type_name(&ctx.query.name);
            writeln!(src, "  const rows = db.prepare({sql_expr}).all({args}) as {flat_name}[];")?;
        },
        JsOutput::JavaScript => {
            writeln!(src, "  const rows = db.prepare({sql_expr}).all({args});")?;
        },
    }
    let generic = map_generic(&parent_name, &ctx.contract.output);
    writeln!(src, "  const grouped = new Map{generic}();")?;
    emit_seen_sets(src, ctx.query, &ctx.contract.output)?;
    writeln!(src, "  for (const row of rows) {{")?;
    emit_aggregation_loop_body(src, ctx.query, &parent_cols)?;
    writeln!(src, "  }}")?;
    writeln!(src, "  return Array.from(grouped.values());")?;
    Ok(())
}

/// Emit the row-aggregation body for a nested `:one` query (SQLite target).
fn emit_sqlite_nested_one_body(src: &mut String, ctx: &QueryContext, sql_expr: &str, args: &str) -> anyhow::Result<()> {
    let parent_name = inline_row_type_name(&ctx.query.name);
    let parent_cols = ctx.query.parent_columns();

    match ctx.contract.output {
        JsOutput::TypeScript => {
            let flat_name = flat_row_type_name(&ctx.query.name);
            writeln!(src, "  const rows = db.prepare({sql_expr}).all({args}) as {flat_name}[];")?;
        },
        JsOutput::JavaScript => {
            writeln!(src, "  const rows = db.prepare({sql_expr}).all({args});")?;
        },
    }
    writeln!(src, "  if (rows.length === 0) return null;")?;
    let parent_init = build_parent_initializer(&parent_cols, &ctx.query.nested_groups, "rows[0]");
    let ann = ts_type_annotation(&parent_name, &ctx.contract.output);
    writeln!(src, "  const parent{ann} = {{ {parent_init} }};")?;
    emit_seen_sets(src, ctx.query, &ctx.contract.output)?;
    writeln!(src, "  for (const row of rows) {{")?;
    emit_one_nested_push_body(src, &ctx.query.nested_groups)?;
    writeln!(src, "  }}")?;
    writeln!(src, "  return parent;")?;
    Ok(())
}

/// Emit the row-aggregation body for a nested `:many` query (MySQL target).
fn emit_mysql_nested_many_body(src: &mut String, ctx: &QueryContext, sql_expr: &str, args: &str) -> anyhow::Result<()> {
    let parent_name = inline_row_type_name(&ctx.query.name);
    let parent_cols = ctx.query.parent_columns();
    let rdp = mysql_type_param(&ctx.contract.output, "RowDataPacket[]");

    match ctx.contract.output {
        JsOutput::TypeScript => {
            let flat_name = flat_row_type_name(&ctx.query.name);
            writeln!(src, "  const [rows] = await db.query{rdp}({sql_expr}, {args});")?;
            writeln!(src, "  const typed = rows as unknown as {flat_name}[];")?;
        },
        JsOutput::JavaScript => {
            writeln!(src, "  const [rows] = await db.query({sql_expr}, {args});")?;
            writeln!(src, "  const typed = rows;")?;
        },
    }
    let generic = map_generic(&parent_name, &ctx.contract.output);
    writeln!(src, "  const grouped = new Map{generic}();")?;
    emit_seen_sets(src, ctx.query, &ctx.contract.output)?;
    writeln!(src, "  for (const row of typed) {{")?;
    emit_aggregation_loop_body(src, ctx.query, &parent_cols)?;
    writeln!(src, "  }}")?;
    writeln!(src, "  return Array.from(grouped.values());")?;
    Ok(())
}

/// Emit the row-aggregation body for a nested `:one` query (MySQL target).
fn emit_mysql_nested_one_body(src: &mut String, ctx: &QueryContext, sql_expr: &str, args: &str) -> anyhow::Result<()> {
    let parent_name = inline_row_type_name(&ctx.query.name);
    let parent_cols = ctx.query.parent_columns();
    let rdp = mysql_type_param(&ctx.contract.output, "RowDataPacket[]");

    match ctx.contract.output {
        JsOutput::TypeScript => {
            let flat_name = flat_row_type_name(&ctx.query.name);
            writeln!(src, "  const [rows] = await db.query{rdp}({sql_expr}, {args});")?;
            writeln!(src, "  const typed = rows as unknown as {flat_name}[];")?;
        },
        JsOutput::JavaScript => {
            writeln!(src, "  const [rows] = await db.query({sql_expr}, {args});")?;
            writeln!(src, "  const typed = rows;")?;
        },
    }
    writeln!(src, "  if (typed.length === 0) return null;")?;
    let parent_init = build_parent_initializer(&parent_cols, &ctx.query.nested_groups, "typed[0]");
    let ann = ts_type_annotation(&parent_name, &ctx.contract.output);
    writeln!(src, "  const parent{ann} = {{ {parent_init} }};")?;
    emit_seen_sets(src, ctx.query, &ctx.contract.output)?;
    writeln!(src, "  for (const row of typed) {{")?;
    emit_one_nested_push_body(src, &ctx.query.nested_groups)?;
    writeln!(src, "  }}")?;
    writeln!(src, "  return parent;")?;
    Ok(())
}

/// Emit the inner aggregation loop body shared by all targets for `:many` nested queries.
///
/// Emits parent-key computation, parent lookup/creation in `grouped`, and
/// child push logic with dedup checks using `seen_<group>` sets that must
/// already be declared by the caller.
fn emit_aggregation_loop_body(src: &mut String, query: &Query, parent_cols: &[&crate::ir::ResultColumn]) -> anyhow::Result<()> {
    let key_fields: Vec<String> = parent_cols.iter().map(|c| format!("row.{}", c.name)).collect();
    let parent_init = build_parent_initializer(parent_cols, &query.nested_groups, "row");
    writeln!(src, "    const key = JSON.stringify([{}]);", key_fields.join(", "))?;
    writeln!(src, "    let parent = grouped.get(key);")?;
    writeln!(src, "    if (!parent) {{")?;
    writeln!(src, "      parent = {{ {parent_init} }};")?;
    writeln!(src, "      grouped.set(key, parent);")?;
    writeln!(src, "    }}")?;
    for group in &query.nested_groups {
        if group.columns.is_empty() {
            continue;
        }
        let first_src = &group.columns[0].source_name;
        let child_init = build_child_initializer(group);
        let child_key_fields: Vec<String> = group.columns.iter().map(|nc| format!("row.{}", nc.source_name)).collect();
        writeln!(src, "    if (row.{first_src} != null) {{")?;
        writeln!(src, "      const {}_key = JSON.stringify([{}]);", group.field_name, child_key_fields.join(", "))?;
        writeln!(src, "      if (!seen_{}.has({}_key)) {{", group.field_name, group.field_name)?;
        writeln!(src, "        seen_{}.add({}_key);", group.field_name, group.field_name)?;
        writeln!(src, "        parent.{}.push({{ {child_init} }});", group.field_name)?;
        writeln!(src, "      }}")?;
        writeln!(src, "    }}")?;
    }
    Ok(())
}

/// Emit the nested-push body for `:one` queries, with deduplication via seen-sets.
fn emit_one_nested_push_body(src: &mut String, groups: &[NestedGroup]) -> anyhow::Result<()> {
    for group in groups {
        if group.columns.is_empty() {
            continue;
        }
        let first_src = &group.columns[0].source_name;
        let child_init = build_child_initializer(group);
        let child_key_fields: Vec<String> = group.columns.iter().map(|nc| format!("row.{}", nc.source_name)).collect();
        writeln!(src, "    if (row.{first_src} != null) {{")?;
        writeln!(src, "      const {}_key = JSON.stringify([{}]);", group.field_name, child_key_fields.join(", "))?;
        writeln!(src, "      if (!seen_{}.has({}_key)) {{", group.field_name, group.field_name)?;
        writeln!(src, "        seen_{}.add({}_key);", group.field_name, group.field_name)?;
        writeln!(src, "        parent.{}.push({{ {child_init} }});", group.field_name)?;
        writeln!(src, "      }}")?;
        writeln!(src, "    }}")?;
    }
    Ok(())
}

/// Emit `const seen_<field> = new Set<string>();` declarations for child deduplication.
fn emit_seen_sets(src: &mut String, query: &Query, output: &JsOutput) -> anyhow::Result<()> {
    for group in &query.nested_groups {
        let generic = match output {
            JsOutput::TypeScript => "<string>",
            JsOutput::JavaScript => "",
        };
        writeln!(src, "  const seen_{} = new Set{generic}();", group.field_name)?;
    }
    Ok(())
}

/// Build `"id: <src>.id, name: <src>.name, company: []"` for the parent object initializer.
///
/// `row_var` is the variable holding the flat row (e.g. `"row"` inside a for-loop,
/// or `"result.rows[0]"` for `:one` initialisation outside the loop).
fn build_parent_initializer(parent_cols: &[&crate::ir::ResultColumn], groups: &[NestedGroup], row_var: &str) -> String {
    let field_inits: Vec<String> = parent_cols
        .iter()
        .map(|c| format!("{}: {row_var}.{}", c.name, c.name))
        .chain(groups.iter().map(|g| format!("{}: []", g.field_name)))
        .collect();
    field_inits.join(", ")
}

/// Build `"id: row.company_id, name: row.company_name"` for a child object initializer.
fn build_child_initializer(group: &NestedGroup) -> String {
    group.columns.iter().map(|nc| format!("{}: row.{}", nc.target_name, nc.source_name)).collect::<Vec<_>>().join(", ")
}

/// Returns `: Type` for TypeScript variable annotations or empty string for JS.
fn ts_type_annotation(ts_type: &str, output: &JsOutput) -> String {
    match output {
        JsOutput::TypeScript => format!(": {ts_type}"),
        JsOutput::JavaScript => String::new(),
    }
}

/// Returns `<string, T>` for TypeScript Map generics or empty string for JS.
fn map_generic(value_type: &str, output: &JsOutput) -> String {
    match output {
        JsOutput::TypeScript => format!("<string, {value_type}>"),
        JsOutput::JavaScript => String::new(),
    }
}

/// Map a SQL type to its JavaScript/TypeScript type string.
fn js_type_with_contract(sql_type: &SqlType, nullable: bool, contract: &TsCoreContract) -> String {
    let base = js_base_type(sql_type, contract);
    if nullable {
        format!("{base} | null")
    } else {
        base
    }
}

fn js_base_type(sql_type: &SqlType, contract: &TsCoreContract) -> String {
    match sql_type {
        SqlType::Boolean => "boolean".to_string(),
        SqlType::SmallInt | SqlType::Integer | SqlType::BigInt => "number".to_string(),
        SqlType::Real | SqlType::Double | SqlType::Decimal => "number".to_string(),
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => "string".to_string(),
        SqlType::Interval | SqlType::Uuid => "string".to_string(),
        SqlType::Bytes => "Buffer".to_string(),
        // mysql2 returns DATE columns as 'YYYY-MM-DD' strings (not Date objects) and
        // sending a JS Date for a DATE param causes timezone-shift bugs. Use string.
        SqlType::Date => {
            if contract.date_as_string {
                "string".to_string()
            } else {
                "Date".to_string()
            }
        },
        SqlType::Time | SqlType::Timestamp | SqlType::TimestampTz => "Date".to_string(),
        SqlType::Json | SqlType::Jsonb => "unknown".to_string(),
        SqlType::Array(inner) => format!("{}[]", js_base_type(inner, contract)),
        SqlType::Custom(_) => "unknown".to_string(),
    }
}

#[cfg(test)]
pub(super) fn js_type(sql_type: &SqlType, nullable: bool, target: &JsTarget) -> String {
    let contract = super::adapter::resolve_ts_contract(*target, JsOutput::TypeScript);
    js_type_with_contract(sql_type, nullable, &contract)
}

// ─── Queries file ─────────────────────────────────────────────────────────────

fn build_queries_file_with_contract(
    group: &str,
    queries: &[Query],
    schema: &Schema,
    contract: &TsCoreContract,
    config: &OutputConfig,
) -> anyhow::Result<String> {
    let strategy = config.list_params.clone().unwrap_or_default();
    let mut src = String::new();
    writeln!(src, "// Generated by sqltgen. Do not edit.")?;
    writeln!(src, "// Runtime: {}", contract.runtime_hint)?;
    emit_runtime_imports(&mut src, contract)?;
    emit_table_imports(&mut src, &needed_tables(queries, schema), &contract.output)?;
    writeln!(src)?;
    emit_sql_constants(&mut src, queries, contract, &strategy)?;
    for query in queries {
        writeln!(src)?;
        if query.has_nested_groups() {
            emit_nested_types(&mut src, query, contract, config)?;
            writeln!(src)?;
        } else if has_inline_rows(query, schema) {
            emit_inline_row_type_with_contract(&mut src, query, contract, config)?;
            writeln!(src)?;
        }
        emit_query(&mut src, query, schema, contract, config, &strategy)?;
    }
    if !queries.is_empty() {
        writeln!(src)?;
        emit_querier_class(&mut src, group, queries, schema, contract, config)?;
    }
    Ok(src)
}

#[cfg(test)]
pub(super) fn build_queries_file(
    group: &str,
    queries: &[Query],
    schema: &Schema,
    target: &JsTarget,
    output: &JsOutput,
    config: &OutputConfig,
) -> anyhow::Result<String> {
    let contract = super::adapter::resolve_ts_contract(*target, *output);
    build_queries_file_with_contract(group, queries, schema, &contract, config)
}

#[cfg(test)]
pub(super) fn emit_inline_row_type(src: &mut String, query: &Query, output: &JsOutput, target: &JsTarget, config: &OutputConfig) -> anyhow::Result<()> {
    let contract = super::adapter::resolve_ts_contract(*target, *output);
    emit_inline_row_type_with_contract(src, query, &contract, config)
}

fn emit_querier_class(
    src: &mut String,
    group: &str,
    queries: &[Query],
    schema: &Schema,
    contract: &TsCoreContract,
    config: &OutputConfig,
) -> anyhow::Result<()> {
    let class_name = querier_class_name(group);
    match contract.output {
        JsOutput::TypeScript => {
            writeln!(src, "export class {class_name} {{")?;
            writeln!(src, "  constructor(private readonly connect: ConnectFn) {{}}")?;
            for query in queries {
                writeln!(src)?;
                emit_ts_querier_method(src, query, schema, contract, config)?;
            }
            writeln!(src, "}}")?;
        },
        JsOutput::JavaScript => {
            writeln!(src, "export class {class_name} {{")?;
            writeln!(src, "  /** @param {{ConnectFn}} connect */")?;
            writeln!(src, "  constructor(connect) {{")?;
            writeln!(src, "    this.connect = connect;")?;
            writeln!(src, "  }}")?;
            for query in queries {
                writeln!(src)?;
                emit_js_querier_method(src, query)?;
            }
            writeln!(src, "}}")?;
        },
    }
    Ok(())
}

fn emit_ts_querier_method(src: &mut String, query: &Query, schema: &Schema, contract: &TsCoreContract, config: &OutputConfig) -> anyhow::Result<()> {
    let fn_name = to_camel_case(&query.name);
    let ret = return_type(query, schema);
    let params = query
        .params
        .iter()
        .map(|p| {
            let ty = if p.is_list {
                let elem = js_type_resolved(&p.sql_type, false, contract, config);
                format!("{elem}[]")
            } else {
                js_type_resolved(&p.sql_type, p.nullable, contract, config)
            };
            format!("{}: {ty}", to_camel_case(&p.name))
        })
        .collect::<Vec<_>>()
        .join(", ");
    let args = query.params.iter().map(|p| to_camel_case(&p.name)).collect::<Vec<_>>().join(", ");

    writeln!(src, "  async {fn_name}({params}): Promise<{ret}> {{")?;
    writeln!(src, "    const db = await this.connect();")?;
    writeln!(src, "    try {{")?;
    if args.is_empty() {
        writeln!(src, "      return {fn_name}(db);")?;
    } else {
        writeln!(src, "      return {fn_name}(db, {args});")?;
    }
    writeln!(src, "    }} finally {{")?;
    writeln!(src, "      await releaseDb(db);")?;
    writeln!(src, "    }}")?;
    writeln!(src, "  }}")?;
    Ok(())
}

fn emit_js_querier_method(src: &mut String, query: &Query) -> anyhow::Result<()> {
    let fn_name = to_camel_case(&query.name);
    let params = query.params.iter().map(|p| to_camel_case(&p.name)).collect::<Vec<_>>().join(", ");
    let args = if params.is_empty() { "db".to_string() } else { format!("db, {params}") };
    writeln!(src, "  async {fn_name}({params}) {{")?;
    writeln!(src, "    const db = await this.connect();")?;
    writeln!(src, "    try {{")?;
    writeln!(src, "      return {fn_name}({args});")?;
    writeln!(src, "    }} finally {{")?;
    writeln!(src, "      await releaseDb(db);")?;
    writeln!(src, "    }}")?;
    writeln!(src, "  }}")?;
    Ok(())
}

/// Collect the names of schema tables used as return types by any query.
fn needed_tables<'a>(queries: &[Query], schema: &'a Schema) -> BTreeSet<&'a str> {
    let mut set = BTreeSet::new();
    for query in queries {
        if let Some(name) = infer_table(query, schema) {
            set.insert(name);
        }
    }
    set
}

/// Emit imports for the generated runtime helper module.
fn emit_runtime_imports(src: &mut String, contract: &TsCoreContract) -> anyhow::Result<()> {
    match contract.output {
        JsOutput::TypeScript => {
            writeln!(src, "import type {{ ConnectFn, Db }} from './_sqltgen';")?;
            writeln!(src, "import {{ releaseDb }} from './_sqltgen';")?;
        },
        JsOutput::JavaScript => {
            writeln!(src, "import {{ releaseDb }} from './_sqltgen.js';")?;
            writeln!(src, "/** @typedef {{import('./_sqltgen.js').Db}} Db */")?;
            writeln!(src, "/** @typedef {{import('./_sqltgen.js').ConnectFn}} ConnectFn */")?;
        },
    }
    Ok(())
}

/// Emit table type imports into the queries file.
fn emit_table_imports(src: &mut String, tables: &BTreeSet<&str>, output: &JsOutput) -> anyhow::Result<()> {
    if tables.is_empty() {
        return Ok(());
    }
    writeln!(src)?;
    for name in tables {
        let class_name = to_pascal_case(name);
        match output {
            JsOutput::TypeScript => writeln!(src, "import type {{ {class_name} }} from './{name}';")?,
            JsOutput::JavaScript => writeln!(src, "/** @typedef {{import('./{name}.js').{class_name}}} {class_name} */")?,
        }
    }
    Ok(())
}

/// Emit SQL string constants for all non-dynamic-list queries.
fn emit_sql_constants(src: &mut String, queries: &[Query], contract: &TsCoreContract, strategy: &ListParamStrategy) -> anyhow::Result<()> {
    for query in queries {
        if query.params.iter().any(|p| p.is_list) && *strategy == ListParamStrategy::Dynamic {
            continue;
        }
        let const_name = sql_const_name(&query.name);
        // NOTE: only one list parameter per query is currently supported.
        let base_sql = match query.params.iter().find(|p| p.is_list) {
            Some(lp) => lp.native_list_sql.clone().unwrap_or_else(|| query.sql.clone()),
            None => query.sql.clone(),
        };
        let sql = normalize_sql(&base_sql, contract);
        let sql = sql.trim_end().trim_end_matches(';');
        let sql = sql.replace('`', "\\`").replace("${", "\\${");
        writeln!(src, "const {const_name} = `{sql}`;")?;
    }
    Ok(())
}

/// Rewrite `$N`/`?N` placeholders for the target driver.
/// PostgreSQL keeps `$N`, SQLite and MySQL rewrite to anonymous `?`.
/// Rewrite SQL placeholders for the target driver.
///
/// PostgreSQL (`pg`) accepts `$N` natively; leave the SQL unchanged.
/// SQLite (`better-sqlite3`) and MySQL (`mysql2`) require anonymous `?`.
fn normalize_sql(sql: &str, contract: &TsCoreContract) -> String {
    (contract.normalize_sql)(sql)
}

// ─── Query function emission ──────────────────────────────────────────────────

fn emit_query(
    src: &mut String,
    query: &Query,
    schema: &Schema,
    contract: &TsCoreContract,
    config: &OutputConfig,
    strategy: &ListParamStrategy,
) -> anyhow::Result<()> {
    // Defensive guard: frontend should already reject this combination,
    // but keep a backend-level check for manually constructed IR in tests/tools.
    ensure_no_nested_list_combo(query)?;
    let ctx = QueryContext::new(query, schema, contract, config, "Db");
    (contract.emit_query)(src, &ctx, strategy)
}

/// Emit the JSDoc annotation block for a query function (JS output only).
fn emit_jsdoc(src: &mut String, ctx: &QueryContext, params: &[&Parameter]) -> anyhow::Result<()> {
    if matches!(ctx.contract.output, JsOutput::TypeScript) {
        return Ok(());
    }
    writeln!(src, "/**")?;
    writeln!(src, " * @param {{{}}} db", ctx.conn_type)?;
    for p in params {
        let ty = if p.is_list {
            let elem = js_type_resolved(&p.sql_type, false, ctx.contract, ctx.config);
            format!("{elem}[]")
        } else {
            js_type_resolved(&p.sql_type, p.nullable, ctx.contract, ctx.config)
        };
        writeln!(src, " * @param {{{ty}}} {}", to_camel_case(&p.name))?;
    }
    writeln!(src, " * @returns {{Promise<{}>}}", ctx.ret)?;
    writeln!(src, " */")?;
    Ok(())
}

/// Emit the `export async function` opening line.
/// TypeScript includes type annotations; JavaScript uses plain parameter names.
fn emit_fn_open(src: &mut String, ctx: &QueryContext, params: &[&Parameter]) -> anyhow::Result<()> {
    match ctx.contract.output {
        JsOutput::TypeScript => {
            let typed: Vec<String> = params
                .iter()
                .map(|p| {
                    let ty = if p.is_list {
                        let elem = js_type_resolved(&p.sql_type, false, ctx.contract, ctx.config);
                        format!("{elem}[]")
                    } else {
                        js_type_resolved(&p.sql_type, p.nullable, ctx.contract, ctx.config)
                    };
                    format!("{}: {ty}", to_camel_case(&p.name))
                })
                .collect();
            let sig = std::iter::once(format!("db: {}", ctx.conn_type)).chain(typed).collect::<Vec<_>>().join(", ");
            writeln!(src, "export async function {}({sig}): Promise<{}> {{", ctx.fn_name, ctx.ret)?;
        },
        JsOutput::JavaScript => {
            let names: Vec<String> = std::iter::once("db".to_string()).chain(params.iter().map(|p| to_camel_case(&p.name))).collect();
            writeln!(src, "export async function {}({}) {{", ctx.fn_name, names.join(", "))?;
        },
    }
    Ok(())
}

/// Compute the JS/TS return type string for a query.
fn return_type(query: &Query, schema: &Schema) -> String {
    let row = ts_row_type(query, schema);
    match query.cmd {
        QueryCmd::One => format!("{row} | null"),
        QueryCmd::Many => format!("{row}[]"),
        QueryCmd::Exec => "void".to_string(),
        QueryCmd::ExecRows => "number".to_string(),
    }
}

/// Compute the row type name for a query result (table name or `{Query}Row`).
///
/// Nested-group queries always use the inline `{Query}Row` name because the
/// parent type is structurally different from any schema table.
fn ts_row_type(query: &Query, schema: &Schema) -> String {
    if query.has_nested_groups() {
        return inline_row_type_name(&query.name);
    }
    infer_row_type_name(query, schema).unwrap_or_else(|| inline_row_type_name(&query.name))
}

// ─── PostgreSQL (pg) ─────────────────────────────────────────────────────────

pub(super) fn emit_pg_query(src: &mut String, ctx: &QueryContext, strategy: &ListParamStrategy) -> anyhow::Result<()> {
    if let Some(lp) = ctx.query.params.iter().find(|p| p.is_list) {
        return emit_pg_list_query(src, ctx, strategy, lp);
    }
    let const_name = sql_const_name(&ctx.query.name);
    let params = ctx.params();
    emit_jsdoc(src, ctx, &params)?;
    emit_fn_open(src, ctx, &params)?;
    let args = pg_params_array(ctx.query, ctx.config, ctx.contract);
    if ctx.query.has_nested_groups() {
        match ctx.query.cmd {
            QueryCmd::Many => emit_pg_nested_many_body(src, ctx, &const_name, &args)?,
            QueryCmd::One => emit_pg_nested_one_body(src, ctx, &const_name, &args)?,
            _ => emit_pg_body(src, ctx, &const_name, &args)?,
        }
    } else {
        emit_pg_body(src, ctx, &const_name, &args)?;
    }
    writeln!(src, "}}")?;
    Ok(())
}

/// Emit the query body for a PostgreSQL function: the `db.query(...)` call and result handling.
///
/// Accepts either a SQL constant name (static query) or a runtime `sql` variable (dynamic list
/// expansion) as `sql_expr`, and the pre-built params array string as `args`.
fn emit_pg_body(src: &mut String, ctx: &QueryContext, sql_expr: &str, args: &str) -> anyhow::Result<()> {
    match ctx.query.cmd {
        QueryCmd::Exec => writeln!(src, "  await db.query({sql_expr}, {args});")?,
        QueryCmd::ExecRows => {
            writeln!(src, "  const result = await db.query({sql_expr}, {args});")?;
            writeln!(src, "  return result.rowCount ?? 0;")?;
        },
        QueryCmd::One => {
            let row = ts_row_type(ctx.query, ctx.schema);
            let call = pg_query_call(sql_expr, &row, args, &ctx.contract.output);
            writeln!(src, "  const result = await {call};")?;
            if let Some(transform) = row_transform_expr(ctx.query, ctx.config, ctx.contract, "raw") {
                writeln!(src, "  const raw = result.rows[0];")?;
                writeln!(src, "  if (!raw) return null;")?;
                writeln!(src, "  return {transform};")?;
            } else {
                writeln!(src, "  return result.rows[0] ?? null;")?;
            }
        },
        QueryCmd::Many => {
            let row = ts_row_type(ctx.query, ctx.schema);
            let call = pg_query_call(sql_expr, &row, args, &ctx.contract.output);
            writeln!(src, "  const result = await {call};")?;
            if let Some(transform) = row_transform_expr(ctx.query, ctx.config, ctx.contract, "raw") {
                writeln!(src, "  return result.rows.map(raw => ({transform}));")?;
            } else {
                writeln!(src, "  return result.rows;")?;
            }
        },
    }
    Ok(())
}

/// Build `db.query<Row>(sql, args)` for TypeScript or `db.query(sql, args)` for JavaScript.
fn pg_query_call(const_name: &str, row_type: &str, args: &str, output: &JsOutput) -> String {
    match output {
        JsOutput::TypeScript => format!("db.query<{row_type}>({const_name}, {args})"),
        JsOutput::JavaScript => format!("db.query({const_name}, {args})"),
    }
}

/// Build the `[p1, p2, ...]` params array for a pg query (unique params by index).
///
/// PostgreSQL uses `$N` reference-by-number, so each unique parameter index appears
/// exactly once in the bound array regardless of how many times it is referenced in
/// the SQL. Contrast with [`mysql_params_array`], which uses [`positional_bind_names`]
/// to repeat values for every anonymous `?` occurrence.
fn pg_params_array(query: &Query, config: &OutputConfig, contract: &TsCoreContract) -> String {
    let mut params: Vec<&Parameter> = query.params.iter().collect();
    params.sort_by_key(|p| p.index);
    let exprs: Vec<String> = params.iter().map(|p| ts_write_expr(p, config, contract)).collect();
    format!("[{}]", exprs.join(", "))
}

fn emit_pg_list_query(src: &mut String, ctx: &QueryContext, strategy: &ListParamStrategy, lp: &Parameter) -> anyhow::Result<()> {
    let const_name = sql_const_name(&ctx.query.name);
    let params = ctx.params();
    let lp_name = to_camel_case(&lp.name);
    emit_jsdoc(src, ctx, &params)?;
    emit_fn_open(src, ctx, &params)?;
    match strategy {
        ListParamStrategy::Native => {
            let args = pg_params_array(ctx.query, ctx.config, ctx.contract);
            emit_pg_body(src, ctx, &const_name, &args)?;
        },
        ListParamStrategy::Dynamic => {
            let scalar_params: Vec<&Parameter> = ctx.query.params.iter().filter(|p| !p.is_list).collect();
            let (before_raw, after_raw) = split_at_in_clause(&ctx.query.sql, lp.index).unwrap_or_else(|| (ctx.query.sql.clone(), String::new()));
            let before = before_raw.replace('"', "\\\"").replace('\n', " ");
            let after = after_raw.replace('"', "\\\"").replace('\n', " ");
            let before = before.trim_end().trim_end_matches(';');
            let after = after.trim_start();
            let offset = scalar_params.iter().filter(|p| p.index < lp.index).count() + 1;
            writeln!(src, "  const placeholders = {lp_name}.map((_, i) => '$' + ({offset} + i)).join(', ');")?;
            writeln!(src, r#"  const sql = `{before}IN (${{placeholders}}){after}`;"#)?;
            let before_args: Vec<String> = scalar_params.iter().filter(|p| p.index < lp.index).map(|p| ts_write_expr(p, ctx.config, ctx.contract)).collect();
            let after_args: Vec<String> = scalar_params.iter().filter(|p| p.index > lp.index).map(|p| ts_write_expr(p, ctx.config, ctx.contract)).collect();
            let all_args = [before_args, vec![format!("...{lp_name}")], after_args].concat().join(", ");
            emit_pg_body(src, ctx, "sql", &format!("[{all_args}]"))?;
        },
    }
    writeln!(src, "}}")?;
    Ok(())
}

// ─── SQLite (better-sqlite3) ─────────────────────────────────────────────────

pub(super) fn emit_sqlite_query(src: &mut String, ctx: &QueryContext, strategy: &ListParamStrategy) -> anyhow::Result<()> {
    if let Some(lp) = ctx.query.params.iter().find(|p| p.is_list) {
        return emit_sqlite_list_query(src, ctx, strategy, lp);
    }
    let const_name = sql_const_name(&ctx.query.name);
    let params = ctx.params();
    emit_jsdoc(src, ctx, &params)?;
    emit_fn_open(src, ctx, &params)?;
    let args = sqlite_spread_args(ctx.query, ctx.config, ctx.contract);
    if ctx.query.has_nested_groups() {
        match ctx.query.cmd {
            QueryCmd::Many => emit_sqlite_nested_many_body(src, ctx, &const_name, &args)?,
            QueryCmd::One => emit_sqlite_nested_one_body(src, ctx, &const_name, &args)?,
            _ => emit_sqlite_body(src, ctx, &const_name, &args)?,
        }
    } else {
        emit_sqlite_body(src, ctx, &const_name, &args)?;
    }
    writeln!(src, "}}")?;
    Ok(())
}

/// Build the spread argument list for a better-sqlite3 prepared statement call.
///
/// better-sqlite3 uses anonymous `?` placeholders; the arg list must follow the SQL
/// occurrence order including repeated params (e.g. a `@genre` used twice → two args).
fn sqlite_spread_args(query: &Query, config: &OutputConfig, contract: &TsCoreContract) -> String {
    positional_bind_names(query)
        .iter()
        .map(|&n| {
            let param = query.params.iter().find(|p| p.name == n);
            param.map(|p| ts_write_expr(p, config, contract)).unwrap_or_else(|| to_camel_case(n))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Returns ` as Type` for TypeScript or empty string for JavaScript.
///
/// Used for better-sqlite3 `.get()`/`.all()` return types and mysql2 row casts,
/// both of which need an explicit `as T` only in TypeScript output.
fn ts_cast(ts_type: &str, output: &JsOutput) -> String {
    match output {
        JsOutput::TypeScript => format!(" as {ts_type}"),
        JsOutput::JavaScript => String::new(),
    }
}

fn emit_sqlite_list_query(src: &mut String, ctx: &QueryContext, strategy: &ListParamStrategy, lp: &Parameter) -> anyhow::Result<()> {
    let const_name = sql_const_name(&ctx.query.name);
    let params = ctx.params();
    let lp_name = to_camel_case(&lp.name);
    emit_jsdoc(src, ctx, &params)?;
    emit_fn_open(src, ctx, &params)?;
    match strategy {
        ListParamStrategy::Native => {
            writeln!(src, "  const {lp_name}Json = JSON.stringify({lp_name});")?;
            let args = sqlite_list_spread_args(ctx.query, lp, &format!("{lp_name}Json"), ctx.config, ctx.contract);
            emit_sqlite_body(src, ctx, &const_name, &args)?;
        },
        ListParamStrategy::Dynamic => {
            let scalar_params: Vec<&Parameter> = ctx.query.params.iter().filter(|p| !p.is_list).collect();
            let (before_raw, after_raw) = split_at_in_clause(&ctx.query.sql, lp.index).unwrap_or_else(|| (ctx.query.sql.clone(), String::new()));
            let before = rewrite_to_anon_params(&before_raw).replace('"', "\\\"").replace('\n', " ");
            let after = rewrite_to_anon_params(&after_raw).replace('"', "\\\"").replace('\n', " ");
            let before = before.trim_end().trim_end_matches(';');
            let after = after.trim_start();
            writeln!(src, r#"  const placeholders = {lp_name}.map(() => "?").join(", ");"#)?;
            writeln!(src, r#"  const sql = `{before}IN (${{placeholders}}){after}`;"#)?;
            let before_args: Vec<String> = scalar_params.iter().filter(|p| p.index < lp.index).map(|p| ts_write_expr(p, ctx.config, ctx.contract)).collect();
            let after_args: Vec<String> = scalar_params.iter().filter(|p| p.index > lp.index).map(|p| ts_write_expr(p, ctx.config, ctx.contract)).collect();
            let all_args = [before_args, vec![format!("...{lp_name}")], after_args].concat().join(", ");
            emit_sqlite_body(src, ctx, "sql", &all_args)?;
        },
    }
    writeln!(src, "}}")?;
    Ok(())
}

/// Build the spread args string for a SQLite native-list query.
///
/// Follows SQL occurrence order (via [`positional_bind_names`]), substituting the list
/// param's JSON expression wherever that param's name would appear.
fn sqlite_list_spread_args(query: &Query, lp: &Parameter, lp_expr: &str, config: &OutputConfig, contract: &TsCoreContract) -> String {
    let lp_camel = to_camel_case(&lp.name);
    positional_bind_names(query)
        .iter()
        .map(|&n| {
            let cn = to_camel_case(n);
            if cn == lp_camel {
                lp_expr.to_string()
            } else {
                let param = query.params.iter().find(|p| p.name == n);
                param.map(|p| ts_write_expr(p, config, contract)).unwrap_or(cn)
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn emit_sqlite_body(src: &mut String, ctx: &QueryContext, sql_expr: &str, args: &str) -> anyhow::Result<()> {
    match ctx.query.cmd {
        QueryCmd::Exec => writeln!(src, "  db.prepare({sql_expr}).run({args});")?,
        QueryCmd::ExecRows => {
            writeln!(src, "  const result = db.prepare({sql_expr}).run({args});")?;
            writeln!(src, "  return result.changes;")?;
        },
        QueryCmd::One => {
            let row = ts_row_type(ctx.query, ctx.schema);
            let cast = ts_cast(&format!("{row} | undefined"), &ctx.contract.output);
            if let Some(transform) = row_transform_expr(ctx.query, ctx.config, ctx.contract, "raw") {
                writeln!(src, "  const raw = db.prepare({sql_expr}).get({args}){cast};")?;
                writeln!(src, "  if (!raw) return null;")?;
                writeln!(src, "  return {transform};")?;
            } else {
                writeln!(src, "  const row = db.prepare({sql_expr}).get({args}){cast};")?;
                writeln!(src, "  return row ?? null;")?;
            }
        },
        QueryCmd::Many => {
            let row = ts_row_type(ctx.query, ctx.schema);
            let cast = ts_cast(&format!("{row}[]"), &ctx.contract.output);
            if let Some(transform) = row_transform_expr(ctx.query, ctx.config, ctx.contract, "raw") {
                writeln!(src, "  return (db.prepare({sql_expr}).all({args}){cast}).map(raw => ({transform}));")?;
            } else {
                writeln!(src, "  return db.prepare({sql_expr}).all({args}){cast};")?;
            }
        },
    }
    Ok(())
}

// ─── MySQL (mysql2) ───────────────────────────────────────────────────────────

pub(super) fn emit_mysql_query(src: &mut String, ctx: &QueryContext, strategy: &ListParamStrategy) -> anyhow::Result<()> {
    if let Some(lp) = ctx.query.params.iter().find(|p| p.is_list) {
        return emit_mysql_list_query(src, ctx, strategy, lp);
    }
    let const_name = sql_const_name(&ctx.query.name);
    let params = ctx.params();
    emit_jsdoc(src, ctx, &params)?;
    emit_fn_open(src, ctx, &params)?;
    let args = mysql_params_array(ctx.query, ctx.config, ctx.contract);
    if ctx.query.has_nested_groups() {
        match ctx.query.cmd {
            QueryCmd::Many => emit_mysql_nested_many_body(src, ctx, &const_name, &args)?,
            QueryCmd::One => emit_mysql_nested_one_body(src, ctx, &const_name, &args)?,
            _ => emit_mysql_body(src, ctx, &const_name, &args)?,
        }
    } else {
        emit_mysql_body(src, ctx, &const_name, &args)?;
    }
    writeln!(src, "}}")?;
    Ok(())
}

/// Build the `[p1, p2, ...]` params array for mysql2 (positional `?`, params in SQL order).
fn mysql_params_array(query: &Query, config: &OutputConfig, contract: &TsCoreContract) -> String {
    let exprs: Vec<String> = positional_bind_names(query)
        .iter()
        .map(|&n| {
            let param = query.params.iter().find(|p| p.name == n);
            param.map(|p| ts_write_expr(p, config, contract)).unwrap_or_else(|| to_camel_case(n))
        })
        .collect();
    format!("[{}]", exprs.join(", "))
}

fn emit_mysql_body(src: &mut String, ctx: &QueryContext, sql_expr: &str, args: &str) -> anyhow::Result<()> {
    // mysql2's execute() sends all JS `number` values as DOUBLE in the binary
    // protocol. MySQL rejects DOUBLE for LIMIT/OFFSET and other integer contexts.
    // Using query() sends parameters via the text protocol (client-side escaping),
    // which avoids the type mismatch and works correctly for all param types.
    match ctx.query.cmd {
        QueryCmd::Exec => writeln!(src, "  await db.query({sql_expr}, {args});")?,
        QueryCmd::ExecRows => {
            let rsh = mysql_type_param(&ctx.contract.output, "ResultSetHeader");
            writeln!(src, "  const [result] = await db.query{rsh}({sql_expr}, {args});")?;
            writeln!(src, "  return result.affectedRows;")?;
        },
        QueryCmd::One => {
            let row = ts_row_type(ctx.query, ctx.schema);
            let rdp = mysql_type_param(&ctx.contract.output, "RowDataPacket[]");
            let cast = ts_cast(&format!("{row} | undefined"), &ctx.contract.output);
            writeln!(src, "  const [rows] = await db.query{rdp}({sql_expr}, {args});")?;
            if let Some(transform) = row_transform_expr(ctx.query, ctx.config, ctx.contract, "raw") {
                writeln!(src, "  const raw = rows[0]{cast};")?;
                writeln!(src, "  if (!raw) return null;")?;
                writeln!(src, "  return {transform};")?;
            } else {
                writeln!(src, "  return (rows[0]{cast}) ?? null;")?;
            }
        },
        QueryCmd::Many => {
            let row = ts_row_type(ctx.query, ctx.schema);
            let rdp = mysql_type_param(&ctx.contract.output, "RowDataPacket[]");
            let cast = ts_cast(&format!("{row}[]"), &ctx.contract.output);
            writeln!(src, "  const [rows] = await db.query{rdp}({sql_expr}, {args});")?;
            if let Some(transform) = row_transform_expr(ctx.query, ctx.config, ctx.contract, "raw") {
                writeln!(src, "  return (rows{cast}).map(raw => ({transform}));")?;
            } else {
                writeln!(src, "  return rows{cast};")?;
            }
        },
    }
    Ok(())
}

/// Returns `<Type>` (TS) or empty string (JS) for a mysql2 generic type parameter.
/// Emit `<T>` (TypeScript generic type argument) or empty string (JavaScript).
///
/// Used to make mysql2 `execute` calls typed: `db.execute<RowDataPacket[]>(...)`.
fn mysql_type_param(output: &JsOutput, ts_type: &str) -> String {
    match output {
        JsOutput::TypeScript => format!("<{ts_type}>"),
        JsOutput::JavaScript => String::new(),
    }
}

fn emit_mysql_list_query(src: &mut String, ctx: &QueryContext, strategy: &ListParamStrategy, lp: &Parameter) -> anyhow::Result<()> {
    let const_name = sql_const_name(&ctx.query.name);
    let params = ctx.params();
    let lp_name = to_camel_case(&lp.name);
    emit_jsdoc(src, ctx, &params)?;
    emit_fn_open(src, ctx, &params)?;
    match strategy {
        ListParamStrategy::Native => {
            writeln!(src, "  const {lp_name}Json = JSON.stringify({lp_name});")?;
            let args = mysql_list_params_array(ctx.query, lp, &format!("{lp_name}Json"), ctx.config, ctx.contract);
            emit_mysql_body(src, ctx, &const_name, &args)?;
        },
        ListParamStrategy::Dynamic => {
            let scalar_params: Vec<&Parameter> = ctx.query.params.iter().filter(|p| !p.is_list).collect();
            let (before_raw, after_raw) = split_at_in_clause(&ctx.query.sql, lp.index).unwrap_or_else(|| (ctx.query.sql.clone(), String::new()));
            let before = rewrite_to_anon_params(&before_raw).replace('"', "\\\"").replace('\n', " ");
            let after = rewrite_to_anon_params(&after_raw).replace('"', "\\\"").replace('\n', " ");
            let before = before.trim_end().trim_end_matches(';');
            let after = after.trim_start();
            writeln!(src, r#"  const placeholders = {lp_name}.map(() => "?").join(", ");"#)?;
            writeln!(src, r#"  const sql = `{before}IN (${{placeholders}}){after}`;"#)?;
            let before_args: Vec<String> = scalar_params.iter().filter(|p| p.index < lp.index).map(|p| ts_write_expr(p, ctx.config, ctx.contract)).collect();
            let after_args: Vec<String> = scalar_params.iter().filter(|p| p.index > lp.index).map(|p| ts_write_expr(p, ctx.config, ctx.contract)).collect();
            let all_args = [before_args, vec![format!("...{lp_name}")], after_args].concat().join(", ");
            emit_mysql_body(src, ctx, "sql", &format!("[{all_args}]"))?;
        },
    }
    writeln!(src, "}}")?;
    Ok(())
}

/// Build the `[p1, p2, ...]` array for a MySQL native list query,
/// substituting the JSON-stringified expression for the list param slot.
fn mysql_list_params_array(query: &Query, lp: &Parameter, lp_expr: &str, config: &OutputConfig, contract: &TsCoreContract) -> String {
    let mut params: Vec<&Parameter> = query.params.iter().collect();
    params.sort_by_key(|p| p.index);
    let exprs: Vec<String> = params.iter().map(|p| if p.index == lp.index { lp_expr.to_string() } else { ts_write_expr(p, config, contract) }).collect();
    format!("[{}]", exprs.join(", "))
}
