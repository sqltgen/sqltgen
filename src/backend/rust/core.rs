use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::common::{group_queries, has_inline_rows, infer_row_type_name, infer_table, querier_class_name, queries_file_stem, row_type_name};
use crate::backend::naming::{to_pascal_case, to_snake_case};
use crate::backend::sql_rewrite::{positional_bind_names, rewrite_to_anon_params, split_at_in_clause};
use crate::backend::GeneratedFile;
use crate::config::{ListParamStrategy, OutputConfig};
use crate::ir::{EnumType, NativeListBind, Parameter, Query, QueryCmd, Schema, SqlType};

use super::adapter::{RustBindMode, RustCoreContract, RustPlaceholderMode, RustSqlNormMode};
use super::typemap::RustTypeMap;

pub(super) fn generate_core_files(
    schema: &Schema,
    queries: &[Query],
    contract: &RustCoreContract,
    config: &OutputConfig,
    type_map: &RustTypeMap,
) -> anyhow::Result<Vec<GeneratedFile>> {
    let mut files = Vec::new();

    // One struct file per table
    for table in &schema.tables {
        let struct_name = to_pascal_case(&table.name);
        let mut src = String::new();
        writeln!(src, "#[derive(Debug, sqlx::FromRow)]")?;
        writeln!(src, "pub struct {struct_name} {{")?;
        for col in &table.columns {
            writeln!(src, "    pub {}: {},", col.name, type_map.field_type(&col.sql_type, col.nullable))?;
        }
        writeln!(src, "}}")?;

        let path = PathBuf::from(&config.out).join("models").join(format!("{}.rs", table.name));
        files.push(GeneratedFile { path, content: src });
    }

    // One enum file per CREATE TYPE ... AS ENUM
    for e in &schema.enums {
        let mut src = String::new();
        emit_rust_enum(&mut src, e)?;
        let path = PathBuf::from(&config.out).join("models").join(format!("{}.rs", e.name));
        files.push(GeneratedFile { path, content: src });
    }

    // One .rs file per query group
    let strategy = config.list_params.clone().unwrap_or_default();
    let groups = group_queries(queries);
    let mut group_stems: Vec<String> = Vec::new();
    for (group, group_queries) in &groups {
        let stem = queries_file_stem(group).to_string();
        group_stems.push(stem.clone());

        let mut src = String::new();
        writeln!(src, "use super::super::sqltgen::DbPool;")?;
        writeln!(src)?;

        // Import only table structs that are actually used as return types
        let needed: HashSet<&str> = group_queries.iter().filter_map(|q| infer_table(q, schema)).collect();
        let mut needed_sorted: Vec<&str> = needed.iter().copied().collect();
        needed_sorted.sort();
        for name in &needed_sorted {
            writeln!(src, "use super::super::models::{}::{};", name, to_pascal_case(name))?;
        }
        let enum_names = crate::backend::common::needed_enums(&group_queries.iter().collect::<Vec<_>>());
        for name in &enum_names {
            writeln!(src, "use super::super::models::{}::{};", name, to_pascal_case(name))?;
        }
        if !needed.is_empty() || !enum_names.is_empty() {
            writeln!(src)?;
        }

        // Custom row structs for queries that don't return a whole table
        for query in group_queries {
            if has_inline_rows(query, schema) {
                emit_row_struct(&mut src, query, type_map)?;
                writeln!(src)?;
            }
        }

        // Query functions
        for (i, query) in group_queries.iter().enumerate() {
            if i > 0 {
                writeln!(src)?;
            }
            emit_rust_query(&mut src, query, schema, "DbPool", contract, &strategy, type_map)?;
        }

        if !group_queries.is_empty() {
            writeln!(src)?;
            emit_rust_querier(&mut src, group, group_queries, schema, "DbPool", type_map)?;
        }

        let path = PathBuf::from(&config.out).join("queries").join(format!("{stem}.rs"));
        files.push(GeneratedFile { path, content: src });
    }

    // Root mod.rs
    {
        let mut src = String::new();
        writeln!(src, "#![allow(dead_code)]")?;
        writeln!(src)?;
        writeln!(src, "pub mod sqltgen;")?;
        if !schema.tables.is_empty() || !schema.enums.is_empty() {
            writeln!(src, "pub mod models;")?;
        }
        if !group_stems.is_empty() {
            writeln!(src, "pub mod queries;")?;
        }
        let path = PathBuf::from(&config.out).join("mod.rs");
        files.push(GeneratedFile { path, content: src });
    }

    // models/mod.rs
    if !schema.tables.is_empty() || !schema.enums.is_empty() {
        let mut src = String::new();
        for table in &schema.tables {
            writeln!(src, "pub mod {};", table.name)?;
        }
        for e in &schema.enums {
            writeln!(src, "pub mod {};", e.name)?;
        }
        let path = PathBuf::from(&config.out).join("models").join("mod.rs");
        files.push(GeneratedFile { path, content: src });
    }

    // queries/mod.rs
    if !group_stems.is_empty() {
        let mut src = String::new();
        for stem in &group_stems {
            writeln!(src, "pub mod {stem};")?;
        }
        let path = PathBuf::from(&config.out).join("queries").join("mod.rs");
        files.push(GeneratedFile { path, content: src });
    }

    Ok(files)
}

fn emit_row_struct(src: &mut String, query: &Query, type_map: &RustTypeMap) -> anyhow::Result<()> {
    let name = row_type_name(&query.name);
    writeln!(src, "#[derive(Debug, sqlx::FromRow)]")?;
    writeln!(src, "pub struct {name} {{")?;
    for col in &query.result_columns {
        writeln!(src, "    pub {}: {},", col.name, type_map.field_type(&col.sql_type, col.nullable))?;
    }
    writeln!(src, "}}")?;
    Ok(())
}

fn emit_rust_query(
    src: &mut String,
    query: &Query,
    schema: &Schema,
    pool_type: &str,
    contract: &RustCoreContract,
    strategy: &ListParamStrategy,
    type_map: &RustTypeMap,
) -> anyhow::Result<()> {
    let fn_name = to_snake_case(&query.name);
    let row_type = result_row_type(query, schema);

    let return_type = match query.cmd {
        QueryCmd::One => format!("Result<Option<{row_type}>, sqlx::Error>"),
        QueryCmd::Many => format!("Result<Vec<{row_type}>, sqlx::Error>"),
        QueryCmd::Exec => "Result<(), sqlx::Error>".to_string(),
        QueryCmd::ExecRows => "Result<u64, sqlx::Error>".to_string(),
    };

    let params_sig: String =
        std::iter::once(format!("pool: &{pool_type}")).chain(query.params.iter().map(|p| rust_param_sig(p, type_map))).collect::<Vec<_>>().join(", ");

    writeln!(src, "pub async fn {fn_name}({params_sig}) -> {return_type} {{")?;

    let list_param = query.params.iter().find(|p| p.is_list);
    if let Some(lp) = list_param {
        emit_rust_list_query(src, query, &row_type, contract, strategy, lp)?;
    } else {
        emit_rust_scalar_query(src, query, &row_type, contract)?;
    }

    writeln!(src, "}}")?;
    Ok(())
}

fn emit_rust_querier(src: &mut String, group: &str, queries: &[Query], schema: &Schema, pool_type: &str, type_map: &RustTypeMap) -> anyhow::Result<()> {
    let struct_name = querier_class_name(group);
    writeln!(src, "pub struct {struct_name}<'a> {{")?;
    writeln!(src, "    pool: &'a {pool_type},")?;
    writeln!(src, "}}")?;
    writeln!(src)?;
    writeln!(src, "impl<'a> {struct_name}<'a> {{")?;
    writeln!(src, "    pub fn new(pool: &'a {pool_type}) -> Self {{")?;
    writeln!(src, "        Self {{ pool }}")?;
    writeln!(src, "    }}")?;

    for query in queries {
        writeln!(src)?;
        emit_rust_querier_method(src, query, schema, type_map)?;
    }

    writeln!(src, "}}")?;
    Ok(())
}

fn emit_rust_querier_method(src: &mut String, query: &Query, schema: &Schema, type_map: &RustTypeMap) -> anyhow::Result<()> {
    let fn_name = to_snake_case(&query.name);
    let row_type = result_row_type(query, schema);
    let return_type = match query.cmd {
        QueryCmd::One => format!("Result<Option<{row_type}>, sqlx::Error>"),
        QueryCmd::Many => format!("Result<Vec<{row_type}>, sqlx::Error>"),
        QueryCmd::Exec => "Result<(), sqlx::Error>".to_string(),
        QueryCmd::ExecRows => "Result<u64, sqlx::Error>".to_string(),
    };
    let params_sig = query.params.iter().map(|p| rust_param_sig(p, type_map)).collect::<Vec<_>>().join(", ");
    let args = query.params.iter().map(|p| to_snake_case(&p.name)).collect::<Vec<_>>().join(", ");
    let call_args = if args.is_empty() { "self.pool".to_string() } else { format!("self.pool, {args}") };

    let params_part = if params_sig.is_empty() { String::new() } else { format!(", {params_sig}") };
    writeln!(src, "    pub async fn {fn_name}(&self{params_part}) -> {return_type} {{")?;
    writeln!(src, "        {fn_name}({call_args}).await")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit `let sql = r##"..."##;` for the given SQL text.
///
/// Uses double-`#` raw strings (`r##"..."##`) so any `"#` sequence in SQL is handled
/// without escaping. Callers pass `sql_expr = "sql"` to [`emit_rust_sqlx_call`].
fn emit_rust_sql_let(src: &mut String, sql: &str) -> anyhow::Result<()> {
    writeln!(src, "    let sql = r##\"")?;
    for line in sql.lines() {
        writeln!(src, "        {line}")?;
    }
    writeln!(src, "    \"##;")?;
    Ok(())
}

/// Emit the body for a query with no list parameters.
fn emit_rust_scalar_query(src: &mut String, query: &Query, row_type: &str, contract: &RustCoreContract) -> anyhow::Result<()> {
    let raw_sql = normalize_sql_for_sqlx(&query.sql, contract.sql_norm_mode);
    let raw_sql = raw_sql.trim_end().trim_end_matches(';');
    emit_rust_sql_let(src, raw_sql)?;
    // Postgres $N is reference-by-number — one .bind() per unique param suffices.
    // SQLite/MySQL normalize to ? (positional sequential) — bind once per occurrence.
    let bind_names: Vec<&str> = match contract.bind_mode {
        RustBindMode::UniqueParams => query.params.iter().map(|p| p.name.as_str()).collect(),
        RustBindMode::Positional => positional_bind_names(query),
    };
    emit_rust_sqlx_call(src, query, "sql", &bind_names, row_type)
}

/// Emit the body for a query that contains a list parameter.
fn emit_rust_list_query(
    src: &mut String,
    query: &Query,
    row_type: &str,
    contract: &RustCoreContract,
    strategy: &ListParamStrategy,
    list_param: &Parameter,
) -> anyhow::Result<()> {
    // Use the pre-computed native SQL from the IR when available and strategy is Native.
    // The native SQL was produced by the frontend and uses $N placeholders; each backend
    // applies its own standard placeholder rewriting (a general rule, not dialect logic).
    if *strategy == ListParamStrategy::Native {
        if let Some(native_sql) = &list_param.native_list_sql {
            return emit_rust_native_list_query(src, query, row_type, list_param, native_sql, contract);
        }
    }
    // Dynamic expansion: language-specific, not dialect-specific.
    let lp_name = to_snake_case(&list_param.name);
    match contract.placeholder_mode {
        RustPlaceholderMode::NumberedDollar => {
            let base = list_param.index;
            writeln!(src, "    let placeholders: String = ({lp_name}).iter().enumerate()")?;
            writeln!(src, "        .map(|(i, _)| format!(\"${{}}\", {base} + i))")?;
            writeln!(src, "        .collect::<Vec<_>>().join(\", \");")?;
        },
        RustPlaceholderMode::QuestionMark => {
            writeln!(src, "    let placeholders = ({lp_name}).iter().map(|_| \"?\").collect::<Vec<_>>().join(\", \");")?;
        },
    }
    emit_rust_dynamic_query(src, query, row_type, list_param, contract)
}

/// Emit a native list query using the pre-computed `native_sql` from the IR.
///
/// The `native_sql` already contains the dialect-specific SQL (e.g. `json_each`,
/// `= ANY`, `JSON_TABLE`) with `$N` placeholders. This function applies the
/// backend's standard placeholder rewriting and emits the appropriate bind call.
fn emit_rust_native_list_query(
    src: &mut String,
    query: &Query,
    row_type: &str,
    list_param: &Parameter,
    native_sql: &str,
    contract: &RustCoreContract,
) -> anyhow::Result<()> {
    let raw_sql = normalize_sql_for_sqlx(native_sql, contract.sql_norm_mode);
    let raw_sql = raw_sql.trim_end().trim_end_matches(';');
    emit_rust_sql_let(src, raw_sql)?;
    let lp_name = to_snake_case(&list_param.name);
    match list_param.native_list_bind {
        Some(NativeListBind::Array) => {
            // sqlx-postgres: bind the list directly (sqlx handles Vec<T> → ANY($1))
            let bind_names: Vec<&str> = query.params.iter().map(|p| p.name.as_str()).collect();
            emit_rust_sqlx_call(src, query, "sql", &bind_names, row_type)
        },
        Some(NativeListBind::Json) | None => {
            // SQLite/MySQL: list param is bound as a JSON array string
            writeln!(src, "    let {lp_name}_json = {};", json_list_expr(&lp_name, &list_param.sql_type))?;
            let bind_names: Vec<String> = query.params.iter().map(|p| if p.is_list { format!("{lp_name}_json") } else { to_snake_case(&p.name) }).collect();
            let bind_refs: Vec<&str> = bind_names.iter().map(|s| s.as_str()).collect();
            emit_rust_sqlx_call(src, query, "sql", &bind_refs, row_type)
        },
    }
}

/// Emit the shared tail of a dynamic list query: build SQL, bind scalars, bind list.
fn emit_rust_dynamic_query(src: &mut String, query: &Query, row_type: &str, list_param: &Parameter, contract: &RustCoreContract) -> anyhow::Result<()> {
    let scalar_params: Vec<&Parameter> = query.params.iter().filter(|p| !p.is_list).collect();
    let lp_name = to_snake_case(&list_param.name);
    let (before, after) = split_at_in_clause(&query.sql, list_param.index).unwrap_or_else(|| (query.sql.clone(), String::new()));
    let before_esc = normalize_sql_for_sqlx(&before, contract.sql_norm_mode).replace('"', "\\\"").replace('\n', " ");
    let after_esc = normalize_sql_for_sqlx(&after, contract.sql_norm_mode).replace('"', "\\\"").replace('\n', " ");
    writeln!(src, "    let sql = format!(\"{before_esc}IN ({{placeholders}}){after_esc}\");")?;
    writeln!(src, "    let mut q = sqlx::query_as::<_, {row_type}>(&sql);")?;
    for sp in &scalar_params {
        writeln!(src, "    q = q.bind({});", to_snake_case(&sp.name))?;
    }
    writeln!(src, "    for v in {lp_name} {{")?;
    writeln!(src, "        q = q.bind(v);")?;
    writeln!(src, "    }}")?;
    writeln!(src, "    q.{}.await", fetch_method(query))?;
    Ok(())
}

/// Generate inline Rust code that builds a JSON array string from a list parameter.
///
/// Produces an expression like `format!("[{}]", ids.iter().map(|x| ...).join(","))`.
/// Avoids any dependency on serde_json by formatting each element inline.
fn json_list_expr(lp_name: &str, sql_type: &SqlType) -> String {
    let elem = match sql_type {
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => r#"format!("\"{}\"", x.replace('\\', "\\\\").replace('"', "\\\""))"#.to_string(),
        SqlType::Uuid => r#"format!("\"{}\"", x)"#.to_string(),
        _ => "x.to_string()".to_string(),
    };
    format!(r#"format!("[{{}}]", {lp_name}.iter().map(|x| {elem}).collect::<Vec<_>>().join(","))"#)
}

fn fetch_method(query: &Query) -> &'static str {
    match query.cmd {
        QueryCmd::One => "fetch_optional(pool)",
        QueryCmd::Many => "fetch_all(pool)",
        QueryCmd::Exec => "execute(pool).map(|_| ())",
        QueryCmd::ExecRows => "execute(pool).map(|r| r.rows_affected())",
    }
}

/// Build the `name: type` signature fragment for a single parameter.
fn rust_param_sig(p: &Parameter, type_map: &RustTypeMap) -> String {
    let name = to_snake_case(&p.name);
    if p.is_list {
        let elem_ty = type_map.param_type(&p.sql_type, false);
        format!("{name}: &[{elem_ty}]")
    } else {
        format!("{name}: {}", type_map.param_type(&p.sql_type, p.nullable))
    }
}

/// Emit the sqlx query/query_as call given a Rust expression that evaluates to the SQL.
///
/// `sql_expr` is the Rust expression passed to `sqlx::query`/`sqlx::query_as` —
/// typically the local variable `"sql"` (defined just before by the caller).
/// When the same bind name appears multiple times (positional dialects like MySQL/SQLite),
/// all occurrences except the last are emitted with `.clone()`.
fn emit_rust_sqlx_call(src: &mut String, query: &Query, sql_expr: &str, bind_names: &[&str], row_type: &str) -> anyhow::Result<()> {
    // Count remaining occurrences so we know when to clone vs. move.
    let mut remaining: HashMap<&str, usize> = HashMap::new();
    for &name in bind_names {
        *remaining.entry(name).or_insert(0) += 1;
    }

    let bind_expr = |name: &str, remaining: &mut HashMap<&str, usize>| -> String {
        let snake = to_snake_case(name);
        let count = remaining.get_mut(name).unwrap();
        *count -= 1;
        if *count > 0 {
            format!("{snake}.clone()")
        } else {
            snake
        }
    };

    match query.cmd {
        QueryCmd::Exec | QueryCmd::ExecRows => {
            writeln!(src, "    sqlx::query({sql_expr})")?;
            for &name in bind_names {
                writeln!(src, "        .bind({})", bind_expr(name, &mut remaining))?;
            }
            writeln!(src, "        .execute(pool)")?;
            writeln!(src, "        .await")?;
            if matches!(query.cmd, QueryCmd::Exec) {
                writeln!(src, "        .map(|_| ())")?;
            } else {
                writeln!(src, "        .map(|r| r.rows_affected())")?;
            }
        },
        QueryCmd::One | QueryCmd::Many => {
            writeln!(src, "    sqlx::query_as::<_, {row_type}>({sql_expr})")?;
            for &name in bind_names {
                writeln!(src, "        .bind({})", bind_expr(name, &mut remaining))?;
            }
            if matches!(query.cmd, QueryCmd::One) {
                writeln!(src, "        .fetch_optional(pool)")?;
            } else {
                writeln!(src, "        .fetch_all(pool)")?;
            }
            writeln!(src, "        .await")?;
        },
    }
    Ok(())
}

fn result_row_type(query: &Query, schema: &Schema) -> String {
    infer_row_type_name(query, schema).unwrap_or_else(|| "serde_json::Value".to_string())
}

// ─── SQL helpers ──────────────────────────────────────────────────────────────

/// Normalize SQL placeholders for the target sqlx driver:
/// - SQLite `?N`/`$N` → `?` (sqlx sqlite uses anonymous `?`)
/// - MySQL `$N`/`?N` → `?` (sqlx mysql uses anonymous `?`)
/// - PostgreSQL `$N` → unchanged (sqlx postgres uses `$N`)
fn normalize_sql_for_sqlx(sql: &str, mode: RustSqlNormMode) -> String {
    match mode {
        RustSqlNormMode::AnonParams => rewrite_to_anon_params(sql),
        RustSqlNormMode::Preserve => sql.to_string(),
    }
}

/// Generate the Rust source for a single enum type.
///
/// Emits a `pub enum` with `Display` and `FromStr` implementations so that
/// enum values round-trip through their SQL string representation.
fn emit_rust_enum(src: &mut String, e: &EnumType) -> anyhow::Result<()> {
    let name = to_pascal_case(&e.name);

    writeln!(src, "use std::fmt;")?;
    writeln!(src, "use std::str::FromStr;")?;
    writeln!(src)?;
    writeln!(src, "#[derive(Debug, Clone, PartialEq, Eq)]")?;
    writeln!(src, "pub enum {name} {{")?;
    for variant in &e.variants {
        writeln!(src, "    {},", to_pascal_case(variant))?;
    }
    writeln!(src, "}}")?;
    writeln!(src)?;

    // Display impl — returns the original SQL label
    writeln!(src, "impl fmt::Display for {name} {{")?;
    writeln!(src, "    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {{")?;
    writeln!(src, "        f.write_str(match self {{")?;
    for variant in &e.variants {
        writeln!(src, "            Self::{} => \"{}\",", to_pascal_case(variant), variant)?;
    }
    writeln!(src, "        }})")?;
    writeln!(src, "    }}")?;
    writeln!(src, "}}")?;
    writeln!(src)?;

    // FromStr impl — parses from the SQL label
    writeln!(src, "impl FromStr for {name} {{")?;
    writeln!(src, "    type Err = String;")?;
    writeln!(src)?;
    writeln!(src, "    fn from_str(s: &str) -> Result<Self, Self::Err> {{")?;
    writeln!(src, "        match s {{")?;
    for variant in &e.variants {
        writeln!(src, "            \"{}\" => Ok(Self::{}),", variant, to_pascal_case(variant))?;
    }
    writeln!(src, "            _ => Err(format!(\"unknown {name}: {{}}\", s)),")?;
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    writeln!(src, "}}")?;

    Ok(())
}
