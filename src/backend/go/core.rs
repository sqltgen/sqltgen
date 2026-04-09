use std::collections::BTreeSet;
use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::common::{
    group_queries, has_inline_rows, infer_row_type_name, infer_table, querier_class_name, queries_file_stem, row_type_name, sql_const_name,
};
use crate::backend::naming::to_pascal_case;
use crate::backend::sql_rewrite::{parse_placeholder_indices, positional_bind_names, rewrite_to_anon_params, split_at_in_clause};
use crate::backend::GeneratedFile;
use crate::config::{ListParamStrategy, OutputConfig};
use crate::ir::{EnumType, NativeListBind, Parameter, Query, QueryCmd, ResultColumn, Schema, SqlType};

use super::adapter::{GoBindMode, GoCoreContract, GoPlaceholderMode};
use super::typemap::GoTypeMap;

// ─── Package name ─────────────────────────────────────────────────────────────

/// Derive the Go package name from the config.
///
/// Uses `config.package` if non-empty; otherwise falls back to the last
/// path segment of `config.out` (e.g. `"src/db"` → `"db"`).
pub(super) fn package_name(config: &OutputConfig) -> String {
    if !config.package.is_empty() {
        return config.package.clone();
    }
    PathBuf::from(&config.out).file_name().and_then(|n| n.to_str()).unwrap_or("db").to_string()
}

// ─── Import tracking ──────────────────────────────────────────────────────────

#[derive(Default)]
struct GoImports {
    context: bool,
    database_sql: bool,
    encoding_json: bool,
    fmt: bool,
    strings: bool,
    time: bool,
    pq: bool,
    extra: BTreeSet<String>,
}

impl GoImports {
    /// Add a type-derived import path (as returned by `GoTypeMap::import_for`).
    fn add_import(&mut self, imp: Option<String>) {
        match imp.as_deref() {
            Some("\"time\"") => self.time = true,
            Some("\"database/sql\"") => self.database_sql = true,
            Some(s) => {
                self.extra.insert(s.to_string());
            },
            None => {},
        }
    }

    /// Return true if any import is needed.
    fn has_any(&self) -> bool {
        self.context || self.database_sql || self.encoding_json || self.fmt || self.strings || self.time || self.pq || !self.extra.is_empty()
    }

    fn write(&self, src: &mut String) {
        let mut imports: Vec<&str> = Vec::new();
        if self.context {
            imports.push("\"context\"");
        }
        if self.database_sql {
            imports.push("\"database/sql\"");
        }
        if self.encoding_json {
            imports.push("\"encoding/json\"");
        }
        if self.fmt {
            imports.push("\"fmt\"");
        }
        if self.strings {
            imports.push("\"strings\"");
        }
        if self.time {
            imports.push("\"time\"");
        }

        let std_imports: Vec<&str> = imports.clone();
        let has_pq = self.pq;
        let extra: Vec<&str> = self.extra.iter().map(|s| s.as_str()).collect();

        if std_imports.is_empty() && !has_pq && extra.is_empty() {
            return;
        }

        src.push_str("import (\n");
        for imp in &std_imports {
            src.push_str(&format!("\t{imp}\n"));
        }
        if has_pq || !extra.is_empty() {
            if !std_imports.is_empty() {
                src.push('\n');
            }
            if has_pq {
                src.push_str("\t\"github.com/lib/pq\"\n");
            }
            for imp in &extra {
                src.push_str(&format!("\t{imp}\n"));
            }
        }
        src.push_str(")\n");
    }
}

// ─── Field name ───────────────────────────────────────────────────────────────

/// Convert a snake_case column name to an exported Go field name (PascalCase).
fn field_name(col_name: &str) -> String {
    to_pascal_case(col_name)
}

// ─── SQL normalization ────────────────────────────────────────────────────────

/// Rewrite numbered placeholders for a Go target.
///
/// PostgreSQL keeps `$N`; SQLite and MySQL rewrite to `?`.
fn normalize_sql(sql: &str, contract: &GoCoreContract) -> String {
    match contract.placeholder_mode {
        GoPlaceholderMode::NumberedDollar => sql.to_string(),
        GoPlaceholderMode::QuestionMark => rewrite_to_anon_params(sql),
    }
}

// ─── Top-level file generators ────────────────────────────────────────────────

/// Generate all Go source files for the given schema and queries.
pub(super) fn generate_core_files(
    schema: &Schema,
    queries: &[Query],
    contract: &GoCoreContract,
    config: &OutputConfig,
    type_map: &GoTypeMap,
) -> anyhow::Result<Vec<GeneratedFile>> {
    let pkg = package_name(config);
    let mut files = Vec::new();

    // All table structs and enum types in one models.go file
    if !schema.tables.is_empty() || !schema.enums.is_empty() {
        files.push(emit_models_file(schema, config, type_map, &pkg)?);
    }

    // One queries file per group
    let groups = group_queries(queries);
    for (group, group_queries) in &groups {
        let stem = queries_file_stem(group);
        let filename = if group.is_empty() { "queries.go".to_string() } else { format!("queries_{stem}.go") };
        let content = build_queries_file(group, group_queries, schema, contract, config, type_map, &pkg)?;
        let path = PathBuf::from(&config.out).join(filename);
        files.push(GeneratedFile { path, content });
    }

    // Package-level mod.go
    files.push(emit_mod_file(&pkg, config));

    Ok(files)
}

/// Emit `mod.go` with just the package declaration and generated-code header.
fn emit_mod_file(pkg: &str, config: &OutputConfig) -> GeneratedFile {
    let content = format!("// Code generated by sqltgen. Do not edit.\npackage {pkg}\n");
    GeneratedFile { path: PathBuf::from(&config.out).join("mod.go"), content }
}

/// Emit `models.go` containing all table structs.
fn emit_models_file(schema: &Schema, config: &OutputConfig, type_map: &GoTypeMap, pkg: &str) -> anyhow::Result<GeneratedFile> {
    let mut src = String::new();
    writeln!(src, "// Code generated by sqltgen. Do not edit.")?;
    writeln!(src, "package {pkg}")?;
    writeln!(src)?;

    let mut imports = GoImports::default();
    for table in &schema.tables {
        for col in &table.columns {
            imports.add_import(type_map.import_for(&col.sql_type, col.nullable));
        }
    }
    if imports.has_any() {
        imports.write(&mut src);
        writeln!(src)?;
    }

    for table in &schema.tables {
        let struct_name = to_pascal_case(&table.name);
        writeln!(src, "// {struct_name} represents a row from the {table_name} table.", table_name = table.name)?;
        writeln!(src, "type {struct_name} struct {{")?;
        for col in &table.columns {
            let go_ty = type_map.field_type(&col.sql_type, col.nullable);
            writeln!(src, "\t{}\t{}", field_name(&col.name), go_ty)?;
        }
        writeln!(src, "}}")?;
        writeln!(src)?;
    }

    for e in &schema.enums {
        emit_go_enum(&mut src, e)?;
        writeln!(src)?;
    }

    let path = PathBuf::from(&config.out).join("models.go");
    Ok(GeneratedFile { path, content: src })
}

// ─── Enum types ──────────────────────────────────────────────────────────────

/// Emit a Go enum type (string newtype + const block) for a SQL enum.
fn emit_go_enum(src: &mut String, e: &EnumType) -> anyhow::Result<()> {
    let type_name = to_pascal_case(&e.name);
    writeln!(src, "// {type_name} represents the SQL enum type `{raw_name}`.", raw_name = e.name)?;
    writeln!(src, "type {type_name} string")?;
    writeln!(src)?;
    writeln!(src, "const (")?;
    for v in &e.variants {
        let const_name = format!("{type_name}{}", to_pascal_case(v));
        writeln!(src, "\t{const_name}\t{type_name} = \"{v}\"")?;
    }
    writeln!(src, ")")?;
    Ok(())
}

// ─── Queries file ─────────────────────────────────────────────────────────────

/// Build the full content of a queries `.go` file for one query group.
pub(super) fn build_queries_file(
    group: &str,
    queries: &[Query],
    schema: &Schema,
    contract: &GoCoreContract,
    config: &OutputConfig,
    type_map: &GoTypeMap,
    pkg: &str,
) -> anyhow::Result<String> {
    let strategy = config.list_params.clone().unwrap_or_default();
    let mut src = String::new();

    writeln!(src, "// Code generated by sqltgen. Do not edit.")?;
    writeln!(src, "package {pkg}")?;
    writeln!(src)?;

    // Collect all needed imports across all queries
    let imports = collect_query_imports(queries, schema, contract, type_map, &strategy);
    imports.write(&mut src);
    writeln!(src)?;

    // SQL constants (skip dynamic-list queries — SQL is built at runtime)
    for query in queries {
        if query.params.iter().any(|p| p.is_list) && strategy == ListParamStrategy::Dynamic {
            continue;
        }
        let base_sql = if let Some(lp) = query.params.iter().find(|p| p.is_list) {
            lp.native_list_sql.clone().unwrap_or_else(|| query.sql.clone())
        } else {
            query.sql.clone()
        };
        let sql = normalize_sql(&base_sql, contract);
        let sql = sql.trim_end().trim_end_matches(';');
        // Escape backticks — Go raw string literals cannot contain backticks
        let sql = sql.replace('`', "` + \"`\" + `");
        let const_name = sql_const_name(&query.name);
        writeln!(src, "const {const_name} = `")?;
        for line in sql.lines() {
            writeln!(src, "{line}")?;
        }
        writeln!(src, "`")?;
    }

    // Inline row struct types for non-table results
    for query in queries {
        if has_inline_rows(query, schema) {
            writeln!(src)?;
            emit_row_struct(&mut src, query, type_map)?;
        }
    }

    // Table imports: collect which table types are reused
    let needed_tables: Vec<&str> = {
        let mut tables: Vec<&str> = queries.iter().filter_map(|q| infer_table(q, schema)).collect();
        tables.sort_unstable();
        tables.dedup();
        tables
    };
    // (In Go, table types live in the same package, so no import needed.)
    let _ = needed_tables;

    // Query functions
    for query in queries {
        writeln!(src)?;
        emit_query_func(&mut src, query, schema, contract, type_map, &strategy)?;
    }

    // Querier struct
    if !queries.is_empty() {
        writeln!(src)?;
        emit_querier(&mut src, group, queries, schema, contract, type_map)?;
    }

    Ok(src)
}

/// Collect all imports needed for the queries file.
fn collect_query_imports(queries: &[Query], _schema: &Schema, contract: &GoCoreContract, type_map: &GoTypeMap, strategy: &ListParamStrategy) -> GoImports {
    let mut imp = GoImports { context: true, database_sql: true, ..Default::default() };

    for query in queries {
        for p in &query.params {
            if p.is_list {
                match strategy {
                    ListParamStrategy::Dynamic => {
                        imp.fmt = contract.placeholder_mode == GoPlaceholderMode::NumberedDollar;
                        imp.strings = true;
                    },
                    ListParamStrategy::Native => match &p.native_list_bind {
                        Some(NativeListBind::Json) | None => imp.encoding_json = true,
                        Some(NativeListBind::Array) => imp.pq = true,
                    },
                }
            }
            if matches!(&p.sql_type, SqlType::Array(_)) && contract.wrap_array_params {
                imp.pq = true;
            }
        }

        for col in &query.result_columns {
            imp.add_import(type_map.import_for(&col.sql_type, col.nullable));
        }

        for p in &query.params {
            imp.add_import(type_map.import_for(&p.sql_type, p.nullable));
        }
    }

    imp
}

// ─── Inline row struct ────────────────────────────────────────────────────────

/// Emit a `type {Query}Row struct { ... }` for queries with custom result shapes.
fn emit_row_struct(src: &mut String, query: &Query, type_map: &GoTypeMap) -> anyhow::Result<()> {
    let name = row_type_name(&query.name);
    writeln!(src, "// {name} is the result row type for {query_name}.", query_name = query.name)?;
    writeln!(src, "type {name} struct {{")?;
    for col in &query.result_columns {
        let go_ty = type_map.field_type(&col.sql_type, col.nullable);
        writeln!(src, "\t{}\t{}", field_name(&col.name), go_ty)?;
    }
    writeln!(src, "}}")?;
    Ok(())
}

// ─── Query function ───────────────────────────────────────────────────────────

/// Emit the complete Go function for one query.
fn emit_query_func(
    src: &mut String,
    query: &Query,
    schema: &Schema,
    contract: &GoCoreContract,
    type_map: &GoTypeMap,
    strategy: &ListParamStrategy,
) -> anyhow::Result<()> {
    if let Some(lp) = query.params.iter().find(|p| p.is_list) {
        return emit_list_query_func(src, query, schema, contract, type_map, strategy, lp);
    }
    emit_standard_query_func(src, query, schema, contract, type_map)
}

/// Return the Go return type for a query command.
fn query_return_type(query: &Query, schema: &Schema) -> String {
    match query.cmd {
        QueryCmd::One => {
            let row_type = result_row_type(query, schema);
            format!("(*{row_type}, error)")
        },
        QueryCmd::Many => {
            let row_type = result_row_type(query, schema);
            format!("([]{row_type}, error)")
        },
        QueryCmd::Exec => "error".to_string(),
        QueryCmd::ExecRows => "(int64, error)".to_string(),
    }
}

/// The Go type name for the result row (table name or `{Query}Row`).
fn result_row_type(query: &Query, schema: &Schema) -> String {
    infer_row_type_name(query, schema).unwrap_or_else(|| "any".to_string())
}

/// Build the parameter list for a query function signature.
fn params_sig(query: &Query, type_map: &GoTypeMap) -> String {
    let mut parts: Vec<String> = vec!["ctx context.Context".to_string(), "db *sql.DB".to_string()];
    for p in &query.params {
        let ty = if p.is_list { format!("[]{}", type_map.param_type(&p.sql_type, false)) } else { type_map.param_type(&p.sql_type, p.nullable) };
        parts.push(format!("{} {ty}", p.name));
    }
    parts.join(", ")
}

/// Emit a standard (non-list) query function.
fn emit_standard_query_func(src: &mut String, query: &Query, schema: &Schema, contract: &GoCoreContract, type_map: &GoTypeMap) -> anyhow::Result<()> {
    let fn_name = to_pascal_case(&query.name);
    let ret = query_return_type(query, schema);
    let sig = params_sig(query, type_map);
    let const_name = sql_const_name(&query.name);

    writeln!(src, "// {fn_name} executes the {name} query.", name = query.name)?;
    writeln!(src, "func {fn_name}({sig}) {ret} {{")?;

    let args = build_bind_args(query, contract);

    match query.cmd {
        QueryCmd::Exec => {
            if args.is_empty() {
                writeln!(src, "\t_, err := db.ExecContext(ctx, {const_name})")?;
            } else {
                writeln!(src, "\t_, err := db.ExecContext(ctx, {const_name}, {args})")?;
            }
            writeln!(src, "\treturn err")?;
        },
        QueryCmd::ExecRows => {
            if args.is_empty() {
                writeln!(src, "\treturn execRows(ctx, db, {const_name})")?;
            } else {
                writeln!(src, "\treturn execRows(ctx, db, {const_name}, {args})")?;
            }
        },
        QueryCmd::One => {
            if args.is_empty() {
                writeln!(src, "\trow := db.QueryRowContext(ctx, {const_name})")?;
            } else {
                writeln!(src, "\trow := db.QueryRowContext(ctx, {const_name}, {args})")?;
            }
            emit_scan_one(src, query, schema, contract, type_map)?;
        },
        QueryCmd::Many => {
            if args.is_empty() {
                writeln!(src, "\trows, err := db.QueryContext(ctx, {const_name})")?;
            } else {
                writeln!(src, "\trows, err := db.QueryContext(ctx, {const_name}, {args})")?;
            }
            writeln!(src, "\tif err != nil {{")?;
            writeln!(src, "\t\treturn nil, err")?;
            writeln!(src, "\t}}")?;
            writeln!(src, "\tdefer rows.Close()")?;
            emit_scan_many(src, query, schema, contract, type_map)?;
        },
    }

    writeln!(src, "}}")?;
    Ok(())
}

/// Build the bind argument list for a standard (non-list) query.
fn build_bind_args(query: &Query, contract: &GoCoreContract) -> String {
    if query.params.is_empty() {
        return String::new();
    }
    let raw_names: Vec<&str> = match contract.bind_mode {
        GoBindMode::UniqueParams => query.params.iter().map(|p| p.name.as_str()).collect(),
        GoBindMode::Positional => positional_bind_names(query),
    };
    if !contract.wrap_array_params {
        return raw_names.join(", ");
    }
    let wrapped: Vec<String> = raw_names
        .iter()
        .zip(query.params.iter())
        .map(|(name, param)| if matches!(&param.sql_type, SqlType::Array(_)) { format!("pq.Array({name})") } else { name.to_string() })
        .collect();
    wrapped.join(", ")
}

/// Emit the Scan + return block for a `:one` query.
fn emit_scan_one(src: &mut String, query: &Query, schema: &Schema, _contract: &GoCoreContract, type_map: &GoTypeMap) -> anyhow::Result<()> {
    let row_type = result_row_type(query, schema);
    let plan = scan_plan(&query.result_columns, type_map);
    writeln!(src, "\tvar r {row_type}")?;
    for line in &plan.pre_lines {
        writeln!(src, "\t{line}")?;
    }
    writeln!(src, "\terr := row.Scan({})", plan.scan_args.join(", "))?;
    writeln!(src, "\tif err == sql.ErrNoRows {{")?;
    writeln!(src, "\t\treturn nil, nil")?;
    writeln!(src, "\t}}")?;
    writeln!(src, "\tif err != nil {{")?;
    writeln!(src, "\t\treturn nil, err")?;
    writeln!(src, "\t}}")?;
    for line in &plan.post_lines {
        writeln!(src, "\t{line}")?;
    }
    writeln!(src, "\treturn &r, nil")?;
    Ok(())
}

/// Emit the rows.Next() loop for a `:many` query.
fn emit_scan_many(src: &mut String, query: &Query, schema: &Schema, _contract: &GoCoreContract, type_map: &GoTypeMap) -> anyhow::Result<()> {
    let row_type = result_row_type(query, schema);
    let plan = scan_plan(&query.result_columns, type_map);
    writeln!(src, "\tvar results []{row_type}")?;
    writeln!(src, "\tfor rows.Next() {{")?;
    writeln!(src, "\t\tvar r {row_type}")?;
    for line in &plan.pre_lines {
        writeln!(src, "\t\t{line}")?;
    }
    writeln!(src, "\t\tif err := rows.Scan({}); err != nil {{", plan.scan_args.join(", "))?;
    writeln!(src, "\t\t\treturn nil, err")?;
    writeln!(src, "\t\t}}")?;
    for line in &plan.post_lines {
        writeln!(src, "\t\t{line}")?;
    }
    writeln!(src, "\t\tresults = append(results, r)")?;
    writeln!(src, "\t}}")?;
    writeln!(src, "\tif err := rows.Err(); err != nil {{")?;
    writeln!(src, "\t\treturn nil, err")?;
    writeln!(src, "\t}}")?;
    writeln!(src, "\treturn results, nil")?;
    Ok(())
}

struct ScanPlan {
    pre_lines: Vec<String>,
    scan_args: Vec<String>,
    post_lines: Vec<String>,
}

fn scan_plan(cols: &[ResultColumn], type_map: &GoTypeMap) -> ScanPlan {
    let mut pre_lines = Vec::new();
    let mut scan_args = Vec::new();
    let mut post_lines = Vec::new();

    for (i, col) in cols.iter().enumerate() {
        let field = field_name(&col.name);
        match &col.sql_type {
            SqlType::Array(inner) if matches!(inner.as_ref(), SqlType::Enum(_)) => {
                // Enum arrays: pq.Array can't scan into []EnumType directly;
                // scan into []string then convert to the enum slice.
                let tmp = format!("_arr{}", i + 1);
                let enum_ty = type_map.field_type(inner, false);
                pre_lines.push(format!("var {tmp} []string"));
                scan_args.push(format!("pq.Array(&{tmp})"));
                if col.nullable {
                    post_lines.push(format!("if {tmp} != nil {{"));
                    post_lines.push(format!("\t_conv{i} := make([]{enum_ty}, len({tmp}))"));
                    post_lines.push(format!("\tfor _j, _s := range {tmp} {{ _conv{i}[_j] = {enum_ty}(_s) }}"));
                    post_lines.push(format!("\tr.{field} = &_conv{i}"));
                    post_lines.push("}".to_string());
                } else {
                    post_lines.push(format!("r.{field} = make([]{enum_ty}, len({tmp}))"));
                    post_lines.push(format!("for _j, _s := range {tmp} {{ r.{field}[_j] = {enum_ty}(_s) }}"));
                }
            },
            SqlType::Array(inner) => {
                if col.nullable {
                    let inner_ty = type_map.field_type(inner, false);
                    let tmp = format!("arr{}", i + 1);
                    pre_lines.push(format!("var {tmp} []{inner_ty}"));
                    scan_args.push(format!("scanArray(&{tmp})"));
                    post_lines.push(format!("if {tmp} != nil {{"));
                    post_lines.push(format!("\tr.{field} = &{tmp}"));
                    post_lines.push("}".to_string());
                } else {
                    scan_args.push(format!("scanArray(&r.{field})"));
                }
            },
            _ => scan_args.push(format!("&r.{field}")),
        }
    }

    ScanPlan { pre_lines, scan_args, post_lines }
}

// ─── List query function ──────────────────────────────────────────────────────

/// Emit a query function that has a list parameter.
fn emit_list_query_func(
    src: &mut String,
    query: &Query,
    schema: &Schema,
    contract: &GoCoreContract,
    type_map: &GoTypeMap,
    strategy: &ListParamStrategy,
    lp: &Parameter,
) -> anyhow::Result<()> {
    let fn_name = to_pascal_case(&query.name);
    let ret = query_return_type(query, schema);
    let sig = params_sig(query, type_map);
    let const_name = sql_const_name(&query.name);

    writeln!(src, "// {fn_name} executes the {name} query.", name = query.name)?;
    writeln!(src, "func {fn_name}({sig}) {ret} {{")?;

    match strategy {
        ListParamStrategy::Native => emit_native_list_body(src, query, schema, contract, type_map, &const_name, lp)?,
        ListParamStrategy::Dynamic => emit_dynamic_list_body(src, query, schema, contract, type_map, lp)?,
    }

    writeln!(src, "}}")?;
    Ok(())
}

/// Emit the body for a native list param query.
fn emit_native_list_body(
    src: &mut String,
    query: &Query,
    schema: &Schema,
    contract: &GoCoreContract,
    type_map: &GoTypeMap,
    const_name: &str,
    lp: &Parameter,
) -> anyhow::Result<()> {
    let scalar_params: Vec<&Parameter> = query.params.iter().filter(|p| !p.is_list).collect();

    match &lp.native_list_bind {
        Some(NativeListBind::Array) => {
            // PostgreSQL: use pq.Array
            let args = build_native_pg_args(&scalar_params, lp);
            emit_list_exec(src, query, schema, contract, type_map, const_name, &args)
        },
        Some(NativeListBind::Json) | None => {
            // SQLite / MySQL: JSON-encode the list
            writeln!(src, "\t{lp_name}JSON, err := json.Marshal({lp_name})", lp_name = lp.name)?;
            writeln!(src, "\tif err != nil {{")?;
            writeln!(src, "\t\treturn {}", error_zero_return(query))?;
            writeln!(src, "\t}}")?;
            let json_expr = format!("{}JSON", lp.name);
            let args = build_native_json_args(&scalar_params, lp, &json_expr);
            emit_list_exec(src, query, schema, contract, type_map, const_name, &args)
        },
    }
}

/// Error zero return appropriate for the query command.
fn error_zero_return(query: &Query) -> &'static str {
    match query.cmd {
        QueryCmd::Exec => "err",
        QueryCmd::ExecRows => "0, err",
        QueryCmd::One => "nil, err",
        QueryCmd::Many => "nil, err",
    }
}

fn build_native_pg_args(scalar_params: &[&Parameter], lp: &Parameter) -> String {
    let mut args: Vec<String> = Vec::new();
    let before: Vec<String> = scalar_params.iter().filter(|p| p.index < lp.index).map(|p| p.name.clone()).collect();
    let after: Vec<String> = scalar_params.iter().filter(|p| p.index > lp.index).map(|p| p.name.clone()).collect();
    args.extend(before);
    args.push(format!("pq.Array({})", lp.name));
    args.extend(after);
    args.join(", ")
}

fn build_native_json_args(scalar_params: &[&Parameter], lp: &Parameter, json_expr: &str) -> String {
    let mut args: Vec<String> = Vec::new();
    let before: Vec<String> = scalar_params.iter().filter(|p| p.index < lp.index).map(|p| p.name.clone()).collect();
    let after: Vec<String> = scalar_params.iter().filter(|p| p.index > lp.index).map(|p| p.name.clone()).collect();
    args.extend(before);
    args.push(json_expr.to_string());
    args.extend(after);
    args.join(", ")
}

/// Emit the body for a dynamic list param query (builds IN (?,?,…) at runtime).
fn emit_dynamic_list_body(
    src: &mut String,
    query: &Query,
    schema: &Schema,
    contract: &GoCoreContract,
    type_map: &GoTypeMap,
    lp: &Parameter,
) -> anyhow::Result<()> {
    let (before_raw, after_raw) = split_at_in_clause(&query.sql, lp.index).unwrap_or_else(|| (query.sql.clone(), String::new()));
    let before_sql = normalize_sql(&before_raw, contract).replace('`', "` + \"`\" + `");
    let after_sql = normalize_sql(&after_raw, contract).replace('`', "` + \"`\" + `");
    let scalar_params: Vec<&Parameter> = query.params.iter().filter(|p| !p.is_list).collect();

    // Count scalar params before the list param to compute startIdx for $N style
    let scalar_before_count = scalar_params.iter().filter(|p| p.index < lp.index).count();

    match contract.placeholder_mode {
        GoPlaceholderMode::NumberedDollar => {
            // We need to compute the start index for the dynamic placeholders.
            // Scalar params before the list already consumed (scalar_before_count) slots.
            writeln!(src, "\tplaceholders := make([]string, len({lp_name}))", lp_name = lp.name)?;
            writeln!(src, "\tfor i := range {lp_name} {{", lp_name = lp.name)?;
            writeln!(src, "\t\tplaceholders[i] = fmt.Sprintf(\"${{}}\", {start}+i)", start = scalar_before_count + 1)?;
            writeln!(src, "\t}}")?;
            writeln!(src, "\tsql := `{before_sql}` + \"IN (\" + strings.Join(placeholders, \", \") + \")\" + `{after_sql}`")?;
        },
        GoPlaceholderMode::QuestionMark => {
            writeln!(src, "\tplaceholders := strings.Repeat(\"?, \", len({lp_name}))", lp_name = lp.name)?;
            writeln!(src, "\tif len(placeholders) > 0 {{")?;
            writeln!(src, "\t\tplaceholders = placeholders[:len(placeholders)-2]")?;
            writeln!(src, "\t}}")?;
            writeln!(src, "\tsql := `{before_sql}IN (\" + placeholders + \"){after_sql}`")?;
        },
    }

    // Build the args slice: scalars before + list elements + scalars after
    let before_scalar_names: Vec<String> = scalar_params.iter().filter(|p| p.index < lp.index).map(|p| p.name.clone()).collect();
    let after_scalar_names: Vec<String> = scalar_params.iter().filter(|p| p.index > lp.index).map(|p| p.name.clone()).collect();

    writeln!(src, "\targs := make([]any, 0, {capacity}+len({lp_name}))", capacity = scalar_params.len(), lp_name = lp.name)?;
    for name in &before_scalar_names {
        writeln!(src, "\targs = append(args, {name})")?;
    }
    writeln!(src, "\tfor _, v := range {lp_name} {{", lp_name = lp.name)?;
    writeln!(src, "\t\targs = append(args, v)")?;
    writeln!(src, "\t}}")?;
    for name in &after_scalar_names {
        writeln!(src, "\targs = append(args, {name})")?;
    }

    // Exec with the dynamic SQL
    let query_sql_var = "sql";
    match query.cmd {
        QueryCmd::Exec => {
            writeln!(src, "\t_, err := db.ExecContext(ctx, {query_sql_var}, args...)")?;
            writeln!(src, "\treturn err")?;
        },
        QueryCmd::ExecRows => {
            writeln!(src, "\treturn execRows(ctx, db, {query_sql_var}, args...)")?;
        },
        QueryCmd::One => {
            writeln!(src, "\trow := db.QueryRowContext(ctx, {query_sql_var}, args...)")?;
            emit_scan_one(src, query, schema, contract, type_map)?;
        },
        QueryCmd::Many => {
            writeln!(src, "\trows, err := db.QueryContext(ctx, {query_sql_var}, args...)")?;
            writeln!(src, "\tif err != nil {{")?;
            writeln!(src, "\t\treturn nil, err")?;
            writeln!(src, "\t}}")?;
            writeln!(src, "\tdefer rows.Close()")?;
            emit_scan_many(src, query, schema, contract, type_map)?;
        },
    }

    Ok(())
}

/// Emit the exec/query/scan block for a native list query using a pre-built args expression.
fn emit_list_exec(
    src: &mut String,
    query: &Query,
    schema: &Schema,
    contract: &GoCoreContract,
    type_map: &GoTypeMap,
    const_name: &str,
    args: &str,
) -> anyhow::Result<()> {
    let _ = contract;
    match query.cmd {
        QueryCmd::Exec => {
            writeln!(src, "\t_, err := db.ExecContext(ctx, {const_name}, {args})")?;
            writeln!(src, "\treturn err")?;
        },
        QueryCmd::ExecRows => {
            writeln!(src, "\treturn execRows(ctx, db, {const_name}, {args})")?;
        },
        QueryCmd::One => {
            writeln!(src, "\trow := db.QueryRowContext(ctx, {const_name}, {args})")?;
            emit_scan_one(src, query, schema, contract, type_map)?;
        },
        QueryCmd::Many => {
            writeln!(src, "\trows, err := db.QueryContext(ctx, {const_name}, {args})")?;
            writeln!(src, "\tif err != nil {{")?;
            writeln!(src, "\t\treturn nil, err")?;
            writeln!(src, "\t}}")?;
            writeln!(src, "\tdefer rows.Close()")?;
            emit_scan_many(src, query, schema, contract, type_map)?;
        },
    }
    Ok(())
}

// ─── Querier struct ───────────────────────────────────────────────────────────

/// Emit the `Querier` struct and its constructor + methods.
fn emit_querier(src: &mut String, group: &str, queries: &[Query], schema: &Schema, contract: &GoCoreContract, type_map: &GoTypeMap) -> anyhow::Result<()> {
    let struct_name = querier_class_name(group);
    writeln!(src, "// {struct_name} wraps a *sql.DB and exposes named query methods.")?;
    writeln!(src, "type {struct_name} struct {{")?;
    writeln!(src, "\tdb *sql.DB")?;
    writeln!(src, "}}")?;
    writeln!(src)?;
    writeln!(src, "// New{struct_name} returns a new {struct_name} backed by db.")?;
    writeln!(src, "func New{struct_name}(db *sql.DB) *{struct_name} {{")?;
    writeln!(src, "\treturn &{struct_name}{{db: db}}")?;
    writeln!(src, "}}")?;

    for query in queries {
        writeln!(src)?;
        emit_querier_method(src, &struct_name, query, schema, contract, type_map)?;
    }

    Ok(())
}

/// Emit a single method on the Querier struct that delegates to the top-level function.
fn emit_querier_method(
    src: &mut String,
    struct_name: &str,
    query: &Query,
    schema: &Schema,
    _contract: &GoCoreContract,
    type_map: &GoTypeMap,
) -> anyhow::Result<()> {
    let fn_name = to_pascal_case(&query.name);
    let ret = query_return_type(query, schema);

    // Build method params (no db, just ctx + query params)
    let mut parts: Vec<String> = vec!["ctx context.Context".to_string()];
    for p in &query.params {
        let ty = if p.is_list { format!("[]{}", type_map.param_type(&p.sql_type, false)) } else { type_map.param_type(&p.sql_type, p.nullable) };
        parts.push(format!("{} {ty}", p.name));
    }
    let method_sig = parts.join(", ");

    // Build call arguments (ctx + q.db + all param names)
    let call_args: Vec<String> =
        std::iter::once("ctx".to_string()).chain(std::iter::once("q.db".to_string())).chain(query.params.iter().map(|p| p.name.clone())).collect();
    let call_args_str = call_args.join(", ");

    writeln!(src, "// {fn_name} delegates to the package-level {fn_name} function.")?;
    writeln!(src, "func (q *{struct_name}) {fn_name}({method_sig}) {ret} {{")?;
    writeln!(src, "\treturn {fn_name}({call_args_str})")?;
    writeln!(src, "}}")?;
    Ok(())
}

// ─── Helpers needed by dynamic list emit ─────────────────────────────────────

/// Count how many `$N` / `?N` occurrences appear for params before the list param.
fn _scalar_placeholder_count_before(query: &Query, lp_index: usize) -> usize {
    let by_idx: std::collections::HashMap<usize, usize> = query.params.iter().map(|p| (p.index, p.index)).collect();
    parse_placeholder_indices(&query.sql).iter().filter(|&&i| by_idx.contains_key(&i) && i < lp_index).count()
}
