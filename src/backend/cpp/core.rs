use std::collections::BTreeSet;
use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::common::{group_queries, has_inline_rows, infer_row_type_name, infer_table, querier_class_name, queries_file_stem, sql_const_name};
use crate::backend::naming::{to_pascal_case, to_snake_case};
use crate::backend::sql_rewrite::rewrite_to_anon_params;
use crate::backend::GeneratedFile;
use crate::config::OutputConfig;
use crate::ir::{Query, QueryCmd, Schema, SqlType, Table};

use super::adapter::{CppCoreContract, CppParamStyle, CppQueryContext};

// ─── Type mappings ─────────────────────────────────────────────────────────────

/// Map a SQL type to its C++ representation. Wraps in `std::optional` when nullable.
pub(super) fn cpp_type(sql_type: &SqlType, nullable: bool) -> String {
    let base = match sql_type {
        SqlType::Boolean => "bool".to_string(),
        SqlType::SmallInt => "std::int16_t".to_string(),
        SqlType::Integer => "std::int32_t".to_string(),
        SqlType::BigInt => "std::int64_t".to_string(),
        SqlType::Real => "float".to_string(),
        SqlType::Double => "double".to_string(),
        SqlType::Decimal => "std::string".to_string(), // double loses precision
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => "std::string".to_string(),
        SqlType::Bytes => "std::vector<std::uint8_t>".to_string(),
        SqlType::Date | SqlType::Time | SqlType::Timestamp | SqlType::TimestampTz | SqlType::Interval => "std::string".to_string(),
        SqlType::Uuid => "std::string".to_string(),
        SqlType::Json | SqlType::Jsonb => "std::string".to_string(),
        SqlType::Array(inner) => format!("std::vector<{}>", cpp_type(inner, false)),
        SqlType::Enum(_) | SqlType::Custom(_) => "std::string".to_string(),
    };

    if nullable {
        format!("std::optional<{base}>")
    } else {
        base
    }
}

// Helpers for collecting `#include`s needed

#[derive(Default)]
struct CppIncludes {
    set: BTreeSet<&'static str>,
}

impl CppIncludes {
    /// Record the standard-library headers required by `sql_type`.
    fn scan(&mut self, sql_type: &SqlType, nullable: bool) {
        if nullable {
            self.set.insert("<optional>");
        }

        match sql_type {
            SqlType::Boolean | SqlType::Real | SqlType::Double => {},
            SqlType::SmallInt | SqlType::Integer | SqlType::BigInt => {
                self.set.insert("<cstdint>");
            },
            SqlType::Decimal
            | SqlType::Text
            | SqlType::Char(_)
            | SqlType::VarChar(_)
            | SqlType::Date
            | SqlType::Time
            | SqlType::Timestamp
            | SqlType::TimestampTz
            | SqlType::Interval
            | SqlType::Uuid
            | SqlType::Json
            | SqlType::Jsonb
            | SqlType::Enum(_)
            | SqlType::Custom(_) => {
                self.set.insert("<string>");
            },
            SqlType::Bytes => {
                self.set.insert("<cstdint>");
                self.set.insert("<vector>");
            },
            SqlType::Array(inner) => {
                self.set.insert("<vector>");
                self.scan(inner, false);
            },
        }
    }
}

/// Collect all standard-library includes needed by a table's column types.
fn scan_table_includes(table: &Table) -> CppIncludes {
    let mut includes = CppIncludes::default();
    for col in &table.columns {
        includes.scan(&col.sql_type, col.nullable);
    }
    includes
}

/// Collect all standard-library includes needed by a query file.
/// Function signatures, inline row structs, SQL string constants, and other types.
fn scan_query_includes(queries: &[Query], schema: &Schema) -> CppIncludes {
    let mut includes = CppIncludes::default();
    // Always need <string> for SQL string constants.
    includes.set.insert("<string>");
    // Always need <stdexcept> for std::runtime_error in query bodies.
    includes.set.insert("<stdexcept>");

    for query in queries {
        // Scan parameter types.
        for p in &query.params {
            if p.is_list {
                includes.set.insert("<vector>");
                includes.scan(&p.sql_type, false);
            } else {
                includes.scan(&p.sql_type, p.nullable);
            }
        }
        // Scan return type.
        match query.cmd {
            QueryCmd::Exec => {},
            QueryCmd::ExecRows => {
                includes.set.insert("<cstdint>");
            },
            QueryCmd::One => {
                includes.set.insert("<optional>");
                // Scan result column types for inline row structs.
                if has_inline_rows(query, schema) {
                    for col in &query.result_columns {
                        includes.scan(&col.sql_type, col.nullable);
                    }
                }
            },
            QueryCmd::Many => {
                includes.set.insert("<vector>");
                if has_inline_rows(query, schema) {
                    for col in &query.result_columns {
                        includes.scan(&col.sql_type, col.nullable);
                    }
                }
            },
        }
    }
    includes
}

/// Return the C++ return type string for a query.
fn query_return_type(query: &Query, schema: &Schema) -> String {
    match query.cmd {
        QueryCmd::Exec => "void".to_string(),
        QueryCmd::ExecRows => "std::int64_t".to_string(),
        QueryCmd::One => format!("std::optional<{}>", result_row_type(query, schema)),
        QueryCmd::Many => format!("std::vector<{}>", result_row_type(query, schema)),
    }
}

// ─── Table header generation - .hpp ──────────────────────────────────────────────────────

/// Emit a complete `.hpp` header for a single table struct.
fn emit_table_header(table: &Table, namespace: &str) -> anyhow::Result<String> {
    let mut src = String::new();
    let struct_name = to_pascal_case(&table.name);
    let includes = scan_table_includes(table);

    writeln!(src, "#pragma once")?;
    writeln!(src, "// Generated by sqltgen. Do not edit.")?;
    writeln!(src)?;

    for include in &includes.set {
        writeln!(src, "#include {include}")?;
    }

    if !includes.set.is_empty() {
        writeln!(src)?;
    }

    if !namespace.is_empty() {
        writeln!(src, "namespace {namespace} {{")?;
        writeln!(src)?;
    }

    writeln!(src, "struct {struct_name} {{")?;
    for col in &table.columns {
        writeln!(src, "    {} {};", cpp_type(&col.sql_type, col.nullable), col.name)?;
    }
    writeln!(src, "}};")?;

    if !namespace.is_empty() {
        writeln!(src)?;
        writeln!(src, "}} // namespace {namespace}")?;
    }

    Ok(src)
}

/// Generate one `.hpp` model header per table in the schema.
pub(super) fn generate_table_files(schema: &Schema, config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
    let mut files = Vec::new();

    for table in &schema.tables {
        files.push(GeneratedFile {
            path: PathBuf::from(&config.out).join("models").join(format!("{}.hpp", table.name)),
            content: emit_table_header(table, &config.package)?,
        });
    }

    Ok(files)
}

// ─── Query generation - .hpp and .cpp ──────────────────────────────────────────────────

/// Normalize SQL placeholders for the target engine's client library.
fn normalize_sql(sql: &str, style: CppParamStyle) -> String {
    match style {
        // libpqxx uses $1, $2, … — keep as-is from the IR.
        CppParamStyle::Dollar => sql.to_string(),
        // sqlite3 uses ?1, ?2, … — already in this form for SQLite-frontend queries,
        // but Postgres-style $N needs rewriting. For now we keep as-is since the
        // frontend already emits the right style per engine.
        CppParamStyle::QuestionNumbered => sql.to_string(),
        // libmysqlclient uses anonymous ? — rewrite $N/? N to ?.
        CppParamStyle::QuestionAnon => rewrite_to_anon_params(sql),
    }
}

/// Derive the row type name for a query's result columns.
pub(super) fn result_row_type(query: &Query, schema: &Schema) -> String {
    infer_row_type_name(query, schema).unwrap_or_else(|| "std::string".to_string())
}

/// C++ keywords and reserved identifiers that must not be used as variable names.
const CPP_KEYWORDS: &[&str] = &[
    "alignas",
    "alignof",
    "and",
    "and_eq",
    "asm",
    "auto",
    "bitand",
    "bitor",
    "bool",
    "break",
    "case",
    "catch",
    "char",
    "class",
    "compl",
    "concept",
    "const",
    "consteval",
    "constexpr",
    "constinit",
    "const_cast",
    "continue",
    "co_await",
    "co_return",
    "co_yield",
    "decltype",
    "default",
    "delete",
    "do",
    "double",
    "dynamic_cast",
    "else",
    "enum",
    "explicit",
    "export",
    "extern",
    "false",
    "float",
    "for",
    "friend",
    "goto",
    "if",
    "inline",
    "int",
    "long",
    "mutable",
    "namespace",
    "new",
    "noexcept",
    "not",
    "not_eq",
    "nullptr",
    "operator",
    "or",
    "or_eq",
    "private",
    "protected",
    "public",
    "register",
    "reinterpret_cast",
    "requires",
    "return",
    "short",
    "signed",
    "sizeof",
    "static",
    "static_assert",
    "static_cast",
    "struct",
    "switch",
    "template",
    "this",
    "thread_local",
    "throw",
    "true",
    "try",
    "typedef",
    "typeid",
    "typename",
    "union",
    "unsigned",
    "using",
    "virtual",
    "void",
    "volatile",
    "wchar_t",
    "while",
    "xor",
    "xor_eq",
];

/// Return true if `name` is a C++ keyword or reserved word.
fn cpp_keyword_or_reserved(name: &str) -> bool {
    CPP_KEYWORDS.contains(&name)
}

/// Sanitize a column name into a valid C++ local variable name.
fn sanitize_cpp_local_base(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut prev_us = false;

    for ch in name.chars() {
        let ch = ch.to_ascii_lowercase();
        let mapped = if ch.is_ascii_alphanumeric() || ch == '_' { ch } else { '_' };
        if mapped == '_' {
            if !prev_us {
                out.push('_');
            }
            prev_us = true;
        } else {
            out.push(mapped);
            prev_us = false;
        }
    }

    let trimmed = out.trim_matches('_');
    let mut base = if trimmed.is_empty() { "col".to_string() } else { trimmed.to_string() };

    if base.as_bytes().first().is_some_and(|b| b.is_ascii_digit()) {
        base = format!("col_{base}");
    }

    if cpp_keyword_or_reserved(&base) {
        base.push_str("_kw");
    }

    base
}

/// Generate unique local variable names for each result column, avoiding collisions
/// with parameter names and reserved identifiers.
fn result_binding_names(query: &Query) -> Vec<String> {
    use std::collections::HashSet;

    let mut taken: HashSet<String> = query.params.iter().map(|p| to_snake_case(&p.name)).collect();
    for fixed in ["db", "txn", "opt", "rows", "affected", "result"] {
        taken.insert(fixed.to_string());
    }

    let mut out = Vec::with_capacity(query.result_columns.len());
    for col in &query.result_columns {
        let base = sanitize_cpp_local_base(&col.name);
        let mut candidate = format!("{base}_");
        if !taken.insert(candidate.clone()) {
            let mut i = 2;
            loop {
                candidate = format!("{base}_{i}_");
                if taken.insert(candidate.clone()) {
                    break;
                }
                i += 1;
            }
        }
        out.push(candidate);
    }
    out
}

/// Return a comma-separated list of binding variable names for structured bindings.
pub(super) fn field_bindings(query: &Query) -> String {
    result_binding_names(query).join(", ")
}

/// Return a comma-separated list of `std::move(name)` expressions for each result column.
pub(super) fn move_fields(query: &Query) -> String {
    result_binding_names(query).into_iter().map(|n| format!("std::move({n})")).collect::<Vec<_>>().join(", ")
}

/// Return the C++ parameter declaration type for a query parameter.
fn cpp_param_decl_type(sql_type: &SqlType, nullable: bool, is_list: bool) -> String {
    if is_list {
        return format!("const std::vector<{}>&", cpp_type(sql_type, false));
    }

    let ty = cpp_type(sql_type, nullable);
    if nullable {
        return format!("const {ty}&");
    }

    match sql_type {
        SqlType::Boolean | SqlType::SmallInt | SqlType::Integer | SqlType::BigInt | SqlType::Real | SqlType::Double => ty,
        _ => format!("const {ty}&"),
    }
}

/// Build the C++ parameter list string for a query function signature.
fn params_signature(query: &Query, conn_type: &str) -> String {
    let mut parts = vec![format!("{conn_type} db")];
    for p in &query.params {
        let decl_ty = cpp_param_decl_type(&p.sql_type, p.nullable, p.is_list);
        parts.push(format!("{decl_ty} {}", to_snake_case(&p.name)));
    }
    parts.join(", ")
}

/// Emit a per-query inline row struct for queries whose result columns don't match
/// a known table.
fn emit_inline_row_struct(src: &mut String, query: &Query) -> anyhow::Result<()> {
    let name = format!("{}Row", to_pascal_case(&query.name));
    writeln!(src, "struct {name} {{")?;
    for col in &query.result_columns {
        writeln!(src, "    {} {};", cpp_type(&col.sql_type, col.nullable), col.name)?;
    }
    writeln!(src, "}};")?;
    Ok(())
}

/// Emit a SQL string constant in multiline raw-string form for readability.
fn emit_sql_constant(src: &mut String, query: &Query, param_style: CppParamStyle) -> anyhow::Result<()> {
    let const_name = sql_const_name(&query.name);
    let raw_sql = query.params.iter().find(|p| p.is_list).and_then(|p| p.native_list_sql.as_deref()).unwrap_or(&query.sql);
    let sql = normalize_sql(raw_sql, param_style);
    let sql = sql.trim_end().trim_end_matches(';');
    writeln!(src, "inline const std::string {const_name} = R\"sql(")?;
    writeln!(src, "{sql}")?;
    writeln!(src, ")sql\";")?;
    Ok(())
}

/// Emit a single function declaration.
fn emit_function_decl(src: &mut String, query: &Query, schema: &Schema, conn_type: &str) -> anyhow::Result<()> {
    let fn_name = to_snake_case(&query.name);
    let ret = query_return_type(query, schema);
    let params = params_signature(query, conn_type);
    writeln!(src, "{ret} {fn_name}({params});")?;
    Ok(())
}

/// Emit one query's header block: optional inline row struct, SQL constant,
/// and function declaration, kept together for readability.
fn emit_query_header_block(src: &mut String, query: &Query, schema: &Schema, param_style: CppParamStyle, conn_type: &str) -> anyhow::Result<()> {
    if has_inline_rows(query, schema) {
        emit_inline_row_struct(src, query)?;
        writeln!(src)?;
    }
    emit_sql_constant(src, query, param_style)?;
    writeln!(src)?;
    emit_function_decl(src, query, schema, conn_type)?;
    Ok(())
}

/// Emit a single function definition with body.
fn emit_function_def(src: &mut String, query: &Query, schema: &Schema, contract: &CppCoreContract) -> anyhow::Result<()> {
    let fn_name = to_snake_case(&query.name);
    let ret = query_return_type(query, schema);
    let params = params_signature(query, contract.conn_type);
    writeln!(src, "{ret} {fn_name}({params}) {{")?;
    let ctx = CppQueryContext { query, schema, null_flag_type: contract.null_flag_type };
    (contract.emit_query_body)(src, &ctx)?;
    writeln!(src, "}}")?;
    Ok(())
}

// ─── Querier ────────────────────────────────────────────

/// Emit a Querier class declaration that wraps a connection and delegates to free functions.
fn emit_querier_decl(src: &mut String, group: &str, queries: &[Query], schema: &Schema, conn_type: &str) -> anyhow::Result<()> {
    let class_name = querier_class_name(group);
    writeln!(src, "class {class_name} {{")?;
    writeln!(src, "    {conn_type} db_;")?;
    writeln!(src, "public:")?;
    writeln!(src, "    explicit {class_name}({conn_type} db) : db_(db) {{}}")?;

    for query in queries {
        writeln!(src)?;
        let fn_name = to_snake_case(&query.name);
        let ret = query_return_type(query, schema);
        let params_no_db = querier_method_params(query);
        if params_no_db.is_empty() {
            writeln!(src, "    {ret} {fn_name}();")?;
        } else {
            writeln!(src, "    {ret} {fn_name}({params_no_db});")?;
        }
    }

    writeln!(src, "}};")?;
    Ok(())
}

/// Build the parameter list for a Querier method (same as free function, minus the db param).
fn querier_method_params(query: &Query) -> String {
    let parts: Vec<String> = query
        .params
        .iter()
        .map(|p| {
            let decl_ty = cpp_param_decl_type(&p.sql_type, p.nullable, p.is_list);
            format!("{decl_ty} {}", to_snake_case(&p.name))
        })
        .collect();
    parts.join(", ")
}

/// Emit Querier::method() definitions that delegate to the corresponding free functions.
fn emit_querier_method_defs(src: &mut String, group: &str, queries: &[Query], schema: &Schema, package: &str) -> anyhow::Result<()> {
    let class_name = querier_class_name(group);
    let qualified_prefix = if package.is_empty() { "::".to_string() } else { format!("::{package}::") };
    for query in queries {
        writeln!(src)?;
        let fn_name = to_snake_case(&query.name);
        let ret = query_return_type(query, schema);
        let params_no_db = querier_method_params(query);
        if params_no_db.is_empty() {
            writeln!(src, "{ret} {class_name}::{fn_name}() {{")?;
        } else {
            writeln!(src, "{ret} {class_name}::{fn_name}({params_no_db}) {{")?;
        }
        let args: Vec<String> = std::iter::once("db_".to_string()).chain(query.params.iter().map(|p| to_snake_case(&p.name))).collect();
        writeln!(src, "    return {qualified_prefix}{fn_name}({});", args.join(", "))?;
        writeln!(src, "}}")?;
    }
    Ok(())
}

// ─── Emit Query files ────────────────────────────────────────────

/// Generate query files (one `.hpp` + one `.cpp` per query group).
pub(super) fn generate_query_files(
    schema: &Schema,
    queries: &[Query],
    contract: &CppCoreContract,
    config: &OutputConfig,
) -> anyhow::Result<Vec<GeneratedFile>> {
    let mut files = Vec::new();
    let groups = group_queries(queries);

    for (group, group_queries) in &groups {
        let stem = queries_file_stem(group);
        let header = emit_queries_header(group, group_queries, schema, contract, config)?;
        files.push(GeneratedFile { path: PathBuf::from(&config.out).join("queries").join(format!("{stem}.hpp")), content: header });
        let source = emit_queries_source(group, stem, group_queries, schema, contract, config)?;
        files.push(GeneratedFile { path: PathBuf::from(&config.out).join("queries").join(format!("{stem}.cpp")), content: source });
    }

    Ok(files)
}

/// Emit the full content of a queries header file.
fn emit_queries_header(group: &str, queries: &[Query], schema: &Schema, contract: &CppCoreContract, config: &OutputConfig) -> anyhow::Result<String> {
    let mut src = String::new();

    writeln!(src, "#pragma once")?;
    writeln!(src, "// Generated by sqltgen. Do not edit.")?;
    writeln!(src)?;

    // Standard-library includes from types used in signatures.
    let includes = scan_query_includes(queries, schema);
    for inc in &includes.set {
        writeln!(src, "#include {inc}")?;
    }
    if !includes.set.is_empty() {
        writeln!(src)?;
    }

    // Database client include.
    writeln!(src, "#include {}", contract.db_include)?;
    writeln!(src)?;

    // Include table headers that are used as return types.
    let mut needed_tables: BTreeSet<&str> = BTreeSet::new();
    for query in queries {
        if let Some(table_name) = infer_table(query, schema) {
            needed_tables.insert(table_name);
        }
    }
    for table_name in &needed_tables {
        writeln!(src, "#include \"../models/{table_name}.hpp\"")?;
    }
    if !needed_tables.is_empty() {
        writeln!(src)?;
    }

    // Open namespace.
    if !config.package.is_empty() {
        writeln!(src, "namespace {} {{", config.package)?;
        writeln!(src)?;
    }

    // Per-query blocks: inline row struct (if any), SQL constant, and declaration.
    for (i, query) in queries.iter().enumerate() {
        if i > 0 {
            writeln!(src)?;
            writeln!(src)?;
        }
        emit_query_header_block(&mut src, query, schema, contract.param_style, contract.conn_type)?;
    }

    // Querier class.
    if !queries.is_empty() {
        writeln!(src)?;
        emit_querier_decl(&mut src, group, queries, schema, contract.conn_type)?;
    }

    // Close namespace.
    if !config.package.is_empty() {
        writeln!(src)?;
        writeln!(src, "}} // namespace {}", config.package)?;
    }

    Ok(src)
}

/// Emit the full content of a queries source (.cpp) file.
fn emit_queries_source(
    group: &str,
    header_stem: &str,
    queries: &[Query],
    schema: &Schema,
    contract: &CppCoreContract,
    config: &OutputConfig,
) -> anyhow::Result<String> {
    let mut src = String::new();

    writeln!(src, "// Generated by sqltgen. Do not edit.")?;
    writeln!(src, "#include \"{header_stem}.hpp\"")?;
    for inc in contract.source_includes {
        writeln!(src, "#include {inc}")?;
    }
    writeln!(src)?;

    // Open namespace.
    if !config.package.is_empty() {
        writeln!(src, "namespace {} {{", config.package)?;
        writeln!(src)?;
    }

    // Emit static helper functions (engine-specific, provided by the adapter),
    // bracketed by banner comments so the split between helpers and query
    // functions is obvious when scanning the file.
    if !contract.source_helpers.is_empty() {
        writeln!(src, "// ---------- helpers ----------")?;
        writeln!(src)?;
        for helper in contract.source_helpers {
            writeln!(src, "{helper}")?;
        }
        writeln!(src, "// ---------- query functions ----------")?;
        writeln!(src)?;
    }

    // Function definitions.
    for (i, query) in queries.iter().enumerate() {
        if i > 0 {
            writeln!(src)?;
        }
        emit_function_def(&mut src, query, schema, contract)?;
    }

    // Querier method definitions.
    if !queries.is_empty() {
        writeln!(src)?;
        emit_querier_method_defs(&mut src, group, queries, schema, &config.package)?;
    }

    // Close namespace.
    if !config.package.is_empty() {
        writeln!(src)?;
        writeln!(src, "}} // namespace {}", config.package)?;
    }

    Ok(src)
}
