use std::collections::BTreeSet;
use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::common::{
    emit_package, group_queries, has_inline_rows, infer_table, pg_array_type_name, querier_class_name, queries_class_name, row_type_name,
};
use crate::backend::jdbc::{
    self, emit_dynamic_binds, emit_jdbc_binds, prepare_dynamic_sql_parts, prepare_sql_const, prepare_sql_const_from, ListAction, QuerierContext,
};
use crate::backend::naming::{to_camel_case, to_pascal_case};
use crate::backend::GeneratedFile;
use crate::config::{ListParamStrategy, OutputConfig};
use crate::ir::{Parameter, Query, QueryCmd, Schema};

use super::adapter::JvmCoreContract;
use super::typemap::JavaTypeMap;

/// Per-query context computed once in the dispatcher and forwarded to all emitters.
struct QueryContext<'a> {
    query: &'a Query,
    schema: &'a Schema,
    type_map: &'a JavaTypeMap,
    return_type: String,
    params_sig: String,
}

/// Emit all Java files for the given schema and queries.
pub(super) fn generate_core_files(
    schema: &Schema,
    queries: &[Query],
    contract: &JvmCoreContract,
    config: &OutputConfig,
    type_map: &JavaTypeMap,
) -> anyhow::Result<Vec<GeneratedFile>> {
    let mut files = Vec::new();

    // One record class per table
    for table in &schema.tables {
        let class_name = to_pascal_case(&table.name);
        let mut src = String::new();
        let mpkg = models_package(&config.package);
        emit_package(&mut src, &mpkg, ";");
        let table_imports = type_map.table_imports(table);
        for imp in &table_imports {
            writeln!(src, "import {imp};")?;
        }
        if !table_imports.is_empty() {
            writeln!(src)?;
        }
        writeln!(src, "public record {class_name}(")?;
        let params: Vec<String> =
            table.columns.iter().map(|col| format!("    {} {}", type_map.java_type(&col.sql_type, col.nullable), to_camel_case(&col.name))).collect();
        writeln!(src, "{}", params.join(",\n"))?;
        writeln!(src, ") {{}}")?;

        let path = record_path(&config.out, &mpkg, &class_name);
        files.push(GeneratedFile { path, content: src });
    }

    // One class per query group + one DataSource-backed wrapper class per group
    let strategy = config.list_params.clone().unwrap_or_default();
    for (group, group_queries) in group_queries(queries) {
        let class_name = queries_class_name(&group);
        let querier_name = querier_class_name(&group);

        let (override_imports, extra_fields) = type_map.query_metadata(&group_queries);

        let qpkg = queries_package(&config.package);
        let mpkg = models_package(&config.package);
        let mut src = String::new();
        emit_package(&mut src, &qpkg, ";");
        // Standard JDBC imports + any override-specific imports, all sorted
        let mut all_imports: BTreeSet<String> = [
            "java.sql.Connection",
            "java.sql.PreparedStatement",
            "java.sql.ResultSet",
            "java.sql.SQLException",
            "java.util.ArrayList",
            "java.util.List",
            "java.util.Optional",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        all_imports.extend(override_imports.iter().cloned());
        for query in &group_queries {
            if let Some(table_name) = infer_table(query, schema) {
                let model_class = to_pascal_case(table_name);
                all_imports.insert(format!("{mpkg}.{model_class}"));
            }
        }
        for imp in &all_imports {
            writeln!(src, "import {imp};")?;
        }
        writeln!(src)?;
        writeln!(src, "public final class {class_name} {{")?;
        writeln!(src, "    private {class_name}() {{}}")?;
        for ef in &extra_fields {
            writeln!(src, "    {}", ef.declaration)?;
        }

        for query in &group_queries {
            writeln!(src)?;
            if has_inline_rows(query, schema) {
                emit_row_record(&mut src, query, type_map)?;
                writeln!(src)?;
            }
            emit_java_query(&mut src, query, schema, &strategy, contract, type_map)?;
        }

        writeln!(src)?;
        emit_nullable_primitive_helpers(&mut src)?;
        writeln!(src, "}}")?;

        let path = record_path(&config.out, &qpkg, &class_name);
        files.push(GeneratedFile { path, content: src });

        let mut src = String::new();
        emit_package(&mut src, &qpkg, ";");
        let ctx = QuerierContext { class_name: &class_name, querier_name: &querier_name, override_imports: &override_imports, extra_fields: &extra_fields };
        emit_java_querier(&mut src, &group_queries, schema, &ctx, contract, type_map)?;
        let path = record_path(&config.out, &qpkg, &querier_name);
        files.push(GeneratedFile { path, content: src });
    }

    Ok(files)
}

fn emit_java_query(
    src: &mut String,
    query: &Query,
    schema: &Schema,
    strategy: &ListParamStrategy,
    contract: &JvmCoreContract,
    type_map: &JavaTypeMap,
) -> anyhow::Result<()> {
    let ctx = QueryContext {
        query,
        schema,
        type_map,
        return_type: jdbc::jdbc_return_type(query, schema, contract.fallback_type, |r| format!("Optional<{r}>"), |r| format!("List<{r}>"), "void", "long"),
        params_sig: std::iter::once("Connection conn".to_string())
            .chain(query.params.iter().map(|p| format!("{} {}", type_map.java_param_type(p), to_camel_case(&p.name))))
            .collect::<Vec<_>>()
            .join(", "),
    };

    if let Some(lp) = query.params.iter().find(|p| p.is_list) {
        match jdbc::resolve_list_strategy(strategy, lp) {
            ListAction::PgNative(sql) => emit_java_list_pg_native(src, &ctx, lp, &sql, contract),
            ListAction::Dynamic => emit_java_list_dynamic(src, &ctx, lp, contract),
            ListAction::JsonNative(sql) => emit_java_list_json_native(src, &ctx, lp, &sql, contract),
        }
    } else {
        emit_java_scalar_query(src, &ctx, contract)
    }
}

/// Emit a Java text block SQL constant (Java 15+ `"""..."""` syntax).
///
/// Strips the trailing `;` from `raw_sql`, re-appends it to the last content line,
/// and formats the block with 12-space content indent so `javac` strips leading
/// whitespace, leaving the SQL with no indentation at runtime.
fn emit_java_sql_text_block(src: &mut String, sql_const: &str, raw_sql: &str) -> anyhow::Result<()> {
    let trimmed = jdbc::escape_sql_triple_quoted(raw_sql.trim_end().trim_end_matches(';'));
    writeln!(src, "    private static final String {sql_const} = \"\"\"")?;
    let mut lines = trimmed.lines().peekable();
    while let Some(line) = lines.next() {
        if lines.peek().is_none() {
            writeln!(src, "            {line};")?;
        } else {
            writeln!(src, "            {line}")?;
        }
    }
    writeln!(src, "            \"\"\";")?;
    Ok(())
}

fn emit_java_scalar_query(src: &mut String, ctx: &QueryContext, contract: &JvmCoreContract) -> anyhow::Result<()> {
    let (sql_const, raw_sql) = prepare_sql_const(ctx.query);
    emit_java_sql_text_block(src, &sql_const, &raw_sql)?;
    writeln!(src, "    public static {} {}({}) throws SQLException {{", ctx.return_type, to_camel_case(&ctx.query.name), ctx.params_sig)?;
    writeln!(src, "        try (PreparedStatement ps = conn.prepareStatement({sql_const})) {{")?;
    emit_jdbc_binds(src, ctx.query, "", contract.statement_end, "toArray()", contract.json_bind, |p| ctx.type_map.write_expr(p))?;
    emit_java_result_block(src, ctx, contract)?;
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit a PostgreSQL native list query using `= ANY(?)` with a JDBC array.
fn emit_java_list_pg_native(src: &mut String, ctx: &QueryContext, lp: &Parameter, rewritten_sql: &str, contract: &JvmCoreContract) -> anyhow::Result<()> {
    let lp_name = to_camel_case(&lp.name);
    let method_name = to_camel_case(&ctx.query.name);
    let (sql_const, raw_sql) = prepare_sql_const_from(ctx.query, rewritten_sql);
    emit_java_sql_text_block(src, &sql_const, &raw_sql)?;
    writeln!(src, "    public static {} {method_name}({}) throws SQLException {{", ctx.return_type, ctx.params_sig)?;
    let type_name = pg_array_type_name(&lp.sql_type);
    writeln!(src, "        java.sql.Array arr = conn.createArrayOf(\"{type_name}\", {lp_name}.toArray());")?;
    writeln!(src, "        try (PreparedStatement ps = conn.prepareStatement({sql_const})) {{")?;
    emit_jdbc_binds(src, ctx.query, "arr", contract.statement_end, "toArray()", contract.json_bind, |p| ctx.type_map.write_expr(p))?;
    emit_java_result_block(src, ctx, contract)?;
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit a dynamic list query that builds `IN (?,?,…,?)` at runtime.
fn emit_java_list_dynamic(src: &mut String, ctx: &QueryContext, lp: &Parameter, contract: &JvmCoreContract) -> anyhow::Result<()> {
    let lp_name = to_camel_case(&lp.name);
    let method_name = to_camel_case(&ctx.query.name);
    let (before_esc, after_esc) = prepare_dynamic_sql_parts(ctx.query, lp);
    writeln!(src, "    public static {} {method_name}({}) throws SQLException {{", ctx.return_type, ctx.params_sig)?;
    writeln!(src, "        String marks = {lp_name}.stream().map(x -> \"?\").collect(java.util.stream.Collectors.joining(\", \"));")?;
    writeln!(src, "        String sql = \"{before_esc}\" + \"IN (\" + marks + \"){after_esc};\";")?;
    writeln!(src, "        try (PreparedStatement ps = conn.prepareStatement(sql)) {{")?;
    emit_dynamic_binds(src, ctx.query, lp, contract.statement_end, contract.size_access, &|src, lp_name, base, setter| {
        writeln!(src, "            for (int i = 0; i < {lp_name}.size(); i++) {{")?;
        writeln!(src, "                ps.{setter}({base} + i + 1, {lp_name}.get(i));")?;
        writeln!(src, "            }}")?;
        Ok(())
    })?;
    emit_java_result_block(src, ctx, contract)?;
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit a SQLite or MySQL native list query that passes a JSON array string.
///
/// Both engines use the same structure: build a JSON string from the list,
/// then bind it as a regular string parameter. The caller provides the
/// already-rewritten SQL (with `json_each` or `JSON_TABLE`).
fn emit_java_list_json_native(src: &mut String, ctx: &QueryContext, lp: &Parameter, rewritten_sql: &str, contract: &JvmCoreContract) -> anyhow::Result<()> {
    let method_name = to_camel_case(&ctx.query.name);
    let (sql_const, raw_sql) = prepare_sql_const_from(ctx.query, rewritten_sql);
    emit_java_sql_text_block(src, &sql_const, &raw_sql)?;
    writeln!(src, "    public static {} {method_name}({}) throws SQLException {{", ctx.return_type, ctx.params_sig)?;
    emit_java_json_builder(src, lp)?;
    writeln!(src, "        try (PreparedStatement ps = conn.prepareStatement({sql_const})) {{")?;
    emit_jdbc_binds(src, ctx.query, "json", contract.statement_end, "toArray()", contract.json_bind, |p| ctx.type_map.write_expr(p))?;
    emit_java_result_block(src, ctx, contract)?;
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit the `String json = …` line that builds a JSON array from a list param.
///
/// Text-like types need per-element quoting and escaping; numeric/boolean types
/// can use plain `Object::toString`.
fn emit_java_json_builder(src: &mut String, lp: &Parameter) -> anyhow::Result<()> {
    let lp_name = to_camel_case(&lp.name);
    if lp.sql_type.needs_json_quoting() {
        writeln!(
            src,
            "        String json = \"[\" + {lp_name}.stream().map(x -> \"\\\"\" + x.toString().replace(\"\\\\\", \"\\\\\\\\\").replace(\"\\\"\", \"\\\\\\\"\") + \"\\\"\").collect(java.util.stream.Collectors.joining(\",\")) + \"]\";"
        )?;
    } else {
        writeln!(src, "        String json = \"[\" + {lp_name}.stream().map(Object::toString).collect(java.util.stream.Collectors.joining(\",\")) + \"]\";")?;
    }
    Ok(())
}

/// Emit the result-reading block (executeUpdate / executeQuery / fetch loop).
fn emit_java_result_block(src: &mut String, ctx: &QueryContext, contract: &JvmCoreContract) -> anyhow::Result<()> {
    match ctx.query.cmd {
        QueryCmd::Exec => writeln!(src, "            ps.executeUpdate();")?,
        QueryCmd::ExecRows => writeln!(src, "            return ps.executeUpdate();")?,
        QueryCmd::One => {
            writeln!(src, "            try (ResultSet rs = ps.executeQuery()) {{")?;
            writeln!(src, "                if (!rs.next()) return Optional.empty();")?;
            writeln!(src, "                return Optional.of({});", emit_row_constructor(ctx.query, ctx.schema, ctx.type_map, contract))?;
            writeln!(src, "            }}")?;
        },
        QueryCmd::Many => {
            let row_type = result_row_type(ctx.query, ctx.schema, contract);
            writeln!(src, "            List<{row_type}> rows = new ArrayList<>();")?;
            writeln!(src, "            try (ResultSet rs = ps.executeQuery()) {{")?;
            writeln!(src, "                while (rs.next()) rows.add({});", emit_row_constructor(ctx.query, ctx.schema, ctx.type_map, contract))?;
            writeln!(src, "            }}")?;
            writeln!(src, "            return rows;")?;
        },
    }
    Ok(())
}

/// Emits a DataSource-backed querier wrapper that acquires a connection
/// per call and delegates to the static methods in `{class_name}`.
fn emit_java_querier(
    src: &mut String,
    queries: &[Query],
    schema: &Schema,
    ctx: &QuerierContext,
    contract: &JvmCoreContract,
    type_map: &JavaTypeMap,
) -> anyhow::Result<()> {
    let has_one = queries.iter().any(|q| q.cmd == QueryCmd::One);
    let has_many = queries.iter().any(|q| q.cmd == QueryCmd::Many);

    // Emit all imports: standard JDBC + any type-override imports, sorted.
    let mut all_imports: BTreeSet<String> = ["java.sql.Connection", "java.sql.SQLException", "javax.sql.DataSource"].iter().map(|s| s.to_string()).collect();
    if has_many {
        all_imports.insert("java.util.List".to_string());
    }
    if has_one {
        all_imports.insert("java.util.Optional".to_string());
    }
    all_imports.extend(ctx.override_imports.iter().cloned());
    for imp in &all_imports {
        writeln!(src, "import {imp};")?;
    }

    let querier_name = ctx.querier_name;
    writeln!(src)?;
    writeln!(src, "public final class {querier_name} {{")?;
    writeln!(src, "    private final DataSource dataSource;")?;
    for ef in ctx.extra_fields {
        writeln!(src, "    {}", ef.declaration)?;
    }
    writeln!(src)?;
    writeln!(src, "    public {querier_name}(DataSource dataSource) {{")?;
    writeln!(src, "        this.dataSource = dataSource;")?;
    writeln!(src, "    }}")?;

    for query in queries {
        writeln!(src)?;
        emit_java_querier_method(src, query, schema, ctx.class_name, contract, type_map)?;
    }

    writeln!(src, "}}")?;
    Ok(())
}

/// Emits one instance method on the querier class that wraps the corresponding static method.
fn emit_java_querier_method(
    src: &mut String,
    query: &Query,
    schema: &Schema,
    class_name: &str,
    contract: &JvmCoreContract,
    type_map: &JavaTypeMap,
) -> anyhow::Result<()> {
    let row = jdbc::ds_result_row_type(query, schema, contract.fallback_type, class_name);
    let return_type = match query.cmd {
        QueryCmd::One => format!("Optional<{row}>"),
        QueryCmd::Many => format!("List<{row}>"),
        QueryCmd::Exec => "void".to_string(),
        QueryCmd::ExecRows => "long".to_string(),
    };

    let params_sig: String = query.params.iter().map(|p| format!("{} {}", type_map.java_param_type(p), to_camel_case(&p.name))).collect::<Vec<_>>().join(", ");

    let method_name = to_camel_case(&query.name);
    let args: String = query.params.iter().map(|p| to_camel_case(&p.name)).collect::<Vec<_>>().join(", ");
    let call_args = if args.is_empty() { "conn".to_string() } else { format!("conn, {args}") };

    writeln!(src, "    public {return_type} {method_name}({params_sig}) throws SQLException {{")?;
    writeln!(src, "        try (Connection conn = dataSource.getConnection()) {{")?;
    match query.cmd {
        QueryCmd::Exec => writeln!(src, "            {class_name}.{method_name}({call_args});")?,
        _ => writeln!(src, "            return {class_name}.{method_name}({call_args});")?,
    }
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

fn result_row_type(query: &Query, schema: &Schema, contract: &JvmCoreContract) -> String {
    jdbc::result_row_type(query, schema, contract.fallback_type)
}

fn emit_row_record(src: &mut String, query: &Query, type_map: &JavaTypeMap) -> anyhow::Result<()> {
    let name = row_type_name(&query.name);
    writeln!(src, "    public record {name}(")?;
    let fields: Vec<String> =
        query.result_columns.iter().map(|col| format!("        {} {}", type_map.java_type(&col.sql_type, col.nullable), to_camel_case(&col.name))).collect();
    writeln!(src, "{}", fields.join(",\n"))?;
    writeln!(src, "    ) {{}}")?;
    Ok(())
}

fn emit_row_constructor(query: &Query, schema: &Schema, type_map: &JavaTypeMap, contract: &JvmCoreContract) -> String {
    jdbc::build_row_constructor(query, schema, contract.fallback_type, "new ", |sql_type, nullable, idx| type_map.read_expr(sql_type, nullable, idx))
}

/// Emit private static helper methods for null-safe reads of primitive JDBC columns.
///
/// `rs.getObject(col, Integer.class)` and similar calls throw in SQLite JDBC when the
/// column is NULL. These helpers use the `wasNull()` idiom which works across all drivers.
fn emit_nullable_primitive_helpers(src: &mut String) -> anyhow::Result<()> {
    // (method_suffix, boxed_return_type, primitive_type, getter)
    let helpers = [
        ("Boolean", "Boolean", "boolean", "getBoolean"),
        ("Short", "Short", "short", "getShort"),
        ("Int", "Integer", "int", "getInt"),
        ("Long", "Long", "long", "getLong"),
        ("Float", "Float", "float", "getFloat"),
        ("Double", "Double", "double", "getDouble"),
    ];
    for (suffix, boxed, prim, getter) in helpers {
        writeln!(src, "    private static {boxed} getNullable{suffix}(java.sql.ResultSet rs, int col) throws java.sql.SQLException {{")?;
        writeln!(src, "        {prim} v = rs.{getter}(col);")?;
        writeln!(src, "        return rs.wasNull() ? null : v;")?;
        writeln!(src, "    }}")?;
    }
    Ok(())
}

// ─── Path helper ──────────────────────────────────────────────────────────────

fn record_path(out: &str, package: &str, class_name: &str) -> PathBuf {
    let pkg_path = package.replace('.', "/");
    PathBuf::from(out).join(pkg_path).join(format!("{class_name}.java"))
}

fn models_package(base: &str) -> String {
    if base.is_empty() {
        "models".to_string()
    } else {
        format!("{base}.models")
    }
}

fn queries_package(base: &str) -> String {
    if base.is_empty() {
        "queries".to_string()
    } else {
        format!("{base}.queries")
    }
}
