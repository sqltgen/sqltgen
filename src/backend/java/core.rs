use std::collections::BTreeSet;
use std::fmt::Write;
use std::path::PathBuf;

use indoc::{formatdoc, writedoc};

use crate::backend::common::{
    emit_package, group_queries, has_inline_rows, infer_table, model_name, needed_enums, pg_array_type_name, push_indented, querier_class_name,
    queries_class_name, row_type_name,
};
use crate::backend::jdbc::{
    self, emit_dynamic_binds, emit_jdbc_binds, prepare_dynamic_sql_parts, prepare_sql_const, prepare_sql_const_from, JsonBindMode, QuerierContext,
};
use crate::backend::list_strategy::{self, ListAction};
use crate::backend::naming::{to_camel_case, to_pascal_case, to_screaming_snake_case};
use crate::backend::sql_rewrite::rewrite_to_anon_params;
use crate::backend::GeneratedFile;
use crate::config::{ListParamStrategy, OutputConfig};
use crate::ir::{EnumType, Parameter, Query, QueryCmd, Schema};

use super::typemap::JavaTypeMap;

// ─── Language-level constants ────────────────────────────────────────────────

/// Statement-end token appended after JDBC calls.
const STATEMENT_END: &str = ";";
/// Fallback row type when a query has no result columns.
const FALLBACK_TYPE: &str = "Object[]";
/// How to access a `List`'s size in Java.
const SIZE_ACCESS: &str = ".size()";

/// All data needed for a single `generate()` call, bundled to reduce parameter threading.
pub(super) struct GenerationContext<'a> {
    pub schema: &'a Schema,
    pub queries: &'a [Query],
    pub config: &'a OutputConfig,
    pub json_bind: JsonBindMode,
    pub type_map: &'a JavaTypeMap,
    pub strategy: ListParamStrategy,
}

/// Java return type (`Optional<R>`, `List<R>`, `void`, or `long`) for a query.
fn java_return_type(query: &Query, schema: &Schema) -> String {
    jdbc::jdbc_return_type(query, schema, FALLBACK_TYPE, |r| format!("Optional<{r}>"), |r| format!("List<{r}>"), "void", "long")
}

/// Java parameter list signature (always starts with `Connection conn`).
fn java_params_sig(query: &Query, type_map: &JavaTypeMap) -> String {
    std::iter::once("Connection conn".to_string())
        .chain(query.params.iter().map(|p| format!("{} {}", type_map.java_param_type(p), to_camel_case(&p.name))))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Emit all Java files for the given schema and queries.
pub(super) fn generate_core_files(ctx: &GenerationContext) -> anyhow::Result<Vec<GeneratedFile>> {
    let mut files = Vec::new();

    let ds = ctx.schema.default_schema.as_deref();

    // One record class per table
    for table in &ctx.schema.tables {
        let class_name = model_name(table, ds);
        let mut src = String::new();
        let mpkg = models_package(&ctx.config.package);
        emit_package(&mut src, &mpkg, ";");
        let table_imports = ctx.type_map.table_imports(table);
        for imp in &table_imports {
            writeln!(src, "import {imp};")?;
        }
        if !table_imports.is_empty() {
            writeln!(src)?;
        }
        writeln!(src, "public record {class_name}(")?;
        let params: Vec<String> =
            table.columns.iter().map(|col| format!("    {} {}", ctx.type_map.java_type(&col.sql_type, col.nullable), to_camel_case(&col.name))).collect();
        writeln!(src, "{}", params.join(",\n"))?;
        writeln!(src, ") {{}}")?;

        let path = record_path(&ctx.config.out, &mpkg, &class_name);
        files.push(GeneratedFile { path, content: src });
    }

    // One enum class per schema enum
    for enum_type in &ctx.schema.enums {
        let class_name = to_pascal_case(&enum_type.name);
        let mpkg = models_package(&ctx.config.package);
        let mut src = String::new();
        emit_java_enum(&mut src, &mpkg, &class_name, enum_type)?;
        let path = record_path(&ctx.config.out, &mpkg, &class_name);
        files.push(GeneratedFile { path, content: src });
    }

    // One class per query group + one DataSource-backed wrapper class per group
    for (group, group_queries) in group_queries(ctx.queries) {
        let class_name = queries_class_name(&group);
        let querier_name = querier_class_name(&group);

        let (override_imports, extra_fields) = ctx.type_map.query_metadata(&group_queries);

        let qpkg = queries_package(&ctx.config.package);
        let mpkg = models_package(&ctx.config.package);
        let mut src = String::new();
        emit_package(&mut src, &qpkg, ";");
        // Standard JDBC imports + any override-specific imports, all sorted.
        // `Optional`, `List`, `ArrayList` are only emitted when actually used.
        let has_one = group_queries.iter().any(|q| q.cmd == QueryCmd::One);
        let has_many = group_queries.iter().any(|q| q.cmd == QueryCmd::Many);
        let mut all_imports: BTreeSet<String> =
            ["java.sql.Connection", "java.sql.PreparedStatement", "java.sql.ResultSet", "java.sql.SQLException"].iter().map(|s| s.to_string()).collect();
        if has_one {
            all_imports.insert("java.util.Optional".to_string());
        }
        if has_many {
            all_imports.insert("java.util.ArrayList".to_string());
            all_imports.insert("java.util.List".to_string());
        }
        let mut model_imports: BTreeSet<String> = BTreeSet::new();
        for query in &group_queries {
            if let Some(table) = infer_table(query, ctx.schema) {
                let model_class = model_name(table, ds);
                model_imports.insert(format!("{mpkg}.{model_class}"));
            }
        }
        let enum_names = needed_enums(&group_queries.iter().collect::<Vec<_>>());
        for name in &enum_names {
            model_imports.insert(format!("{mpkg}.{}", to_pascal_case(name)));
        }
        all_imports.extend(override_imports.iter().cloned());
        all_imports.extend(model_imports.iter().cloned());
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
            if has_inline_rows(query, ctx.schema) {
                emit_row_record(&mut src, query, ctx.type_map)?;
                writeln!(src)?;
            }
            emit_java_query(&mut src, query, ctx)?;
        }

        writeln!(src)?;
        emit_nullable_primitive_helpers(&mut src)?;
        writeln!(src, "}}")?;

        let path = record_path(&ctx.config.out, &qpkg, &class_name);
        files.push(GeneratedFile { path, content: src });

        let mut src = String::new();
        emit_package(&mut src, &qpkg, ";");
        let querier_ctx = QuerierContext {
            class_name: &class_name,
            querier_name: &querier_name,
            override_imports: &override_imports,
            model_imports: &model_imports,
            extra_fields: &extra_fields,
        };
        emit_java_querier(&mut src, &group_queries, &querier_ctx, ctx)?;
        let path = record_path(&ctx.config.out, &qpkg, &querier_name);
        files.push(GeneratedFile { path, content: src });
    }

    Ok(files)
}

fn emit_java_query(src: &mut String, query: &Query, ctx: &GenerationContext) -> anyhow::Result<()> {
    if let Some(lp) = query.params.iter().find(|p| p.is_list) {
        match list_strategy::resolve(&ctx.strategy, lp) {
            ListAction::SqlArrayBind(sql) => emit_java_list_array_bind(src, ctx, query, lp, &rewrite_to_anon_params(&sql)),
            ListAction::Dynamic => emit_java_list_dynamic(src, ctx, query, lp),
            ListAction::JsonStringBind(sql) => emit_java_list_json_bind(src, ctx, query, lp, &rewrite_to_anon_params(&sql)),
        }
    } else {
        emit_java_scalar_query(src, ctx, query)
    }
}

/// Emit a Java text block SQL constant (Java 15+ `"""..."""` syntax).
///
/// Strips the trailing `;` from `raw_sql`, re-appends it to the last content line,
/// and formats the block with 12-space content indent so `javac` strips leading
/// whitespace, leaving the SQL with no indentation at runtime.
fn emit_java_sql_text_block(src: &mut String, sql_const: &str, raw_sql: &str) -> anyhow::Result<()> {
    let trimmed = jdbc::escape_sql_triple_quoted(raw_sql.trim_end().trim_end_matches(';'));
    writeln!(src, r#"    private static final String {sql_const} = """"#)?;
    let mut lines = trimmed.lines().peekable();
    while let Some(line) = lines.next() {
        if lines.peek().is_none() {
            writeln!(src, "            {line};")?;
        } else {
            writeln!(src, "            {line}")?;
        }
    }
    writeln!(src, r#"            """;"#)?;
    Ok(())
}

fn emit_java_scalar_query(src: &mut String, ctx: &GenerationContext, query: &Query) -> anyhow::Result<()> {
    let (sql_const, raw_sql) = prepare_sql_const(query);
    emit_java_sql_text_block(src, &sql_const, &raw_sql)?;
    writeln!(
        src,
        "    public static {} {}({}) throws SQLException {{",
        java_return_type(query, ctx.schema),
        to_camel_case(&query.name),
        java_params_sig(query, ctx.type_map)
    )?;
    writeln!(src, "        try (PreparedStatement ps = conn.prepareStatement({sql_const})) {{")?;
    emit_jdbc_binds(src, query, "", STATEMENT_END, "toArray()", ctx.json_bind, |p| ctx.type_map.write_expr(p))?;
    emit_java_result_block(src, ctx, query)?;
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit a list query that binds the list as a JDBC `Array` (e.g. `= ANY(?)`).
fn emit_java_list_array_bind(src: &mut String, ctx: &GenerationContext, query: &Query, lp: &Parameter, rewritten_sql: &str) -> anyhow::Result<()> {
    let lp_name = to_camel_case(&lp.name);
    let method_name = to_camel_case(&query.name);
    let (sql_const, raw_sql) = prepare_sql_const_from(query, rewritten_sql);
    emit_java_sql_text_block(src, &sql_const, &raw_sql)?;
    writeln!(src, "    public static {} {method_name}({}) throws SQLException {{", java_return_type(query, ctx.schema), java_params_sig(query, ctx.type_map))?;
    let type_name = pg_array_type_name(&lp.sql_type);
    writeln!(src, r#"        java.sql.Array arr = conn.createArrayOf("{type_name}", {lp_name}.toArray());"#)?;
    writeln!(src, "        try (PreparedStatement ps = conn.prepareStatement({sql_const})) {{")?;
    emit_jdbc_binds(src, query, "arr", STATEMENT_END, "toArray()", ctx.json_bind, |p| ctx.type_map.write_expr(p))?;
    emit_java_result_block(src, ctx, query)?;
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit a dynamic list query that builds `IN (?,?,…,?)` at runtime.
fn emit_java_list_dynamic(src: &mut String, ctx: &GenerationContext, query: &Query, lp: &Parameter) -> anyhow::Result<()> {
    let lp_name = to_camel_case(&lp.name);
    let method_name = to_camel_case(&query.name);
    let (before_esc, after_esc) = prepare_dynamic_sql_parts(query, lp);
    writeln!(src, "    public static {} {method_name}({}) throws SQLException {{", java_return_type(query, ctx.schema), java_params_sig(query, ctx.type_map))?;
    writeln!(src, r#"        String marks = {lp_name}.stream().map(x -> "?").collect(java.util.stream.Collectors.joining(", "));"#)?;
    writeln!(src, r#"        String sql = "{before_esc}" + "IN (" + marks + "){after_esc};";"#)?;
    writeln!(src, "        try (PreparedStatement ps = conn.prepareStatement(sql)) {{")?;
    emit_dynamic_binds(src, query, lp, STATEMENT_END, SIZE_ACCESS, &|src, lp_name, base, setter| {
        push_indented(
            src,
            "            ",
            &formatdoc!(
                r#"
            for (int i = 0; i < {lp_name}.size(); i++) {{
                ps.{setter}({base} + i + 1, {lp_name}.get(i));
            }}
        "#
            ),
        );
        Ok(())
    })?;
    emit_java_result_block(src, ctx, query)?;
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit a list query that binds the list as a JSON-encoded string.
///
/// The caller provides the already-rewritten SQL (e.g. with `json_each` or
/// `JSON_TABLE`); the generated code builds a JSON array literal from the list
/// and binds it as a regular string parameter.
fn emit_java_list_json_bind(src: &mut String, ctx: &GenerationContext, query: &Query, lp: &Parameter, rewritten_sql: &str) -> anyhow::Result<()> {
    let method_name = to_camel_case(&query.name);
    let (sql_const, raw_sql) = prepare_sql_const_from(query, rewritten_sql);
    emit_java_sql_text_block(src, &sql_const, &raw_sql)?;
    writeln!(src, "    public static {} {method_name}({}) throws SQLException {{", java_return_type(query, ctx.schema), java_params_sig(query, ctx.type_map))?;
    emit_java_json_builder(src, lp)?;
    writeln!(src, "        try (PreparedStatement ps = conn.prepareStatement({sql_const})) {{")?;
    emit_jdbc_binds(src, query, "json", STATEMENT_END, "toArray()", ctx.json_bind, |p| ctx.type_map.write_expr(p))?;
    emit_java_result_block(src, ctx, query)?;
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
            r#"        String json = "[" + {lp_name}.stream().map(x -> "\"" + x.toString().replace("\\", "\\\\").replace("\"", "\\\"") + "\"").collect(java.util.stream.Collectors.joining(",")) + "]";"#
        )?;
    } else {
        writeln!(src, r#"        String json = "[" + {lp_name}.stream().map(Object::toString).collect(java.util.stream.Collectors.joining(",")) + "]";"#)?;
    }
    Ok(())
}

/// Emit the result-reading block (executeUpdate / executeQuery / fetch loop).
fn emit_java_result_block(src: &mut String, ctx: &GenerationContext, query: &Query) -> anyhow::Result<()> {
    match query.cmd {
        QueryCmd::Exec => writeln!(src, "            ps.executeUpdate();")?,
        QueryCmd::ExecRows => writeln!(src, "            return ps.executeUpdate();")?,
        QueryCmd::One => {
            let row_ctor = emit_row_constructor(query, ctx.schema, ctx.type_map);
            push_indented(
                src,
                "            ",
                &formatdoc!(
                    r#"
                try (ResultSet rs = ps.executeQuery()) {{
                    if (!rs.next()) return Optional.empty();
                    return Optional.of({row_ctor});
                }}
            "#
                ),
            );
        },
        QueryCmd::Many => {
            let row_type = result_row_type(query, ctx.schema);
            let row_ctor = emit_row_constructor(query, ctx.schema, ctx.type_map);
            push_indented(
                src,
                "            ",
                &formatdoc!(
                    r#"
                List<{row_type}> rows = new ArrayList<>();
                try (ResultSet rs = ps.executeQuery()) {{
                    while (rs.next()) rows.add({row_ctor});
                }}
                return rows;
            "#
                ),
            );
        },
    }
    Ok(())
}

/// Emits a DataSource-backed querier wrapper that acquires a connection
/// per call and delegates to the static methods in `{class_name}`.
fn emit_java_querier(src: &mut String, queries: &[Query], querier_ctx: &QuerierContext, ctx: &GenerationContext) -> anyhow::Result<()> {
    let has_one = queries.iter().any(|q| q.cmd == QueryCmd::One);
    let has_many = queries.iter().any(|q| q.cmd == QueryCmd::Many);

    // Emit all imports: standard JDBC + model imports + any type-override imports, sorted.
    let mut all_imports: BTreeSet<String> = ["java.sql.Connection", "java.sql.SQLException", "javax.sql.DataSource"].iter().map(|s| s.to_string()).collect();
    if has_many {
        all_imports.insert("java.util.List".to_string());
    }
    if has_one {
        all_imports.insert("java.util.Optional".to_string());
    }
    all_imports.extend(querier_ctx.override_imports.iter().cloned());
    all_imports.extend(querier_ctx.model_imports.iter().cloned());
    for imp in &all_imports {
        writeln!(src, "import {imp};")?;
    }

    let querier_name = querier_ctx.querier_name;
    writeln!(src)?;
    writeln!(src, "public final class {querier_name} {{")?;
    writeln!(src, "    private final DataSource dataSource;")?;
    for ef in querier_ctx.extra_fields {
        writeln!(src, "    {}", ef.declaration)?;
    }
    writeln!(src)?;
    writeln!(src, "    public {querier_name}(DataSource dataSource) {{")?;
    writeln!(src, "        this.dataSource = dataSource;")?;
    writeln!(src, "    }}")?;

    for query in queries {
        writeln!(src)?;
        emit_java_querier_method(src, query, querier_ctx.class_name, ctx)?;
    }

    writeln!(src, "}}")?;
    Ok(())
}

/// Emits one instance method on the querier class that wraps the corresponding static method.
fn emit_java_querier_method(src: &mut String, query: &Query, class_name: &str, ctx: &GenerationContext) -> anyhow::Result<()> {
    let row = jdbc::ds_result_row_type(query, ctx.schema, FALLBACK_TYPE, class_name);
    let return_type = match query.cmd {
        QueryCmd::One => format!("Optional<{row}>"),
        QueryCmd::Many => format!("List<{row}>"),
        QueryCmd::Exec => "void".to_string(),
        QueryCmd::ExecRows => "long".to_string(),
    };

    let params_sig: String =
        query.params.iter().map(|p| format!("{} {}", ctx.type_map.java_param_type(p), to_camel_case(&p.name))).collect::<Vec<_>>().join(", ");

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

fn result_row_type(query: &Query, schema: &Schema) -> String {
    jdbc::result_row_type(query, schema, FALLBACK_TYPE)
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

fn emit_row_constructor(query: &Query, schema: &Schema, type_map: &JavaTypeMap) -> String {
    jdbc::build_row_constructor(query, schema, FALLBACK_TYPE, "new ", |sql_type, nullable, idx| type_map.read_expr(sql_type, nullable, idx))
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

/// Emit a Java enum class for a SQL enum type.
fn emit_java_enum(src: &mut String, package: &str, class_name: &str, enum_type: &EnumType) -> anyhow::Result<()> {
    emit_package(src, package, ";");
    writeln!(src, "public enum {class_name} {{")?;
    let variants: Vec<String> = enum_type.variants.iter().map(|v| format!(r#"    {}("{}")"#, to_screaming_snake_case(v), v)).collect();
    writeln!(src, "{};", variants.join(",\n"))?;
    writedoc!(
        src,
        r#"

        private final String value;

        {class_name}(String value) {{
            this.value = value;
        }}

        public String getValue() {{
            return value;
        }}

        public static {class_name} fromValue(String value) {{
            for ({class_name} e : values()) {{
                if (e.value.equals(value)) {{
                    return e;
                }}
            }}
            throw new IllegalArgumentException("Unknown {class_name}: " + value);
        }}
    }}
    "#
    )?;
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
