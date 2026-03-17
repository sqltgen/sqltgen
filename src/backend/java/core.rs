use std::collections::BTreeSet;
use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::common::{
    emit_package, group_queries, has_inline_rows, needs_null_safe_getter, pg_array_type_name, querier_class_name, queries_class_name,
};
use crate::backend::jdbc::{
    self, emit_dynamic_binds, emit_jdbc_binds, prepare_dynamic_sql_parts, prepare_sql_const, prepare_sql_const_from, uses_get_object, ListAction,
};
use crate::backend::naming::{to_camel_case, to_pascal_case};
use crate::backend::GeneratedFile;
use crate::config::{
    is_known_type_preset, resolve_type_ref, warn_unsupported_type_preset, ExtraField, Language, ListParamStrategy, OutputConfig, ResolvedType, TypeVariant,
};
use crate::ir::{Parameter, Query, QueryCmd, Schema, SqlType};

use super::adapter::JvmCoreContract;

/// Resolve a known Java/Kotlin preset name to a [`ResolvedType`].
///
/// Returns `None` for unknown names (handled by [`resolve_type_ref`] instead).
fn try_preset_java(name: &str) -> Option<ResolvedType> {
    match name {
        "jackson" => Some(ResolvedType {
            name: "JsonNode".to_string(),
            import: Some("com.fasterxml.jackson.databind.JsonNode".to_string()),
            read_expr: Some("objectMapper.readValue({raw}, JsonNode.class)".to_string()),
            write_expr: Some("objectMapper.writeValueAsString({value})".to_string()),
            extra_fields: vec![ExtraField {
                declaration: "private static final ObjectMapper objectMapper = new ObjectMapper();".to_string(),
                import: Some("com.fasterxml.jackson.databind.ObjectMapper".to_string()),
            }],
        }),
        "gson" => Some(ResolvedType {
            name: "JsonElement".to_string(),
            import: Some("com.google.gson.JsonElement".to_string()),
            read_expr: Some("GSON.fromJson({raw}, JsonElement.class)".to_string()),
            write_expr: Some("GSON.toJson({value})".to_string()),
            extra_fields: vec![ExtraField {
                declaration: "private static final Gson GSON = new Gson();".to_string(),
                import: Some("com.google.gson.Gson".to_string()),
            }],
        }),
        _ => None,
    }
}

/// Resolve a type override for the given SQL type and variant, combining preset lookup
/// with the generic [`resolve_type_ref`] fallback.
fn get_type_override_java(sql_type: &SqlType, variant: TypeVariant, config: &OutputConfig) -> Option<ResolvedType> {
    let type_ref = config.get_type_ref(sql_type, variant)?;
    if let crate::config::TypeRef::String(s) = type_ref {
        if let Some(r) = try_preset_java(s) {
            return Some(r);
        }
        if is_known_type_preset(s) {
            warn_unsupported_type_preset(Language::Java, s, sql_type, variant);
            return None;
        }
    }
    resolve_type_ref(type_ref)
}

/// Return the Java field type for a SQL type, applying any configured type override first.
fn java_field_type(sql_type: &SqlType, nullable: bool, config: &OutputConfig) -> String {
    if let Some(resolved) = get_type_override_java(sql_type, TypeVariant::Field, config) {
        return apply_nullable_java(&resolved.name, nullable, false);
    }
    java_type(sql_type, nullable)
}

/// Return the Java parameter type, applying any configured param type override first.
fn java_param_type_resolved(p: &Parameter, config: &OutputConfig) -> String {
    if p.is_list {
        let elem =
            if let Some(resolved) = get_type_override_java(&p.sql_type, TypeVariant::Param, config) { resolved.name } else { java_type_boxed(&p.sql_type) };
        return format!("List<{elem}>");
    }
    if let Some(resolved) = get_type_override_java(&p.sql_type, TypeVariant::Param, config) {
        return apply_nullable_java(&resolved.name, p.nullable, false);
    }
    java_param_type(p)
}

/// Apply Java nullability to a type name.
///
/// Primitive types use boxed names when nullable; reference types are always the same.
/// `is_array` controls the `@Nullable` annotation path for List types.
fn apply_nullable_java(name: &str, nullable: bool, is_array: bool) -> String {
    if nullable && is_array {
        format!("@Nullable {name}")
    } else {
        name.to_string()
    }
}

/// Return the JDBC bind value expression for a parameter, applying any configured write_expr.
///
/// Normally this is just the camel-case param name. When a write_expr is configured,
/// it wraps the param name (e.g. `objectMapper.writeValueAsString(payload)`).
fn java_write_expr(p: &Parameter, config: &OutputConfig) -> String {
    let name = to_camel_case(&p.name);
    if let Some(resolved) = get_type_override_java(&p.sql_type, TypeVariant::Param, config) {
        if let Some(expr) = &resolved.write_expr {
            return expr.replace("{value}", &name);
        }
    }
    name
}

/// Resolve a ResultSet read expression, applying any configured read_expr override.
///
/// - Override with `read_expr`: substitute `{raw}` with `rs.getString(idx)` — safe for
///   any JDBC driver regardless of whether it knows the override type.
/// - Override without `read_expr` on a `getObject` type: emit `rs.getObject(idx, T.class)`
///   using the override class name instead of the hardcoded default.
/// - No override: existing hardcoded expression.
fn resolve_java_read_expr(sql_type: &SqlType, nullable: bool, idx: usize, config: &OutputConfig) -> String {
    if let Some(resolved) = get_type_override_java(sql_type, TypeVariant::Field, config) {
        if let Some(expr) = &resolved.read_expr {
            return expr.replace("{raw}", &format!("rs.getString({idx})"));
        }
        if uses_get_object(sql_type) {
            return format!("rs.getObject({idx}, {}.class)", resolved.name);
        }
    }
    resultset_read_expr(sql_type, nullable, idx)
}

/// Collect override-specific imports and extra_fields across all queries in a group.
/// Collect override imports needed by a table's columns for its record file.
fn collect_table_java_imports(table: &crate::ir::Table, config: &OutputConfig) -> BTreeSet<String> {
    let mut imports: BTreeSet<String> = BTreeSet::new();
    for col in &table.columns {
        if let Some(resolved) = get_type_override_java(&col.sql_type, TypeVariant::Field, config) {
            if let Some(imp) = resolved.import {
                imports.insert(imp);
            }
        }
    }
    imports
}

fn collect_java_override_metadata(queries: &[Query], config: &OutputConfig) -> (BTreeSet<String>, Vec<ExtraField>) {
    let mut imports: BTreeSet<String> = BTreeSet::new();
    let mut extra_fields: Vec<ExtraField> = Vec::new();
    for query in queries {
        for col in &query.result_columns {
            if let Some(resolved) = get_type_override_java(&col.sql_type, TypeVariant::Field, config) {
                if let Some(imp) = resolved.import {
                    imports.insert(imp);
                }
                for ef in resolved.extra_fields {
                    if let Some(imp) = &ef.import {
                        imports.insert(imp.clone());
                    }
                    if !extra_fields.iter().any(|e| e.declaration == ef.declaration) {
                        extra_fields.push(ef);
                    }
                }
            }
        }
        for p in &query.params {
            if let Some(resolved) = get_type_override_java(&p.sql_type, TypeVariant::Param, config) {
                if let Some(imp) = resolved.import {
                    imports.insert(imp);
                }
            }
        }
    }
    (imports, extra_fields)
}

/// Per-query context computed once in the dispatcher and forwarded to all emitters.
struct QueryContext<'a> {
    query: &'a Query,
    schema: &'a Schema,
    config: &'a OutputConfig,
    return_type: String,
    params_sig: String,
}

/// Emit all Java files for the given schema and queries.
pub(super) fn generate_core_files(schema: &Schema, queries: &[Query], contract: &JvmCoreContract, config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
    let mut files = Vec::new();

    // One record class per table
    for table in &schema.tables {
        let class_name = to_pascal_case(&table.name);
        let mut src = String::new();
        emit_package(&mut src, &config.package, ";");
        let table_imports = collect_table_java_imports(table, config);
        for imp in &table_imports {
            writeln!(src, "import {imp};")?;
        }
        if !table_imports.is_empty() {
            writeln!(src)?;
        }
        writeln!(src, "public record {class_name}(")?;
        let params: Vec<String> = table
            .columns
            .iter()
            .map(|col| {
                let ty = java_field_type(&col.sql_type, col.nullable, config);
                format!("    {} {}", ty, to_camel_case(&col.name))
            })
            .collect();
        writeln!(src, "{}", params.join(",\n"))?;
        writeln!(src, ") {{}}")?;

        let path = record_path(&config.out, &config.package, &class_name);
        files.push(GeneratedFile { path, content: src });
    }

    // One class per query group + one DataSource-backed wrapper class per group
    let strategy = config.list_params.clone().unwrap_or_default();
    for (group, group_queries) in group_queries(queries) {
        let class_name = queries_class_name(&group);
        let querier_name = querier_class_name(&group);

        let (override_imports, extra_fields) = collect_java_override_metadata(&group_queries, config);

        let mut src = String::new();
        emit_package(&mut src, &config.package, ";");
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
        all_imports.extend(override_imports);
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
                emit_row_record(&mut src, query, config)?;
                writeln!(src)?;
            }
            emit_java_query(&mut src, query, schema, &strategy, contract, config)?;
        }

        writeln!(src)?;
        emit_nullable_primitive_helpers(&mut src)?;
        writeln!(src, "}}")?;

        let path = record_path(&config.out, &config.package, &class_name);
        files.push(GeneratedFile { path, content: src });

        let mut src = String::new();
        emit_package(&mut src, &config.package, ";");
        emit_java_querier(&mut src, &group_queries, schema, &class_name, &querier_name, contract)?;
        let path = record_path(&config.out, &config.package, &querier_name);
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
    config: &OutputConfig,
) -> anyhow::Result<()> {
    let ctx = QueryContext {
        query,
        schema,
        config,
        return_type: jdbc::jdbc_return_type(query, schema, contract.fallback_type, |r| format!("Optional<{r}>"), |r| format!("List<{r}>"), "void", "long"),
        params_sig: std::iter::once("Connection conn".to_string())
            .chain(query.params.iter().map(|p| format!("{} {}", java_param_type_resolved(p, config), to_camel_case(&p.name))))
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
    emit_jdbc_binds(src, ctx.query, "", contract.statement_end, "toArray()", |p| java_write_expr(p, ctx.config))?;
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
    emit_jdbc_binds(src, ctx.query, "arr", contract.statement_end, "toArray()", |p| java_write_expr(p, ctx.config))?;
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
    emit_dynamic_binds(src, ctx.query, lp, contract.statement_end, &|src, lp_name, base, setter| {
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
    emit_jdbc_binds(src, ctx.query, "json", contract.statement_end, "toArray()", |p| java_write_expr(p, ctx.config))?;
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
            writeln!(src, "                return Optional.of({});", emit_row_constructor(ctx.query, ctx.schema, ctx.config, contract))?;
            writeln!(src, "            }}")?;
        },
        QueryCmd::Many => {
            let row_type = result_row_type(ctx.query, ctx.schema, contract);
            writeln!(src, "            List<{row_type}> rows = new ArrayList<>();")?;
            writeln!(src, "            try (ResultSet rs = ps.executeQuery()) {{")?;
            writeln!(src, "                while (rs.next()) rows.add({});", emit_row_constructor(ctx.query, ctx.schema, ctx.config, contract))?;
            writeln!(src, "            }}")?;
            writeln!(src, "            return rows;")?;
        },
    }
    Ok(())
}

/// Return the Java type for a parameter (no override), using `List<T>` for list params.
fn java_param_type(p: &Parameter) -> String {
    if p.is_list {
        format!("List<{}>", java_type_boxed(&p.sql_type))
    } else {
        java_type(&p.sql_type, p.nullable)
    }
}

/// Emits a DataSource-backed querier wrapper that acquires a connection
/// per call and delegates to the static methods in `{class_name}`.
fn emit_java_querier(
    src: &mut String,
    queries: &[Query],
    schema: &Schema,
    class_name: &str,
    querier_name: &str,
    contract: &JvmCoreContract,
) -> anyhow::Result<()> {
    let has_one = queries.iter().any(|q| q.cmd == QueryCmd::One);
    let has_many = queries.iter().any(|q| q.cmd == QueryCmd::Many);

    writeln!(src, "import java.sql.Connection;")?;
    writeln!(src, "import java.sql.SQLException;")?;
    if has_many {
        writeln!(src, "import java.util.List;")?;
    }
    if has_one {
        writeln!(src, "import java.util.Optional;")?;
    }
    writeln!(src, "import javax.sql.DataSource;")?;
    writeln!(src)?;
    writeln!(src, "public final class {querier_name} {{")?;
    writeln!(src, "    private final DataSource dataSource;")?;
    writeln!(src)?;
    writeln!(src, "    public {querier_name}(DataSource dataSource) {{")?;
    writeln!(src, "        this.dataSource = dataSource;")?;
    writeln!(src, "    }}")?;

    for query in queries {
        writeln!(src)?;
        emit_java_querier_method(src, query, schema, class_name, contract)?;
    }

    writeln!(src, "}}")?;
    Ok(())
}

/// Emits one instance method on the querier class that wraps the corresponding static method.
fn emit_java_querier_method(src: &mut String, query: &Query, schema: &Schema, class_name: &str, contract: &JvmCoreContract) -> anyhow::Result<()> {
    let row = jdbc::ds_result_row_type(query, schema, contract.fallback_type, class_name);
    let return_type = match query.cmd {
        QueryCmd::One => format!("Optional<{row}>"),
        QueryCmd::Many => format!("List<{row}>"),
        QueryCmd::Exec => "void".to_string(),
        QueryCmd::ExecRows => "long".to_string(),
    };

    let params_sig: String = query.params.iter().map(|p| format!("{} {}", java_param_type(p), to_camel_case(&p.name))).collect::<Vec<_>>().join(", "); // Ds uses plain types (no config needed — matches what callers expect)

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

fn emit_row_record(src: &mut String, query: &Query, config: &OutputConfig) -> anyhow::Result<()> {
    let name = format!("{}Row", to_pascal_case(&query.name));
    writeln!(src, "    public record {name}(")?;
    let fields: Vec<String> = query
        .result_columns
        .iter()
        .map(|col| format!("        {} {}", java_field_type(&col.sql_type, col.nullable, config), to_camel_case(&col.name)))
        .collect();
    writeln!(src, "{}", fields.join(",\n"))?;
    writeln!(src, "    ) {{}}")?;
    Ok(())
}

fn emit_row_constructor(query: &Query, schema: &Schema, config: &OutputConfig, contract: &JvmCoreContract) -> String {
    jdbc::build_row_constructor(query, schema, contract.fallback_type, "new ", |sql_type, nullable, idx| {
        resolve_java_read_expr(sql_type, nullable, idx, config)
    })
}

// ─── Type helpers ─────────────────────────────────────────────────────────────

fn java_type(sql_type: &SqlType, nullable: bool) -> String {
    // Each variant maps to (non_nullable_type, nullable_type). Java primitives
    // use their boxed counterparts when nullable; reference types are the same
    // regardless of nullability.
    let (base, boxed) = match sql_type {
        SqlType::Boolean => ("boolean", "Boolean"),
        SqlType::SmallInt => ("short", "Short"),
        SqlType::Integer => ("int", "Integer"),
        SqlType::BigInt => ("long", "Long"),
        SqlType::Real => ("float", "Float"),
        SqlType::Double => ("double", "Double"),
        SqlType::Decimal => ("java.math.BigDecimal", "java.math.BigDecimal"),
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => ("String", "String"),
        SqlType::Bytes => ("byte[]", "byte[]"),
        SqlType::Date => ("java.time.LocalDate", "java.time.LocalDate"),
        SqlType::Time => ("java.time.LocalTime", "java.time.LocalTime"),
        SqlType::Timestamp => ("java.time.LocalDateTime", "java.time.LocalDateTime"),
        SqlType::TimestampTz => ("java.time.OffsetDateTime", "java.time.OffsetDateTime"),
        SqlType::Interval => ("String", "String"),
        SqlType::Uuid => ("java.util.UUID", "java.util.UUID"),
        SqlType::Json | SqlType::Jsonb => ("String", "String"),
        SqlType::Array(inner) => {
            let t = format!("java.util.List<{}>", java_type_boxed(inner));
            return if nullable { format!("@Nullable {t}") } else { t };
        },
        SqlType::Custom(_) => ("Object", "Object"),
    };
    (if nullable { boxed } else { base }).into()
}

fn java_type_boxed(sql_type: &SqlType) -> String {
    match sql_type {
        SqlType::Boolean => "Boolean".into(),
        SqlType::SmallInt => "Short".into(),
        SqlType::Integer => "Integer".into(),
        SqlType::BigInt => "Long".into(),
        SqlType::Real => "Float".into(),
        SqlType::Double => "Double".into(),
        other => java_type(other, false),
    }
}

fn resultset_read_expr(sql_type: &SqlType, nullable: bool, idx: usize) -> String {
    // Primitive getters (getInt, getBoolean, …) return 0/false for SQL NULL.
    // For nullable primitive columns we call private getNullable* helpers that
    // use wasNull() — this is compatible with all JDBC drivers including SQLite,
    // which does not support getObject(col, Integer.class) for NULL values.
    if nullable && needs_null_safe_getter(sql_type) {
        return match sql_type {
            SqlType::Boolean => format!("getNullableBoolean(rs, {idx})"),
            SqlType::SmallInt => format!("getNullableShort(rs, {idx})"),
            SqlType::Integer => format!("getNullableInt(rs, {idx})"),
            SqlType::BigInt => format!("getNullableLong(rs, {idx})"),
            SqlType::Real => format!("getNullableFloat(rs, {idx})"),
            SqlType::Double => format!("getNullableDouble(rs, {idx})"),
            _ => unreachable!("needs_null_safe_getter returned true for non-primitive"),
        };
    }
    match sql_type {
        SqlType::Boolean => format!("rs.getBoolean({idx})"),
        SqlType::SmallInt => format!("rs.getShort({idx})"),
        SqlType::Integer => format!("rs.getInt({idx})"),
        SqlType::BigInt => format!("rs.getLong({idx})"),
        SqlType::Real => format!("rs.getFloat({idx})"),
        SqlType::Double => format!("rs.getDouble({idx})"),
        SqlType::Decimal => format!("rs.getBigDecimal({idx})"),
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) | SqlType::Json | SqlType::Jsonb | SqlType::Interval => format!("rs.getString({idx})"),
        SqlType::Bytes => format!("rs.getBytes({idx})"),
        SqlType::Date => format!("rs.getObject({idx}, java.time.LocalDate.class)"),
        SqlType::Time => format!("rs.getObject({idx}, java.time.LocalTime.class)"),
        SqlType::Timestamp => format!("rs.getObject({idx}, java.time.LocalDateTime.class)"),
        SqlType::TimestampTz => format!("rs.getObject({idx}, java.time.OffsetDateTime.class)"),
        SqlType::Uuid => format!("rs.getObject({idx}, java.util.UUID.class)"),
        SqlType::Array(inner) => jdbc_array_read_expr(inner, nullable, idx),
        _ => format!("rs.getObject({idx})"),
    }
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

/// Build a JDBC expression that reads a SQL ARRAY column and converts it to `java.util.List<T>`.
fn jdbc_array_read_expr(inner: &SqlType, nullable: bool, idx: usize) -> String {
    let boxed = java_type_boxed(inner);
    if nullable {
        format!("rs.getArray({idx}) == null ? null : java.util.Arrays.asList(({boxed}[]) rs.getArray({idx}).getArray())")
    } else {
        format!("java.util.Arrays.asList(({boxed}[]) rs.getArray({idx}).getArray())")
    }
}

/// Test shim: exposes `java_type` to the parent module's `#[cfg(test)]` helpers.
#[cfg(test)]
pub(super) fn java_type_pub(sql_type: &SqlType, nullable: bool) -> String {
    java_type(sql_type, nullable)
}

/// Test shim: exposes `resultset_read_expr` to the parent module's `#[cfg(test)]` helpers.
#[cfg(test)]
pub(super) fn resultset_read_expr_pub(sql_type: &SqlType, nullable: bool, idx: usize) -> String {
    resultset_read_expr(sql_type, nullable, idx)
}

// ─── Path helper ──────────────────────────────────────────────────────────────

fn record_path(out: &str, package: &str, class_name: &str) -> PathBuf {
    let pkg_path = package.replace('.', "/");
    PathBuf::from(out).join(pkg_path).join(format!("{class_name}.java"))
}
