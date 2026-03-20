use std::collections::BTreeSet;
use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::common::{
    emit_package, group_queries, has_inline_rows, needs_null_safe_getter, pg_array_type_name, querier_class_name, queries_class_name, row_type_name,
};
use crate::backend::jdbc::{
    self, collect_override_metadata, collect_table_imports, emit_dynamic_binds, emit_jdbc_binds, prepare_dynamic_sql_parts, prepare_sql_const,
    prepare_sql_const_from, preset_gson, preset_jackson, uses_get_object, ListAction,
};
use crate::backend::naming::{to_camel_case, to_pascal_case};
use crate::backend::GeneratedFile;
use crate::config::{resolve_type_override, ExtraField, Language, ListParamStrategy, OutputConfig, ResolvedType, TypeVariant};
use crate::ir::{Parameter, Query, QueryCmd, Schema, SqlType};

use super::adapter::JvmCoreContract;

/// Resolve a known Kotlin/Java preset name to a [`ResolvedType`].
fn try_preset_kotlin(name: &str) -> Option<ResolvedType> {
    match name {
        "jackson" => {
            let mut rt = preset_jackson("JsonNode::class.java", "private val objectMapper = ObjectMapper()");
            // Wrap read/write in helpers with null guards. ObjectMapper.readValue(null, …)
            // returns NullNode, and writeValueAsString(null) returns "null" — both wrong.
            rt.read_expr = Some("parseJson({raw})".to_string());
            rt.write_expr = Some("toJson({value})".to_string());
            rt.extra_fields.push(ExtraField {
                declaration: "private fun parseJson(raw: String): com.fasterxml.jackson.databind.JsonNode = objectMapper.readValue(raw, com.fasterxml.jackson.databind.JsonNode::class.java)".to_string(),
                import: None,
            });
            rt.extra_fields.push(ExtraField {
                declaration: "private fun toJson(value: com.fasterxml.jackson.databind.JsonNode?): String? = if (value == null) null else objectMapper.writeValueAsString(value)".to_string(),
                import: None,
            });
            Some(rt)
        },
        "gson" => {
            let mut rt = preset_gson("JsonElement::class.java", "private val gson = Gson()");
            rt.read_expr = Some("parseJson({raw})".to_string());
            rt.write_expr = Some("toJson({value})".to_string());
            rt.extra_fields.push(ExtraField {
                declaration: "private fun parseJson(raw: String): com.google.gson.JsonElement = gson.fromJson(raw, com.google.gson.JsonElement::class.java)"
                    .to_string(),
                import: None,
            });
            rt.extra_fields.push(ExtraField {
                declaration: "private fun toJson(value: com.google.gson.JsonElement?): String? = if (value == null) null else gson.toJson(value)".to_string(),
                import: None,
            });
            Some(rt)
        },
        _ => None,
    }
}

fn get_type_override_kotlin(sql_type: &SqlType, variant: TypeVariant, config: &OutputConfig) -> Option<ResolvedType> {
    resolve_type_override(sql_type, variant, config, Language::Kotlin, try_preset_kotlin)
}

fn kotlin_field_type(sql_type: &SqlType, nullable: bool, config: &OutputConfig) -> String {
    if let Some(resolved) = get_type_override_kotlin(sql_type, TypeVariant::Field, config) {
        return if nullable { format!("{}?", resolved.name) } else { resolved.name };
    }
    kotlin_type(sql_type, nullable)
}

fn kotlin_param_type_resolved(p: &Parameter, config: &OutputConfig) -> String {
    if p.is_list {
        let elem = if let Some(resolved) = get_type_override_kotlin(&p.sql_type, TypeVariant::Param, config) {
            resolved.name
        } else {
            kotlin_type(&p.sql_type, false)
        };
        return format!("List<{elem}>");
    }
    if let Some(resolved) = get_type_override_kotlin(&p.sql_type, TypeVariant::Param, config) {
        return if p.nullable { format!("{}?", resolved.name) } else { resolved.name };
    }
    kotlin_param_type(p)
}

/// Resolve a ResultSet read expression, applying any configured read_expr override.
///
/// - Override with `read_expr`: substitute `{raw}` with `rs.getString(idx)` — safe for
///   any JDBC driver regardless of whether it knows the override type.
/// - Override without `read_expr` on a `getObject` type: emit `rs.getObject(idx, T::class.java)`
///   using the override class name instead of the hardcoded default.
/// - No override: existing hardcoded expression.
fn resolve_kotlin_read_expr(sql_type: &SqlType, nullable: bool, idx: usize, config: &OutputConfig) -> String {
    if let Some(resolved) = get_type_override_kotlin(sql_type, TypeVariant::Field, config) {
        if let Some(expr) = &resolved.read_expr {
            if nullable {
                // Kotlin null-safety: call the read helper only when the raw value is non-null
                return format!("rs.getString({idx})?.let {{ {}  }}", expr.replace("{raw}", "it"));
            }
            return expr.replace("{raw}", &format!("rs.getString({idx})"));
        }
        if uses_get_object(sql_type) {
            return format!("rs.getObject({idx}, {}::class.java)", resolved.name);
        }
    }
    resultset_read_expr(sql_type, nullable, idx)
}

/// Return the JDBC bind value expression for a parameter, applying any configured write_expr.
///
/// Normally this is just the camel-case param name. When a write_expr is configured,
/// it wraps the param name (e.g. `objectMapper.writeValueAsString(payload)`).
fn kotlin_write_expr(p: &Parameter, config: &OutputConfig) -> String {
    let name = to_camel_case(&p.name);
    if let Some(resolved) = get_type_override_kotlin(&p.sql_type, TypeVariant::Param, config) {
        if let Some(expr) = &resolved.write_expr {
            return expr.replace("{value}", &name);
        }
    }
    name
}

/// Per-query context computed once in the dispatcher and forwarded to all emitters.
struct QueryContext<'a> {
    query: &'a Query,
    schema: &'a Schema,
    config: &'a OutputConfig,
    return_type: String,
    params_sig: String,
}

/// Emit all Kotlin files for the given schema and queries.
pub(super) fn generate_core_files(schema: &Schema, queries: &[Query], contract: &JvmCoreContract, config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
    let mut files = Vec::new();

    // One data class per table
    for table in &schema.tables {
        let class_name = to_pascal_case(&table.name);
        let mut src = String::new();
        emit_package(&mut src, &config.package, "");
        let table_imports = collect_table_imports(table, config, get_type_override_kotlin);
        for imp in &table_imports {
            writeln!(src, "import {imp}")?;
        }
        if !table_imports.is_empty() {
            writeln!(src)?;
        }
        writeln!(src, "data class {class_name}(")?;
        let params: Vec<String> = table
            .columns
            .iter()
            .map(|col| {
                let ty = kotlin_field_type(&col.sql_type, col.nullable, config);
                format!("    val {}: {}", to_camel_case(&col.name), ty)
            })
            .collect();
        writeln!(src, "{}", params.join(",\n"))?;
        writeln!(src, ")")?;

        let path = source_path(&config.out, &config.package, &class_name, "kt");
        files.push(GeneratedFile { path, content: src });
    }

    // One object per query group + one DataSource-backed wrapper class per group
    let strategy = config.list_params.clone().unwrap_or_default();
    for (group, group_queries) in group_queries(queries) {
        let class_name = queries_class_name(&group);
        let querier_name = querier_class_name(&group);

        let (override_imports, extra_fields) = collect_override_metadata(&group_queries, config, get_type_override_kotlin);

        let mut src = String::new();
        emit_package(&mut src, &config.package, "");
        // Emit override-specific imports (standard import is just Connection)
        writeln!(src, "import java.sql.Connection")?;
        for imp in &override_imports {
            writeln!(src, "import {imp}")?;
        }
        writeln!(src)?;
        writeln!(src, "object {class_name} {{")?;
        for ef in &extra_fields {
            writeln!(src, "    {}", ef.declaration)?;
        }

        for query in &group_queries {
            writeln!(src)?;
            if has_inline_rows(query, schema) {
                emit_row_class(&mut src, query, config)?;
                writeln!(src)?;
            }
            emit_kotlin_query(&mut src, query, schema, &strategy, contract, config)?;
        }

        writeln!(src)?;
        emit_nullable_primitive_helpers(&mut src)?;
        writeln!(src, "}}")?;

        let path = source_path(&config.out, &config.package, &class_name, "kt");
        files.push(GeneratedFile { path, content: src });

        let mut src = String::new();
        emit_package(&mut src, &config.package, "");
        emit_kotlin_querier(&mut src, &group_queries, schema, &class_name, &querier_name, contract, config, &override_imports, &extra_fields)?;
        let path = source_path(&config.out, &config.package, &querier_name, "kt");
        files.push(GeneratedFile { path, content: src });
    }

    Ok(files)
}

fn emit_kotlin_query(
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
        return_type: jdbc::jdbc_return_type(query, schema, contract.fallback_type, |r| format!("{r}?"), |r| format!("List<{r}>"), "Unit", "Long"),
        params_sig: std::iter::once("conn: Connection".to_string())
            .chain(query.params.iter().map(|p| format!("{}: {}", to_camel_case(&p.name), kotlin_param_type_resolved(p, config))))
            .collect::<Vec<_>>()
            .join(", "),
    };

    if let Some(lp) = query.params.iter().find(|p| p.is_list) {
        match jdbc::resolve_list_strategy(strategy, lp) {
            ListAction::PgNative(sql) => emit_kotlin_list_pg_native(src, &ctx, lp, &sql, contract),
            ListAction::Dynamic => emit_kotlin_list_dynamic(src, &ctx, lp, contract),
            ListAction::JsonNative(sql) => emit_kotlin_list_json_native(src, &ctx, lp, &sql, contract),
        }
    } else {
        emit_kotlin_scalar_query(src, &ctx, contract)
    }
}

/// Emit a Kotlin triple-quoted SQL constant with `.trimIndent()`.
///
/// Strips the trailing `;` from `raw_sql`, re-appends it to the last content line,
/// and uses `.trimIndent()` to strip the 8-space content indent at runtime.
/// Uses `val` (not `const val`) because `.trimIndent()` is not a constant expression.
fn emit_kotlin_sql_triple_quoted(src: &mut String, sql_const: &str, raw_sql: &str) -> anyhow::Result<()> {
    let trimmed = raw_sql.trim_end().trim_end_matches(';');
    let escaped = jdbc::escape_sql_triple_quoted(trimmed);
    let escaped = escape_kotlin_dollar(&escaped);
    writeln!(src, "    private val {sql_const} = \"\"\"")?;
    let mut lines = escaped.lines().peekable();
    while let Some(line) = lines.next() {
        if lines.peek().is_none() {
            writeln!(src, "        {line};")?;
        } else {
            writeln!(src, "        {line}")?;
        }
    }
    writeln!(src, "    \"\"\".trimIndent()")?;
    Ok(())
}

/// Escape `$` followed by an identifier character to prevent Kotlin string interpolation.
///
/// In Kotlin string templates (including raw `"""` strings), `$name` and `${expr}`
/// trigger interpolation. After JDBC placeholder rewriting all `$N` become `?`,
/// so this handles the rare case where SQL contains `$`-prefixed identifiers.
fn escape_kotlin_dollar(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '$' {
            if let Some(&next) = chars.peek() {
                if next.is_alphabetic() || next == '_' || next == '{' {
                    result.push_str("${'$'}");
                    continue;
                }
            }
        }
        result.push(c);
    }
    result
}

fn emit_kotlin_scalar_query(src: &mut String, ctx: &QueryContext, contract: &JvmCoreContract) -> anyhow::Result<()> {
    let (sql_const, raw_sql) = prepare_sql_const(ctx.query);
    emit_kotlin_sql_triple_quoted(src, &sql_const, &raw_sql)?;
    writeln!(src, "    fun {}({}): {} {{", to_camel_case(&ctx.query.name), ctx.params_sig, ctx.return_type)?;
    writeln!(src, "        conn.prepareStatement({sql_const}).use {{ ps ->")?;
    emit_jdbc_binds(src, ctx.query, "", contract.statement_end, "toTypedArray()", contract.json_bind, |p| kotlin_write_expr(p, ctx.config))?;
    emit_kotlin_result_block(src, ctx, contract)?;
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit a PostgreSQL native list query using `= ANY(?)` with a JDBC array.
fn emit_kotlin_list_pg_native(src: &mut String, ctx: &QueryContext, lp: &Parameter, rewritten_sql: &str, contract: &JvmCoreContract) -> anyhow::Result<()> {
    let lp_name = to_camel_case(&lp.name);
    let method_name = to_camel_case(&ctx.query.name);
    let (sql_const, raw_sql) = prepare_sql_const_from(ctx.query, rewritten_sql);
    emit_kotlin_sql_triple_quoted(src, &sql_const, &raw_sql)?;
    writeln!(src, "    fun {method_name}({}): {} {{", ctx.params_sig, ctx.return_type)?;
    let type_name = pg_array_type_name(&lp.sql_type);
    writeln!(src, "        val arr = conn.createArrayOf(\"{type_name}\", {lp_name}.toTypedArray())")?;
    writeln!(src, "        conn.prepareStatement({sql_const}).use {{ ps ->")?;
    emit_jdbc_binds(src, ctx.query, "arr", contract.statement_end, "toTypedArray()", contract.json_bind, |p| kotlin_write_expr(p, ctx.config))?;
    emit_kotlin_result_block(src, ctx, contract)?;
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit a dynamic list query that builds `IN (?,?,…,?)` at runtime.
fn emit_kotlin_list_dynamic(src: &mut String, ctx: &QueryContext, lp: &Parameter, contract: &JvmCoreContract) -> anyhow::Result<()> {
    let lp_name = to_camel_case(&lp.name);
    let method_name = to_camel_case(&ctx.query.name);
    let (before_esc, after_esc) = prepare_dynamic_sql_parts(ctx.query, lp);
    writeln!(src, "    fun {method_name}({}): {} {{", ctx.params_sig, ctx.return_type)?;
    writeln!(src, "        val marks = {lp_name}.joinToString(\", \") {{ \"?\" }}")?;
    writeln!(src, "        val sql = \"{before_esc}\" + \"IN (${{marks}}){after_esc};\"")?;
    writeln!(src, "        conn.prepareStatement(sql).use {{ ps ->")?;
    emit_dynamic_binds(src, ctx.query, lp, contract.statement_end, contract.size_access, &|src, lp_name, base, setter| {
        writeln!(src, "            {lp_name}.forEachIndexed {{ i, v -> ps.{setter}({base} + i + 1, v) }}")?;
        Ok(())
    })?;
    emit_kotlin_result_block(src, ctx, contract)?;
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit a SQLite or MySQL native list query that passes a JSON array string.
///
/// Both engines use the same structure: build a JSON string from the list,
/// then bind it as a regular string parameter. The caller provides the
/// already-rewritten SQL (with `json_each` or `JSON_TABLE`).
fn emit_kotlin_list_json_native(src: &mut String, ctx: &QueryContext, lp: &Parameter, rewritten_sql: &str, contract: &JvmCoreContract) -> anyhow::Result<()> {
    let method_name = to_camel_case(&ctx.query.name);
    let (sql_const, raw_sql) = prepare_sql_const_from(ctx.query, rewritten_sql);
    emit_kotlin_sql_triple_quoted(src, &sql_const, &raw_sql)?;
    writeln!(src, "    fun {method_name}({}): {} {{", ctx.params_sig, ctx.return_type)?;
    emit_kotlin_json_builder(src, lp)?;
    writeln!(src, "        conn.prepareStatement({sql_const}).use {{ ps ->")?;
    emit_jdbc_binds(src, ctx.query, "json", contract.statement_end, "toTypedArray()", contract.json_bind, |p| kotlin_write_expr(p, ctx.config))?;
    emit_kotlin_result_block(src, ctx, contract)?;
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit the `val json = …` line that builds a JSON array from a list param.
///
/// Text-like types need per-element quoting and escaping; numeric/boolean types
/// can use plain `joinToString`.
fn emit_kotlin_json_builder(src: &mut String, lp: &Parameter) -> anyhow::Result<()> {
    let lp_name = to_camel_case(&lp.name);
    if lp.sql_type.needs_json_quoting() {
        writeln!(
            src,
            r#"        val json = "[" + {lp_name}.joinToString(",") {{ "\"" + it.toString().replace("\\", "\\\\").replace("\"", "\\\"") + "\"" }} + "]""#
        )?;
    } else {
        writeln!(src, "        val json = \"[\" + {lp_name}.joinToString(\",\") + \"]\"")?;
    }
    Ok(())
}

/// Emit the result block inside `ps.use { ps -> ... }`.
fn emit_kotlin_result_block(src: &mut String, ctx: &QueryContext, contract: &JvmCoreContract) -> anyhow::Result<()> {
    match ctx.query.cmd {
        QueryCmd::Exec => writeln!(src, "            ps.executeUpdate()")?,
        QueryCmd::ExecRows => writeln!(src, "            return ps.executeUpdate().toLong()")?,
        QueryCmd::One => {
            writeln!(src, "            ps.executeQuery().use {{ rs ->")?;
            writeln!(src, "                if (!rs.next()) return null")?;
            writeln!(src, "                return {}", emit_row_constructor(ctx.query, ctx.schema, ctx.config, contract))?;
            writeln!(src, "            }}")?;
        },
        QueryCmd::Many => {
            let row_type = result_row_type(ctx.query, ctx.schema, contract);
            writeln!(src, "            val rows = mutableListOf<{row_type}>()")?;
            writeln!(src, "            ps.executeQuery().use {{ rs ->")?;
            writeln!(src, "                while (rs.next()) rows.add({})", emit_row_constructor(ctx.query, ctx.schema, ctx.config, contract))?;
            writeln!(src, "            }}")?;
            writeln!(src, "            return rows")?;
        },
    }
    Ok(())
}

/// Return the Kotlin type for a parameter (uses `List<T>` for list params).
fn kotlin_param_type(p: &Parameter) -> String {
    if p.is_list {
        format!("List<{}>", kotlin_type(&p.sql_type, false))
    } else {
        kotlin_type(&p.sql_type, p.nullable)
    }
}

/// Emits a DataSource-backed querier class that acquires a connection
/// per call via `dataSource.connection.use { }` and delegates to `{class_name}`.
fn emit_kotlin_querier(
    src: &mut String,
    queries: &[Query],
    schema: &Schema,
    class_name: &str,
    querier_name: &str,
    contract: &JvmCoreContract,
    config: &OutputConfig,
    override_imports: &BTreeSet<String>,
    extra_fields: &[ExtraField],
) -> anyhow::Result<()> {
    // Emit all imports: standard + any type-override imports, sorted.
    let mut all_imports: BTreeSet<String> = ["javax.sql.DataSource".to_string()].into();
    all_imports.extend(override_imports.iter().cloned());
    for imp in &all_imports {
        writeln!(src, "import {imp}")?;
    }

    writeln!(src)?;
    writeln!(src, "class {querier_name}(private val dataSource: DataSource) {{")?;
    for ef in extra_fields {
        writeln!(src, "    {}", ef.declaration)?;
    }

    for query in queries {
        writeln!(src)?;
        emit_kotlin_querier_method(src, query, schema, class_name, contract, config)?;
    }

    writeln!(src, "}}")?;
    Ok(())
}

/// Emits one method on the querier class that wraps the corresponding method in `{class_name}`.
fn emit_kotlin_querier_method(
    src: &mut String,
    query: &Query,
    schema: &Schema,
    class_name: &str,
    contract: &JvmCoreContract,
    config: &OutputConfig,
) -> anyhow::Result<()> {
    let row = jdbc::ds_result_row_type(query, schema, contract.fallback_type, class_name);
    let return_type = match query.cmd {
        QueryCmd::One => format!("{row}?"),
        QueryCmd::Many => format!("List<{row}>"),
        QueryCmd::Exec => "Unit".to_string(),
        QueryCmd::ExecRows => "Long".to_string(),
    };

    let params_sig: String =
        query.params.iter().map(|p| format!("{}: {}", to_camel_case(&p.name), kotlin_param_type_resolved(p, config))).collect::<Vec<_>>().join(", ");

    let method_name = to_camel_case(&query.name);
    let args: String = query.params.iter().map(|p| to_camel_case(&p.name)).collect::<Vec<_>>().join(", ");
    let call_args = if args.is_empty() { "conn".to_string() } else { format!("conn, {args}") };

    writeln!(src, "    fun {method_name}({params_sig}): {return_type} =")?;
    writeln!(src, "        dataSource.connection.use {{ conn -> {class_name}.{method_name}({call_args}) }}")?;
    Ok(())
}

fn result_row_type(query: &Query, schema: &Schema, contract: &JvmCoreContract) -> String {
    jdbc::result_row_type(query, schema, contract.fallback_type)
}

fn emit_row_class(src: &mut String, query: &Query, config: &OutputConfig) -> anyhow::Result<()> {
    let name = row_type_name(&query.name);
    writeln!(src, "    data class {name}(")?;
    let fields: Vec<String> = query
        .result_columns
        .iter()
        .map(|col| format!("        val {}: {}", to_camel_case(&col.name), kotlin_field_type(&col.sql_type, col.nullable, config)))
        .collect();
    writeln!(src, "{}", fields.join(",\n"))?;
    writeln!(src, "    )")?;
    Ok(())
}

fn emit_row_constructor(query: &Query, schema: &Schema, config: &OutputConfig, contract: &JvmCoreContract) -> String {
    jdbc::build_row_constructor(query, schema, contract.fallback_type, "", |sql_type, nullable, idx| resolve_kotlin_read_expr(sql_type, nullable, idx, config))
}

// ─── Type helpers ─────────────────────────────────────────────────────────────

fn kotlin_type(sql_type: &SqlType, nullable: bool) -> String {
    let base = match sql_type {
        SqlType::Boolean => "Boolean",
        SqlType::SmallInt => "Short",
        SqlType::Integer => "Int",
        SqlType::BigInt => "Long",
        SqlType::Real => "Float",
        SqlType::Double => "Double",
        SqlType::Decimal => "java.math.BigDecimal",
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => "String",
        SqlType::Bytes => "ByteArray",
        SqlType::Date => "java.time.LocalDate",
        SqlType::Time => "java.time.LocalTime",
        SqlType::Timestamp => "java.time.LocalDateTime",
        SqlType::TimestampTz => "java.time.OffsetDateTime",
        SqlType::Interval => "String",
        SqlType::Uuid => "java.util.UUID",
        SqlType::Json | SqlType::Jsonb => "String",
        SqlType::Array(inner) => {
            let t = format!("List<{}>", kotlin_type(inner, false));
            return if nullable { format!("{t}?") } else { t };
        },
        SqlType::Custom(_) => "Any",
    };
    if nullable {
        format!("{base}?")
    } else {
        base.to_string()
    }
}

fn resultset_read_expr(sql_type: &SqlType, nullable: bool, idx: usize) -> String {
    // Primitive getters return 0/false for SQL NULL. For nullable primitive columns,
    // use getObject with the Java boxed type so the result can be null,
    // matching the nullable Kotlin type (e.g. Long? instead of Long).
    if nullable && needs_null_safe_getter(sql_type) {
        // Call private getNullable* helpers that use wasNull() — compatible with all
        // JDBC drivers including SQLite, which rejects getObject(col, Integer::class.java)
        // for NULL values.
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
        SqlType::Date => format!("rs.getObject({idx}, java.time.LocalDate::class.java)"),
        SqlType::Time => format!("rs.getObject({idx}, java.time.LocalTime::class.java)"),
        SqlType::Timestamp => format!("rs.getObject({idx}, java.time.LocalDateTime::class.java)"),
        SqlType::TimestampTz => format!("rs.getObject({idx}, java.time.OffsetDateTime::class.java)"),
        SqlType::Uuid => format!("rs.getObject({idx}, java.util.UUID::class.java)"),
        SqlType::Array(inner) => jdbc_array_read_expr(inner, nullable, idx),
        _ => format!("rs.getObject({idx})"),
    }
}

/// Emit private helper functions for null-safe reads of primitive JDBC columns.
///
/// `rs.getObject(col, Integer::class.java)` throws in SQLite JDBC when the column is NULL.
/// These helpers use the `wasNull()` idiom which works across all JDBC drivers.
fn emit_nullable_primitive_helpers(src: &mut String) -> anyhow::Result<()> {
    let helpers = [
        ("Boolean", "Boolean", "getBoolean"),
        ("Short", "Short", "getShort"),
        ("Int", "Integer", "getInt"),
        ("Long", "Long", "getLong"),
        ("Float", "Float", "getFloat"),
        ("Double", "Double", "getDouble"),
    ];
    for (kt, _java, getter) in helpers {
        writeln!(src, "    private fun getNullable{kt}(rs: java.sql.ResultSet, col: Int): {kt}? {{")?;
        writeln!(src, "        val v = rs.{getter}(col)")?;
        writeln!(src, "        return if (rs.wasNull()) null else v")?;
        writeln!(src, "    }}")?;
    }
    Ok(())
}

/// Build a JDBC expression that reads a SQL ARRAY column and converts it to `List<T>`.
fn jdbc_array_read_expr(inner: &SqlType, nullable: bool, idx: usize) -> String {
    let kt = kotlin_type(inner, false);
    if nullable {
        format!("rs.getArray({idx})?.let {{ (it.array as Array<{kt}>).toList() }}")
    } else {
        format!("(rs.getArray({idx}).array as Array<{kt}>).toList()")
    }
}

/// Test shim: exposes `kotlin_type` to the parent module's `#[cfg(test)]` helpers.
#[cfg(test)]
pub(super) fn kotlin_type_pub(sql_type: &SqlType, nullable: bool) -> String {
    kotlin_type(sql_type, nullable)
}

/// Test shim: exposes `resultset_read_expr` to the parent module's `#[cfg(test)]` helpers.
#[cfg(test)]
pub(super) fn resultset_read_expr_pub(sql_type: &SqlType, nullable: bool, idx: usize) -> String {
    resultset_read_expr(sql_type, nullable, idx)
}

// ─── Path helper ──────────────────────────────────────────────────────────────

fn source_path(out: &str, package: &str, name: &str, ext: &str) -> PathBuf {
    let pkg_path = package.replace('.', "/");
    PathBuf::from(out).join(pkg_path).join(format!("{name}.{ext}"))
}
