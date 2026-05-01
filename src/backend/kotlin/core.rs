use std::collections::BTreeSet;
use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::common::{
    emit_package, group_queries, has_inline_rows, infer_table, model_name, needed_enums, pg_array_type_name, querier_class_name, queries_class_name,
    row_type_name,
};
use crate::backend::jdbc::{
    self, emit_dynamic_binds, emit_jdbc_binds, prepare_dynamic_sql_parts, prepare_sql_const, prepare_sql_const_from, ListAction, QuerierContext,
};
use crate::backend::naming::{to_camel_case, to_pascal_case, to_screaming_snake_case};
use crate::backend::GeneratedFile;
use crate::config::{ExtraField, ListParamStrategy, OutputConfig};
use crate::ir::{EnumType, Parameter, Query, QueryCmd, Schema, SqlType, Table};

use super::adapter::JvmCoreContract;
use super::typemap::{KotlinTypeEntry, KotlinTypeMap};

/// Return the Kotlin type string for a parameter, resolved from the type map.
pub(super) fn kotlin_param_type(p: &Parameter, type_map: &KotlinTypeMap) -> String {
    if p.is_list {
        let inner = if let SqlType::Enum(name) = &p.sql_type { to_pascal_case(name) } else { type_map.get(&p.sql_type).param_type.clone() };
        format!("List<{inner}>")
    } else {
        type_map.kotlin_param_type(&p.sql_type, p.nullable)
    }
}

/// Return the JDBC bind value expression for a parameter, resolved from the type map.
///
/// When the type has a `write` expression, substitutes `{value}` with the param name.
fn kotlin_write_expr(p: &Parameter, type_map: &KotlinTypeMap) -> String {
    let name = to_camel_case(&p.name);
    if matches!(&p.sql_type, SqlType::Enum(_)) {
        return if p.nullable { format!("{name}?.value") } else { format!("{name}.value") };
    }
    if let Some(expr) = &type_map.get(&p.sql_type).write {
        expr.replace("{value}", &name)
    } else {
        name
    }
}

/// All data needed for a single `generate()` call, bundled to reduce parameter threading.
pub(super) struct GenerationContext<'a> {
    pub schema: &'a Schema,
    pub queries: &'a [Query],
    pub config: &'a OutputConfig,
    pub contract: &'a JvmCoreContract,
    pub type_map: &'a KotlinTypeMap,
    pub strategy: ListParamStrategy,
}

/// Per-query context computed once in the dispatcher and forwarded to all emitters.
struct QueryContext<'a> {
    query: &'a Query,
    schema: &'a Schema,
    type_map: &'a KotlinTypeMap,
    contract: &'a JvmCoreContract,
    return_type: String,
    params_sig: String,
}

/// Emit all Kotlin files for the given schema and queries.
pub(super) fn generate_core_files(ctx: &GenerationContext) -> anyhow::Result<Vec<GeneratedFile>> {
    let mut files = Vec::new();

    let ds = ctx.schema.default_schema.as_deref();

    // One data class per table
    for table in &ctx.schema.tables {
        let class_name = model_name(table, ds);
        let mut src = String::new();
        let mpkg = models_package(&ctx.config.package);
        emit_package(&mut src, &mpkg, "");
        let table_imports = collect_table_imports_from_map(table, ctx.type_map);
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
            .map(|col| format!("    val {}: {}", to_camel_case(&col.name), ctx.type_map.kotlin_type(&col.sql_type, col.nullable)))
            .collect();
        writeln!(src, "{}", params.join(",\n"))?;
        writeln!(src, ")")?;

        let path = source_path(&ctx.config.out, &mpkg, &class_name, "kt");
        files.push(GeneratedFile { path, content: src });
    }

    // One enum class per schema enum
    for enum_type in &ctx.schema.enums {
        let class_name = to_pascal_case(&enum_type.name);
        let mpkg = models_package(&ctx.config.package);
        let mut src = String::new();
        emit_kotlin_enum(&mut src, &mpkg, &class_name, enum_type)?;
        let path = source_path(&ctx.config.out, &mpkg, &class_name, "kt");
        files.push(GeneratedFile { path, content: src });
    }

    // One object per query group + one DataSource-backed wrapper class per group
    for (group, group_queries) in group_queries(ctx.queries) {
        let class_name = queries_class_name(&group);
        let querier_name = querier_class_name(&group);

        let (override_imports, extra_fields) = collect_query_metadata_from_map(&group_queries, ctx.type_map);

        let qpkg = queries_package(&ctx.config.package);
        let mpkg = models_package(&ctx.config.package);
        let mut src = String::new();
        emit_package(&mut src, &qpkg, "");
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
        // Emit override-specific imports (standard import is just Connection)
        writeln!(src, "import java.sql.Connection")?;
        for imp in &override_imports {
            writeln!(src, "import {imp}")?;
        }
        for imp in &model_imports {
            writeln!(src, "import {imp}")?;
        }
        writeln!(src)?;
        writeln!(src, "object {class_name} {{")?;
        for ef in &extra_fields {
            writeln!(src, "    {}", ef.declaration)?;
        }

        for query in &group_queries {
            writeln!(src)?;
            if has_inline_rows(query, ctx.schema) {
                emit_row_class(&mut src, query, ctx.type_map)?;
                writeln!(src)?;
            }
            emit_kotlin_query(&mut src, query, ctx)?;
        }

        writeln!(src)?;
        emit_nullable_primitive_helpers(&mut src)?;
        writeln!(src)?;
        emit_array_helper(&mut src)?;
        writeln!(src, "}}")?;

        let path = source_path(&ctx.config.out, &qpkg, &class_name, "kt");
        files.push(GeneratedFile { path, content: src });

        let mut src = String::new();
        emit_package(&mut src, &qpkg, "");
        let querier_ctx = QuerierContext {
            class_name: &class_name,
            querier_name: &querier_name,
            override_imports: &override_imports,
            model_imports: &model_imports,
            extra_fields: &extra_fields,
        };
        emit_kotlin_querier(&mut src, &group_queries, &querier_ctx, ctx)?;
        let path = source_path(&ctx.config.out, &qpkg, &querier_name, "kt");
        files.push(GeneratedFile { path, content: src });
    }

    Ok(files)
}

fn emit_kotlin_query(src: &mut String, query: &Query, ctx: &GenerationContext) -> anyhow::Result<()> {
    let qctx = QueryContext {
        query,
        schema: ctx.schema,
        type_map: ctx.type_map,
        contract: ctx.contract,
        return_type: jdbc::jdbc_return_type(query, ctx.schema, ctx.contract.fallback_type, |r| format!("{r}?"), |r| format!("List<{r}>"), "Unit", "Long"),
        params_sig: std::iter::once("conn: Connection".to_string())
            .chain(query.params.iter().map(|p| format!("{}: {}", to_camel_case(&p.name), kotlin_param_type(p, ctx.type_map))))
            .collect::<Vec<_>>()
            .join(", "),
    };

    if let Some(lp) = query.params.iter().find(|p| p.is_list) {
        match jdbc::resolve_list_strategy(&ctx.strategy, lp) {
            ListAction::SqlArrayBind(sql) => emit_kotlin_list_array_bind(src, &qctx, lp, &sql),
            ListAction::Dynamic => emit_kotlin_list_dynamic(src, &qctx, lp),
            ListAction::JsonStringBind(sql) => emit_kotlin_list_json_bind(src, &qctx, lp, &sql),
        }
    } else {
        emit_kotlin_scalar_query(src, &qctx)
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

fn emit_kotlin_scalar_query(src: &mut String, ctx: &QueryContext) -> anyhow::Result<()> {
    let (sql_const, raw_sql) = prepare_sql_const(ctx.query);
    emit_kotlin_sql_triple_quoted(src, &sql_const, &raw_sql)?;
    writeln!(src, "    fun {}({}): {} {{", to_camel_case(&ctx.query.name), ctx.params_sig, ctx.return_type)?;
    writeln!(src, "        conn.prepareStatement({sql_const}).use {{ ps ->")?;
    emit_jdbc_binds(src, ctx.query, "", ctx.contract.statement_end, "toTypedArray()", ctx.contract.json_bind, |p| kotlin_write_expr(p, ctx.type_map))?;
    emit_kotlin_result_block(src, ctx)?;
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit a list query that binds the list as a JDBC `Array` (e.g. `= ANY(?)`).
fn emit_kotlin_list_array_bind(src: &mut String, ctx: &QueryContext, lp: &Parameter, rewritten_sql: &str) -> anyhow::Result<()> {
    let lp_name = to_camel_case(&lp.name);
    let method_name = to_camel_case(&ctx.query.name);
    let (sql_const, raw_sql) = prepare_sql_const_from(ctx.query, rewritten_sql);
    emit_kotlin_sql_triple_quoted(src, &sql_const, &raw_sql)?;
    writeln!(src, "    fun {method_name}({}): {} {{", ctx.params_sig, ctx.return_type)?;
    let type_name = pg_array_type_name(&lp.sql_type);
    writeln!(src, "        val arr = conn.createArrayOf(\"{type_name}\", {lp_name}.toTypedArray())")?;
    writeln!(src, "        conn.prepareStatement({sql_const}).use {{ ps ->")?;
    emit_jdbc_binds(src, ctx.query, "arr", ctx.contract.statement_end, "toTypedArray()", ctx.contract.json_bind, |p| kotlin_write_expr(p, ctx.type_map))?;
    emit_kotlin_result_block(src, ctx)?;
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit a dynamic list query that builds `IN (?,?,…,?)` at runtime.
fn emit_kotlin_list_dynamic(src: &mut String, ctx: &QueryContext, lp: &Parameter) -> anyhow::Result<()> {
    let lp_name = to_camel_case(&lp.name);
    let method_name = to_camel_case(&ctx.query.name);
    let (before_esc, after_esc) = prepare_dynamic_sql_parts(ctx.query, lp);
    writeln!(src, "    fun {method_name}({}): {} {{", ctx.params_sig, ctx.return_type)?;
    writeln!(src, "        val marks = {lp_name}.joinToString(\", \") {{ \"?\" }}")?;
    writeln!(src, "        val sql = \"{before_esc}\" + \"IN (${{marks}}){after_esc};\"")?;
    writeln!(src, "        conn.prepareStatement(sql).use {{ ps ->")?;
    emit_dynamic_binds(src, ctx.query, lp, ctx.contract.statement_end, ctx.contract.size_access, &|src, lp_name, base, setter| {
        writeln!(src, "            {lp_name}.forEachIndexed {{ i, v -> ps.{setter}({base} + i + 1, v) }}")?;
        Ok(())
    })?;
    emit_kotlin_result_block(src, ctx)?;
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit a list query that binds the list as a JSON-encoded string.
///
/// The caller provides the already-rewritten SQL (e.g. with `json_each` or
/// `JSON_TABLE`); the generated code builds a JSON array literal from the list
/// and binds it as a regular string parameter.
fn emit_kotlin_list_json_bind(src: &mut String, ctx: &QueryContext, lp: &Parameter, rewritten_sql: &str) -> anyhow::Result<()> {
    let method_name = to_camel_case(&ctx.query.name);
    let (sql_const, raw_sql) = prepare_sql_const_from(ctx.query, rewritten_sql);
    emit_kotlin_sql_triple_quoted(src, &sql_const, &raw_sql)?;
    writeln!(src, "    fun {method_name}({}): {} {{", ctx.params_sig, ctx.return_type)?;
    emit_kotlin_json_builder(src, lp)?;
    writeln!(src, "        conn.prepareStatement({sql_const}).use {{ ps ->")?;
    emit_jdbc_binds(src, ctx.query, "json", ctx.contract.statement_end, "toTypedArray()", ctx.contract.json_bind, |p| kotlin_write_expr(p, ctx.type_map))?;
    emit_kotlin_result_block(src, ctx)?;
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
fn emit_kotlin_result_block(src: &mut String, ctx: &QueryContext) -> anyhow::Result<()> {
    match ctx.query.cmd {
        QueryCmd::Exec => writeln!(src, "            ps.executeUpdate()")?,
        QueryCmd::ExecRows => writeln!(src, "            return ps.executeUpdate().toLong()")?,
        QueryCmd::One => {
            writeln!(src, "            ps.executeQuery().use {{ rs ->")?;
            writeln!(src, "                if (!rs.next()) return null")?;
            writeln!(src, "                return {}", emit_row_constructor(ctx.query, ctx.schema, ctx.type_map, ctx.contract))?;
            writeln!(src, "            }}")?;
        },
        QueryCmd::Many => {
            let row_type = result_row_type(ctx.query, ctx.schema, ctx.contract);
            writeln!(src, "            val rows = mutableListOf<{row_type}>()")?;
            writeln!(src, "            ps.executeQuery().use {{ rs ->")?;
            writeln!(src, "                while (rs.next()) rows.add({})", emit_row_constructor(ctx.query, ctx.schema, ctx.type_map, ctx.contract))?;
            writeln!(src, "            }}")?;
            writeln!(src, "            return rows")?;
        },
    }
    Ok(())
}

/// Emits a DataSource-backed querier class that acquires a connection
/// per call via `dataSource.connection.use { }` and delegates to `{class_name}`.
fn emit_kotlin_querier(src: &mut String, queries: &[Query], querier_ctx: &QuerierContext, ctx: &GenerationContext) -> anyhow::Result<()> {
    // Emit all imports: standard + model imports + any type-override imports, sorted.
    let mut all_imports: BTreeSet<String> = ["javax.sql.DataSource".to_string()].into();
    all_imports.extend(querier_ctx.override_imports.iter().cloned());
    all_imports.extend(querier_ctx.model_imports.iter().cloned());
    for imp in &all_imports {
        writeln!(src, "import {imp}")?;
    }

    let querier_name = querier_ctx.querier_name;
    writeln!(src)?;
    writeln!(src, "class {querier_name}(private val dataSource: DataSource) {{")?;
    for ef in querier_ctx.extra_fields {
        writeln!(src, "    {}", ef.declaration)?;
    }

    for query in queries {
        writeln!(src)?;
        emit_kotlin_querier_method(src, query, querier_ctx.class_name, ctx)?;
    }

    writeln!(src, "}}")?;
    Ok(())
}

/// Emits one method on the querier class that wraps the corresponding method in `{class_name}`.
fn emit_kotlin_querier_method(src: &mut String, query: &Query, class_name: &str, ctx: &GenerationContext) -> anyhow::Result<()> {
    let row = jdbc::ds_result_row_type(query, ctx.schema, ctx.contract.fallback_type, class_name);
    let return_type = match query.cmd {
        QueryCmd::One => format!("{row}?"),
        QueryCmd::Many => format!("List<{row}>"),
        QueryCmd::Exec => "Unit".to_string(),
        QueryCmd::ExecRows => "Long".to_string(),
    };

    let params_sig: String =
        query.params.iter().map(|p| format!("{}: {}", to_camel_case(&p.name), kotlin_param_type(p, ctx.type_map))).collect::<Vec<_>>().join(", ");

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

fn emit_row_class(src: &mut String, query: &Query, type_map: &KotlinTypeMap) -> anyhow::Result<()> {
    let name = row_type_name(&query.name);
    writeln!(src, "    data class {name}(")?;
    let fields: Vec<String> = query
        .result_columns
        .iter()
        .map(|col| format!("        val {}: {}", to_camel_case(&col.name), type_map.kotlin_type(&col.sql_type, col.nullable)))
        .collect();
    writeln!(src, "{}", fields.join(",\n"))?;
    writeln!(src, "    )")?;
    Ok(())
}

fn emit_row_constructor(query: &Query, schema: &Schema, type_map: &KotlinTypeMap, contract: &JvmCoreContract) -> String {
    jdbc::build_row_constructor(query, schema, contract.fallback_type, "", |sql_type, nullable, idx| resultset_read_expr(sql_type, nullable, idx, type_map))
}

// ─── Emitter helpers (consume the type map) ───────────────────────────────────

fn resultset_read_expr(sql_type: &SqlType, nullable: bool, idx: usize, type_map: &KotlinTypeMap) -> String {
    if let SqlType::Enum(name) = sql_type {
        let ty = to_pascal_case(name);
        return if nullable { format!("rs.getString({idx})?.let {{ {ty}.fromValue(it) }}") } else { format!("{ty}.fromValue(rs.getString({idx}))") };
    }
    if let SqlType::Array(inner) = sql_type {
        if let SqlType::Enum(name) = inner.as_ref() {
            let ty = to_pascal_case(name);
            return if nullable {
                format!("rs.getArray({idx})?.let {{ a -> (a.array as Array<*>).map {{ {ty}.fromValue(it as String) }}.toList() }}")
            } else {
                format!("(rs.getArray({idx}).array as Array<*>).map {{ {ty}.fromValue(it as String) }}.toList()")
            };
        }
        return jdbc_array_read_expr(inner, nullable, idx, type_map);
    }
    let entry = type_map.get(sql_type);
    let template = if nullable { &entry.read_nullable } else { &entry.read };
    template.replace("{idx}", &idx.to_string())
}

fn jdbc_array_read_expr(inner: &SqlType, nullable: bool, idx: usize, type_map: &KotlinTypeMap) -> String {
    let array_elem = &type_map.get(inner).array_elem;
    if let Some(elem_expr) = array_elem {
        if nullable {
            format!("rs.getArray({idx})?.let {{ a -> (a.array as Array<*>).map {{ {elem_expr} }}.toList() }}")
        } else {
            format!("(rs.getArray({idx}).array as Array<*>).map {{ {elem_expr} }}.toList()")
        }
    } else if nullable {
        format!("rs.getArray({idx})?.let {{ jdbcArrayToList(it) }}")
    } else {
        format!("jdbcArrayToList(rs.getArray({idx}))")
    }
}

/// Collect import paths needed by a table's columns, from the resolved type map.
fn collect_table_imports_from_map(table: &Table, type_map: &KotlinTypeMap) -> BTreeSet<String> {
    table.columns.iter().filter_map(|col| type_map.get(&col.sql_type).import.clone()).collect()
}

/// Collect import paths and extra fields needed by a query group, from the resolved type map.
fn collect_query_metadata_from_map(queries: &[Query], type_map: &KotlinTypeMap) -> (BTreeSet<String>, Vec<ExtraField>) {
    let mut imports = BTreeSet::new();
    let mut extra_fields: Vec<ExtraField> = Vec::new();
    for query in queries {
        for col in &query.result_columns {
            absorb_entry_metadata(type_map.get(&col.sql_type), &mut imports, &mut extra_fields);
        }
        for p in &query.params {
            absorb_entry_metadata(type_map.get(&p.sql_type), &mut imports, &mut extra_fields);
        }
    }
    (imports, extra_fields)
}

fn absorb_entry_metadata(entry: &KotlinTypeEntry, imports: &mut BTreeSet<String>, extra_fields: &mut Vec<ExtraField>) {
    if let Some(imp) = &entry.import {
        imports.insert(imp.clone());
    }
    for ef in &entry.extra_fields {
        if let Some(imp) = &ef.import {
            imports.insert(imp.clone());
        }
        if !extra_fields.iter().any(|e| e.declaration == ef.declaration) {
            extra_fields.push(ef.clone());
        }
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

/// Emit the `jdbcArrayToList` helper that isolates the unchecked JDBC array cast to one site.
///
/// All SQL ARRAY column reads are routed through this function so the single
/// `@Suppress("UNCHECKED_CAST")` annotation covers every array type.
fn emit_array_helper(src: &mut String) -> anyhow::Result<()> {
    writeln!(src, "    @Suppress(\"UNCHECKED_CAST\")")?;
    writeln!(src, "    private fun <T> jdbcArrayToList(arr: java.sql.Array): List<T> =")?;
    writeln!(src, "        (arr.array as Array<T>).toList()")?;
    Ok(())
}

/// Test shim: exposes `resultset_read_expr` to the parent module's `#[cfg(test)]` helpers.
#[cfg(test)]
pub(super) fn resultset_read_expr_pub(sql_type: &SqlType, nullable: bool, idx: usize, type_map: &KotlinTypeMap) -> String {
    resultset_read_expr(sql_type, nullable, idx, type_map)
}

/// Emit a Kotlin enum class for a SQL enum type.
fn emit_kotlin_enum(src: &mut String, package: &str, class_name: &str, enum_type: &EnumType) -> anyhow::Result<()> {
    emit_package(src, package, "");
    writeln!(src, "enum class {class_name}(val value: String) {{")?;
    let variants: Vec<String> = enum_type.variants.iter().map(|v| format!("    {}(\"{}\")", to_screaming_snake_case(v), v)).collect();
    writeln!(src, "{};", variants.join(",\n"))?;
    writeln!(src)?;
    writeln!(src, "    companion object {{")?;
    writeln!(src, "        fun fromValue(value: String): {class_name} =")?;
    writeln!(src, "            entries.first {{ it.value == value }}")?;
    writeln!(src, "    }}")?;
    writeln!(src, "}}")?;
    Ok(())
}

// ─── Path helper ──────────────────────────────────────────────────────────────

fn source_path(out: &str, package: &str, name: &str, ext: &str) -> PathBuf {
    let pkg_path = package.replace('.', "/");
    PathBuf::from(out).join(pkg_path).join(format!("{name}.{ext}"))
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
