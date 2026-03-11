use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::common::{emit_package, has_inline_rows, needs_null_safe_getter, pg_array_type_name, to_camel_case, to_pascal_case};
use crate::backend::jdbc::{
    self, emit_dynamic_binds, emit_jdbc_binds, prepare_dynamic_sql_parts, prepare_sql_const, prepare_sql_const_from, JdbcTarget, ListAction,
};
use crate::backend::{Codegen, GeneratedFile};
use crate::config::{ListParamStrategy, OutputConfig};
use crate::ir::{Parameter, Query, QueryCmd, Schema, SqlType};

/// Statement-end terminator for Kotlin.
const SE: &str = "";
/// Fallback row type when no table match is found.
const FALLBACK_TYPE: &str = "Any";

pub struct KotlinCodegen {
    pub target: JdbcTarget,
}

/// Per-query context computed once in the dispatcher and forwarded to all emitters.
struct QueryContext<'a> {
    query: &'a Query,
    schema: &'a Schema,
    return_type: String,
    params_sig: String,
}

impl Codegen for KotlinCodegen {
    fn generate(&self, schema: &Schema, queries: &[Query], config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
        let mut files = Vec::new();

        // One data class per table
        for table in &schema.tables {
            let class_name = to_pascal_case(&table.name);
            let mut src = String::new();
            emit_package(&mut src, &config.package, "");
            writeln!(src, "data class {class_name}(")?;
            let params: Vec<String> = table
                .columns
                .iter()
                .map(|col| {
                    let ty = kotlin_type(&col.sql_type, col.nullable);
                    format!("    val {}: {}", to_camel_case(&col.name), ty)
                })
                .collect();
            writeln!(src, "{}", params.join(",\n"))?;
            writeln!(src, ")")?;

            let path = source_path(&config.out, &config.package, &class_name, "kt");
            files.push(GeneratedFile { path, content: src });
        }

        // One Queries object (static-style) + one QueriesDs class backed by DataSource
        if !queries.is_empty() {
            let mut src = String::new();
            emit_package(&mut src, &config.package, "");
            writeln!(src, "import java.sql.Connection")?;
            writeln!(src)?;
            writeln!(src, "object Queries {{")?;

            let strategy = config.list_params.clone().unwrap_or_default();
            for query in queries {
                writeln!(src)?;
                if has_inline_rows(query, schema) {
                    emit_row_class(&mut src, query)?;
                    writeln!(src)?;
                }
                emit_kotlin_query(&mut src, query, schema, self.target, &strategy)?;
            }

            writeln!(src, "}}")?;

            let path = source_path(&config.out, &config.package, "Queries", "kt");
            files.push(GeneratedFile { path, content: src });

            let mut src = String::new();
            emit_package(&mut src, &config.package, "");
            emit_kotlin_queries_ds(&mut src, queries, schema)?;
            let path = source_path(&config.out, &config.package, "QueriesDs", "kt");
            files.push(GeneratedFile { path, content: src });
        }

        Ok(files)
    }
}

fn emit_kotlin_query(src: &mut String, query: &Query, schema: &Schema, target: JdbcTarget, strategy: &ListParamStrategy) -> anyhow::Result<()> {
    let ctx = QueryContext {
        query,
        schema,
        return_type: jdbc::jdbc_return_type(query, schema, FALLBACK_TYPE, |r| format!("{r}?"), |r| format!("List<{r}>"), "Unit", "Long"),
        params_sig: std::iter::once("conn: Connection".to_string())
            .chain(query.params.iter().map(|p| format!("{}: {}", to_camel_case(&p.name), kotlin_param_type(p))))
            .collect::<Vec<_>>()
            .join(", "),
    };

    if let Some(lp) = query.params.iter().find(|p| p.is_list) {
        match jdbc::resolve_list_strategy(target, strategy, query, lp) {
            ListAction::PgNative(sql) => emit_kotlin_list_pg_native(src, &ctx, lp, &sql),
            ListAction::Dynamic => emit_kotlin_list_dynamic(src, &ctx, lp),
            ListAction::JsonNative(sql) => emit_kotlin_list_json_native(src, &ctx, lp, &sql),
        }
    } else {
        emit_kotlin_scalar_query(src, &ctx)
    }
}

fn emit_kotlin_scalar_query(src: &mut String, ctx: &QueryContext) -> anyhow::Result<()> {
    let (sql_const, escaped) = prepare_sql_const(ctx.query);
    writeln!(src, "    private const val {sql_const} = \"{escaped};\"")?;
    writeln!(src, "    fun {}({}): {} {{", to_camel_case(&ctx.query.name), ctx.params_sig, ctx.return_type)?;
    writeln!(src, "        conn.prepareStatement({sql_const}).use {{ ps ->")?;
    emit_jdbc_binds(src, ctx.query, "", SE)?;
    emit_kotlin_result_block(src, ctx.query, ctx.schema)?;
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit a PostgreSQL native list query using `= ANY(?)` with a JDBC array.
fn emit_kotlin_list_pg_native(src: &mut String, ctx: &QueryContext, lp: &Parameter, rewritten_sql: &str) -> anyhow::Result<()> {
    let lp_name = to_camel_case(&lp.name);
    let method_name = to_camel_case(&ctx.query.name);
    let (sql_const, escaped) = prepare_sql_const_from(ctx.query, rewritten_sql);
    writeln!(src, "    private const val {sql_const} = \"{escaped};\"")?;
    writeln!(src, "    fun {method_name}({}): {} {{", ctx.params_sig, ctx.return_type)?;
    let type_name = pg_array_type_name(&lp.sql_type);
    writeln!(src, "        val arr = conn.createArrayOf(\"{type_name}\", {lp_name}.toTypedArray())")?;
    writeln!(src, "        conn.prepareStatement({sql_const}).use {{ ps ->")?;
    emit_jdbc_binds(src, ctx.query, "arr", SE)?;
    emit_kotlin_result_block(src, ctx.query, ctx.schema)?;
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
    emit_dynamic_binds(src, ctx.query, lp, SE, &|src, lp_name, base, setter| {
        writeln!(src, "            {lp_name}.forEachIndexed {{ i, v -> ps.{setter}({base} + i + 1, v) }}")?;
        Ok(())
    })?;
    emit_kotlin_result_block(src, ctx.query, ctx.schema)?;
    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

/// Emit a SQLite or MySQL native list query that passes a JSON array string.
///
/// Both engines use the same structure: build a JSON string from the list,
/// then bind it as a regular string parameter. The caller provides the
/// already-rewritten SQL (with `json_each` or `JSON_TABLE`).
fn emit_kotlin_list_json_native(src: &mut String, ctx: &QueryContext, lp: &Parameter, rewritten_sql: &str) -> anyhow::Result<()> {
    let method_name = to_camel_case(&ctx.query.name);
    let (sql_const, escaped) = prepare_sql_const_from(ctx.query, rewritten_sql);
    writeln!(src, "    private const val {sql_const} = \"{escaped};\"")?;
    writeln!(src, "    fun {method_name}({}): {} {{", ctx.params_sig, ctx.return_type)?;
    emit_kotlin_json_builder(src, lp)?;
    writeln!(src, "        conn.prepareStatement({sql_const}).use {{ ps ->")?;
    emit_jdbc_binds(src, ctx.query, "json", SE)?;
    emit_kotlin_result_block(src, ctx.query, ctx.schema)?;
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
fn emit_kotlin_result_block(src: &mut String, query: &Query, schema: &Schema) -> anyhow::Result<()> {
    match query.cmd {
        QueryCmd::Exec => writeln!(src, "            ps.executeUpdate()")?,
        QueryCmd::ExecRows => writeln!(src, "            return ps.executeUpdate().toLong()")?,
        QueryCmd::One => {
            writeln!(src, "            ps.executeQuery().use {{ rs ->")?;
            writeln!(src, "                if (!rs.next()) return null")?;
            writeln!(src, "                return {}", emit_row_constructor(query, schema))?;
            writeln!(src, "            }}")?;
        },
        QueryCmd::Many => {
            let row_type = result_row_type(query, schema);
            writeln!(src, "            val rows = mutableListOf<{row_type}>()")?;
            writeln!(src, "            ps.executeQuery().use {{ rs ->")?;
            writeln!(src, "                while (rs.next()) rows.add({})", emit_row_constructor(query, schema))?;
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

/// Emits `QueriesDs.kt` — a DataSource-backed class that acquires a connection
/// per call via `dataSource.connection.use { }` and delegates to `Queries`.
fn emit_kotlin_queries_ds(src: &mut String, queries: &[Query], schema: &Schema) -> anyhow::Result<()> {
    writeln!(src, "import javax.sql.DataSource")?;
    writeln!(src)?;
    writeln!(src, "class QueriesDs(private val dataSource: DataSource) {{")?;

    for query in queries {
        writeln!(src)?;
        emit_kotlin_ds_method(src, query, schema)?;
    }

    writeln!(src, "}}")?;
    Ok(())
}

/// Emits one method on `QueriesDs` that wraps the corresponding `Queries` method.
fn emit_kotlin_ds_method(src: &mut String, query: &Query, schema: &Schema) -> anyhow::Result<()> {
    let return_type = jdbc::jdbc_ds_return_type(query, schema, FALLBACK_TYPE, |r| format!("{r}?"), |r| format!("List<{r}>"), "Unit", "Long");

    let params_sig: String = query.params.iter().map(|p| format!("{}: {}", to_camel_case(&p.name), kotlin_param_type(p))).collect::<Vec<_>>().join(", ");

    let method_name = to_camel_case(&query.name);
    let args: String = query.params.iter().map(|p| to_camel_case(&p.name)).collect::<Vec<_>>().join(", ");
    let call_args = if args.is_empty() { "conn".to_string() } else { format!("conn, {args}") };

    writeln!(src, "    fun {method_name}({params_sig}): {return_type} =")?;
    writeln!(src, "        dataSource.connection.use {{ conn -> Queries.{method_name}({call_args}) }}")?;
    Ok(())
}

fn result_row_type(query: &Query, schema: &Schema) -> String {
    jdbc::result_row_type(query, schema, FALLBACK_TYPE)
}

fn row_class_name(query_name: &str) -> String {
    format!("{}Row", to_pascal_case(query_name))
}

fn emit_row_class(src: &mut String, query: &Query) -> anyhow::Result<()> {
    let name = row_class_name(&query.name);
    writeln!(src, "    data class {name}(")?;
    let fields: Vec<String> =
        query.result_columns.iter().map(|col| format!("        val {}: {}", to_camel_case(&col.name), kotlin_type(&col.sql_type, col.nullable))).collect();
    writeln!(src, "{}", fields.join(",\n"))?;
    writeln!(src, "    )")?;
    Ok(())
}

fn emit_row_constructor(query: &Query, schema: &Schema) -> String {
    jdbc::build_row_constructor(query, schema, FALLBACK_TYPE, "", resultset_read_expr)
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
        // getObject returns a Java boxed type (platform type T!). Kotlin requires
        // an explicit conversion to its nullable primitive (e.g. Int?) because the
        // platform type is not automatically widened to the Kotlin nullable primitive.
        return match sql_type {
            SqlType::Boolean => format!("rs.getObject({idx}, java.lang.Boolean::class.java) as Boolean?"),
            SqlType::SmallInt => format!("rs.getObject({idx}, java.lang.Short::class.java)?.toShort()"),
            SqlType::Integer => format!("rs.getObject({idx}, java.lang.Integer::class.java)?.toInt()"),
            SqlType::BigInt => format!("rs.getObject({idx}, java.lang.Long::class.java)?.toLong()"),
            SqlType::Real => format!("rs.getObject({idx}, java.lang.Float::class.java)?.toFloat()"),
            SqlType::Double => format!("rs.getObject({idx}, java.lang.Double::class.java)?.toDouble()"),
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

/// Build a JDBC expression that reads a SQL ARRAY column and converts it to `List<T>`.
fn jdbc_array_read_expr(inner: &SqlType, nullable: bool, idx: usize) -> String {
    let kt = kotlin_type(inner, false);
    if nullable {
        format!("rs.getArray({idx})?.let {{ (it.array as Array<{kt}>).toList() }}")
    } else {
        format!("(rs.getArray({idx}).array as Array<{kt}>).toList()")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::test_helpers::{cfg, get_file, user_table};
    use crate::config::OutputConfig;
    use crate::ir::{Parameter, Query, QueryCmd, ResultColumn, Schema, SqlType};

    fn cfg_pkg() -> OutputConfig {
        OutputConfig { out: "out".to_string(), package: "com.example.db".to_string(), list_params: None }
    }

    fn pg() -> KotlinCodegen {
        KotlinCodegen { target: JdbcTarget::Postgres }
    }

    // ─── kotlin_type ───────────────────────────────────────────────────────

    #[test]
    fn test_kotlin_type_boolean_non_nullable() {
        // Kotlin has no primitive/boxed split — Boolean is always Boolean
        assert_eq!(kotlin_type(&SqlType::Boolean, false), "Boolean");
    }

    #[test]
    fn test_kotlin_type_boolean_nullable() {
        assert_eq!(kotlin_type(&SqlType::Boolean, true), "Boolean?");
    }

    #[test]
    fn test_kotlin_type_integer_non_nullable() {
        assert_eq!(kotlin_type(&SqlType::Integer, false), "Int");
    }

    #[test]
    fn test_kotlin_type_integer_nullable() {
        assert_eq!(kotlin_type(&SqlType::Integer, true), "Int?");
    }

    #[test]
    fn test_kotlin_type_bigint_non_nullable() {
        assert_eq!(kotlin_type(&SqlType::BigInt, false), "Long");
    }

    #[test]
    fn test_kotlin_type_bigint_nullable() {
        assert_eq!(kotlin_type(&SqlType::BigInt, true), "Long?");
    }

    #[test]
    fn test_kotlin_type_text_non_nullable() {
        assert_eq!(kotlin_type(&SqlType::Text, false), "String");
    }

    #[test]
    fn test_kotlin_type_text_nullable() {
        assert_eq!(kotlin_type(&SqlType::Text, true), "String?");
    }

    #[test]
    fn test_kotlin_type_decimal() {
        assert_eq!(kotlin_type(&SqlType::Decimal, false), "java.math.BigDecimal");
    }

    #[test]
    fn test_kotlin_type_temporal() {
        assert_eq!(kotlin_type(&SqlType::Date, false), "java.time.LocalDate");
        assert_eq!(kotlin_type(&SqlType::Time, false), "java.time.LocalTime");
        assert_eq!(kotlin_type(&SqlType::Timestamp, false), "java.time.LocalDateTime");
        assert_eq!(kotlin_type(&SqlType::TimestampTz, false), "java.time.OffsetDateTime");
    }

    #[test]
    fn test_kotlin_type_uuid() {
        assert_eq!(kotlin_type(&SqlType::Uuid, false), "java.util.UUID");
    }

    #[test]
    fn test_kotlin_type_json() {
        assert_eq!(kotlin_type(&SqlType::Json, false), "String");
        assert_eq!(kotlin_type(&SqlType::Jsonb, false), "String");
    }

    #[test]
    fn test_kotlin_type_array_non_nullable() {
        assert_eq!(kotlin_type(&SqlType::Array(Box::new(SqlType::Text)), false), "List<String>");
    }

    #[test]
    fn test_kotlin_type_array_nullable() {
        assert_eq!(kotlin_type(&SqlType::Array(Box::new(SqlType::Text)), true), "List<String>?");
    }

    #[test]
    fn test_kotlin_type_array_of_integers() {
        // Inner type is non-nullable (List element, not the List itself)
        assert_eq!(kotlin_type(&SqlType::Array(Box::new(SqlType::Integer)), false), "List<Int>");
    }

    #[test]
    fn test_resultset_read_array_text() {
        let expr = resultset_read_expr(&SqlType::Array(Box::new(SqlType::Text)), false, 3);
        assert_eq!(expr, "(rs.getArray(3).array as Array<String>).toList()");
    }

    #[test]
    fn test_resultset_read_array_nullable() {
        let expr = resultset_read_expr(&SqlType::Array(Box::new(SqlType::Text)), true, 5);
        assert_eq!(expr, "rs.getArray(5)?.let { (it.array as Array<String>).toList() }");
    }

    #[test]
    fn test_kotlin_type_custom() {
        assert_eq!(kotlin_type(&SqlType::Custom("citext".to_string()), false), "Any");
    }

    // ─── generate: data class ──────────────────────────────────────────────

    #[test]
    fn test_generate_table_data_class() {
        let schema = Schema { tables: vec![user_table()] };
        let files = pg().generate(&schema, &[], &cfg()).unwrap();
        let src = get_file(&files, "User.kt");
        assert!(src.contains("data class User("));
        assert!(src.contains("val id: Long"));
        assert!(src.contains("val name: String"));
        assert!(src.contains("val bio: String?")); // nullable → String?
    }

    #[test]
    fn test_generate_package_declaration() {
        let schema = Schema { tables: vec![user_table()] };
        let files = pg().generate(&schema, &[], &cfg_pkg()).unwrap();
        let src = get_file(&files, "User.kt");
        // Kotlin package has no semicolon
        assert!(src.contains("package com.example.db\n"));
        assert!(!src.contains("package com.example.db;"));
    }

    #[test]
    fn test_generate_no_queries_produces_no_queries_file() {
        let schema = Schema { tables: vec![user_table()] };
        let files = pg().generate(&schema, &[], &cfg()).unwrap();
        assert_eq!(files.len(), 1);
    }

    // ─── generate: query commands ───────────────────────────────────────────

    #[test]
    fn test_generate_exec_query() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "DeleteUser".to_string(),
            cmd: QueryCmd::Exec,
            sql: "DELETE FROM user WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
            result_columns: vec![],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        assert!(src.contains("fun deleteUser(conn: Connection, id: Long): Unit"));
        assert!(src.contains("ps.executeUpdate()"));
    }

    #[test]
    fn test_generate_execrows_query() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "DeleteUsers".to_string(),
            cmd: QueryCmd::ExecRows,
            sql: "DELETE FROM user WHERE active = $1".to_string(),
            params: vec![Parameter::scalar(1, "active", SqlType::Boolean, false)],
            result_columns: vec![],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        assert!(src.contains("fun deleteUsers(conn: Connection, active: Boolean): Long"));
        assert!(src.contains("return ps.executeUpdate().toLong()"));
    }

    #[test]
    fn test_generate_one_query_infers_table_return_type() {
        let schema = Schema { tables: vec![user_table()] };
        let query = Query {
            name: "GetUser".to_string(),
            cmd: QueryCmd::One,
            sql: "SELECT id, name, bio FROM user WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
            result_columns: vec![
                ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
                ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
                ResultColumn { name: "bio".to_string(), sql_type: SqlType::Text, nullable: true },
            ],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        // Kotlin :one return type is nullable (T?) not Optional<T>
        assert!(src.contains("fun getUser(conn: Connection, id: Long): User?"));
        assert!(src.contains("if (!rs.next()) return null"));
        assert!(src.contains("return User("));
    }

    #[test]
    fn test_generate_many_query_infers_table_return_type() {
        let schema = Schema { tables: vec![user_table()] };
        let query = Query {
            name: "ListUsers".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id, name, bio FROM user".to_string(),
            params: vec![],
            result_columns: vec![
                ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
                ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
                ResultColumn { name: "bio".to_string(), sql_type: SqlType::Text, nullable: true },
            ],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        assert!(src.contains("fun listUsers(conn: Connection): List<User>"));
        assert!(src.contains("while (rs.next()) rows.add(User("));
        assert!(src.contains("return rows"));
    }

    // ─── generate: SQL constant name ────────────────────────────────────────

    #[test]
    fn test_generate_sql_const_name_is_screaming_snake_case() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetUserById".to_string(),
            cmd: QueryCmd::Exec,
            sql: "DELETE FROM user WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
            result_columns: vec![],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        assert!(src.contains("SQL_GET_USER_BY_ID"));
    }

    // ─── generate: inline row data class ────────────────────────────────────

    #[test]
    fn test_generate_inline_row_class_for_partial_result() {
        let schema = Schema { tables: vec![user_table()] };
        let query = Query {
            name: "GetUserName".to_string(),
            cmd: QueryCmd::One,
            sql: "SELECT name FROM user WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false }],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        assert!(src.contains("data class GetUserNameRow("));
        assert!(src.contains("GetUserNameRow?"));
    }

    // ─── generate: nullable result column uses getObject ────────────────────

    #[test]
    fn test_generate_nullable_long_result_uses_get_object() {
        // rs.getLong returns 0L for NULL; nullable Long? columns must use getObject
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetCount".to_string(),
            cmd: QueryCmd::One,
            sql: "SELECT count FROM stats WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "count".to_string(), sql_type: SqlType::BigInt, nullable: true }],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        assert!(src.contains("rs.getObject(1, java.lang.Long::class.java)?.toLong()"));
        assert!(!src.contains("rs.getLong(1)"));
    }

    #[test]
    fn test_generate_non_nullable_long_result_uses_get_long() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetCount".to_string(),
            cmd: QueryCmd::One,
            sql: "SELECT count FROM stats WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "count".to_string(), sql_type: SqlType::BigInt, nullable: false }],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        assert!(src.contains("rs.getLong(1)"));
    }

    // ─── generate: QueriesDs ────────────────────────────────────────────────

    #[test]
    fn test_generate_queries_ds_file_is_emitted() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "DeleteUser".to_string(),
            cmd: QueryCmd::Exec,
            sql: "DELETE FROM user WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
            result_columns: vec![],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        assert!(files.iter().any(|f| f.path.file_name().is_some_and(|n| n == "QueriesDs.kt")));
    }

    #[test]
    fn test_generate_queries_ds_class_and_datasource_import() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "DeleteUser".to_string(),
            cmd: QueryCmd::Exec,
            sql: "DELETE FROM user WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
            result_columns: vec![],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "QueriesDs.kt");
        assert!(src.contains("import javax.sql.DataSource"));
        assert!(src.contains("class QueriesDs(private val dataSource: DataSource)"));
    }

    #[test]
    fn test_generate_queries_ds_exec_method_delegates_via_use() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "DeleteUser".to_string(),
            cmd: QueryCmd::Exec,
            sql: "DELETE FROM user WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
            result_columns: vec![],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "QueriesDs.kt");
        assert!(src.contains("fun deleteUser(id: Long): Unit ="));
        assert!(src.contains("dataSource.connection.use { conn -> Queries.deleteUser(conn, id) }"));
    }

    #[test]
    fn test_generate_queries_ds_one_method_returns_nullable() {
        let schema = Schema { tables: vec![user_table()] };
        let query = Query {
            name: "GetUser".to_string(),
            cmd: QueryCmd::One,
            sql: "SELECT id, name, bio FROM user WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
            result_columns: vec![
                ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
                ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
                ResultColumn { name: "bio".to_string(), sql_type: SqlType::Text, nullable: true },
            ],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "QueriesDs.kt");
        assert!(src.contains("fun getUser(id: Long): User? ="));
        assert!(src.contains("dataSource.connection.use { conn -> Queries.getUser(conn, id) }"));
    }

    #[test]
    fn test_generate_queries_ds_many_method_returns_list() {
        let schema = Schema { tables: vec![user_table()] };
        let query = Query {
            name: "ListUsers".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id, name, bio FROM user".to_string(),
            params: vec![],
            result_columns: vec![
                ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
                ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
                ResultColumn { name: "bio".to_string(), sql_type: SqlType::Text, nullable: true },
            ],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "QueriesDs.kt");
        assert!(src.contains("fun listUsers(): List<User> ="));
        assert!(src.contains("dataSource.connection.use { conn -> Queries.listUsers(conn) }"));
    }

    // ─── generate: repeated parameter binding ───────────────────────────────

    #[test]
    fn test_generate_repeated_param_emits_bind_per_occurrence() {
        // $1 appears 4 times, $2 once — must emit 5 bind calls in SQL order
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "FindItems".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT * FROM t WHERE a = $1 OR $1 = -1 AND b = $1 OR $1 = 0 AND c = $2".to_string(),
            params: vec![Parameter::scalar(1, "accountId", SqlType::BigInt, false), Parameter::scalar(2, "inputData", SqlType::Text, false)],
            result_columns: vec![],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        assert!(src.contains("ps.setLong(1, accountId)"));
        assert!(src.contains("ps.setLong(2, accountId)"));
        assert!(src.contains("ps.setLong(3, accountId)"));
        assert!(src.contains("ps.setLong(4, accountId)"));
        assert!(src.contains("ps.setString(5, inputData)"));
    }

    // ─── generate: parameter binding ────────────────────────────────────────

    #[test]
    fn test_generate_nullable_param_uses_set_object() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "UpdateBio".to_string(),
            cmd: QueryCmd::Exec,
            sql: "UPDATE user SET bio = $1 WHERE id = $2".to_string(),
            params: vec![Parameter::scalar(1, "bio", SqlType::Text, true), Parameter::scalar(2, "id", SqlType::BigInt, false)],
            result_columns: vec![],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        assert!(src.contains("ps.setObject(1, bio)")); // nullable → setObject
        assert!(src.contains("ps.setLong(2, id)")); // non-nullable → typed setter
    }

    // ─── generate: list params ──────────────────────────────────────────────

    #[test]
    fn test_generate_pg_native_list_param() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetByIds".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id FROM t WHERE id IN ($1)".to_string(),
            params: vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        assert!(src.contains("ids: List<Long>"), "should use List<Long> for list param");
        assert!(src.contains("= ANY(?)"), "PG native should use ANY");
        assert!(src.contains("createArrayOf(\"bigint\""), "should call createArrayOf");
        assert!(src.contains("ps.setArray(1, arr)"), "should setArray");
    }

    #[test]
    fn test_generate_pg_dynamic_list_param() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetByIds".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id FROM t WHERE id IN ($1)".to_string(),
            params: vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
        };
        let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic) };
        let files = pg().generate(&schema, &[query], &cfg).unwrap();
        let src = get_file(&files, "Queries.kt");
        assert!(src.contains("ids: List<Long>"), "should use List<Long> for list param");
        assert!(src.contains("joinToString"), "dynamic builds IN at runtime");
        assert!(src.contains("forEachIndexed"), "dynamic must have a bind loop for list elements");
    }

    #[test]
    fn test_generate_sqlite_native_list_param() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetByIds".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id FROM t WHERE id IN ($1)".to_string(),
            params: vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
        };
        let files = KotlinCodegen { target: JdbcTarget::Sqlite }.generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        assert!(src.contains("json_each(?)"), "SQLite native should use json_each");
        assert!(!src.contains("IN ($1)"), "IN clause must be replaced by json_each rewrite");
        assert!(!src.contains("JSON_TABLE"), "SQLite should not use MySQL JSON_TABLE");
        assert!(src.contains("ps.setString"), "should bind JSON string");
    }

    // ─── Array column reads and Array/JSON param binds ─────────────────────

    #[test]
    fn test_generate_array_result_column_uses_get_array() {
        // Bug: Array columns previously fell through to rs.getObject(idx),
        // which returns a raw JDBC Array object instead of a typed List.
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetTags".to_string(),
            cmd: QueryCmd::One,
            sql: "SELECT tags FROM t WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "tags".to_string(), sql_type: SqlType::Array(Box::new(SqlType::Text)), nullable: false }],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        assert!(src.contains("rs.getArray(1)"), "should read array column via getArray: {src}");
        assert!(!src.contains("rs.getObject(1)"), "should not fall through to getObject for array column");
        assert!(src.contains("rs.getArray(1).array as Array<String>"), "should cast array to Array<String>");
    }

    #[test]
    fn test_generate_array_param_uses_set_array() {
        // Bug: Array params previously used ps.setObject(idx, val),
        // which doesn't work with PostgreSQL JDBC for array types.
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "UpdateTags".to_string(),
            cmd: QueryCmd::Exec,
            sql: "UPDATE t SET tags = $1 WHERE id = $2".to_string(),
            params: vec![Parameter::scalar(1, "tags", SqlType::Array(Box::new(SqlType::Text)), false), Parameter::scalar(2, "id", SqlType::BigInt, false)],
            result_columns: vec![],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        assert!(src.contains("createArrayOf(\"text\", tags.toArray())"), "should create JDBC array: {src}");
        assert!(src.contains("ps.setArray(1,"), "should bind array param via setArray: {src}");
    }

    #[test]
    fn test_generate_jsonb_param_uses_types_other() {
        // Bug: JSONB params previously used ps.setObject(idx, val) without
        // the Types.OTHER hint, which PostgreSQL JDBC rejects.
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "UpdateMeta".to_string(),
            cmd: QueryCmd::Exec,
            sql: "UPDATE t SET meta = $1 WHERE id = $2".to_string(),
            params: vec![Parameter::scalar(1, "metadata", SqlType::Jsonb, false), Parameter::scalar(2, "id", SqlType::BigInt, false)],
            result_columns: vec![],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        assert!(src.contains("ps.setObject(1, metadata, java.sql.Types.OTHER)"), "JSONB must use Types.OTHER: {src}");
    }

    // ─── Bug A: JSON escaping for text list params in native strategy ────────────

    #[test]
    fn test_bug_a_sqlite_native_text_list_json_escaping() {
        // Bug A: The SQLite/MySQL native strategy uses joinToString(",") with no
        // transform for all element types. For Text params this produces bare
        // unquoted strings — invalid JSON. This test fails until fixed.
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetByTags".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id FROM t WHERE tag IN ($1)".to_string(),
            params: vec![Parameter::list(1, "tags", SqlType::Text, false)],
            result_columns: vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
        };
        let files = KotlinCodegen { target: JdbcTarget::Sqlite }.generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        // Bare joinToString(",") produces unquoted strings — invalid JSON for Text.
        assert!(!src.contains(r#"joinToString(",") + "]""#), "text list must not use bare joinToString (produces unquoted strings)");
        // The fix must use a transform lambda that wraps each element in \"...\"
        // and escapes special characters.
        assert!(src.contains(r#"joinToString(",") {"#), "text list must use joinToString with a transform lambda");
        assert!(src.contains(r#".replace("\\", "\\\\")"#), "backslashes in text values must be escaped");
    }

    #[test]
    fn test_bug_a_numeric_list_no_quoting_needed() {
        // Numeric types produce valid JSON via toString() — no per-element quoting
        // is needed. Confirm the fix does not add a quoting lambda for numeric types.
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetByIds".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id FROM t WHERE id IN ($1)".to_string(),
            params: vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
        };
        let files = KotlinCodegen { target: JdbcTarget::Sqlite }.generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        assert!(src.contains("json_each(?)"), "SQLite native should use json_each");
        assert!(!src.contains(r#"joinToString(",") {"#), "numeric list must not add a per-element quoting lambda");
    }

    // ─── Bug B: dynamic strategy binds scalars at wrong slot when scalar follows IN

    #[test]
    fn test_bug_b_dynamic_scalar_after_in_binding_order() {
        // Bug B: when a scalar param appears *after* the IN clause in the SQL, the
        // Dynamic strategy incorrectly binds it at slot 1 (before list elements).
        // Correct order: [list elements] + [scalar-after].
        // This test fails until the root cause is fixed.
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetActiveByIds".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id FROM t WHERE id IN ($1) AND active = $2".to_string(),
            params: vec![Parameter::list(1, "ids", SqlType::BigInt, false), Parameter::scalar(2, "active", SqlType::Boolean, false)],
            result_columns: vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
        };
        let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic) };
        let files = KotlinCodegen { target: JdbcTarget::Postgres }.generate(&schema, &[query], &cfg).unwrap();
        let src = get_file(&files, "Queries.kt");
        // Bug: active is incorrectly bound at slot 1 before the list elements.
        assert!(!src.contains("ps.setBoolean(1, active)"), "active must not bind at slot 1 when it follows IN");
        // Fix: forEachIndexed (list loop) must appear before the scalar-after binding.
        let loop_pos = src.find("forEachIndexed").expect("list binding loop not found");
        let active_pos = src.find("setBoolean").expect("active binding not found");
        assert!(loop_pos < active_pos, "list binding loop must precede the scalar-after binding");
        // Fix: slot for active depends on the runtime list size.
        assert!(src.contains("ids.size"), "slot for active must be computed from ids.size at runtime");
    }

    #[test]
    fn test_bug_b_dynamic_scalar_before_in_no_regression() {
        // When the scalar param appears *before* the IN clause, the current binding
        // order is correct. Confirm the fix preserves this common pattern.
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetActiveByIds".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id FROM t WHERE active = $1 AND id IN ($2)".to_string(),
            params: vec![Parameter::scalar(1, "active", SqlType::Boolean, false), Parameter::list(2, "ids", SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
        };
        let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(crate::config::ListParamStrategy::Dynamic) };
        let files = KotlinCodegen { target: JdbcTarget::Postgres }.generate(&schema, &[query], &cfg).unwrap();
        let src = get_file(&files, "Queries.kt");
        // active is before IN in the SQL — must still bind at slot 1.
        assert!(src.contains("ps.setBoolean(1, active)"), "scalar before IN must bind at slot 1");
        // The scalar binding must precede the list forEachIndexed.
        let active_pos = src.find("ps.setBoolean(1, active)").unwrap();
        let loop_pos = src.find("forEachIndexed").expect("list binding loop not found");
        assert!(active_pos < loop_pos, "before-scalar binding must precede the list binding loop");
    }
}

// ─── Path helper ──────────────────────────────────────────────────────────────

fn source_path(out: &str, package: &str, name: &str, ext: &str) -> PathBuf {
    let pkg_path = package.replace('.', "/");
    PathBuf::from(out).join(pkg_path).join(format!("{name}.{ext}"))
}
