use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::common::{
    emit_package, infer_table, jdbc_sql, sql_const_name, to_camel_case, to_pascal_case,
};
use crate::backend::{Codegen, GeneratedFile};
use crate::config::OutputConfig;
use crate::ir::{Query, QueryCmd, Schema, SqlType};

pub struct KotlinCodegen;

impl Codegen for KotlinCodegen {
    fn generate(
        &self,
        schema: &Schema,
        queries: &[Query],
        config: &OutputConfig,
    ) -> anyhow::Result<Vec<GeneratedFile>> {
        let mut files = Vec::new();

        // One data class per table
        for table in &schema.tables {
            let class_name = to_pascal_case(&table.name);
            let mut src = String::new();
            emit_package(&mut src, &config.package, "");
            writeln!(src, "data class {class_name}(")?;
            let params: Vec<String> = table.columns.iter().map(|col| {
                let ty = kotlin_type(&col.sql_type, col.nullable);
                format!("    val {}: {}", to_camel_case(&col.name), ty)
            }).collect();
            writeln!(src, "{}", params.join(",\n"))?;
            writeln!(src, ")")?;

            let path = source_path(&config.out, &config.package, &class_name, "kt");
            files.push(GeneratedFile { path, content: src });
        }

        // One Queries object
        if !queries.is_empty() {
            let mut src = String::new();
            emit_package(&mut src, &config.package, "");
            writeln!(src, "import java.sql.Connection")?;
            writeln!(src)?;
            writeln!(src, "object Queries {{")?;

            for query in queries {
                writeln!(src)?;
                if infer_table(query, schema).is_none() && !query.result_columns.is_empty() {
                    emit_row_class(&mut src, query)?;
                    writeln!(src)?;
                }
                emit_kotlin_query(&mut src, query, schema)?;
            }

            writeln!(src, "}}")?;

            let path = source_path(&config.out, &config.package, "Queries", "kt");
            files.push(GeneratedFile { path, content: src });
        }

        Ok(files)
    }
}

fn emit_kotlin_query(src: &mut String, query: &Query, schema: &Schema) -> anyhow::Result<()> {
    let sql_const = sql_const_name(&query.name);
    let sql = jdbc_sql(&query.sql);
    writeln!(src, "    private const val {sql_const} = \"{};\"", sql.replace('\n', " ").replace('"', "\\\""))?;

    let return_type = match query.cmd {
        QueryCmd::One => {
            let row_type = result_row_type(query, schema);
            format!("{row_type}?")
        }
        QueryCmd::Many => {
            let row_type = result_row_type(query, schema);
            format!("List<{row_type}>")
        }
        QueryCmd::Exec => "Unit".to_string(),
        QueryCmd::ExecRows => "Long".to_string(),
    };

    let params_sig: String = std::iter::once("conn: Connection".to_string())
        .chain(query.params.iter().map(|p| {
            format!("{}: {}", to_camel_case(&p.name), kotlin_type(&p.sql_type, p.nullable))
        }))
        .collect::<Vec<_>>()
        .join(", ");

    writeln!(src, "    fun {}({params_sig}): {return_type} {{", to_camel_case(&query.name))?;
    writeln!(src, "        conn.prepareStatement({sql_const}).use {{ ps ->")?;

    for p in &query.params {
        if p.nullable {
            writeln!(src, "            ps.setObject({}, {})", p.index, to_camel_case(&p.name))?;
        } else {
            let setter = jdbc_setter(&p.sql_type);
            writeln!(src, "            ps.{setter}({}, {})", p.index, to_camel_case(&p.name))?;
        }
    }

    match query.cmd {
        QueryCmd::Exec => {
            writeln!(src, "            ps.executeUpdate()")?;
        }
        QueryCmd::ExecRows => {
            writeln!(src, "            return ps.executeUpdate().toLong()")?;
        }
        QueryCmd::One => {
            writeln!(src, "            ps.executeQuery().use {{ rs ->")?;
            writeln!(src, "                if (!rs.next()) return null")?;
            writeln!(src, "                return {}", emit_row_constructor(query, schema))?;
            writeln!(src, "            }}")?;
        }
        QueryCmd::Many => {
            let row_type = result_row_type(query, schema);
            writeln!(src, "            val rows = mutableListOf<{row_type}>()")?;
            writeln!(src, "            ps.executeQuery().use {{ rs ->")?;
            writeln!(src, "                while (rs.next()) rows.add({})", emit_row_constructor(query, schema))?;
            writeln!(src, "            }}")?;
            writeln!(src, "            return rows")?;
        }
    }

    writeln!(src, "        }}")?;
    writeln!(src, "    }}")?;
    Ok(())
}

fn result_row_type(query: &Query, schema: &Schema) -> String {
    if let Some(table_name) = infer_table(query, schema) {
        return to_pascal_case(table_name);
    }
    if !query.result_columns.is_empty() {
        return row_class_name(&query.name);
    }
    "Any".to_string()
}

fn row_class_name(query_name: &str) -> String {
    format!("{}Row", to_pascal_case(query_name))
}

fn emit_row_class(src: &mut String, query: &Query) -> anyhow::Result<()> {
    let name = row_class_name(&query.name);
    writeln!(src, "    data class {name}(")?;
    let fields: Vec<String> = query.result_columns.iter().map(|col| {
        format!("        val {}: {}", to_camel_case(&col.name), kotlin_type(&col.sql_type, col.nullable))
    }).collect();
    writeln!(src, "{}", fields.join(",\n"))?;
    writeln!(src, "    )")?;
    Ok(())
}

fn emit_row_constructor(query: &Query, schema: &Schema) -> String {
    let class = result_row_type(query, schema);
    let args: Vec<String> = query.result_columns.iter().enumerate().map(|(i, col)| {
        rs_read_expr(&col.sql_type, col.nullable, i + 1)
    }).collect();
    format!("{class}({})", args.join(", "))
}

// ─── Type helpers ─────────────────────────────────────────────────────────────

pub fn kotlin_type(sql_type: &SqlType, nullable: bool) -> String {
    let base = match sql_type {
        SqlType::Boolean   => "Boolean",
        SqlType::SmallInt  => "Short",
        SqlType::Integer   => "Int",
        SqlType::BigInt    => "Long",
        SqlType::Real      => "Float",
        SqlType::Double    => "Double",
        SqlType::Decimal   => "java.math.BigDecimal",
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => "String",
        SqlType::Bytes     => "ByteArray",
        SqlType::Date      => "java.time.LocalDate",
        SqlType::Time      => "java.time.LocalTime",
        SqlType::Timestamp => "java.time.LocalDateTime",
        SqlType::TimestampTz => "java.time.OffsetDateTime",
        SqlType::Interval  => "String",
        SqlType::Uuid      => "java.util.UUID",
        SqlType::Json | SqlType::Jsonb => "String",
        SqlType::Array(inner) => {
            let t = format!("List<{}>", kotlin_type(inner, false));
            return if nullable { format!("{t}?") } else { t };
        }
        SqlType::Custom(_) => "Any",
    };
    if nullable { format!("{base}?") } else { base.to_string() }
}

fn jdbc_setter(sql_type: &SqlType) -> &'static str {
    match sql_type {
        SqlType::Boolean               => "setBoolean",
        SqlType::SmallInt              => "setShort",
        SqlType::Integer               => "setInt",
        SqlType::BigInt                => "setLong",
        SqlType::Real                  => "setFloat",
        SqlType::Double                => "setDouble",
        SqlType::Decimal               => "setBigDecimal",
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => "setString",
        SqlType::Bytes                 => "setBytes",
        _                              => "setObject",
    }
}

fn rs_read_expr(sql_type: &SqlType, nullable: bool, idx: usize) -> String {
    // Primitive getters return 0/false for SQL NULL. For nullable primitive columns,
    // use getObject with the Java boxed type so the result can be null,
    // matching the nullable Kotlin type (e.g. Long? instead of Long).
    if nullable {
        match sql_type {
            SqlType::Boolean  => return format!("rs.getObject({idx}, java.lang.Boolean::class.java)"),
            SqlType::SmallInt => return format!("rs.getObject({idx}, java.lang.Short::class.java)"),
            SqlType::Integer  => return format!("rs.getObject({idx}, java.lang.Integer::class.java)"),
            SqlType::BigInt   => return format!("rs.getObject({idx}, java.lang.Long::class.java)"),
            SqlType::Real     => return format!("rs.getObject({idx}, java.lang.Float::class.java)"),
            SqlType::Double   => return format!("rs.getObject({idx}, java.lang.Double::class.java)"),
            _ => {} // reference types already return null naturally
        }
    }
    match sql_type {
        SqlType::Boolean    => format!("rs.getBoolean({idx})"),
        SqlType::SmallInt   => format!("rs.getShort({idx})"),
        SqlType::Integer    => format!("rs.getInt({idx})"),
        SqlType::BigInt     => format!("rs.getLong({idx})"),
        SqlType::Real       => format!("rs.getFloat({idx})"),
        SqlType::Double     => format!("rs.getDouble({idx})"),
        SqlType::Decimal    => format!("rs.getBigDecimal({idx})"),
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => format!("rs.getString({idx})"),
        SqlType::Bytes      => format!("rs.getBytes({idx})"),
        SqlType::Date       => format!("rs.getObject({idx}, java.time.LocalDate::class.java)"),
        SqlType::Time       => format!("rs.getObject({idx}, java.time.LocalTime::class.java)"),
        SqlType::Timestamp  => format!("rs.getObject({idx}, java.time.LocalDateTime::class.java)"),
        SqlType::TimestampTz => format!("rs.getObject({idx}, java.time.OffsetDateTime::class.java)"),
        SqlType::Uuid       => format!("rs.getObject({idx}, java.util.UUID::class.java)"),
        _                   => format!("rs.getObject({idx})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::OutputConfig;
    use crate::ir::{Column, Parameter, Query, QueryCmd, ResultColumn, Schema, SqlType, Table};

    fn cfg() -> OutputConfig {
        OutputConfig { out: "out".to_string(), package: String::new() }
    }

    fn cfg_pkg() -> OutputConfig {
        OutputConfig { out: "out".to_string(), package: "com.example.db".to_string() }
    }

    fn get_file<'a>(files: &'a [GeneratedFile], name: &str) -> &'a str {
        files.iter()
            .find(|f| f.path.file_name().is_some_and(|n| n == name))
            .unwrap_or_else(|| panic!("file {name:?} not found"))
            .content.as_str()
    }

    fn user_table() -> Table {
        Table {
            name: "user".to_string(),
            columns: vec![
                Column { name: "id".to_string(),   sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                Column { name: "name".to_string(), sql_type: SqlType::Text,   nullable: false, is_primary_key: false },
                Column { name: "bio".to_string(),  sql_type: SqlType::Text,   nullable: true,  is_primary_key: false },
            ],
        }
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
        assert_eq!(kotlin_type(&SqlType::Date, false),        "java.time.LocalDate");
        assert_eq!(kotlin_type(&SqlType::Time, false),        "java.time.LocalTime");
        assert_eq!(kotlin_type(&SqlType::Timestamp, false),   "java.time.LocalDateTime");
        assert_eq!(kotlin_type(&SqlType::TimestampTz, false), "java.time.OffsetDateTime");
    }

    #[test]
    fn test_kotlin_type_uuid() {
        assert_eq!(kotlin_type(&SqlType::Uuid, false), "java.util.UUID");
    }

    #[test]
    fn test_kotlin_type_json() {
        assert_eq!(kotlin_type(&SqlType::Json, false),  "String");
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
    fn test_kotlin_type_custom() {
        assert_eq!(kotlin_type(&SqlType::Custom("citext".to_string()), false), "Any");
    }

    // ─── generate: data class ──────────────────────────────────────────────

    #[test]
    fn test_generate_table_data_class() {
        let schema = Schema { tables: vec![user_table()] };
        let files = KotlinCodegen.generate(&schema, &[], &cfg()).unwrap();
        let src = get_file(&files, "User.kt");
        assert!(src.contains("data class User("));
        assert!(src.contains("val id: Long"));
        assert!(src.contains("val name: String"));
        assert!(src.contains("val bio: String?"));   // nullable → String?
    }

    #[test]
    fn test_generate_package_declaration() {
        let schema = Schema { tables: vec![user_table()] };
        let files = KotlinCodegen.generate(&schema, &[], &cfg_pkg()).unwrap();
        let src = get_file(&files, "User.kt");
        // Kotlin package has no semicolon
        assert!(src.contains("package com.example.db\n"));
        assert!(!src.contains("package com.example.db;"));
    }

    #[test]
    fn test_generate_no_queries_produces_no_queries_file() {
        let schema = Schema { tables: vec![user_table()] };
        let files = KotlinCodegen.generate(&schema, &[], &cfg()).unwrap();
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
            params: vec![Parameter { index: 1, name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
            result_columns: vec![],
        };
        let files = KotlinCodegen.generate(&schema, &[query], &cfg()).unwrap();
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
            params: vec![Parameter { index: 1, name: "active".to_string(), sql_type: SqlType::Boolean, nullable: false }],
            result_columns: vec![],
        };
        let files = KotlinCodegen.generate(&schema, &[query], &cfg()).unwrap();
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
            params: vec![Parameter { index: 1, name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
            result_columns: vec![
                ResultColumn { name: "id".to_string(),   sql_type: SqlType::BigInt, nullable: false },
                ResultColumn { name: "name".to_string(), sql_type: SqlType::Text,   nullable: false },
                ResultColumn { name: "bio".to_string(),  sql_type: SqlType::Text,   nullable: true },
            ],
        };
        let files = KotlinCodegen.generate(&schema, &[query], &cfg()).unwrap();
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
                ResultColumn { name: "id".to_string(),   sql_type: SqlType::BigInt, nullable: false },
                ResultColumn { name: "name".to_string(), sql_type: SqlType::Text,   nullable: false },
                ResultColumn { name: "bio".to_string(),  sql_type: SqlType::Text,   nullable: true },
            ],
        };
        let files = KotlinCodegen.generate(&schema, &[query], &cfg()).unwrap();
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
            params: vec![Parameter { index: 1, name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
            result_columns: vec![],
        };
        let files = KotlinCodegen.generate(&schema, &[query], &cfg()).unwrap();
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
            params: vec![Parameter { index: 1, name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
            result_columns: vec![
                ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
            ],
        };
        let files = KotlinCodegen.generate(&schema, &[query], &cfg()).unwrap();
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
            params: vec![Parameter { index: 1, name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
            result_columns: vec![
                ResultColumn { name: "count".to_string(), sql_type: SqlType::BigInt, nullable: true },
            ],
        };
        let files = KotlinCodegen.generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        assert!(src.contains("rs.getObject(1, java.lang.Long::class.java)"));
        assert!(!src.contains("rs.getLong(1)"));
    }

    #[test]
    fn test_generate_non_nullable_long_result_uses_get_long() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetCount".to_string(),
            cmd: QueryCmd::One,
            sql: "SELECT count FROM stats WHERE id = $1".to_string(),
            params: vec![Parameter { index: 1, name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
            result_columns: vec![
                ResultColumn { name: "count".to_string(), sql_type: SqlType::BigInt, nullable: false },
            ],
        };
        let files = KotlinCodegen.generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        assert!(src.contains("rs.getLong(1)"));
    }

    // ─── generate: parameter binding ────────────────────────────────────────

    #[test]
    fn test_generate_nullable_param_uses_set_object() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "UpdateBio".to_string(),
            cmd: QueryCmd::Exec,
            sql: "UPDATE user SET bio = $1 WHERE id = $2".to_string(),
            params: vec![
                Parameter { index: 1, name: "bio".to_string(), sql_type: SqlType::Text,   nullable: true },
                Parameter { index: 2, name: "id".to_string(),  sql_type: SqlType::BigInt, nullable: false },
            ],
            result_columns: vec![],
        };
        let files = KotlinCodegen.generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.kt");
        assert!(src.contains("ps.setObject(1, bio)"));    // nullable → setObject
        assert!(src.contains("ps.setLong(2, id)"));       // non-nullable → typed setter
    }
}

// ─── Path helper ──────────────────────────────────────────────────────────────

fn source_path(out: &str, package: &str, name: &str, ext: &str) -> PathBuf {
    let pkg_path = package.replace('.', "/");
    PathBuf::from(out).join(pkg_path).join(format!("{name}.{ext}"))
}