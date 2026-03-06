use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::common::{emit_package, infer_table, jdbc_sql, sql_const_name, to_camel_case, to_pascal_case};
use crate::backend::{Codegen, GeneratedFile};
use crate::config::OutputConfig;
use crate::ir::{Query, QueryCmd, Schema, SqlType};

pub struct JavaCodegen;

impl Codegen for JavaCodegen {
    fn generate(&self, schema: &Schema, queries: &[Query], config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
        let mut files = Vec::new();

        // One record class per table
        for table in &schema.tables {
            let class_name = to_pascal_case(&table.name);
            let mut src = String::new();
            emit_package(&mut src, &config.package, ";");
            writeln!(src, "public record {class_name}(")?;
            let params: Vec<String> = table
                .columns
                .iter()
                .map(|col| {
                    let ty = java_type(&col.sql_type, col.nullable);
                    format!("    {} {}", ty, to_camel_case(&col.name))
                })
                .collect();
            writeln!(src, "{}", params.join(",\n"))?;
            writeln!(src, ") {{}}")?;

            let path = record_path(&config.out, &config.package, &class_name);
            files.push(GeneratedFile { path, content: src });
        }

        // One Queries class with static methods
        if !queries.is_empty() {
            let mut src = String::new();
            emit_package(&mut src, &config.package, ";");
            writeln!(src, "import java.sql.Connection;")?;
            writeln!(src, "import java.sql.PreparedStatement;")?;
            writeln!(src, "import java.sql.ResultSet;")?;
            writeln!(src, "import java.sql.SQLException;")?;
            writeln!(src, "import java.util.ArrayList;")?;
            writeln!(src, "import java.util.List;")?;
            writeln!(src, "import java.util.Optional;")?;
            writeln!(src)?;
            writeln!(src, "public final class Queries {{")?;
            writeln!(src, "    private Queries() {{}}")?;

            for query in queries {
                writeln!(src)?;
                if infer_table(query, schema).is_none() && !query.result_columns.is_empty() {
                    emit_row_record(&mut src, query)?;
                    writeln!(src)?;
                }
                emit_java_query(&mut src, query, schema)?;
            }

            writeln!(src, "}}")?;

            let path = record_path(&config.out, &config.package, "Queries");
            files.push(GeneratedFile { path, content: src });
        }

        Ok(files)
    }
}

fn emit_java_query(src: &mut String, query: &Query, schema: &Schema) -> anyhow::Result<()> {
    let sql_const = sql_const_name(&query.name);
    let sql = jdbc_sql(&query.sql);
    writeln!(src, "    private static final String {sql_const} =")?;
    writeln!(src, "        \"{};\";", sql.replace('\n', " ").replace('"', "\\\""))?;

    let return_type = match query.cmd {
        QueryCmd::One => {
            let row_type = result_row_type(query, schema);
            format!("Optional<{row_type}>")
        },
        QueryCmd::Many => {
            let row_type = result_row_type(query, schema);
            format!("List<{row_type}>")
        },
        QueryCmd::Exec => "void".to_string(),
        QueryCmd::ExecRows => "long".to_string(),
    };

    let params_sig: String = std::iter::once("Connection conn".to_string())
        .chain(query.params.iter().map(|p| format!("{} {}", java_type(&p.sql_type, p.nullable), to_camel_case(&p.name))))
        .collect::<Vec<_>>()
        .join(", ");

    writeln!(src, "    public static {return_type} {}({params_sig}) throws SQLException {{", to_camel_case(&query.name))?;
    writeln!(src, "        try (PreparedStatement ps = conn.prepareStatement({sql_const})) {{")?;

    for p in &query.params {
        if p.nullable {
            writeln!(src, "            ps.setObject({}, {});", p.index, to_camel_case(&p.name))?;
        } else {
            let setter = jdbc_setter(&p.sql_type);
            writeln!(src, "            ps.{setter}({}, {});", p.index, to_camel_case(&p.name))?;
        }
    }

    match query.cmd {
        QueryCmd::Exec => {
            writeln!(src, "            ps.executeUpdate();")?;
        },
        QueryCmd::ExecRows => {
            writeln!(src, "            return ps.executeUpdate();")?;
        },
        QueryCmd::One => {
            writeln!(src, "            try (ResultSet rs = ps.executeQuery()) {{")?;
            writeln!(src, "                if (!rs.next()) return Optional.empty();")?;
            writeln!(src, "                return Optional.of({});", emit_row_constructor(query, schema))?;
            writeln!(src, "            }}")?;
        },
        QueryCmd::Many => {
            let row_type = result_row_type(query, schema);
            writeln!(src, "            List<{row_type}> rows = new ArrayList<>();")?;
            writeln!(src, "            try (ResultSet rs = ps.executeQuery()) {{")?;
            writeln!(src, "                while (rs.next()) rows.add({});", emit_row_constructor(query, schema))?;
            writeln!(src, "            }}")?;
            writeln!(src, "            return rows;")?;
        },
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
        return row_record_name(&query.name);
    }
    "Object[]".to_string()
}

fn row_record_name(query_name: &str) -> String {
    format!("{}Row", to_pascal_case(query_name))
}

fn emit_row_record(src: &mut String, query: &Query) -> anyhow::Result<()> {
    let name = row_record_name(&query.name);
    writeln!(src, "    public record {name}(")?;
    let fields: Vec<String> =
        query.result_columns.iter().map(|col| format!("        {} {}", java_type(&col.sql_type, col.nullable), to_camel_case(&col.name))).collect();
    writeln!(src, "{}", fields.join(",\n"))?;
    writeln!(src, "    ) {{}}")?;
    Ok(())
}

fn emit_row_constructor(query: &Query, schema: &Schema) -> String {
    let class = result_row_type(query, schema);
    let args: Vec<String> = query.result_columns.iter().enumerate().map(|(i, col)| rs_read_expr(&col.sql_type, col.nullable, i + 1)).collect();
    format!("new {class}({})", args.join(", "))
}

// ─── Type helpers ─────────────────────────────────────────────────────────────

pub fn java_type(sql_type: &SqlType, nullable: bool) -> String {
    match sql_type {
        SqlType::Boolean => {
            if nullable {
                "Boolean".into()
            } else {
                "boolean".into()
            }
        },
        SqlType::SmallInt => {
            if nullable {
                "Short".into()
            } else {
                "short".into()
            }
        },
        SqlType::Integer => {
            if nullable {
                "Integer".into()
            } else {
                "int".into()
            }
        },
        SqlType::BigInt => {
            if nullable {
                "Long".into()
            } else {
                "long".into()
            }
        },
        SqlType::Real => {
            if nullable {
                "Float".into()
            } else {
                "float".into()
            }
        },
        SqlType::Double => {
            if nullable {
                "Double".into()
            } else {
                "double".into()
            }
        },
        SqlType::Decimal => "java.math.BigDecimal".into(),
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => "String".into(),
        SqlType::Bytes => "byte[]".into(),
        SqlType::Date => "java.time.LocalDate".into(),
        SqlType::Time => "java.time.LocalTime".into(),
        SqlType::Timestamp => "java.time.LocalDateTime".into(),
        SqlType::TimestampTz => "java.time.OffsetDateTime".into(),
        SqlType::Interval => "String".into(),
        SqlType::Uuid => "java.util.UUID".into(),
        SqlType::Json | SqlType::Jsonb => "String".into(),
        SqlType::Array(inner) => {
            let t = format!("java.util.List<{}>", java_type_boxed(inner));
            if nullable {
                format!("@Nullable {t}")
            } else {
                t
            }
        },
        SqlType::Custom(_) => "Object".into(),
    }
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

fn jdbc_setter(sql_type: &SqlType) -> &'static str {
    match sql_type {
        SqlType::Boolean => "setBoolean",
        SqlType::SmallInt => "setShort",
        SqlType::Integer => "setInt",
        SqlType::BigInt => "setLong",
        SqlType::Real => "setFloat",
        SqlType::Double => "setDouble",
        SqlType::Decimal => "setBigDecimal",
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => "setString",
        SqlType::Bytes => "setBytes",
        SqlType::Date => "setObject",
        SqlType::Time => "setObject",
        SqlType::Timestamp => "setObject",
        SqlType::TimestampTz => "setObject",
        SqlType::Uuid => "setObject",
        _ => "setObject",
    }
}

fn rs_read_expr(sql_type: &SqlType, nullable: bool, idx: usize) -> String {
    // Primitive getters (getInt, getBoolean, …) return 0/false for SQL NULL.
    // For nullable primitive columns we must use getObject with the boxed type
    // so that the result can be null, matching the @Nullable field declaration.
    if nullable {
        match sql_type {
            SqlType::Boolean => return format!("rs.getObject({idx}, Boolean.class)"),
            SqlType::SmallInt => return format!("rs.getObject({idx}, Short.class)"),
            SqlType::Integer => return format!("rs.getObject({idx}, Integer.class)"),
            SqlType::BigInt => return format!("rs.getObject({idx}, Long.class)"),
            SqlType::Real => return format!("rs.getObject({idx}, Float.class)"),
            SqlType::Double => return format!("rs.getObject({idx}, Double.class)"),
            _ => {}, // reference types already return null naturally
        }
    }
    match sql_type {
        SqlType::Boolean => format!("rs.getBoolean({idx})"),
        SqlType::SmallInt => format!("rs.getShort({idx})"),
        SqlType::Integer => format!("rs.getInt({idx})"),
        SqlType::BigInt => format!("rs.getLong({idx})"),
        SqlType::Real => format!("rs.getFloat({idx})"),
        SqlType::Double => format!("rs.getDouble({idx})"),
        SqlType::Decimal => format!("rs.getBigDecimal({idx})"),
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => format!("rs.getString({idx})"),
        SqlType::Bytes => format!("rs.getBytes({idx})"),
        SqlType::Date => format!("rs.getObject({idx}, java.time.LocalDate.class)"),
        SqlType::Time => format!("rs.getObject({idx}, java.time.LocalTime.class)"),
        SqlType::Timestamp => format!("rs.getObject({idx}, java.time.LocalDateTime.class)"),
        SqlType::TimestampTz => format!("rs.getObject({idx}, java.time.OffsetDateTime.class)"),
        SqlType::Uuid => format!("rs.getObject({idx}, java.util.UUID.class)"),
        _ => format!("rs.getObject({idx})"),
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
        files.iter().find(|f| f.path.file_name().is_some_and(|n| n == name)).unwrap_or_else(|| panic!("file {name:?} not found")).content.as_str()
    }

    fn user_table() -> Table {
        Table {
            name: "user".to_string(),
            columns: vec![
                Column { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                Column { name: "name".to_string(), sql_type: SqlType::Text, nullable: false, is_primary_key: false },
                Column { name: "bio".to_string(), sql_type: SqlType::Text, nullable: true, is_primary_key: false },
            ],
        }
    }

    // ─── java_type ─────────────────────────────────────────────────────────

    #[test]
    fn test_java_type_boolean_non_nullable() {
        assert_eq!(java_type(&SqlType::Boolean, false), "boolean");
    }

    #[test]
    fn test_java_type_boolean_nullable() {
        assert_eq!(java_type(&SqlType::Boolean, true), "Boolean");
    }

    #[test]
    fn test_java_type_integer_non_nullable() {
        assert_eq!(java_type(&SqlType::Integer, false), "int");
    }

    #[test]
    fn test_java_type_integer_nullable() {
        assert_eq!(java_type(&SqlType::Integer, true), "Integer");
    }

    #[test]
    fn test_java_type_bigint_non_nullable() {
        assert_eq!(java_type(&SqlType::BigInt, false), "long");
    }

    #[test]
    fn test_java_type_bigint_nullable() {
        assert_eq!(java_type(&SqlType::BigInt, true), "Long");
    }

    #[test]
    fn test_java_type_text_ignores_nullability() {
        // String is a reference type — same in both cases
        assert_eq!(java_type(&SqlType::Text, false), "String");
        assert_eq!(java_type(&SqlType::Text, true), "String");
    }

    #[test]
    fn test_java_type_decimal() {
        assert_eq!(java_type(&SqlType::Decimal, false), "java.math.BigDecimal");
    }

    #[test]
    fn test_java_type_temporal() {
        assert_eq!(java_type(&SqlType::Date, false), "java.time.LocalDate");
        assert_eq!(java_type(&SqlType::Time, false), "java.time.LocalTime");
        assert_eq!(java_type(&SqlType::Timestamp, false), "java.time.LocalDateTime");
        assert_eq!(java_type(&SqlType::TimestampTz, false), "java.time.OffsetDateTime");
    }

    #[test]
    fn test_java_type_uuid() {
        assert_eq!(java_type(&SqlType::Uuid, false), "java.util.UUID");
    }

    #[test]
    fn test_java_type_json() {
        assert_eq!(java_type(&SqlType::Json, false), "String");
        assert_eq!(java_type(&SqlType::Jsonb, false), "String");
    }

    #[test]
    fn test_java_type_array_non_nullable() {
        assert_eq!(java_type(&SqlType::Array(Box::new(SqlType::Text)), false), "java.util.List<String>");
    }

    #[test]
    fn test_java_type_array_nullable() {
        assert_eq!(java_type(&SqlType::Array(Box::new(SqlType::Text)), true), "@Nullable java.util.List<String>");
    }

    #[test]
    fn test_java_type_array_of_integers_uses_boxed_type() {
        // Array elements must be boxed — List<int> is invalid Java
        assert_eq!(java_type(&SqlType::Array(Box::new(SqlType::Integer)), false), "java.util.List<Integer>");
    }

    #[test]
    fn test_java_type_custom() {
        assert_eq!(java_type(&SqlType::Custom("citext".to_string()), false), "Object");
    }

    // ─── generate: table record ─────────────────────────────────────────────

    #[test]
    fn test_generate_table_record() {
        let schema = Schema { tables: vec![user_table()] };
        let files = JavaCodegen.generate(&schema, &[], &cfg()).unwrap();
        let src = get_file(&files, "User.java");
        assert!(src.contains("public record User("));
        assert!(src.contains("long id"));
        assert!(src.contains("String name"));
        assert!(src.contains("String bio"));
    }

    #[test]
    fn test_generate_package_declaration() {
        let schema = Schema { tables: vec![user_table()] };
        let files = JavaCodegen.generate(&schema, &[], &cfg_pkg()).unwrap();
        let src = get_file(&files, "User.java");
        assert!(src.contains("package com.example.db;"));
    }

    #[test]
    fn test_generate_no_queries_produces_no_queries_file() {
        let schema = Schema { tables: vec![user_table()] };
        let files = JavaCodegen.generate(&schema, &[], &cfg()).unwrap();
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
        let files = JavaCodegen.generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("public static void deleteUser(Connection conn, long id)"));
        assert!(src.contains("ps.executeUpdate();"));
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
        let files = JavaCodegen.generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("public static long deleteUsers("));
        assert!(src.contains("return ps.executeUpdate();"));
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
                ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
                ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
                ResultColumn { name: "bio".to_string(), sql_type: SqlType::Text, nullable: true },
            ],
        };
        let files = JavaCodegen.generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("public static Optional<User> getUser("));
        assert!(src.contains("if (!rs.next()) return Optional.empty();"));
        assert!(src.contains("return Optional.of(new User("));
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
        let files = JavaCodegen.generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("public static List<User> listUsers(Connection conn)"));
        assert!(src.contains("while (rs.next()) rows.add(new User("));
        assert!(src.contains("return rows;"));
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
        let files = JavaCodegen.generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("SQL_GET_USER_BY_ID"));
    }

    // ─── generate: inline row record ────────────────────────────────────────

    #[test]
    fn test_generate_inline_row_record_for_partial_result() {
        let schema = Schema { tables: vec![user_table()] };
        let query = Query {
            name: "GetUserName".to_string(),
            cmd: QueryCmd::One,
            sql: "SELECT name FROM user WHERE id = $1".to_string(),
            params: vec![Parameter { index: 1, name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
            result_columns: vec![ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false }],
        };
        let files = JavaCodegen.generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("public record GetUserNameRow("));
        assert!(src.contains("Optional<GetUserNameRow>"));
    }

    // ─── generate: nullable result column uses getObject ────────────────────

    #[test]
    fn test_generate_nullable_integer_result_uses_get_object() {
        // rs.getInt returns 0 for NULL; nullable Integer columns must use getObject
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetCount".to_string(),
            cmd: QueryCmd::One,
            sql: "SELECT count FROM stats WHERE id = $1".to_string(),
            params: vec![Parameter { index: 1, name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
            result_columns: vec![ResultColumn { name: "count".to_string(), sql_type: SqlType::Integer, nullable: true }],
        };
        let files = JavaCodegen.generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("rs.getObject(1, Integer.class)"));
        assert!(!src.contains("rs.getInt(1)"));
    }

    #[test]
    fn test_generate_non_nullable_integer_result_uses_get_int() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetCount".to_string(),
            cmd: QueryCmd::One,
            sql: "SELECT count FROM stats WHERE id = $1".to_string(),
            params: vec![Parameter { index: 1, name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
            result_columns: vec![ResultColumn { name: "count".to_string(), sql_type: SqlType::Integer, nullable: false }],
        };
        let files = JavaCodegen.generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("rs.getInt(1)"));
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
                Parameter { index: 1, name: "bio".to_string(), sql_type: SqlType::Text, nullable: true },
                Parameter { index: 2, name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
            ],
            result_columns: vec![],
        };
        let files = JavaCodegen.generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "Queries.java");
        assert!(src.contains("ps.setObject(1, bio)")); // nullable → setObject
        assert!(src.contains("ps.setLong(2, id)")); // non-nullable → typed setter
    }
}

// ─── Path helper ──────────────────────────────────────────────────────────────

fn record_path(out: &str, package: &str, class_name: &str) -> PathBuf {
    let pkg_path = package.replace('.', "/");
    PathBuf::from(out).join(pkg_path).join(format!("{class_name}.java"))
}
