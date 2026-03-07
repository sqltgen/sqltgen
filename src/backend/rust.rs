use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::common::{infer_table, positional_bind_names, replace_list_in_clause, split_at_in_clause, to_pascal_case, to_snake_case};
use crate::backend::{Codegen, GeneratedFile};
use crate::config::{ListParamStrategy, OutputConfig};
use crate::ir::{Parameter, Query, QueryCmd, Schema, SqlType};

pub enum RustTarget {
    Postgres,
    Sqlite,
    Mysql,
}

pub struct RustCodegen {
    pub target: RustTarget,
}

impl Codegen for RustCodegen {
    fn generate(&self, schema: &Schema, queries: &[Query], config: &OutputConfig) -> anyhow::Result<Vec<GeneratedFile>> {
        let mut files = Vec::new();

        // One struct file per table
        for table in &schema.tables {
            let struct_name = to_pascal_case(&table.name);
            let mut src = String::new();
            writeln!(src, "#[derive(Debug, sqlx::FromRow)]")?;
            writeln!(src, "pub struct {struct_name} {{")?;
            for col in &table.columns {
                writeln!(src, "    pub {}: {},", col.name, rust_type(&col.sql_type, col.nullable, &self.target))?;
            }
            writeln!(src, "}}")?;

            let path = PathBuf::from(&config.out).join(format!("{}.rs", table.name));
            files.push(GeneratedFile { path, content: src });
        }

        // queries.rs
        if !queries.is_empty() {
            let pool_type = match self.target {
                RustTarget::Postgres => "PgPool",
                RustTarget::Sqlite => "SqlitePool",
                RustTarget::Mysql => "MySqlPool",
            };
            let mut src = String::new();
            writeln!(src, "use sqlx::{pool_type};")?;
            writeln!(src)?;

            // Import only table structs that are actually used as return types
            let needed: HashSet<&str> = queries.iter().filter_map(|q| infer_table(q, schema)).collect();
            for name in &needed {
                writeln!(src, "use super::{}::{};", name, to_pascal_case(name))?;
            }
            if !needed.is_empty() {
                writeln!(src)?;
            }

            // Custom row structs for queries that don't return a whole table
            for query in queries {
                if infer_table(query, schema).is_none() && !query.result_columns.is_empty() {
                    emit_row_struct(&mut src, query, &self.target)?;
                    writeln!(src)?;
                }
            }

            // Query functions
            let strategy = config.list_params.clone().unwrap_or_default();
            for (i, query) in queries.iter().enumerate() {
                if i > 0 {
                    writeln!(src)?;
                }
                emit_rust_query(&mut src, query, schema, pool_type, &self.target, &strategy)?;
            }

            let path = PathBuf::from(&config.out).join("queries.rs");
            files.push(GeneratedFile { path, content: src });
        }

        // mod.rs
        {
            let mut src = String::new();
            writeln!(src, "#![allow(dead_code)]")?;
            writeln!(src)?;
            for table in &schema.tables {
                writeln!(src, "pub mod {};", table.name)?;
            }
            if !queries.is_empty() {
                writeln!(src, "pub mod queries;")?;
            }
            let path = PathBuf::from(&config.out).join("mod.rs");
            files.push(GeneratedFile { path, content: src });
        }

        Ok(files)
    }
}

fn emit_row_struct(src: &mut String, query: &Query, target: &RustTarget) -> anyhow::Result<()> {
    let name = row_struct_name(&query.name);
    writeln!(src, "#[derive(Debug, sqlx::FromRow)]")?;
    writeln!(src, "pub struct {name} {{")?;
    for col in &query.result_columns {
        writeln!(src, "    pub {}: {},", col.name, rust_type(&col.sql_type, col.nullable, target))?;
    }
    writeln!(src, "}}")?;
    Ok(())
}

fn emit_rust_query(src: &mut String, query: &Query, schema: &Schema, pool_type: &str, target: &RustTarget, strategy: &ListParamStrategy) -> anyhow::Result<()> {
    let fn_name = to_snake_case(&query.name);
    let row_type = result_row_type(query, schema);

    let return_type = match query.cmd {
        QueryCmd::One => format!("Result<Option<{row_type}>, sqlx::Error>"),
        QueryCmd::Many => format!("Result<Vec<{row_type}>, sqlx::Error>"),
        QueryCmd::Exec => "Result<(), sqlx::Error>".to_string(),
        QueryCmd::ExecRows => "Result<u64, sqlx::Error>".to_string(),
    };

    let params_sig: String =
        std::iter::once(format!("pool: &{pool_type}")).chain(query.params.iter().map(|p| rust_param_sig(p, target))).collect::<Vec<_>>().join(", ");

    writeln!(src, "pub async fn {fn_name}({params_sig}) -> {return_type} {{")?;

    let list_param = query.params.iter().find(|p| p.is_list);
    if let Some(lp) = list_param {
        emit_rust_list_query(src, query, row_type, target, strategy, lp)?;
    } else {
        emit_rust_scalar_query(src, query, row_type, target)?;
    }

    writeln!(src, "}}")?;
    Ok(())
}

/// Emit the body for a query with no list parameters.
fn emit_rust_scalar_query(src: &mut String, query: &Query, row_type: String, target: &RustTarget) -> anyhow::Result<()> {
    let sql = normalize_sql_for_sqlx(&query.sql, target).replace('"', "\\\"").replace('\n', " ");
    // Postgres $N is reference-by-number — one .bind() per unique param suffices.
    // SQLite/MySQL normalize to ? (positional sequential) — bind once per occurrence.
    let bind_names: Vec<&str> = match target {
        RustTarget::Postgres => query.params.iter().map(|p| p.name.as_str()).collect(),
        RustTarget::Sqlite | RustTarget::Mysql => positional_bind_names(query),
    };
    emit_rust_sqlx_call(src, query, &sql, &bind_names, &row_type)
}

/// Emit the body for a query that contains a list parameter.
fn emit_rust_list_query(
    src: &mut String,
    query: &Query,
    row_type: String,
    target: &RustTarget,
    strategy: &ListParamStrategy,
    list_param: &Parameter,
) -> anyhow::Result<()> {
    let scalar_params: Vec<&Parameter> = query.params.iter().filter(|p| !p.is_list).collect();

    match (target, strategy) {
        (RustTarget::Postgres, ListParamStrategy::Native) => {
            let repl = format!("= ANY(${})", list_param.index);
            let rewritten = replace_list_in_clause(&query.sql, list_param.index, &repl).unwrap_or_else(|| {
                eprintln!("warning: list param {} not found in IN clause, treating as scalar", list_param.name);
                query.sql.clone()
            });
            let sql = rewritten.replace('"', "\\\"").replace('\n', " ");
            let bind_names: Vec<&str> = query.params.iter().map(|p| p.name.as_str()).collect();
            emit_rust_sqlx_call(src, query, &sql, &bind_names, &row_type)
        },
        (RustTarget::Postgres, ListParamStrategy::Dynamic) => {
            let (before, after) = split_at_in_clause(&query.sql, list_param.index).unwrap_or_else(|| (query.sql.clone(), String::new()));
            let lp_name = to_snake_case(&list_param.name);
            let before_esc = before.replace('"', "\\\"").replace('\n', " ");
            let after_esc = after.replace('"', "\\\"").replace('\n', " ");
            // Other scalar params occupy indices 1..list_param.index-1; list elements start after.
            let base = list_param.index;
            writeln!(src, "    let placeholders: String = ({lp_name}).iter().enumerate()")?;
            writeln!(src, "        .map(|(i, _)| format!(\"${{}}\", {base} + i))")?;
            writeln!(src, "        .collect::<Vec<_>>().join(\", \");")?;
            writeln!(src, "    let sql = format!(\"{before_esc}IN ({{placeholders}}){after_esc}\");")?;
            writeln!(src, "    let mut q = sqlx::query_as::<_, {row_type}>(&sql);")?;
            for sp in &scalar_params {
                writeln!(src, "    q = q.bind({});", to_snake_case(&sp.name))?;
            }
            writeln!(src, "    for v in {lp_name} {{")?;
            writeln!(src, "        q = q.bind(v);")?;
            writeln!(src, "    }}")?;
            writeln!(src, "    q.{}.await", fetch_method(query))?;
            Ok(())
        },
        (RustTarget::Sqlite, ListParamStrategy::Native) => {
            let repl = "IN (SELECT value FROM json_each(?))";
            let rewritten = replace_list_in_clause(&query.sql, list_param.index, repl).unwrap_or_else(|| {
                eprintln!("warning: list param {} not found in IN clause, treating as scalar", list_param.name);
                query.sql.clone()
            });
            let sql = normalize_sql_for_sqlx(&rewritten, target).replace('"', "\\\"").replace('\n', " ");
            let lp_name = to_snake_case(&list_param.name);
            writeln!(src, "    let {lp_name}_json = {};", json_list_expr(&lp_name, &list_param.sql_type))?;
            let bind_names: Vec<String> = query.params.iter().map(|p| if p.is_list { format!("{lp_name}_json") } else { to_snake_case(&p.name) }).collect();
            let bind_refs: Vec<&str> = bind_names.iter().map(|s| s.as_str()).collect();
            emit_rust_sqlx_call(src, query, &sql, &bind_refs, &row_type)
        },
        (RustTarget::Sqlite, ListParamStrategy::Dynamic) | (RustTarget::Mysql, ListParamStrategy::Dynamic) => {
            let (before, after) = split_at_in_clause(&query.sql, list_param.index).unwrap_or_else(|| (query.sql.clone(), String::new()));
            let lp_name = to_snake_case(&list_param.name);
            let before_esc = normalize_sql_for_sqlx(&before, target).replace('"', "\\\"").replace('\n', " ");
            let after_esc = normalize_sql_for_sqlx(&after, target).replace('"', "\\\"").replace('\n', " ");
            writeln!(src, "    let placeholders = ({lp_name}).iter().map(|_| \"?\").collect::<Vec<_>>().join(\", \");")?;
            writeln!(src, "    let sql = format!(\"{before_esc}IN ({{placeholders}}){after_esc}\");")?;
            writeln!(src, "    let mut q = sqlx::query_as::<_, {row_type}>(&sql);")?;
            for sp in &scalar_params {
                writeln!(src, "    q = q.bind({});", to_snake_case(&sp.name))?;
            }
            writeln!(src, "    for v in {lp_name} {{")?;
            writeln!(src, "        q = q.bind(v);")?;
            writeln!(src, "    }}")?;
            writeln!(src, "    q.{}.await", fetch_method(query))?;
            Ok(())
        },
        (RustTarget::Mysql, ListParamStrategy::Native) => {
            let col_type = mysql_json_col_type(&list_param.sql_type);
            let repl = format!("IN (SELECT value FROM JSON_TABLE(?,'$[*]' COLUMNS(value {col_type} PATH '$')) t)");
            let rewritten = replace_list_in_clause(&query.sql, list_param.index, &repl).unwrap_or_else(|| {
                eprintln!("warning: list param {} not found in IN clause, treating as scalar", list_param.name);
                query.sql.clone()
            });
            let sql = normalize_sql_for_sqlx(&rewritten, target).replace('"', "\\\"").replace('\n', " ");
            let lp_name = to_snake_case(&list_param.name);
            writeln!(src, "    let {lp_name}_json = {};", json_list_expr(&lp_name, &list_param.sql_type))?;
            let bind_names: Vec<String> = query.params.iter().map(|p| if p.is_list { format!("{lp_name}_json") } else { to_snake_case(&p.name) }).collect();
            let bind_refs: Vec<&str> = bind_names.iter().map(|s| s.as_str()).collect();
            emit_rust_sqlx_call(src, query, &sql, &bind_refs, &row_type)
        },
    }
}

/// Generate inline Rust code that builds a JSON array string from a list parameter.
///
/// Produces an expression like `format!("[{}]", ids.iter().map(|x| ...).join(","))`.
/// Avoids any dependency on serde_json by formatting each element inline.
fn json_list_expr(lp_name: &str, sql_type: &SqlType) -> String {
    let elem = match sql_type {
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => r#"format!("\"{}\"", x.replace('\\', "\\\\").replace('"', "\\\""))"#.to_string(),
        SqlType::Uuid => r#"format!("\"{}\"", x)"#.to_string(),
        _ => "x.to_string()".to_string(),
    };
    format!(r#"format!("[{{}}]", {lp_name}.iter().map(|x| {elem}).collect::<Vec<_>>().join(","))"#)
}

/// Map a SqlType to the MySQL JSON_TABLE column type declaration.
fn mysql_json_col_type(sql_type: &SqlType) -> &'static str {
    match sql_type {
        SqlType::Boolean => "BOOLEAN",
        SqlType::SmallInt => "SMALLINT",
        SqlType::Integer => "INT",
        SqlType::BigInt => "BIGINT",
        SqlType::Real => "FLOAT",
        SqlType::Double => "DOUBLE",
        SqlType::Decimal => "DECIMAL(38,10)",
        _ => "CHAR(255)",
    }
}

fn fetch_method(query: &Query) -> &'static str {
    match query.cmd {
        QueryCmd::One => "fetch_optional(pool)",
        QueryCmd::Many => "fetch_all(pool)",
        QueryCmd::Exec => "execute(pool).map(|_| ())",
        QueryCmd::ExecRows => "execute(pool).map(|r| r.rows_affected())",
    }
}

/// Build the `name: type` signature fragment for a single parameter.
fn rust_param_sig(p: &Parameter, target: &RustTarget) -> String {
    let name = to_snake_case(&p.name);
    if p.is_list {
        let elem_ty = rust_type(&p.sql_type, false, target);
        format!("{name}: &[{elem_ty}]")
    } else {
        format!("{name}: {}", rust_type(&p.sql_type, p.nullable, target))
    }
}

/// Emit the sqlx query/query_as call with static SQL and a list of bind names.
///
/// When the same name appears multiple times (positional dialects like MySQL/SQLite
/// that repeat binds for repeated params), all occurrences except the last are
/// emitted with `.clone()` to avoid a move-before-use compile error.
fn emit_rust_sqlx_call(src: &mut String, query: &Query, sql: &str, bind_names: &[&str], row_type: &str) -> anyhow::Result<()> {
    // Count remaining occurrences so we know when to clone vs. move.
    let mut remaining: HashMap<&str, usize> = HashMap::new();
    for &name in bind_names {
        *remaining.entry(name).or_insert(0) += 1;
    }

    let bind_expr = |name: &str, remaining: &mut HashMap<&str, usize>| -> String {
        let snake = to_snake_case(name);
        let count = remaining.get_mut(name).unwrap();
        *count -= 1;
        if *count > 0 {
            format!("{snake}.clone()")
        } else {
            snake
        }
    };

    match query.cmd {
        QueryCmd::Exec | QueryCmd::ExecRows => {
            writeln!(src, "    sqlx::query(\"{sql}\")")?;
            for &name in bind_names {
                writeln!(src, "        .bind({})", bind_expr(name, &mut remaining))?;
            }
            writeln!(src, "        .execute(pool)")?;
            writeln!(src, "        .await")?;
            if matches!(query.cmd, QueryCmd::Exec) {
                writeln!(src, "        .map(|_| ())")?;
            } else {
                writeln!(src, "        .map(|r| r.rows_affected())")?;
            }
        },
        QueryCmd::One | QueryCmd::Many => {
            writeln!(src, "    sqlx::query_as::<_, {row_type}>(\"{sql}\")")?;
            for &name in bind_names {
                writeln!(src, "        .bind({})", bind_expr(name, &mut remaining))?;
            }
            if matches!(query.cmd, QueryCmd::One) {
                writeln!(src, "        .fetch_optional(pool)")?;
            } else {
                writeln!(src, "        .fetch_all(pool)")?;
            }
            writeln!(src, "        .await")?;
        },
    }
    Ok(())
}

fn result_row_type(query: &Query, schema: &Schema) -> String {
    if let Some(table_name) = infer_table(query, schema) {
        return to_pascal_case(table_name);
    }
    if !query.result_columns.is_empty() {
        return row_struct_name(&query.name);
    }
    "serde_json::Value".to_string()
}

fn row_struct_name(query_name: &str) -> String {
    format!("{}Row", to_pascal_case(query_name))
}

// ─── SQL helpers ──────────────────────────────────────────────────────────────

/// Normalize SQL placeholders for the target driver:
/// - SQLite `?N` → `?` (sqlx sqlite uses anonymous `?`)
/// - MySQL `$N` → `?` (sqlx mysql uses anonymous `?`)
/// - PostgreSQL `$N` → unchanged (sqlx postgres uses `$N`)
fn normalize_sql_for_sqlx(sql: &str, target: &RustTarget) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        match target {
            RustTarget::Sqlite => {
                // Rewrite `?N` or `$N` → `?` (sqlx sqlite uses bare `?`)
                if (ch == '?' || ch == '$') && chars.peek().is_some_and(|c| c.is_ascii_digit()) {
                    out.push('?');
                    while chars.peek().is_some_and(|c| c.is_ascii_digit()) {
                        chars.next();
                    }
                } else {
                    out.push(ch);
                }
            },
            RustTarget::Mysql => {
                // Replace `$N` with `?`
                if ch == '$' && chars.peek().is_some_and(|c| c.is_ascii_digit()) {
                    out.push('?');
                    while chars.peek().is_some_and(|c| c.is_ascii_digit()) {
                        chars.next();
                    }
                } else {
                    out.push(ch);
                }
            },
            RustTarget::Postgres => {
                out.push(ch);
            },
        }
    }
    out
}

// ─── Type mapping ─────────────────────────────────────────────────────────────

fn rust_type(sql_type: &SqlType, nullable: bool, target: &RustTarget) -> String {
    let base = match sql_type {
        SqlType::Boolean => "bool".to_string(),
        SqlType::SmallInt => "i16".to_string(),
        SqlType::Integer => "i32".to_string(),
        SqlType::BigInt => "i64".to_string(),
        SqlType::Real => "f32".to_string(),
        SqlType::Double => "f64".to_string(),
        // SQLite stores DECIMAL as REAL (f64); PostgreSQL and MySQL DECIMAL needs exact decimal.
        SqlType::Decimal => match target {
            RustTarget::Sqlite => "f64".to_string(),
            _ => "rust_decimal::Decimal".to_string(),
        },
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => "String".to_string(),
        SqlType::Bytes => "Vec<u8>".to_string(),
        SqlType::Date => "time::Date".to_string(),
        SqlType::Time => "time::Time".to_string(),
        SqlType::Timestamp => "time::PrimitiveDateTime".to_string(),
        SqlType::TimestampTz => "time::OffsetDateTime".to_string(),
        SqlType::Interval => "String".to_string(),
        SqlType::Uuid => "uuid::Uuid".to_string(),
        SqlType::Json | SqlType::Jsonb => "serde_json::Value".to_string(),
        SqlType::Array(inner) => {
            let inner_ty = rust_type(inner, false, target);
            let vec_ty = format!("Vec<{inner_ty}>");
            return if nullable { format!("Option<{vec_ty}>") } else { vec_ty };
        },
        SqlType::Custom(_) => "serde_json::Value".to_string(),
    };
    if nullable {
        format!("Option<{base}>")
    } else {
        base
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ListParamStrategy, OutputConfig};
    use crate::ir::{Column, Parameter, Query, QueryCmd, ResultColumn, Schema, SqlType, Table};

    fn cfg() -> OutputConfig {
        OutputConfig { out: "out".to_string(), package: String::new(), list_params: None }
    }

    fn get_file<'a>(files: &'a [GeneratedFile], name: &str) -> &'a str {
        files.iter().find(|f| f.path.file_name().is_some_and(|n| n == name)).unwrap_or_else(|| panic!("file {name:?} not found")).content.as_str()
    }

    fn pg() -> RustCodegen {
        RustCodegen { target: RustTarget::Postgres }
    }
    fn sqlite() -> RustCodegen {
        RustCodegen { target: RustTarget::Sqlite }
    }
    fn mysql() -> RustCodegen {
        RustCodegen { target: RustTarget::Mysql }
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

    // ─── generate: struct file ──────────────────────────────────────────────

    #[test]
    fn test_generate_table_struct() {
        let schema = Schema { tables: vec![user_table()] };
        let files = pg().generate(&schema, &[], &cfg()).unwrap();
        let src = get_file(&files, "user.rs");
        assert!(src.contains("#[derive(Debug, sqlx::FromRow)]"));
        assert!(src.contains("pub struct User {"));
        assert!(src.contains("pub id: i64,"));
        assert!(src.contains("pub name: String,"));
        assert!(src.contains("pub bio: Option<String>,"));
    }

    #[test]
    fn test_generate_mod_file() {
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
        let src = get_file(&files, "mod.rs");
        assert!(src.contains("pub mod user;"));
        assert!(src.contains("pub mod queries;"));
    }

    // ─── generate: pool type ────────────────────────────────────────────────

    #[test]
    fn test_generate_postgres_uses_pg_pool() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "DeleteUser".to_string(),
            cmd: QueryCmd::Exec,
            sql: "DELETE FROM user WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
            result_columns: vec![],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "queries.rs");
        assert!(src.contains("use sqlx::PgPool;"));
        assert!(src.contains("pool: &PgPool"));
    }

    #[test]
    fn test_generate_sqlite_uses_sqlite_pool() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "DeleteUser".to_string(),
            cmd: QueryCmd::Exec,
            sql: "DELETE FROM user WHERE id = ?1".to_string(),
            params: vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
            result_columns: vec![],
        };
        let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "queries.rs");
        assert!(src.contains("use sqlx::SqlitePool;"));
        assert!(src.contains("pool: &SqlitePool"));
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
        let src = get_file(&files, "queries.rs");
        assert!(src.contains("pub async fn delete_user(pool: &PgPool, id: i64) -> Result<(), sqlx::Error>"));
        assert!(src.contains(".execute(pool)"));
        assert!(src.contains(".map(|_| ())"));
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
        let src = get_file(&files, "queries.rs");
        assert!(src.contains("pub async fn delete_users(pool: &PgPool, active: bool) -> Result<u64, sqlx::Error>"));
        assert!(src.contains(".map(|r| r.rows_affected())"));
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
        let src = get_file(&files, "queries.rs");
        assert!(src.contains("pub async fn get_user(pool: &PgPool, id: i64) -> Result<Option<User>, sqlx::Error>"));
        assert!(src.contains(".fetch_optional(pool)"));
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
        let src = get_file(&files, "queries.rs");
        assert!(src.contains("pub async fn list_users(pool: &PgPool) -> Result<Vec<User>, sqlx::Error>"));
        assert!(src.contains(".fetch_all(pool)"));
    }

    // ─── generate: inline row struct ────────────────────────────────────────

    #[test]
    fn test_generate_inline_row_struct_for_partial_result() {
        let schema = Schema { tables: vec![user_table()] };
        let query = Query {
            name: "GetUserName".to_string(),
            cmd: QueryCmd::One,
            sql: "SELECT name FROM user WHERE id = $1".to_string(),
            params: vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false }],
        };
        let files = pg().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "queries.rs");
        assert!(src.contains("pub struct GetUserNameRow {"));
        assert!(src.contains("Result<Option<GetUserNameRow>, sqlx::Error>"));
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
        let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: None };
        let files = pg().generate(&schema, &[query], &cfg).unwrap();
        let src = get_file(&files, "queries.rs");
        assert!(src.contains("ids: &[i64]"), "signature should use &[i64]");
        assert!(src.contains("= ANY($1)"), "PG native should rewrite to ANY");
        assert!(!src.contains("IN ($1)"), "original IN clause should be gone");
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
        let src = get_file(&files, "queries.rs");
        assert!(src.contains("ids: &[i64]"), "signature should use &[i64]");
        assert!(src.contains("placeholders"), "dynamic mode builds placeholders at runtime");
        assert!(src.contains("for v in ids"), "dynamic mode binds each element");
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
        let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: None };
        let files = sqlite().generate(&schema, &[query], &cfg).unwrap();
        let src = get_file(&files, "queries.rs");
        assert!(src.contains("ids: &[i64]"), "signature should use &[i64]");
        assert!(src.contains("json_each"), "SQLite native uses json_each");
        assert!(src.contains("ids_json"), "should bind the json local variable");
        assert!(!src.contains("serde_json"), "must not require serde_json");
    }

    #[test]
    fn test_generate_mysql_native_list_param() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetByIds".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id FROM t WHERE id IN ($1)".to_string(),
            params: vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
        };
        let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: None };
        let files = mysql().generate(&schema, &[query], &cfg).unwrap();
        let src = get_file(&files, "queries.rs");
        assert!(src.contains("ids: &[i64]"), "signature should use &[i64]");
        assert!(src.contains("JSON_TABLE"), "MySQL native uses JSON_TABLE");
        assert!(src.contains("ids_json"), "should bind the json local variable");
        assert!(!src.contains("serde_json"), "must not require serde_json");
    }

    #[test]
    fn test_generate_mysql_dynamic_list_param() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetByIds".to_string(),
            cmd: QueryCmd::Many,
            sql: "SELECT id FROM t WHERE id IN ($1)".to_string(),
            params: vec![Parameter::list(1, "ids", SqlType::BigInt, false)],
            result_columns: vec![ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false }],
        };
        let cfg = OutputConfig { out: "out".to_string(), package: String::new(), list_params: Some(ListParamStrategy::Dynamic) };
        let files = mysql().generate(&schema, &[query], &cfg).unwrap();
        let src = get_file(&files, "queries.rs");
        assert!(src.contains("ids: &[i64]"), "signature should use &[i64]");
        assert!(src.contains("placeholders"), "dynamic strategy builds placeholders");
        assert!(!src.contains("JSON_TABLE"), "dynamic strategy does not use JSON_TABLE");
    }

    // ─── generate: SQLite placeholder rewriting ─────────────────────────────

    #[test]
    fn test_generate_sqlite_rewrites_placeholders() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "GetUser".to_string(),
            cmd: QueryCmd::Exec,
            sql: "DELETE FROM user WHERE id = ?1".to_string(),
            params: vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
            result_columns: vec![],
        };
        let files = sqlite().generate(&schema, &[query], &cfg()).unwrap();
        let src = get_file(&files, "queries.rs");
        // ?1 should be rewritten to ? for sqlx sqlite
        assert!(src.contains("\"DELETE FROM user WHERE id = ?\""));
    }
}
