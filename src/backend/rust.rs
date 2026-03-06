use std::collections::HashSet;
use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::common::{infer_table, to_pascal_case, to_snake_case};
use crate::backend::{Codegen, GeneratedFile};
use crate::config::OutputConfig;
use crate::ir::{Query, QueryCmd, Schema, SqlType};

pub enum RustTarget {
    Postgres,
    Sqlite,
    Mysql,
}

pub struct RustCodegen {
    pub target: RustTarget,
}

impl Codegen for RustCodegen {
    fn generate(
        &self,
        schema: &Schema,
        queries: &[Query],
        config: &OutputConfig,
    ) -> anyhow::Result<Vec<GeneratedFile>> {
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
            RustTarget::Sqlite   => "SqlitePool",
            RustTarget::Mysql    => "MySqlPool",
        };
            let mut src = String::new();
            writeln!(src, "use sqlx::{pool_type};")?;
            writeln!(src)?;

            // Import only table structs that are actually used as return types
            let needed: HashSet<&str> =
                queries.iter().filter_map(|q| infer_table(q, schema)).collect();
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
            for (i, query) in queries.iter().enumerate() {
                if i > 0 {
                    writeln!(src)?;
                }
                emit_rust_query(&mut src, query, schema, pool_type, &self.target)?;
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

fn emit_rust_query(src: &mut String, query: &Query, schema: &Schema, pool_type: &str, target: &RustTarget) -> anyhow::Result<()> {
    let fn_name = to_snake_case(&query.name);
    let row_type = result_row_type(query, schema);

    let return_type = match query.cmd {
        QueryCmd::One      => format!("Result<Option<{row_type}>, sqlx::Error>"),
        QueryCmd::Many     => format!("Result<Vec<{row_type}>, sqlx::Error>"),
        QueryCmd::Exec     => "Result<(), sqlx::Error>".to_string(),
        QueryCmd::ExecRows => "Result<u64, sqlx::Error>".to_string(),
    };

    let params_sig: String = std::iter::once(format!("pool: &{pool_type}"))
        .chain(query.params.iter().map(|p| {
            format!("{}: {}", to_snake_case(&p.name), rust_type(&p.sql_type, p.nullable, target))
        }))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = normalize_sql_for_sqlx(&query.sql, target).replace('"', "\\\"").replace('\n', " ");

    writeln!(src, "pub async fn {fn_name}({params_sig}) -> {return_type} {{")?;

    match query.cmd {
        QueryCmd::Exec | QueryCmd::ExecRows => {
            writeln!(src, "    sqlx::query(\"{sql}\")")?;
            for p in &query.params {
                writeln!(src, "        .bind({})", to_snake_case(&p.name))?;
            }
            writeln!(src, "        .execute(pool)")?;
            writeln!(src, "        .await")?;
            if matches!(query.cmd, QueryCmd::Exec) {
                writeln!(src, "        .map(|_| ())")?;
            } else {
                writeln!(src, "        .map(|r| r.rows_affected())")?;
            }
        }
        QueryCmd::One | QueryCmd::Many => {
            writeln!(src, "    sqlx::query_as::<_, {row_type}>(\"{sql}\")")?;
            for p in &query.params {
                writeln!(src, "        .bind({})", to_snake_case(&p.name))?;
            }
            if matches!(query.cmd, QueryCmd::One) {
                writeln!(src, "        .fetch_optional(pool)")?;
            } else {
                writeln!(src, "        .fetch_all(pool)")?;
            }
            writeln!(src, "        .await")?;
        }
    }

    writeln!(src, "}}")?;
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
                // Strip digits after `?`
                if ch == '?' && chars.peek().map_or(false, |c| c.is_ascii_digit()) {
                    out.push('?');
                    while chars.peek().map_or(false, |c| c.is_ascii_digit()) {
                        chars.next();
                    }
                } else {
                    out.push(ch);
                }
            }
            RustTarget::Mysql => {
                // Replace `$N` with `?`
                if ch == '$' && chars.peek().map_or(false, |c| c.is_ascii_digit()) {
                    out.push('?');
                    while chars.peek().map_or(false, |c| c.is_ascii_digit()) {
                        chars.next();
                    }
                } else {
                    out.push(ch);
                }
            }
            RustTarget::Postgres => {
                out.push(ch);
            }
        }
    }
    out
}

// ─── Type mapping ─────────────────────────────────────────────────────────────

fn rust_type(sql_type: &SqlType, nullable: bool, target: &RustTarget) -> String {
    let base = match sql_type {
        SqlType::Boolean             => "bool".to_string(),
        SqlType::SmallInt            => "i16".to_string(),
        SqlType::Integer             => "i32".to_string(),
        SqlType::BigInt              => "i64".to_string(),
        SqlType::Real                => "f32".to_string(),
        SqlType::Double              => "f64".to_string(),
        // SQLite stores DECIMAL as REAL (f64); PostgreSQL and MySQL DECIMAL needs exact decimal.
        SqlType::Decimal             => match target {
            RustTarget::Sqlite => "f64".to_string(),
            _ => "rust_decimal::Decimal".to_string(),
        },
        SqlType::Text
        | SqlType::Char(_)
        | SqlType::VarChar(_)        => "String".to_string(),
        SqlType::Bytes               => "Vec<u8>".to_string(),
        SqlType::Date                => "time::Date".to_string(),
        SqlType::Time                => "time::Time".to_string(),
        SqlType::Timestamp           => "time::PrimitiveDateTime".to_string(),
        SqlType::TimestampTz         => "time::OffsetDateTime".to_string(),
        SqlType::Interval            => "String".to_string(),
        SqlType::Uuid                => "uuid::Uuid".to_string(),
        SqlType::Json | SqlType::Jsonb => "serde_json::Value".to_string(),
        SqlType::Array(inner)        => {
            let inner_ty = rust_type(inner, false, target);
            let vec_ty = format!("Vec<{inner_ty}>");
            return if nullable { format!("Option<{vec_ty}>") } else { vec_ty };
        }
        SqlType::Custom(_)           => "serde_json::Value".to_string(),
    };
    if nullable { format!("Option<{base}>") } else { base }
}

