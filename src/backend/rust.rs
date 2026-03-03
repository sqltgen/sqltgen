use std::collections::HashSet;
use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::{Codegen, GeneratedFile};
use crate::config::OutputConfig;
use crate::ir::{Query, QueryCmd, Schema, SqlType};

pub struct RustCodegen;

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
                writeln!(src, "    pub {}: {},", col.name, rust_type(&col.sql_type, col.nullable))?;
            }
            writeln!(src, "}}")?;

            let path = PathBuf::from(&config.out).join(format!("{}.rs", table.name));
            files.push(GeneratedFile { path, content: src });
        }

        // queries.rs
        if !queries.is_empty() {
            let mut src = String::new();
            writeln!(src, "use sqlx::PgPool;")?;
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
                    emit_row_struct(&mut src, query)?;
                    writeln!(src)?;
                }
            }

            // Query functions
            for (i, query) in queries.iter().enumerate() {
                if i > 0 {
                    writeln!(src)?;
                }
                emit_rust_query(&mut src, query, schema)?;
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

fn emit_row_struct(src: &mut String, query: &Query) -> anyhow::Result<()> {
    let name = row_struct_name(&query.name);
    writeln!(src, "#[derive(Debug, sqlx::FromRow)]")?;
    writeln!(src, "pub struct {name} {{")?;
    for col in &query.result_columns {
        writeln!(src, "    pub {}: {},", col.name, rust_type(&col.sql_type, col.nullable))?;
    }
    writeln!(src, "}}")?;
    Ok(())
}

fn emit_rust_query(src: &mut String, query: &Query, schema: &Schema) -> anyhow::Result<()> {
    let fn_name = to_snake_case(&query.name);
    let row_type = result_row_type(query, schema);

    let return_type = match query.cmd {
        QueryCmd::One      => format!("Result<Option<{row_type}>, sqlx::Error>"),
        QueryCmd::Many     => format!("Result<Vec<{row_type}>, sqlx::Error>"),
        QueryCmd::Exec     => "Result<(), sqlx::Error>".to_string(),
        QueryCmd::ExecRows => "Result<u64, sqlx::Error>".to_string(),
    };

    let params_sig: String = std::iter::once("pool: &PgPool".to_string())
        .chain(query.params.iter().map(|p| {
            format!("{}: {}", to_snake_case(&p.name), rust_type(&p.sql_type, p.nullable))
        }))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = query.sql.replace('"', "\\\"").replace('\n', " ");

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

fn infer_table<'a>(query: &Query, schema: &'a Schema) -> Option<&'a str> {
    for table in &schema.tables {
        if table.columns.len() == query.result_columns.len()
            && table.columns.iter().zip(&query.result_columns).all(|(a, b)| a.name == b.name)
        {
            return Some(&table.name);
        }
    }
    None
}

// ─── Type mapping ─────────────────────────────────────────────────────────────

fn rust_type(sql_type: &SqlType, nullable: bool) -> String {
    let base = match sql_type {
        SqlType::Boolean             => "bool".to_string(),
        SqlType::SmallInt            => "i16".to_string(),
        SqlType::Integer             => "i32".to_string(),
        SqlType::BigInt              => "i64".to_string(),
        SqlType::Real                => "f32".to_string(),
        SqlType::Double              => "f64".to_string(),
        SqlType::Decimal             => "f64".to_string(),
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
            let inner_ty = rust_type(inner, false);
            let vec_ty = format!("Vec<{inner_ty}>");
            return if nullable { format!("Option<{vec_ty}>") } else { vec_ty };
        }
        SqlType::Custom(_)           => "serde_json::Value".to_string(),
    };
    if nullable { format!("Option<{base}>") } else { base }
}

// ─── Name helpers ─────────────────────────────────────────────────────────────

fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|w| {
            let mut c = w.chars();
            match c.next() {
                None    => String::new(),
                Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
            }
        })
        .collect()
}

/// PascalCase / camelCase → snake_case (column names already snake_case are unchanged).
fn to_snake_case(s: &str) -> String {
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}
