use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::path::PathBuf;

use crate::backend::common::{group_queries, has_inline_rows, infer_row_type_name, infer_table, mysql_json_table_col_type, queries_file_stem};
use crate::backend::naming::{to_pascal_case, to_snake_case};
use crate::backend::sql_rewrite::{positional_bind_names, replace_list_in_clause, rewrite_to_anon_params, split_at_in_clause};
use crate::backend::{Codegen, GeneratedFile};
use crate::config::{Engine, ListParamStrategy, OutputConfig};
use crate::ir::{Parameter, Query, QueryCmd, Schema, SqlType};

pub enum RustTarget {
    Postgres,
    Sqlite,
    Mysql,
}

impl From<Engine> for RustTarget {
    fn from(engine: Engine) -> Self {
        match engine {
            Engine::Postgresql => RustTarget::Postgres,
            Engine::Sqlite => RustTarget::Sqlite,
            Engine::Mysql => RustTarget::Mysql,
        }
    }
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

        // One .rs file per query group
        let pool_type = match self.target {
            RustTarget::Postgres => "PgPool",
            RustTarget::Sqlite => "SqlitePool",
            RustTarget::Mysql => "MySqlPool",
        };
        let strategy = config.list_params.clone().unwrap_or_default();
        let groups = group_queries(queries);
        let mut group_stems: Vec<String> = Vec::new();
        for (group, group_queries) in &groups {
            let stem = queries_file_stem(group).to_string();
            group_stems.push(stem.clone());

            let mut src = String::new();
            writeln!(src, "use sqlx::{pool_type};")?;
            writeln!(src)?;

            // Import only table structs that are actually used as return types
            let needed: HashSet<&str> = group_queries.iter().filter_map(|q| infer_table(q, schema)).collect();
            let mut needed_sorted: Vec<&str> = needed.iter().copied().collect();
            needed_sorted.sort();
            for name in &needed_sorted {
                writeln!(src, "use super::{}::{};", name, to_pascal_case(name))?;
            }
            if !needed.is_empty() {
                writeln!(src)?;
            }

            // Custom row structs for queries that don't return a whole table
            for query in group_queries {
                if has_inline_rows(query, schema) {
                    emit_row_struct(&mut src, query, &self.target)?;
                    writeln!(src)?;
                }
            }

            // Query functions
            for (i, query) in group_queries.iter().enumerate() {
                if i > 0 {
                    writeln!(src)?;
                }
                emit_rust_query(&mut src, query, schema, pool_type, &self.target, &strategy)?;
            }

            let path = PathBuf::from(&config.out).join(format!("{stem}.rs"));
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
            for stem in &group_stems {
                writeln!(src, "pub mod {stem};")?;
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
            let lp_name = to_snake_case(&list_param.name);
            let base = list_param.index;
            writeln!(src, "    let placeholders: String = ({lp_name}).iter().enumerate()")?;
            writeln!(src, "        .map(|(i, _)| format!(\"${{}}\", {base} + i))")?;
            writeln!(src, "        .collect::<Vec<_>>().join(\", \");")?;
            emit_rust_dynamic_query(src, query, &row_type, list_param, target)
        },
        (RustTarget::Sqlite, ListParamStrategy::Native) => {
            let repl = "IN (SELECT value FROM json_each(?))";
            let rewritten = replace_list_in_clause(&query.sql, list_param.index, repl).unwrap_or_else(|| {
                eprintln!("warning: list param {} not found in IN clause, treating as scalar", list_param.name);
                query.sql.clone()
            });
            emit_rust_json_native(src, query, &row_type, list_param, &rewritten, target)
        },
        (RustTarget::Sqlite, ListParamStrategy::Dynamic) | (RustTarget::Mysql, ListParamStrategy::Dynamic) => {
            let lp_name = to_snake_case(&list_param.name);
            writeln!(src, "    let placeholders = ({lp_name}).iter().map(|_| \"?\").collect::<Vec<_>>().join(\", \");")?;
            emit_rust_dynamic_query(src, query, &row_type, list_param, target)
        },
        (RustTarget::Mysql, ListParamStrategy::Native) => {
            let col_type = mysql_json_table_col_type(&list_param.sql_type);
            let repl = format!("IN (SELECT value FROM JSON_TABLE(?,'$[*]' COLUMNS(value {col_type} PATH '$')) t)");
            let rewritten = replace_list_in_clause(&query.sql, list_param.index, &repl).unwrap_or_else(|| {
                eprintln!("warning: list param {} not found in IN clause, treating as scalar", list_param.name);
                query.sql.clone()
            });
            emit_rust_json_native(src, query, &row_type, list_param, &rewritten, target)
        },
    }
}

/// Emit the shared tail of a dynamic list query: build SQL, bind scalars, bind list.
fn emit_rust_dynamic_query(src: &mut String, query: &Query, row_type: &str, list_param: &Parameter, target: &RustTarget) -> anyhow::Result<()> {
    let scalar_params: Vec<&Parameter> = query.params.iter().filter(|p| !p.is_list).collect();
    let lp_name = to_snake_case(&list_param.name);
    let (before, after) = split_at_in_clause(&query.sql, list_param.index).unwrap_or_else(|| (query.sql.clone(), String::new()));
    let before_esc = normalize_sql_for_sqlx(&before, target).replace('"', "\\\"").replace('\n', " ");
    let after_esc = normalize_sql_for_sqlx(&after, target).replace('"', "\\\"").replace('\n', " ");
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
}

/// Emit a SQLite or MySQL native list query that binds a JSON array string.
fn emit_rust_json_native(
    src: &mut String,
    query: &Query,
    row_type: &str,
    list_param: &Parameter,
    rewritten_sql: &str,
    target: &RustTarget,
) -> anyhow::Result<()> {
    let sql = normalize_sql_for_sqlx(rewritten_sql, target).replace('"', "\\\"").replace('\n', " ");
    let lp_name = to_snake_case(&list_param.name);
    writeln!(src, "    let {lp_name}_json = {};", json_list_expr(&lp_name, &list_param.sql_type))?;
    let bind_names: Vec<String> = query.params.iter().map(|p| if p.is_list { format!("{lp_name}_json") } else { to_snake_case(&p.name) }).collect();
    let bind_refs: Vec<&str> = bind_names.iter().map(|s| s.as_str()).collect();
    emit_rust_sqlx_call(src, query, &sql, &bind_refs, row_type)
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
    infer_row_type_name(query, schema).unwrap_or_else(|| "serde_json::Value".to_string())
}

fn row_struct_name(query_name: &str) -> String {
    format!("{}Row", to_pascal_case(query_name))
}

// ─── SQL helpers ──────────────────────────────────────────────────────────────

/// Normalize SQL placeholders for the target sqlx driver:
/// - SQLite `?N`/`$N` → `?` (sqlx sqlite uses anonymous `?`)
/// - MySQL `$N`/`?N` → `?` (sqlx mysql uses anonymous `?`)
/// - PostgreSQL `$N` → unchanged (sqlx postgres uses `$N`)
fn normalize_sql_for_sqlx(sql: &str, target: &RustTarget) -> String {
    match target {
        RustTarget::Sqlite | RustTarget::Mysql => rewrite_to_anon_params(sql),
        RustTarget::Postgres => sql.to_string(),
    }
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
mod tests;
