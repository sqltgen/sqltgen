use super::*;
use crate::ir::{Column, Schema, SqlType, Table};
use sqlparser::dialect::PostgreSqlDialect;

pub fn parse_queries(sql: &str, schema: &Schema) -> anyhow::Result<Vec<Query>> {
    parse_queries_with_config(&PostgreSqlDialect {}, sql, schema, &ResolverConfig::default())
}

pub fn make_schema() -> Schema {
    Schema {
        tables: vec![Table::new(
            "users".into(),
            vec![
                Column { name: "id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                Column { name: "name".into(), sql_type: SqlType::Text, nullable: false, is_primary_key: false },
                Column { name: "email".into(), sql_type: SqlType::Text, nullable: false, is_primary_key: false },
                Column { name: "bio".into(), sql_type: SqlType::Text, nullable: true, is_primary_key: false },
            ],
        )],
        ..Default::default()
    }
}

pub fn make_join_schema() -> Schema {
    Schema {
        tables: vec![
            Table::new(
                "users".into(),
                vec![
                    Column { name: "id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                    Column { name: "name".into(), sql_type: SqlType::Text, nullable: false, is_primary_key: false },
                ],
            ),
            Table::new(
                "posts".into(),
                vec![
                    Column { name: "id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                    Column { name: "user_id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: false },
                    Column { name: "title".into(), sql_type: SqlType::Text, nullable: false, is_primary_key: false },
                ],
            ),
        ],
        ..Default::default()
    }
}

pub fn make_inventory_schema() -> Schema {
    Schema {
        tables: vec![Table::new(
            "inventory".into(),
            vec![
                Column { name: "sku".into(), sql_type: SqlType::Text, nullable: false, is_primary_key: true },
                Column { name: "qty".into(), sql_type: SqlType::Integer, nullable: false, is_primary_key: false },
            ],
        )],
        ..Default::default()
    }
}

pub fn make_upsert_schema() -> Schema {
    Schema {
        tables: vec![Table::new(
            "item".into(),
            vec![
                Column { name: "id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                Column { name: "count".into(), sql_type: SqlType::Integer, nullable: false, is_primary_key: false },
            ],
        )],
        ..Default::default()
    }
}

pub fn make_numeric_schema() -> Schema {
    Schema {
        tables: vec![Table::new(
            "metrics".into(),
            vec![
                Column { name: "id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                Column { name: "small_val".into(), sql_type: SqlType::SmallInt, nullable: false, is_primary_key: false },
                Column { name: "int_val".into(), sql_type: SqlType::Integer, nullable: false, is_primary_key: false },
                Column { name: "big_val".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: false },
                Column { name: "dec_val".into(), sql_type: SqlType::Decimal, nullable: false, is_primary_key: false },
                Column { name: "dbl_val".into(), sql_type: SqlType::Double, nullable: false, is_primary_key: false },
                Column { name: "label".into(), sql_type: SqlType::Text, nullable: false, is_primary_key: false },
            ],
        )],
        ..Default::default()
    }
}

pub fn pg_config() -> ResolverConfig {
    ResolverConfig {
        typemap: crate::frontend::postgres::typemap::map,
        sum_bigint_type: SqlType::Decimal,
        avg_integer_type: SqlType::Decimal,
        ..ResolverConfig::default()
    }
}

pub fn mysql_config() -> ResolverConfig {
    ResolverConfig {
        sum_integer_type: SqlType::Decimal,
        sum_bigint_type: SqlType::Decimal,
        avg_integer_type: SqlType::Decimal,
        typemap: crate::frontend::mysql::typemap::map,
        ..ResolverConfig::default()
    }
}

pub fn sqlite_config() -> ResolverConfig {
    ResolverConfig { typemap: crate::frontend::sqlite::typemap::map, ..ResolverConfig::default() }
}

/// Parse a single aggregate query and return the type and nullability of
/// the named result column. The `expr` argument is the SELECT expression
/// list (e.g. `"SUM(int_val) AS s"`); the helper wraps it in a full query.
pub fn agg_col(expr: &str, schema: &Schema, config: &ResolverConfig, name: &str) -> (SqlType, bool) {
    let sql = format!("-- name: Q :one\nSELECT {expr} FROM metrics;");
    let qs = parse_queries_with_config(&PostgreSqlDialect {}, &sql, schema, config).unwrap();
    assert!(!qs.is_empty(), "query did not parse (check table/column names)");
    let col = qs[0]
        .result_columns
        .iter()
        .find(|c| c.name == name)
        .unwrap_or_else(|| panic!("column {name} not found in {:?}", qs[0].result_columns.iter().map(|c| &c.name).collect::<Vec<_>>()));
    (col.sql_type.clone(), col.nullable)
}

mod aggregates;
mod annotations;
mod ctes;
mod dml;
mod expr;
mod joins;
mod params;
mod select;
mod set_ops;
mod source_table;
mod subqueries;
