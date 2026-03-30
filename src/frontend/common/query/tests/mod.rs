use super::*;
use crate::ir::{Column, Schema, SqlType, Table};
use sqlparser::dialect::PostgreSqlDialect;

pub fn parse_queries(sql: &str, schema: &Schema) -> anyhow::Result<Vec<Query>> {
    parse_queries_with_config(&PostgreSqlDialect {}, sql, schema, &ResolverConfig::default())
}

pub fn make_schema() -> Schema {
    Schema {
        tables: vec![Table::new(
            "users",
            vec![
                Column::new_primary_key("id", SqlType::BigInt),
                Column::new_not_nullable("name", SqlType::Text),
                Column::new_not_nullable("email", SqlType::Text),
                Column::new("bio", SqlType::Text),
            ],
        )],
        ..Default::default()
    }
}

pub fn make_join_schema() -> Schema {
    Schema {
        tables: vec![
            Table::new("users", vec![Column::new_primary_key("id", SqlType::BigInt), Column::new_not_nullable("name", SqlType::Text)]),
            Table::new(
                "posts",
                vec![
                    Column::new_primary_key("id", SqlType::BigInt),
                    Column::new_not_nullable("user_id", SqlType::BigInt),
                    Column::new_not_nullable("title", SqlType::Text),
                ],
            ),
        ],
        ..Default::default()
    }
}

pub fn make_inventory_schema() -> Schema {
    Schema {
        tables: vec![Table::new("inventory", vec![Column::new_primary_key("sku", SqlType::Text), Column::new_not_nullable("qty", SqlType::Integer)])],
        ..Default::default()
    }
}

pub fn make_upsert_schema() -> Schema {
    Schema {
        tables: vec![Table::new("item", vec![Column::new_primary_key("id", SqlType::BigInt), Column::new_not_nullable("count", SqlType::Integer)])],
        ..Default::default()
    }
}

pub fn make_numeric_schema() -> Schema {
    Schema {
        tables: vec![Table::new(
            "metrics",
            vec![
                Column::new_primary_key("id", SqlType::BigInt),
                Column::new_not_nullable("small_val", SqlType::SmallInt),
                Column::new_not_nullable("int_val", SqlType::Integer),
                Column::new_not_nullable("big_val", SqlType::BigInt),
                Column::new_not_nullable("dec_val", SqlType::Decimal),
                Column::new_not_nullable("dbl_val", SqlType::Double),
                Column::new_not_nullable("label", SqlType::Text),
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
mod nest;
mod params;
mod select;
mod set_ops;
mod source_table;
mod subqueries;
