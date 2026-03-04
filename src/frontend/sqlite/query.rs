use sqlparser::dialect::SQLiteDialect;

use crate::ir::{Query, Schema};

pub fn parse_queries(sql: &str, schema: &Schema) -> anyhow::Result<Vec<Query>> {
    crate::frontend::postgres::query::parse_queries_with_dialect(
        &SQLiteDialect {},
        sql,
        schema,
    )
}
