use sqlparser::dialect::SQLiteDialect;

use crate::ir::{Query, Schema};
use crate::frontend::common::query::{parse_queries_with_config, ResolverConfig};

pub fn parse_queries(sql: &str, schema: &Schema) -> anyhow::Result<Vec<Query>> {
    parse_queries_with_config(&SQLiteDialect {}, sql, schema, &ResolverConfig::default())
}
