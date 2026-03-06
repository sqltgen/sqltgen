use sqlparser::dialect::PostgreSqlDialect;

use crate::frontend::common::query::{parse_queries_with_config, ResolverConfig};
use crate::ir::{Query, Schema};

pub fn parse_queries(sql: &str, schema: &Schema) -> anyhow::Result<Vec<Query>> {
    parse_queries_with_config(&PostgreSqlDialect {}, sql, schema, &ResolverConfig::default())
}
