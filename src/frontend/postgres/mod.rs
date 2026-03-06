pub mod query;
pub mod schema;
pub mod typemap;

use crate::frontend::DialectParser;
use crate::ir::{Query, Schema};

pub struct PostgresParser;

impl DialectParser for PostgresParser {
    fn parse_schema(&self, ddl: &str) -> anyhow::Result<Schema> {
        schema::parse_schema(ddl)
    }

    fn parse_queries(&self, sql: &str, schema: &Schema) -> anyhow::Result<Vec<Query>> {
        query::parse_queries(sql, schema)
    }
}
