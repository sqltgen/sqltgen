pub mod query;
pub mod schema;
pub mod typemap;

use crate::frontend::DialectParser;
use crate::ir::{Query, Schema};

pub struct SqliteParser;

impl DialectParser for SqliteParser {
    fn parse_schema(&self, ddl: &str, default_schema: Option<&str>) -> anyhow::Result<Schema> {
        schema::parse_schema(ddl, default_schema)
    }

    fn parse_queries(&self, sql: &str, schema: &Schema, default_schema: Option<&str>) -> anyhow::Result<Vec<Query>> {
        query::parse_queries(sql, schema, default_schema)
    }
}
