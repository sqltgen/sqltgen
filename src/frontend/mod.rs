pub mod postgres;
pub mod sqlite;
pub(crate) mod common;

use crate::ir::{Query, Schema};

pub trait DialectParser {
    fn parse_schema(&self, ddl: &str) -> anyhow::Result<Schema>;
    fn parse_queries(&self, sql: &str, schema: &Schema) -> anyhow::Result<Vec<Query>>;
}
