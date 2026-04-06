pub(crate) mod common;
pub mod mysql;
pub mod postgres;
pub mod sqlite;

use crate::ir::{Query, Schema};

pub trait DialectParser {
    fn parse_schema(&self, ddl: &str, default_schema: Option<&str>) -> anyhow::Result<Schema>;
    fn parse_queries(&self, sql: &str, schema: &Schema, default_schema: Option<&str>) -> anyhow::Result<Vec<Query>>;
}
