use super::types::SqlType;

/// A user-defined scalar function read from `CREATE FUNCTION` DDL.
///
/// Holds enough of the signature to infer result-column types and parameter
/// types at query-parse time. Table-valued functions are excluded — they are
/// not scalar and require different handling.
#[derive(Debug, Clone)]
pub struct ScalarFunction {
    pub name: String,
    pub return_type: SqlType,
    /// Positional types of `IN` parameters (OUT/INOUT parameters are excluded
    /// because they are return values, not inputs).
    pub param_types: Vec<SqlType>,
}

#[derive(Debug, Clone, Default)]
pub struct Schema {
    pub tables: Vec<Table>,
    /// User-defined scalar functions parsed from `CREATE FUNCTION` statements.
    pub functions: Vec<ScalarFunction>,
}

impl Schema {
    /// Construct a schema containing only the given tables, with no functions.
    pub fn with_tables(tables: Vec<Table>) -> Self {
        Self { tables, ..Default::default() }
    }
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub sql_type: SqlType,
    pub nullable: bool,
    pub is_primary_key: bool,
}
