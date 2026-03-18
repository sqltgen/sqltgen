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

/// Distinguishes base tables from views in the schema.
///
/// Backends must skip views when emitting INSERT/UPDATE helpers, since views
/// are read-only virtual tables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableKind {
    /// A base table created with `CREATE TABLE`.
    Table,
    /// A view created with `CREATE VIEW`.
    View,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub kind: TableKind,
}

impl Table {
    /// Construct a base table (from `CREATE TABLE`).
    pub fn new(name: impl Into<String>, columns: Vec<Column>) -> Self {
        Self { name: name.into(), columns, kind: TableKind::Table }
    }

    /// Construct a view (from `CREATE VIEW`).
    pub fn view(name: impl Into<String>, columns: Vec<Column>) -> Self {
        Self { name: name.into(), columns, kind: TableKind::View }
    }

    /// Returns `true` if this entry is a view rather than a base table.
    pub fn is_view(&self) -> bool {
        self.kind == TableKind::View
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub sql_type: SqlType,
    pub nullable: bool,
    pub is_primary_key: bool,
}

impl Column {
    /// Construct a nullable, non-primary-key column.
    pub fn new(name: impl Into<String>, sql_type: SqlType) -> Self {
        Self { name: name.into(), sql_type, nullable: true, is_primary_key: false }
    }

    /// Construct a non-nullable, non-primary-key column.
    pub fn new_not_nullable(name: impl Into<String>, sql_type: SqlType) -> Self {
        Self { name: name.into(), sql_type, nullable: false, is_primary_key: false }
    }

    /// Construct a non-nullable primary key column.
    pub fn new_primary_key(name: impl Into<String>, sql_type: SqlType) -> Self {
        Self { name: name.into(), sql_type, nullable: false, is_primary_key: true }
    }
}
