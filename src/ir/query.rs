use super::types::SqlType;

#[derive(Debug, Clone)]
pub struct Query {
    /// Camel-case name from the `-- name: Foo :cmd` annotation.
    pub name: String,
    pub cmd: QueryCmd,
    /// Original SQL text (with $1/$2 placeholders intact).
    pub sql: String,
    pub params: Vec<Parameter>,
    /// Result columns — empty for :exec and :execrows.
    pub result_columns: Vec<ResultColumn>,
    /// The schema table this query's rows come from, when the SELECT list is an
    /// unambiguous `table.*` or bare `*` over a single non-nullable schema table.
    ///
    /// `None` for explicit column lists, mixed-source projections, CTEs as the
    /// final source, or when the table is on the nullable side of an outer join.
    ///
    /// Backends use this for model reuse: when `Some`, they can return the
    /// table's existing record type instead of emitting a per-query row struct.
    /// Identity — not structural shape — is the criterion.
    pub source_table: Option<String>,
}

impl Query {
    /// Construct a query with no source-table identity.
    ///
    /// Use this for all non-trivial projections (explicit column lists, JOINs
    /// as the final projection source, CTEs, set operations, DML). Set
    /// `source_table` afterwards with [`Query::with_source_table`] when the
    /// projection is an unambiguous `table.*` or bare `*` over a single
    /// non-nullable schema table.
    pub fn new(name: impl Into<String>, cmd: QueryCmd, sql: impl Into<String>, params: Vec<Parameter>, result_columns: Vec<ResultColumn>) -> Self {
        Self { name: name.into(), cmd, sql: sql.into(), params, result_columns, source_table: None }
    }

    /// Set the source table, consuming `self` (builder-style).
    pub fn with_source_table(mut self, table: Option<String>) -> Self {
        self.source_table = table;
        self
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum QueryCmd {
    /// Returns a single optional row.
    One,
    /// Returns all rows.
    Many,
    /// Returns nothing.
    Exec,
    /// Returns the number of affected rows.
    ExecRows,
}

#[derive(Debug, Clone)]
pub struct Parameter {
    /// 1-based index matching `$1`, `$2`, … in the SQL text.
    pub index: usize,
    pub name: String,
    pub sql_type: SqlType,
    pub nullable: bool,
    /// True when this parameter represents a variable-length list of values,
    /// annotated with `-- @name type[] [not null]`.  The generated function
    /// accepts a collection type; the SQL is rewritten per the configured strategy.
    pub is_list: bool,
}

impl Parameter {
    /// Construct a scalar (non-list) parameter.
    pub fn scalar(index: usize, name: impl Into<String>, sql_type: SqlType, nullable: bool) -> Self {
        Self { index, name: name.into(), sql_type, nullable, is_list: false }
    }

    /// Construct a list parameter (`-- @name type[] not null`).
    pub fn list(index: usize, name: impl Into<String>, sql_type: SqlType, nullable: bool) -> Self {
        Self { index, name: name.into(), sql_type, nullable, is_list: true }
    }
}

#[derive(Debug, Clone)]
pub struct ResultColumn {
    pub name: String,
    pub sql_type: SqlType,
    pub nullable: bool,
}
