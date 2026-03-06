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
}

#[derive(Debug, Clone)]
pub struct ResultColumn {
    pub name: String,
    pub sql_type: SqlType,
    pub nullable: bool,
}
