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

impl QueryCmd {
    /// True for `:one` and `:many` — commands that produce typed result rows.
    ///
    /// Use this to decide whether to emit a row type declaration and a result
    /// return type, rather than matching both variants in every backend.
    pub fn has_rows(&self) -> bool {
        matches!(self, QueryCmd::One | QueryCmd::Many)
    }

    /// True for `:execrows` — the command that returns an affected-row count.
    ///
    /// Use this when generating a numeric return type (e.g. `int`, `i64`,
    /// `number`) instead of a row type or `void`.
    pub fn returns_count(&self) -> bool {
        matches!(self, QueryCmd::ExecRows)
    }
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
    #[allow(dead_code)]
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
