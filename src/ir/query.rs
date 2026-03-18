use super::types::SqlType;

/// How a native list parameter is bound to the query at the driver level.
///
/// Set by the dialect frontend alongside [`Parameter::native_list_sql`]. Tells
/// backends which binding code to emit without needing to inspect the source dialect.
#[derive(Debug, Clone, PartialEq)]
pub enum NativeListBind {
    /// Pass the list directly as a driver-native array.
    ///
    /// PostgreSQL: JDBC `createArrayOf`, sqlx `Vec<T>`, psycopg3 list, pg JS array.
    Array,
    /// Serialize the list to a JSON string before binding.
    ///
    /// SQLite (`json_each`) and MySQL (`JSON_TABLE`): the driver receives a single
    /// JSON array string which the database function unpacks inside the query.
    Json,
}

#[derive(Debug, Clone)]
pub struct Query {
    /// Camel-case name from the `-- name: Foo :cmd` annotation.
    pub name: String,
    /// Logical group this query belongs to.
    ///
    /// Set by `main.rs` after parsing each file. An empty string means the
    /// default group; backends render it as `"queries"` (e.g. `Queries.java`,
    /// `queries.ts`). Non-empty values produce per-group output files
    /// (e.g. group `"users"` → `UsersQueries.java`, `users.ts`).
    pub group: String,
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
        Self { name: name.into(), group: String::new(), cmd, sql: sql.into(), params, result_columns, source_table: None }
    }

    /// Construct an `:exec` query (no result columns).
    pub fn exec(name: impl Into<String>, sql: impl Into<String>, params: Vec<Parameter>) -> Self {
        Self::new(name, QueryCmd::Exec, sql, params, vec![])
    }

    /// Construct an `:execrows` query (no result columns).
    pub fn exec_rows(name: impl Into<String>, sql: impl Into<String>, params: Vec<Parameter>) -> Self {
        Self::new(name, QueryCmd::ExecRows, sql, params, vec![])
    }

    /// Construct a `:one` query (returns a single optional row).
    pub fn one(name: impl Into<String>, sql: impl Into<String>, params: Vec<Parameter>, result_columns: Vec<ResultColumn>) -> Self {
        Self::new(name, QueryCmd::One, sql, params, result_columns)
    }

    /// Construct a `:many` query (returns all rows).
    pub fn many(name: impl Into<String>, sql: impl Into<String>, params: Vec<Parameter>, result_columns: Vec<ResultColumn>) -> Self {
        Self::new(name, QueryCmd::Many, sql, params, result_columns)
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
    /// Pre-computed SQL with the dialect-specific native list expansion applied.
    ///
    /// Set by each dialect frontend for every list parameter. Uses `$N`
    /// placeholder style so backends apply their standard rewriting (e.g.
    /// `$N → ?` for JDBC/SQLite/MySQL). `None` for non-list parameters and
    /// for dialects that do not support native list expansion.
    pub native_list_sql: Option<String>,
    /// How this list parameter must be bound when `native_list_sql` is used.
    ///
    /// Set alongside `native_list_sql` by the dialect frontend. `None` when
    /// no native SQL is available.
    pub native_list_bind: Option<NativeListBind>,
}

impl Parameter {
    /// Construct a scalar (non-list) parameter.
    pub fn scalar(index: usize, name: impl Into<String>, sql_type: SqlType, nullable: bool) -> Self {
        Self { index, name: name.into(), sql_type, nullable, is_list: false, native_list_sql: None, native_list_bind: None }
    }

    /// Construct a list parameter (`-- @name type[] not null`).
    pub fn list(index: usize, name: impl Into<String>, sql_type: SqlType, nullable: bool) -> Self {
        Self { index, name: name.into(), sql_type, nullable, is_list: true, native_list_sql: None, native_list_bind: None }
    }

    /// Set `native_list_sql` and `native_list_bind` together, consuming `self` (builder-style).
    ///
    /// Used in tests to simulate what dialect frontends compute.
    pub fn with_native_list(mut self, sql: impl Into<String>, bind: NativeListBind) -> Self {
        self.native_list_sql = Some(sql.into());
        self.native_list_bind = Some(bind);
        self
    }
}

#[derive(Debug, Clone)]
pub struct ResultColumn {
    pub name: String,
    pub sql_type: SqlType,
    pub nullable: bool,
}

impl ResultColumn {
    /// Construct a non-nullable result column.
    pub fn not_nullable(name: impl Into<String>, sql_type: SqlType) -> Self {
        Self { name: name.into(), sql_type, nullable: false }
    }

    /// Construct a nullable result column.
    pub fn nullable(name: impl Into<String>, sql_type: SqlType) -> Self {
        Self { name: name.into(), sql_type, nullable: true }
    }
}
