use super::query::ResultColumn;
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

    /// Find a table by optional schema qualifier and table name.
    ///
    /// Matching rules:
    /// - Both qualified: exact match on schema and name.
    /// - Both unqualified (query=None, table.schema=None): match on name.
    /// - Query unqualified, table qualified: match if table's schema is the default.
    /// - Query qualified, table unqualified: match if query's schema is the default.
    pub fn find_table(&self, query_schema: Option<&str>, table_name: &str, default_schema: Option<&str>) -> Option<&Table> {
        self.tables.iter().find(|t| t.name == table_name && schema_matches(query_schema, t.schema.as_deref(), default_schema))
    }
}

/// Returns true when two schema qualifiers match, considering the default schema.
///
/// Used by both `Schema::find_table` (query resolution) and DDL operations
/// (`ALTER TABLE`, `DROP TABLE`) to consistently resolve qualified/unqualified
/// table references.
pub fn schema_matches(ref_schema: Option<&str>, table_schema: Option<&str>, default_schema: Option<&str>) -> bool {
    match (ref_schema, table_schema) {
        (Some(q), Some(s)) => q == s,
        (None, None) => true,
        (None, Some(s)) => default_schema == Some(s),
        (Some(q), None) => default_schema == Some(q),
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
    /// The schema this table belongs to (e.g. `"public"` for `public.users`).
    /// `None` for unqualified table names.
    pub schema: Option<String>,
    pub columns: Vec<Column>,
    pub kind: TableKind,
}

impl Table {
    /// Construct a base table (from `CREATE TABLE`).
    pub fn new(name: impl Into<String>, columns: Vec<Column>) -> Self {
        Self { name: name.into(), schema: None, columns, kind: TableKind::Table }
    }

    /// Construct a base table belonging to a named schema.
    pub fn with_schema(schema: impl Into<String>, name: impl Into<String>, columns: Vec<Column>) -> Self {
        Self { name: name.into(), schema: Some(schema.into()), columns, kind: TableKind::Table }
    }

    /// Construct a view (from `CREATE VIEW`).
    pub fn view(name: impl Into<String>, columns: Vec<Column>) -> Self {
        Self { name: name.into(), schema: None, columns, kind: TableKind::View }
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

impl From<ResultColumn> for Column {
    fn from(rc: ResultColumn) -> Self {
        Self { name: rc.name, sql_type: rc.sql_type, nullable: rc.nullable, is_primary_key: false }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── Table constructors ─────���───────────────────────────────────────

    #[test]
    fn test_table_new_has_no_schema() {
        let t = Table::new("users", vec![]);
        assert!(t.schema.is_none());
        assert_eq!(t.name, "users");
    }

    #[test]
    fn test_table_with_schema() {
        let t = Table::with_schema("public", "users", vec![]);
        assert_eq!(t.schema.as_deref(), Some("public"));
        assert_eq!(t.name, "users");
    }

    // ─── Schema::find_table ─────────────���───────────────────────────────

    #[test]
    fn test_find_table_unqualified_matches_unqualified() {
        let schema = Schema::with_tables(vec![Table::new("users", vec![])]);
        assert!(schema.find_table(None, "users", None).is_some());
    }

    #[test]
    fn test_find_table_qualified_matches_qualified() {
        let schema = Schema::with_tables(vec![Table::with_schema("public", "users", vec![])]);
        assert!(schema.find_table(Some("public"), "users", None).is_some());
    }

    #[test]
    fn test_find_table_qualified_no_match_wrong_schema() {
        let schema = Schema::with_tables(vec![Table::with_schema("public", "users", vec![])]);
        assert!(schema.find_table(Some("internal"), "users", None).is_none());
    }

    #[test]
    fn test_find_table_unqualified_matches_default_schema() {
        let schema = Schema::with_tables(vec![Table::with_schema("public", "users", vec![])]);
        assert!(schema.find_table(None, "users", Some("public")).is_some());
    }

    #[test]
    fn test_find_table_unqualified_no_match_non_default_schema() {
        let schema = Schema::with_tables(vec![Table::with_schema("internal", "users", vec![])]);
        assert!(schema.find_table(None, "users", Some("public")).is_none());
    }

    #[test]
    fn test_find_table_qualified_default_matches_unqualified_table() {
        let schema = Schema::with_tables(vec![Table::new("users", vec![])]);
        assert!(schema.find_table(Some("public"), "users", Some("public")).is_some());
    }

    #[test]
    fn test_find_table_qualified_non_default_no_match_unqualified_table() {
        let schema = Schema::with_tables(vec![Table::new("users", vec![])]);
        assert!(schema.find_table(Some("internal"), "users", Some("public")).is_none());
    }

    #[test]
    fn test_find_table_same_name_different_schemas() {
        let col_a = vec![Column::new("a", SqlType::Text)];
        let col_b = vec![Column::new("b", SqlType::Integer)];
        let schema = Schema::with_tables(vec![Table::with_schema("s1", "t", col_a), Table::with_schema("s2", "t", col_b)]);
        let t1 = schema.find_table(Some("s1"), "t", None).unwrap();
        assert_eq!(t1.columns[0].name, "a");
        let t2 = schema.find_table(Some("s2"), "t", None).unwrap();
        assert_eq!(t2.columns[0].name, "b");
    }
}
