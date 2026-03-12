use std::collections::HashMap;
use std::fmt::Write;

use crate::backend::naming::{to_pascal_case, to_snake_case};
use crate::backend::sql_rewrite::parse_placeholder_indices;
use crate::ir::{Parameter, Query, Schema, SqlType};

/// Derive the row-type name for a query result, or `None` if the query has no
/// result columns (e.g. `:exec` / `:execrows`).
///
/// - If the result columns exactly match a known table, returns the PascalCase
///   table name.
/// - If the query has result columns but doesn't map to a known table, returns
///   `"{Query}Row"` (PascalCase query name + `"Row"`).
/// - Returns `None` when there are no result columns at all.
///
/// Backends call this and supply a language-specific fallback for the `None`
/// case (e.g. `"Object[]"` for Java, `"Any"` for Kotlin/Python).
pub fn infer_row_type_name(query: &Query, schema: &Schema) -> Option<String> {
    if let Some(table_name) = infer_table(query, schema) {
        return Some(to_pascal_case(table_name));
    }
    if !query.result_columns.is_empty() {
        return Some(format!("{}Row", to_pascal_case(&query.name)));
    }
    None
}

/// True when a query has inline result columns that are *not* matched to a schema table.
///
/// Use this to decide whether to emit a per-query row type declaration (record, dataclass,
/// interface, …). When the result columns match a table, the backend reuses that table's
/// existing type instead; when there are no result columns at all (exec/execrows), no row
/// type is needed.
pub fn has_inline_rows(query: &Query, schema: &Schema) -> bool {
    infer_table(query, schema).is_none() && !query.result_columns.is_empty()
}

/// True when a JDBC primitive getter would return `0`/`false` instead of `null` for
/// a SQL `NULL` value — meaning a nullable column of this type needs `getObject()`
/// with an explicit boxed class argument rather than the typed primitive getter.
///
/// The six JDBC primitive types (`getBoolean`, `getShort`, `getInt`, `getLong`,
/// `getFloat`, `getDouble`) all have this behaviour. All other types (`getString`,
/// `getBigDecimal`, `getBytes`, temporal types, …) already return `null` naturally
/// and do not need special treatment.
pub fn needs_null_safe_getter(sql_type: &SqlType) -> bool {
    matches!(sql_type, SqlType::Boolean | SqlType::SmallInt | SqlType::Integer | SqlType::BigInt | SqlType::Real | SqlType::Double)
}

/// Return the schema table whose model can be reused for this query's result rows.
///
/// **Tier 1 — source identity:** when `query.source_table` is set, the frontend has
/// established that all rows come from that exact table (`SELECT t.*`).  If the table
/// exists in the schema and the result columns fully match (name, type, nullability,
/// count), return it immediately.
///
/// **Tier 2 — structural fallback:** for test-constructed queries where `source_table`
/// is `None`, search for a table whose columns match the result columns exactly on
/// name, type, *and* nullability, and only if that match is unique (exactly one table
/// matches).  This avoids false positives when two tables happen to share the same
/// column structure.
pub fn infer_table<'a>(query: &Query, schema: &'a Schema) -> Option<&'a str> {
    let cols_match = |table: &crate::ir::Table| -> bool {
        table.columns.len() == query.result_columns.len()
            && table.columns.iter().zip(&query.result_columns).all(|(tc, rc)| tc.name == rc.name && tc.sql_type == rc.sql_type && tc.nullable == rc.nullable)
    };

    if let Some(name) = &query.source_table {
        if let Some(table) = schema.tables.iter().find(|t| &t.name == name) {
            if cols_match(table) {
                return Some(&table.name);
            }
        }
        return None;
    }

    let mut matched: Option<&str> = None;
    for table in &schema.tables {
        if cols_match(table) {
            if matched.is_some() {
                return None; // ambiguous
            }
            matched = Some(&table.name);
        }
    }
    matched
}

/// Emit a package declaration if non-empty. Pass `";"` for Java, `""` for Kotlin.
pub fn emit_package(src: &mut String, package: &str, terminator: &str) {
    if !package.is_empty() {
        writeln!(src, "package {package}{terminator}").unwrap();
        writeln!(src).unwrap();
    }
}

/// Generate a SQL constant name: `GetUserById` → `SQL_GET_USER_BY_ID`.
pub fn sql_const_name(query_name: &str) -> String {
    format!("SQL_{}", to_snake_case(query_name).to_uppercase())
}

/// Return the JDBC `PreparedStatement` setter method name for a SQL type.
///
/// Used by the Java and Kotlin backends to emit typed `ps.setXxx()` calls.
/// All temporal, UUID and custom types fall back to `setObject`.
pub fn jdbc_setter(sql_type: &SqlType) -> &'static str {
    match sql_type {
        SqlType::Boolean => "setBoolean",
        SqlType::SmallInt => "setShort",
        SqlType::Integer => "setInt",
        SqlType::BigInt => "setLong",
        SqlType::Real => "setFloat",
        SqlType::Double => "setDouble",
        SqlType::Decimal => "setBigDecimal",
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => "setString",
        SqlType::Bytes => "setBytes",
        _ => "setObject",
    }
}

/// Resolve the placeholder sequence to `(jdbc_slot, &Parameter)` pairs for JDBC binding.
///
/// Scans `$N`/`?N` occurrences in the stored SQL text and returns one entry per
/// `?` slot (1-based JDBC index, reference to the parameter to bind there).
/// A parameter that appears N times in the SQL produces N entries, so every `?`
/// slot is covered — unlike a naïve "one bind per unique param" approach.
pub fn jdbc_bind_sequence<'a>(query: &'a Query) -> Vec<(usize, &'a Parameter)> {
    let by_idx: HashMap<usize, &'a Parameter> = query.params.iter().map(|p| (p.index, p)).collect();
    parse_placeholder_indices(&query.sql).iter().enumerate().filter_map(|(slot, &param_idx)| by_idx.get(&param_idx).map(|p| (slot + 1, *p))).collect()
}

/// Return the PostgreSQL type name for use with `conn.createArrayOf(typeName, …)`.
///
/// Required for the PostgreSQL native list-param strategy in the Java and Kotlin JDBC backends.
pub fn pg_array_type_name(sql_type: &SqlType) -> &'static str {
    match sql_type {
        SqlType::SmallInt => "smallint",
        SqlType::Integer => "integer",
        SqlType::BigInt => "bigint",
        SqlType::Real => "real",
        SqlType::Double => "float8",
        SqlType::Decimal => "numeric",
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => "text",
        SqlType::Boolean => "boolean",
        SqlType::Date => "date",
        SqlType::Time => "time",
        SqlType::Timestamp => "timestamp",
        SqlType::TimestampTz => "timestamptz",
        SqlType::Uuid => "uuid",
        SqlType::Bytes => "bytea",
        _ => "text",
    }
}

/// Return the MySQL `JSON_TABLE` column type keyword for a given SQL type.
///
/// Used when building `JSON_TABLE(?,'$[*]' COLUMNS(value T PATH '$'))` to extract
/// typed elements from a JSON array. Numeric types map to their exact SQL counterparts;
/// all other types (text, temporal, UUID, bytes, JSON, arrays, custom) fall back to
/// `CHAR(255)`, which covers every non-numeric type that reasonably appears in an
/// `IN` clause by extracting the raw string representation.
pub fn mysql_json_table_col_type(sql_type: &SqlType) -> &'static str {
    match sql_type {
        SqlType::Boolean => "BOOLEAN",
        SqlType::SmallInt => "SMALLINT",
        SqlType::Integer => "INT",
        SqlType::BigInt => "BIGINT",
        SqlType::Real => "FLOAT",
        SqlType::Double => "DOUBLE",
        SqlType::Decimal => "DECIMAL(38,10)",
        _ => "CHAR(255)",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ir::{Column, Parameter, Query, QueryCmd, ResultColumn, Schema, SqlType, Table};

    fn make_query(sql: &str, params: Vec<Parameter>) -> Query {
        Query { name: "Test".to_string(), cmd: QueryCmd::Exec, sql: sql.to_string(), params, result_columns: vec![], source_table: None }
    }

    fn make_schema_with_table(table_name: &str, col_names: &[&str]) -> Schema {
        Schema {
            tables: vec![Table {
                name: table_name.to_string(),
                columns: col_names.iter().map(|n| Column { name: n.to_string(), sql_type: SqlType::Text, nullable: false, is_primary_key: false }).collect(),
            }],
        }
    }

    fn make_result_cols(names: &[&str]) -> Vec<ResultColumn> {
        names.iter().map(|n| ResultColumn { name: n.to_string(), sql_type: SqlType::Text, nullable: false }).collect()
    }

    fn make_typed_table(name: &str, cols: &[(&str, SqlType, bool)]) -> Table {
        Table {
            name: name.to_string(),
            columns: cols.iter().map(|(n, t, null)| Column { name: n.to_string(), sql_type: t.clone(), nullable: *null, is_primary_key: false }).collect(),
        }
    }

    fn rc(name: &str, sql_type: SqlType, nullable: bool) -> ResultColumn {
        ResultColumn { name: name.to_string(), sql_type, nullable }
    }

    // ─── needs_null_safe_getter ───────────────────────────────────────────────

    #[test]
    fn test_needs_null_safe_getter_primitives_require_it() {
        assert!(needs_null_safe_getter(&SqlType::Boolean));
        assert!(needs_null_safe_getter(&SqlType::SmallInt));
        assert!(needs_null_safe_getter(&SqlType::Integer));
        assert!(needs_null_safe_getter(&SqlType::BigInt));
        assert!(needs_null_safe_getter(&SqlType::Real));
        assert!(needs_null_safe_getter(&SqlType::Double));
    }

    #[test]
    fn test_needs_null_safe_getter_reference_types_do_not() {
        assert!(!needs_null_safe_getter(&SqlType::Text));
        assert!(!needs_null_safe_getter(&SqlType::Decimal));
        assert!(!needs_null_safe_getter(&SqlType::Date));
        assert!(!needs_null_safe_getter(&SqlType::Timestamp));
        assert!(!needs_null_safe_getter(&SqlType::Uuid));
        assert!(!needs_null_safe_getter(&SqlType::Json));
        assert!(!needs_null_safe_getter(&SqlType::Bytes));
    }

    // ─── infer_row_type_name ──────────────────────────────────────────────────

    #[test]
    fn test_infer_row_type_name_matches_table() {
        let schema = make_schema_with_table("user_account", &["id", "name"]);
        let query = Query {
            name: "GetUser".to_string(),
            cmd: QueryCmd::One,
            sql: "SELECT id, name FROM user_account WHERE id = $1".to_string(),
            params: vec![],
            result_columns: make_result_cols(&["id", "name"]),
            source_table: None,
        };
        assert_eq!(infer_row_type_name(&query, &schema), Some("UserAccount".to_string()));
    }

    #[test]
    fn test_infer_row_type_name_inline_row_when_no_table_match() {
        let schema = make_schema_with_table("user_account", &["id", "name", "email"]);
        let query = Query {
            name: "GetUserSummary".to_string(),
            cmd: QueryCmd::One,
            sql: "SELECT id, name FROM user_account WHERE id = $1".to_string(),
            params: vec![],
            result_columns: make_result_cols(&["id", "name"]),
            source_table: None,
        };
        assert_eq!(infer_row_type_name(&query, &schema), Some("GetUserSummaryRow".to_string()));
    }

    #[test]
    fn test_infer_row_type_name_returns_none_when_no_result_cols() {
        let schema = Schema { tables: vec![] };
        let query = Query {
            name: "DeleteUser".to_string(),
            cmd: QueryCmd::Exec,
            sql: "DELETE FROM users WHERE id = $1".to_string(),
            params: vec![],
            result_columns: vec![],
            source_table: None,
        };
        assert!(infer_row_type_name(&query, &schema).is_none());
    }

    // ─── infer_table (bug 022) ────────────────────────────────────────────────

    /// Structural match must reject when a result column's type differs from the table column.
    ///
    /// A CTE that exputes id as TEXT has the same column NAME as a table where id is BIGINT.
    /// The old name+count check would silently return the table — wrong generated type.
    #[test]
    fn test_infer_table_rejects_type_mismatch() {
        let schema = Schema { tables: vec![make_typed_table("users", &[("id", SqlType::BigInt, false), ("name", SqlType::Text, false)])] };
        let query = Query::new("GetActiveUsers", QueryCmd::Many, "...", vec![], vec![rc("id", SqlType::Text, false), rc("name", SqlType::Text, false)]);
        assert_eq!(infer_table(&query, &schema), None);
    }

    /// Structural match must reject when nullability differs from the table column.
    ///
    /// A JOIN query may produce a nullable id column even though the table's id is NOT NULL.
    /// The old check would silently return the table.
    #[test]
    fn test_infer_table_rejects_nullability_mismatch() {
        let schema = Schema { tables: vec![make_typed_table("users", &[("id", SqlType::BigInt, false), ("name", SqlType::Text, false)])] };
        let query = Query::new("ListUsers", QueryCmd::Many, "...", vec![], vec![rc("id", SqlType::BigInt, true), rc("name", SqlType::Text, false)]);
        assert_eq!(infer_table(&query, &schema), None);
    }

    /// When two tables have the same column structure, structural matching is ambiguous.
    ///
    /// The old first-match-wins behaviour could return the wrong table depending on the
    /// order tables appear in the schema. The fix requires a unique match.
    #[test]
    fn test_infer_table_rejects_ambiguous_structural_match() {
        let users = make_typed_table("users", &[("id", SqlType::BigInt, false), ("name", SqlType::Text, false)]);
        let admins = make_typed_table("admins", &[("id", SqlType::BigInt, false), ("name", SqlType::Text, false)]);
        let schema = Schema { tables: vec![users, admins] };
        let query = Query::new("ListAll", QueryCmd::Many, "...", vec![], vec![rc("id", SqlType::BigInt, false), rc("name", SqlType::Text, false)]);
        assert_eq!(infer_table(&query, &schema), None);
    }

    /// source_table identity takes priority: when set, the named table is returned
    /// without needing to be the unique structural match.
    ///
    /// This is the "two identical tables" case where structural match alone would be
    /// ambiguous, but the frontend recorded which table the rows actually came from.
    #[test]
    fn test_infer_table_source_identity_resolves_ambiguity() {
        let users = make_typed_table("users", &[("id", SqlType::BigInt, false), ("name", SqlType::Text, false)]);
        let admins = make_typed_table("admins", &[("id", SqlType::BigInt, false), ("name", SqlType::Text, false)]);
        let schema = Schema { tables: vec![users, admins] };
        let query = Query::new("ListUsers", QueryCmd::Many, "...", vec![], vec![rc("id", SqlType::BigInt, false), rc("name", SqlType::Text, false)])
            .with_source_table(Some("users".to_string()));
        assert_eq!(infer_table(&query, &schema), Some("users"));
    }

    // ─── jdbc_setter ─────────────────────────────────────────────────────────

    #[test]
    fn test_jdbc_setter_primitives() {
        assert_eq!(jdbc_setter(&SqlType::Boolean), "setBoolean");
        assert_eq!(jdbc_setter(&SqlType::SmallInt), "setShort");
        assert_eq!(jdbc_setter(&SqlType::Integer), "setInt");
        assert_eq!(jdbc_setter(&SqlType::BigInt), "setLong");
        assert_eq!(jdbc_setter(&SqlType::Real), "setFloat");
        assert_eq!(jdbc_setter(&SqlType::Double), "setDouble");
    }

    #[test]
    fn test_jdbc_setter_string_types() {
        assert_eq!(jdbc_setter(&SqlType::Text), "setString");
        assert_eq!(jdbc_setter(&SqlType::Char(Some(10))), "setString");
        assert_eq!(jdbc_setter(&SqlType::VarChar(Some(255))), "setString");
    }

    #[test]
    fn test_jdbc_setter_fallback_types_use_set_object() {
        assert_eq!(jdbc_setter(&SqlType::Date), "setObject");
        assert_eq!(jdbc_setter(&SqlType::Timestamp), "setObject");
        assert_eq!(jdbc_setter(&SqlType::Uuid), "setObject");
        assert_eq!(jdbc_setter(&SqlType::Json), "setObject");
    }

    // ─── jdbc_bind_sequence ─────────────────────────────────────────────────

    #[test]
    fn test_jdbc_bind_sequence_unique_params() {
        let p1 = Parameter::scalar(1, "a", SqlType::Integer, false);
        let p2 = Parameter::scalar(2, "b", SqlType::Text, false);
        let q = make_query("WHERE a = $1 AND b = $2", vec![p1, p2]);
        let seq = jdbc_bind_sequence(&q);
        assert_eq!(seq.len(), 2);
        assert_eq!(seq[0].0, 1);
        assert_eq!(seq[0].1.name, "a");
        assert_eq!(seq[1].0, 2);
        assert_eq!(seq[1].1.name, "b");
    }

    #[test]
    fn test_jdbc_bind_sequence_repeated_param_expands() {
        let p = Parameter::scalar(1, "x", SqlType::BigInt, false);
        let q = make_query("WHERE a = $1 OR b = $1 OR c = $1", vec![p]);
        let seq = jdbc_bind_sequence(&q);
        assert_eq!(seq.len(), 3);
        assert!(seq.iter().enumerate().all(|(i, (jdbc_idx, p))| *jdbc_idx == i + 1 && p.name == "x"));
    }

    // ─── pg_array_type_name ──────────────────────────────────────────────────

    #[test]
    fn test_pg_array_type_name_primitives() {
        assert_eq!(pg_array_type_name(&SqlType::BigInt), "bigint");
        assert_eq!(pg_array_type_name(&SqlType::Integer), "integer");
        assert_eq!(pg_array_type_name(&SqlType::Text), "text");
        assert_eq!(pg_array_type_name(&SqlType::Boolean), "boolean");
        assert_eq!(pg_array_type_name(&SqlType::Uuid), "uuid");
    }
}
