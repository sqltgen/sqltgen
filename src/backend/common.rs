use std::collections::HashMap;
use std::fmt::Write;

use crate::ir::{Parameter, Query, Schema, SqlType};

/// Convert snake_case to PascalCase: `get_user` → `GetUser`.
pub fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|w| {
            let mut c = w.chars();
            match c.next() {
                None => String::new(),
                Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
            }
        })
        .collect()
}

/// Convert snake_case/PascalCase to camelCase: `get_user` → `getUser`.
pub fn to_camel_case(s: &str) -> String {
    let pascal = to_pascal_case(s);
    let mut c = pascal.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_lowercase().collect::<String>() + c.as_str(),
    }
}

/// Convert PascalCase/camelCase to snake_case: `GetUserById` → `get_user_by_id`.
pub fn to_snake_case(s: &str) -> String {
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

/// Check if a query's result columns exactly match a table's columns by name and count.
pub fn infer_table<'a>(query: &Query, schema: &'a Schema) -> Option<&'a str> {
    for table in &schema.tables {
        if table.columns.len() == query.result_columns.len() && table.columns.iter().zip(&query.result_columns).all(|(a, b)| a.name == b.name) {
            return Some(&table.name);
        }
    }
    None
}

/// Return the ordered list of parameter indices referenced by `$N`/`?N` placeholders.
///
/// Each entry corresponds to one anonymous `?` produced by [`rewrite_to_anon_params`]:
/// `indices[i]` is the 1-based `Parameter.index` that must be bound at position `i + 1`.
/// When a parameter is reused the same index appears multiple times, ensuring every `?`
/// slot gets a value even though the logical parameter is unique.
pub fn parse_placeholder_indices(sql: &str) -> Vec<usize> {
    let mut plan = Vec::new();
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        if (ch == '$' || ch == '?') && chars.peek().is_some_and(|c| c.is_ascii_digit()) {
            let mut num = String::new();
            while chars.peek().is_some_and(|c| c.is_ascii_digit()) {
                num.push(chars.next().unwrap());
            }
            if let Ok(n) = num.parse::<usize>() {
                plan.push(n);
            }
        }
    }
    plan
}

/// Replace `$N` or `?N` numbered placeholders with anonymous `?` markers.
///
/// Used by every backend that binds parameters positionally: JDBC (Java, Kotlin),
/// better-sqlite3 (TypeScript/JavaScript), mysql2 (TypeScript/JavaScript), and
/// Python sqlite3. Not specific to any one driver — any driver that accepts `?`
/// as a positional placeholder can use this rewrite.
pub fn rewrite_to_anon_params(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        if (ch == '$' || ch == '?') && chars.peek().is_some_and(|c| c.is_ascii_digit()) {
            out.push('?');
            while chars.peek().is_some_and(|c| c.is_ascii_digit()) {
                chars.next();
            }
        } else {
            out.push(ch);
        }
    }
    out
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

/// Find `IN ($N)` or `IN (?N)` for a list param and replace it with `replacement`.
///
/// Returns the rewritten SQL, or `None` if no matching pattern is found (the caller
/// should then emit a warning and treat the parameter as scalar).
pub fn replace_list_in_clause(sql: &str, param_index: usize, replacement: &str) -> Option<String> {
    for pattern in &[format!("IN (${param_index})"), format!("IN (?{param_index})")] {
        let lower_sql = sql.to_ascii_lowercase();
        let lower_pat = pattern.to_ascii_lowercase();
        if let Some(pos) = lower_sql.find(&lower_pat) {
            let mut result = sql[..pos].to_string();
            result.push_str(replacement);
            result.push_str(&sql[pos + pattern.len()..]);
            return Some(result);
        }
    }
    None
}

/// Split SQL at an `IN ($N)` / `IN (?N)` clause for dynamic list-param expansion.
///
/// Returns `(sql_before, sql_after)` so generated code can assemble them around a
/// runtime-built `IN (?,?,…)` fragment. Returns `None` if the pattern is not found.
pub fn split_at_in_clause(sql: &str, param_index: usize) -> Option<(String, String)> {
    for pattern in &[format!("IN (${param_index})"), format!("IN (?{param_index})")] {
        let lower_sql = sql.to_ascii_lowercase();
        let lower_pat = pattern.to_ascii_lowercase();
        if let Some(pos) = lower_sql.find(&lower_pat) {
            return Some((sql[..pos].to_string(), sql[pos + pattern.len()..].to_string()));
        }
    }
    None
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

/// Resolve the bind plan to parameter names in SQL occurrence order.
///
/// For positional-sequential backends (SQLite sqlx, MySQL sqlx, Python sqlite3,
/// psycopg3, mysql-connector-python) where each `?`/`%s` is filled by the
/// argument at the corresponding position. A parameter used N times in the SQL
/// produces N entries.
///
/// Do NOT use this for PostgreSQL `$N` (reference-by-number): sqlx Postgres
/// resolves all `$1` references to the single bound value at position 1, so
/// one `.bind()` per unique parameter is correct there.
pub fn positional_bind_names<'a>(query: &'a Query) -> Vec<&'a str> {
    let by_idx: HashMap<usize, &'a str> = query.params.iter().map(|p| (p.index, p.name.as_str())).collect();
    parse_placeholder_indices(&query.sql).iter().filter_map(|&i| by_idx.get(&i).copied()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ir::{Parameter, Query, QueryCmd, SqlType};

    fn make_query(sql: &str, params: Vec<Parameter>) -> Query {
        Query { name: "Test".to_string(), cmd: QueryCmd::Exec, sql: sql.to_string(), params, result_columns: vec![] }
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
        // $1 appears 3 times → 3 entries, all pointing to param "x"
        let p = Parameter::scalar(1, "x", SqlType::BigInt, false);
        let q = make_query("WHERE a = $1 OR b = $1 OR c = $1", vec![p]);
        let seq = jdbc_bind_sequence(&q);
        assert_eq!(seq.len(), 3);
        // JDBC indices are 1-based and sequential; name is "x" for each slot
        assert!(seq.iter().enumerate().all(|(i, (jdbc_idx, p))| *jdbc_idx == i + 1 && p.name == "x"));
    }

    // ─── positional_bind_names ───────────────────────────────────────────────

    #[test]
    fn test_positional_bind_names_unique_params() {
        let p1 = Parameter::scalar(1, "alpha", SqlType::Integer, false);
        let p2 = Parameter::scalar(2, "beta", SqlType::Text, false);
        let q = make_query("WHERE a = $1 AND b = $2", vec![p1, p2]);
        assert_eq!(positional_bind_names(&q), vec!["alpha", "beta"]);
    }

    #[test]
    fn test_positional_bind_names_repeated_param_expands() {
        // $1 appears twice — two "genre" entries
        let p = Parameter::scalar(1, "genre", SqlType::Text, false);
        let q = make_query("WHERE $1 = 'all' OR genre = $1", vec![p]);
        assert_eq!(positional_bind_names(&q), vec!["genre", "genre"]);
    }

    // ─── parse_placeholder_indices ──────────────────────────────────────────

    #[test]
    fn test_parse_placeholder_indices_unique_params() {
        // Each placeholder appears once — indices are [1, 2]
        assert_eq!(parse_placeholder_indices("SELECT * FROM t WHERE a = $1 AND b = $2"), vec![1, 2]);
    }

    #[test]
    fn test_parse_placeholder_indices_reused_param() {
        // $1 appears 4 times, $2 once — order matches textual occurrence
        let sql = "WHERE account_id = $1 OR $1 = -1 AND x = $1 OR $1 = 0 AND y = $2";
        assert_eq!(parse_placeholder_indices(sql), vec![1, 1, 1, 1, 2]);
    }

    #[test]
    fn test_parse_placeholder_indices_question_mark_style() {
        // SQLite-style ?N placeholders
        assert_eq!(parse_placeholder_indices("WHERE a = ?1 AND b = ?2 AND c = ?1"), vec![1, 2, 1]);
    }

    #[test]
    fn test_parse_placeholder_indices_no_params() {
        assert_eq!(parse_placeholder_indices("SELECT 1"), Vec::<usize>::new());
    }

    #[test]
    fn test_parse_placeholder_indices_multidigit_index() {
        assert_eq!(parse_placeholder_indices("WHERE a = $10 AND b = $2"), vec![10, 2]);
    }

    // ─── replace_list_in_clause ──────────────────────────────────────────────

    #[test]
    fn test_replace_list_in_clause_dollar_style() {
        let sql = "SELECT * FROM t WHERE id IN ($1)";
        let result = replace_list_in_clause(sql, 1, "= ANY($1)").unwrap();
        assert_eq!(result, "SELECT * FROM t WHERE id = ANY($1)");
    }

    #[test]
    fn test_replace_list_in_clause_question_style() {
        let sql = "SELECT * FROM t WHERE id IN (?1)";
        let result = replace_list_in_clause(sql, 1, "IN (SELECT value FROM json_each(?))").unwrap();
        assert!(result.contains("json_each"));
        assert!(!result.contains("IN (?1)"));
    }

    #[test]
    fn test_replace_list_in_clause_returns_none_when_not_found() {
        let sql = "SELECT * FROM t WHERE id = $1";
        assert!(replace_list_in_clause(sql, 1, "= ANY($1)").is_none());
    }

    // ─── split_at_in_clause ──────────────────────────────────────────────────

    #[test]
    fn test_split_at_in_clause_splits_correctly() {
        let sql = "SELECT * FROM t WHERE id IN ($1) AND active = $2";
        let (before, after) = split_at_in_clause(sql, 1).unwrap();
        assert_eq!(before, "SELECT * FROM t WHERE id ");
        assert_eq!(after, " AND active = $2");
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
