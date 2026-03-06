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
/// Each entry corresponds to one JDBC `?` produced by [`jdbc_sql`]: `bind_plan[i]` is
/// the 1-based `Parameter.index` that must be bound at JDBC position `i + 1`. When a
/// named parameter is reused the same index appears multiple times, ensuring every `?`
/// slot gets a bind call even though the logical parameter is unique.
pub fn jdbc_bind_plan(sql: &str) -> Vec<usize> {
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

/// Replace `$N` or `?N` placeholders with JDBC `?` markers.
pub fn jdbc_sql(sql: &str) -> String {
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

/// Resolve the JDBC bind plan to `(jdbc_index, &Parameter)` pairs.
///
/// Scans `$N`/`?N` occurrences in the stored SQL text and returns one entry per
/// `?` slot (1-based JDBC index, reference to the parameter to bind there).
/// A parameter that appears N times in the SQL produces N entries, so every `?`
/// slot is covered — unlike a naïve "one bind per unique param" approach.
pub fn jdbc_bind_sequence<'a>(query: &'a Query) -> Vec<(usize, &'a Parameter)> {
    let by_idx: HashMap<usize, &'a Parameter> = query.params.iter().map(|p| (p.index, p)).collect();
    jdbc_bind_plan(&query.sql)
        .iter()
        .enumerate()
        .filter_map(|(slot, &param_idx)| by_idx.get(&param_idx).map(|p| (slot + 1, *p)))
        .collect()
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
    jdbc_bind_plan(&query.sql).iter().filter_map(|&i| by_idx.get(&i).copied()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::ir::{Parameter, Query, QueryCmd, SqlType};

    fn make_query(sql: &str, params: Vec<Parameter>) -> Query {
        Query { name: "Test".to_string(), cmd: QueryCmd::Exec, sql: sql.to_string(), params, result_columns: vec![] }
    }

    // ─── jdbc_setter ────────────────────────────────────────────────────────

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
        let p1 = Parameter { index: 1, name: "a".to_string(), sql_type: SqlType::Integer, nullable: false };
        let p2 = Parameter { index: 2, name: "b".to_string(), sql_type: SqlType::Text, nullable: false };
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
        let p = Parameter { index: 1, name: "x".to_string(), sql_type: SqlType::BigInt, nullable: false };
        let q = make_query("WHERE a = $1 OR b = $1 OR c = $1", vec![p]);
        let seq = jdbc_bind_sequence(&q);
        assert_eq!(seq.len(), 3);
        // JDBC indices are 1-based and sequential; name is "x" for each slot
        assert!(seq.iter().enumerate().all(|(i, (jdbc_idx, p))| *jdbc_idx == i + 1 && p.name == "x"));
    }

    // ─── positional_bind_names ───────────────────────────────────────────────

    #[test]
    fn test_positional_bind_names_unique_params() {
        let p1 = Parameter { index: 1, name: "alpha".to_string(), sql_type: SqlType::Integer, nullable: false };
        let p2 = Parameter { index: 2, name: "beta".to_string(), sql_type: SqlType::Text, nullable: false };
        let q = make_query("WHERE a = $1 AND b = $2", vec![p1, p2]);
        assert_eq!(positional_bind_names(&q), vec!["alpha", "beta"]);
    }

    #[test]
    fn test_positional_bind_names_repeated_param_expands() {
        // $1 appears twice — two "genre" entries
        let p = Parameter { index: 1, name: "genre".to_string(), sql_type: SqlType::Text, nullable: false };
        let q = make_query("WHERE $1 = 'all' OR genre = $1", vec![p]);
        assert_eq!(positional_bind_names(&q), vec!["genre", "genre"]);
    }

    // ─── jdbc_bind_plan ─────────────────────────────────────────────────────

    #[test]
    fn test_jdbc_bind_plan_unique_params() {
        // Each placeholder appears once — plan is [1, 2]
        assert_eq!(jdbc_bind_plan("SELECT * FROM t WHERE a = $1 AND b = $2"), vec![1, 2]);
    }

    #[test]
    fn test_jdbc_bind_plan_reused_param() {
        // $1 appears 4 times, $2 once — plan matches textual order
        let sql = "WHERE account_id = $1 OR $1 = -1 AND x = $1 OR $1 = 0 AND y = $2";
        assert_eq!(jdbc_bind_plan(sql), vec![1, 1, 1, 1, 2]);
    }

    #[test]
    fn test_jdbc_bind_plan_question_mark_style() {
        // SQLite-style ?N placeholders
        assert_eq!(jdbc_bind_plan("WHERE a = ?1 AND b = ?2 AND c = ?1"), vec![1, 2, 1]);
    }

    #[test]
    fn test_jdbc_bind_plan_no_params() {
        assert_eq!(jdbc_bind_plan("SELECT 1"), Vec::<usize>::new());
    }

    #[test]
    fn test_jdbc_bind_plan_multidigit_index() {
        assert_eq!(jdbc_bind_plan("WHERE a = $10 AND b = $2"), vec![10, 2]);
    }
}
