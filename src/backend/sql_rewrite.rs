use std::collections::HashMap;

use crate::ir::{Parameter, Query};

/// Target-specific replacement pattern for [`rewrite_list_sql_native`].
pub enum ListRewriteTarget {
    /// PostgreSQL `= ANY($N)` array binding.
    PgArray,
    /// SQLite `json_each` with the given placeholder string (e.g. `"?"`, `"?1"`).
    JsonEach(String),
    /// MySQL `JSON_TABLE` with the given placeholder and column type.
    JsonTable {
        /// The placeholder string to use (e.g. `"?"`, `"%s"`).
        placeholder: String,
        /// The SQL column type for the JSON_TABLE extraction (e.g. `"INT"`, `"CHAR(255)"`).
        col_type: String,
    },
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

/// Replace `$N` or `?N` numbered placeholders with the given `replacement` string.
///
/// This is the general-purpose placeholder rewriter. Every `$N` or `?N` token
/// (where `N` is one or more ASCII digits) is replaced by `replacement` and the
/// digits are consumed.
///
/// Specific shortcuts:
/// - `rewrite_to_anon_params(sql)` — rewrites to `?`
/// - `rewrite_to_percent_s(sql)` — rewrites to `%s`
pub fn rewrite_placeholders(sql: &str, replacement: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(ch) = chars.next() {
        if (ch == '$' || ch == '?') && chars.peek().is_some_and(|c| c.is_ascii_digit()) {
            out.push_str(replacement);
            while chars.peek().is_some_and(|c| c.is_ascii_digit()) {
                chars.next();
            }
        } else {
            out.push(ch);
        }
    }
    out
}

/// Replace `$N` or `?N` numbered placeholders with anonymous `?` markers.
///
/// Used by every backend that binds parameters positionally: JDBC (Java, Kotlin),
/// better-sqlite3 (TypeScript/JavaScript), mysql2 (TypeScript/JavaScript), and
/// Python sqlite3. Not specific to any one driver — any driver that accepts `?`
/// as a positional placeholder can use this rewrite.
pub fn rewrite_to_anon_params(sql: &str) -> String {
    rewrite_placeholders(sql, "?")
}

/// Replace `$N` or `?N` numbered placeholders with `%s` positional markers.
///
/// Used by Python backends that bind parameters via DB-API 2.0 `%s` syntax
/// (psycopg3 for PostgreSQL, mysql-connector-python for MySQL).
pub fn rewrite_to_percent_s(sql: &str) -> String {
    rewrite_placeholders(sql, "%s")
}

/// Rewrite SQL for native list-param strategy: replace `IN ($N)` with the
/// target-specific SQL for the given list parameter.
///
/// Each `target_kind` determines the replacement pattern:
/// - `ListRewriteTarget::PgArray` — `= ANY($N)` (PostgreSQL array binding)
/// - `ListRewriteTarget::JsonEach(placeholder)` — `IN (SELECT value FROM json_each(<ph>))` (SQLite)
/// - `ListRewriteTarget::JsonTable { placeholder, col_type }` — MySQL `JSON_TABLE` expansion
///
/// Falls back to the original SQL with a warning if the `IN ($N)` clause is not found.
pub fn rewrite_list_sql_native(sql: &str, lp: &Parameter, target_kind: ListRewriteTarget) -> String {
    let replacement = match target_kind {
        ListRewriteTarget::PgArray => format!("= ANY(${})", lp.index),
        ListRewriteTarget::JsonEach(placeholder) => {
            format!("IN (SELECT value FROM json_each({placeholder}))")
        },
        ListRewriteTarget::JsonTable { placeholder, col_type } => {
            format!("IN (SELECT value FROM JSON_TABLE({placeholder},'$[*]' COLUMNS(value {col_type} PATH '$')) t)")
        },
    };
    replace_list_in_clause(sql, lp.index, &replacement).unwrap_or_else(|| {
        eprintln!("warning: list param '{}' not found in IN clause, treating as scalar", lp.name);
        sql.to_string()
    })
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

    use crate::ir::{Parameter, Query, SqlType};

    fn make_query(sql: &str, params: Vec<Parameter>) -> Query {
        Query::exec("Test", sql, params)
    }

    // ─── parse_placeholder_indices ──────────────────────────────────────────

    #[test]
    fn test_parse_placeholder_indices_unique_params() {
        assert_eq!(parse_placeholder_indices("SELECT * FROM t WHERE a = $1 AND b = $2"), vec![1, 2]);
    }

    #[test]
    fn test_parse_placeholder_indices_reused_param() {
        let sql = "WHERE account_id = $1 OR $1 = -1 AND x = $1 OR $1 = 0 AND y = $2";
        assert_eq!(parse_placeholder_indices(sql), vec![1, 1, 1, 1, 2]);
    }

    #[test]
    fn test_parse_placeholder_indices_question_mark_style() {
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

    // ─── rewrite_placeholders ───────────────────────────────────────────────

    #[test]
    fn test_rewrite_placeholders_to_percent_s() {
        assert_eq!(rewrite_placeholders("WHERE a = $1 AND b = $2", "%s"), "WHERE a = %s AND b = %s");
    }

    #[test]
    fn test_rewrite_placeholders_to_question_mark() {
        assert_eq!(rewrite_placeholders("WHERE a = $1 AND b = ?2", "?"), "WHERE a = ? AND b = ?");
    }

    #[test]
    fn test_rewrite_placeholders_passthrough_when_no_placeholders() {
        assert_eq!(rewrite_placeholders("SELECT 1", "%s"), "SELECT 1");
    }

    // ─── rewrite_to_percent_s ───────────────────────────────────────────────

    #[test]
    fn test_rewrite_to_percent_s_dollar_params() {
        assert_eq!(rewrite_to_percent_s("WHERE a = $1 AND b = $2"), "WHERE a = %s AND b = %s");
    }

    #[test]
    fn test_rewrite_to_percent_s_question_params() {
        assert_eq!(rewrite_to_percent_s("WHERE a = ?1 AND b = ?2"), "WHERE a = %s AND b = %s");
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

    // ─── rewrite_list_sql_native ────────────────────────────────────────────

    #[test]
    fn test_rewrite_list_sql_native_pg_array() {
        let lp = Parameter { index: 1, name: "ids".to_string(), sql_type: SqlType::Integer, nullable: false, is_list: true };
        let sql = "SELECT * FROM t WHERE id IN ($1)";
        let result = rewrite_list_sql_native(sql, &lp, ListRewriteTarget::PgArray);
        assert_eq!(result, "SELECT * FROM t WHERE id = ANY($1)");
    }

    #[test]
    fn test_rewrite_list_sql_native_json_each() {
        let lp = Parameter { index: 1, name: "ids".to_string(), sql_type: SqlType::Integer, nullable: false, is_list: true };
        let sql = "SELECT * FROM t WHERE id IN ($1)";
        let result = rewrite_list_sql_native(sql, &lp, ListRewriteTarget::JsonEach("?".to_string()));
        assert_eq!(result, "SELECT * FROM t WHERE id IN (SELECT value FROM json_each(?))");
    }

    #[test]
    fn test_rewrite_list_sql_native_json_table() {
        let lp = Parameter { index: 1, name: "ids".to_string(), sql_type: SqlType::Integer, nullable: false, is_list: true };
        let sql = "SELECT * FROM t WHERE id IN ($1)";
        let result = rewrite_list_sql_native(sql, &lp, ListRewriteTarget::JsonTable { placeholder: "%s".to_string(), col_type: "INT".to_string() });
        assert_eq!(result, "SELECT * FROM t WHERE id IN (SELECT value FROM JSON_TABLE(%s,'$[*]' COLUMNS(value INT PATH '$')) t)");
    }

    #[test]
    fn test_rewrite_list_sql_native_not_found_falls_back() {
        let lp = Parameter { index: 1, name: "ids".to_string(), sql_type: SqlType::Integer, nullable: false, is_list: true };
        let sql = "SELECT * FROM t WHERE id = $1";
        let result = rewrite_list_sql_native(sql, &lp, ListRewriteTarget::PgArray);
        assert_eq!(result, sql);
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
        let p = Parameter::scalar(1, "genre", SqlType::Text, false);
        let q = make_query("WHERE $1 = 'all' OR genre = $1", vec![p]);
        assert_eq!(positional_bind_names(&q), vec!["genre", "genre"]);
    }
}
