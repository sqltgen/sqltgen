//! Named parameter preprocessing for query files.
//!
//! Handles the `@param_name` placeholder syntax and `-- @param_name [type] [null|not null]`
//! annotation lines that appear before the SQL body.

use std::collections::{HashMap, HashSet};

use crate::ir::{Parameter, SqlType};

// ─── Public types ─────────────────────────────────────────────────────────────

/// A named parameter extracted from the query body, with optional overrides from annotations.
pub(crate) struct NamedParam {
    /// The param name as written after `@` in the SQL body.
    pub name: String,
    /// Explicit type from a `-- @name type` annotation, if provided.
    pub sql_type: Option<SqlType>,
    /// Explicit nullability from a `-- @name [not] null` annotation, if provided.
    pub nullable: Option<bool>,
}

// ─── Public API ───────────────────────────────────────────────────────────────

/// Rewrites a SQL body that uses `@name` placeholders into positional `$N` form.
///
/// Returns `(rewritten_sql, ordered_params)` where `ordered_params` is in
/// first-appearance order. Returns `None` if the body contains no `@name`
/// placeholders (fast path for queries that use the traditional `$N`/`?N` style).
///
/// Annotation lines (`-- @name [type] [not null]`) are stripped from the returned
/// SQL and used to populate [`NamedParam`] overrides. An annotation whose name does
/// not appear in the SQL body emits a warning and is ignored.
pub(crate) fn preprocess_named_params(sql: &str) -> Option<(String, Vec<NamedParam>)> {
    let overrides = parse_param_annotations(sql);
    let stripped = strip_annotation_lines(sql);
    let param_names = collect_named_param_order(&stripped);

    if param_names.is_empty() {
        for name in overrides.keys() {
            eprintln!("warning: @{name} declared in annotation but not found in query body");
        }
        return None;
    }

    for name in overrides.keys() {
        if !param_names.contains(name) {
            eprintln!("warning: @{name} declared in annotation but not found in query body");
        }
    }

    let name_to_index: HashMap<String, usize> =
        param_names.iter().enumerate().map(|(i, n)| (n.clone(), i + 1)).collect();

    let rewritten = rewrite_named_params_in_sql(&stripped, &name_to_index);

    let ordered_params = param_names
        .into_iter()
        .map(|name| {
            let (sql_type, nullable) =
                overrides.get(&name).cloned().unwrap_or((None, None));
            NamedParam { name, sql_type, nullable }
        })
        .collect();

    Some((rewritten, ordered_params))
}

/// Applies named param names and annotation overrides to an already-inferred `params` list.
///
/// Call this after the standard type-inference pass. Each `NamedParam` at position `i`
/// corresponds to `$(i+1)` in the rewritten SQL.
pub(crate) fn apply_named_param_overrides(
    params: &mut [Parameter],
    named_params: &[NamedParam],
) {
    for (i, np) in named_params.iter().enumerate() {
        let idx = i + 1;
        if let Some(param) = params.iter_mut().find(|p| p.index == idx) {
            param.name = np.name.clone();
            if let Some(t) = &np.sql_type {
                param.sql_type = t.clone();
            }
            if let Some(n) = np.nullable {
                param.nullable = n;
            }
        }
    }
}

// ─── Annotation parsing ───────────────────────────────────────────────────────

/// `(type_override, nullable_override)` keyed by param name.
type AnnotationMap = HashMap<String, (Option<SqlType>, Option<bool>)>;

fn parse_param_annotations(sql: &str) -> AnnotationMap {
    let mut map = HashMap::new();
    for line in sql.lines() {
        if let Some((name, sql_type, nullable)) = parse_annotation_line(line) {
            map.insert(name, (sql_type, nullable));
        }
    }
    map
}

/// Parses a single `-- @name [type] [null | not null]` line.
///
/// Returns `(name, type_override, nullable_override)` or `None` if the line is
/// not a param annotation.
fn parse_annotation_line(line: &str) -> Option<(String, Option<SqlType>, Option<bool>)> {
    let rest = line.trim().strip_prefix("--")?.trim();
    let rest = rest.strip_prefix('@')?;
    let mut parts = rest.splitn(2, char::is_whitespace);
    let name = parts.next()?.trim().to_string();
    if name.is_empty() {
        return None;
    }
    let (sql_type, nullable) = parse_type_and_nullability(parts.next().unwrap_or("").trim());
    Some((name, sql_type, nullable))
}

/// Parses an optional type keyword and optional `[not] null` specifier from a remainder string.
fn parse_type_and_nullability(s: &str) -> (Option<SqlType>, Option<bool>) {
    let lower = s.to_lowercase();
    let tokens: Vec<&str> = lower.split_whitespace().collect();

    let (type_tokens, nullable) = if tokens.ends_with(&["not", "null"]) {
        (&tokens[..tokens.len() - 2], Some(false))
    } else if tokens.ends_with(&["null"]) {
        (&tokens[..tokens.len() - 1], Some(true))
    } else {
        (tokens.as_slice(), None)
    };

    let type_str = type_tokens.join(" ");
    let sql_type = if type_str.is_empty() {
        None
    } else {
        match parse_sql_type_str(&type_str) {
            Some(t) => Some(t),
            None => {
                eprintln!("warning: unknown type {type_str:?} in param annotation, ignoring");
                None
            }
        }
    };

    (sql_type, nullable)
}

/// Maps a lowercase type keyword to a [`SqlType`].
///
/// Covers the most common SQL type names. Unknown strings emit a warning via the
/// caller and return `None`; type inference from SQL context is used instead.
fn parse_sql_type_str(s: &str) -> Option<SqlType> {
    match s {
        "bool" | "boolean" => Some(SqlType::Boolean),
        "smallint" | "int2" => Some(SqlType::SmallInt),
        "int" | "integer" | "int4" => Some(SqlType::Integer),
        "bigint" | "int8" => Some(SqlType::BigInt),
        "real" | "float" | "float4" => Some(SqlType::Real),
        "double" | "float8" | "double precision" => Some(SqlType::Double),
        "decimal" | "numeric" => Some(SqlType::Decimal),
        "text" | "varchar" | "char" | "string" => Some(SqlType::Text),
        "bytea" | "blob" | "bytes" => Some(SqlType::Bytes),
        "date" => Some(SqlType::Date),
        "time" => Some(SqlType::Time),
        "timestamp" => Some(SqlType::Timestamp),
        "timestamptz" | "timestamp with time zone" => Some(SqlType::TimestampTz),
        "interval" => Some(SqlType::Interval),
        "uuid" => Some(SqlType::Uuid),
        "json" => Some(SqlType::Json),
        "jsonb" => Some(SqlType::Jsonb),
        _ => None,
    }
}

// ─── SQL rewriting ────────────────────────────────────────────────────────────

/// Removes `-- @name …` annotation lines from the SQL, preserving all other lines.
fn strip_annotation_lines(sql: &str) -> String {
    sql.lines()
        .filter(|line| !is_param_annotation_line(line))
        .collect::<Vec<_>>()
        .join("\n")
}

fn is_param_annotation_line(line: &str) -> bool {
    match line.trim().strip_prefix("--") {
        Some(rest) => rest.trim().starts_with('@'),
        None => false,
    }
}

/// Scans `sql` for `@identifier` tokens outside comments and string literals,
/// returning their names in first-appearance order, deduplicated.
fn collect_named_param_order(sql: &str) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    let mut chars = sql.chars().peekable();
    let mut in_line_comment = false;
    let mut in_string = false;

    while let Some(c) = chars.next() {
        match c {
            '\n' if in_line_comment => in_line_comment = false,
            _ if in_line_comment => {}
            '\'' if !in_string => in_string = true,
            '\'' if in_string => in_string = false,
            _ if in_string => {}
            '-' if !in_line_comment && chars.peek() == Some(&'-') => {
                chars.next();
                in_line_comment = true;
            }
            '@' => {
                let name: String =
                    std::iter::from_fn(|| chars.next_if(|ch| ch.is_alphanumeric() || *ch == '_'))
                        .collect();
                if !name.is_empty() && seen.insert(name.clone()) {
                    names.push(name);
                }
            }
            _ => {}
        }
    }
    names
}

/// Rewrites every `@name` occurrence in `sql` to its `$N` positional placeholder.
///
/// Occurrences inside line comments and string literals are left unchanged.
fn rewrite_named_params_in_sql(sql: &str, name_to_index: &HashMap<String, usize>) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut in_line_comment = false;
    let mut in_string = false;

    while let Some(c) = chars.next() {
        match c {
            '\n' if in_line_comment => {
                in_line_comment = false;
                out.push(c);
            }
            c if in_line_comment => out.push(c),
            '\'' if !in_string => {
                in_string = true;
                out.push(c);
            }
            '\'' if in_string => {
                in_string = false;
                out.push(c);
            }
            c if in_string => out.push(c),
            '-' if chars.peek() == Some(&'-') => {
                in_line_comment = true;
                out.push(c);
            }
            '@' => {
                let name: String =
                    std::iter::from_fn(|| chars.next_if(|ch| ch.is_alphanumeric() || *ch == '_'))
                        .collect();
                if let Some(&idx) = name_to_index.get(&name) {
                    out.push_str(&format!("${idx}"));
                } else {
                    out.push('@');
                    out.push_str(&name);
                }
            }
            c => out.push(c),
        }
    }
    out
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_named_param_basic_rewrite() {
        let sql = "SELECT id FROM users WHERE id = @user_id";
        let (rewritten, params) = preprocess_named_params(sql).unwrap();
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].name, "user_id");
        assert!(rewritten.contains("$1"));
        assert!(!rewritten.contains("@user_id"));
    }

    #[test]
    fn test_named_param_repeated_maps_to_same_index() {
        let sql = "UPDATE t SET a = @val WHERE b = @val";
        let (rewritten, params) = preprocess_named_params(sql).unwrap();
        assert_eq!(params.len(), 1);
        assert_eq!(rewritten.matches("$1").count(), 2);
    }

    #[test]
    fn test_named_param_multiple_first_appearance_order() {
        let sql = "UPDATE t SET a = @foo WHERE b = @bar AND c = @foo";
        let (rewritten, params) = preprocess_named_params(sql).unwrap();
        assert_eq!(params.len(), 2);
        assert_eq!(params[0].name, "foo");
        assert_eq!(params[1].name, "bar");
        assert!(rewritten.contains("$1") && rewritten.contains("$2"));
    }

    #[test]
    fn test_annotation_type_override() {
        let sql = "-- @my_id bigint\nSELECT id FROM users WHERE id = @my_id";
        let (rewritten, params) = preprocess_named_params(sql).unwrap();
        assert_eq!(params[0].name, "my_id");
        assert_eq!(params[0].sql_type, Some(SqlType::BigInt));
        assert!(!rewritten.contains("-- @my_id"));
    }

    #[test]
    fn test_annotation_nullable_override() {
        let sql = "-- @bio null\nSELECT id FROM users WHERE bio = @bio";
        let (_, params) = preprocess_named_params(sql).unwrap();
        assert_eq!(params[0].nullable, Some(true));
    }

    #[test]
    fn test_annotation_not_null_with_type() {
        let sql = "-- @bio text not null\nSELECT id FROM users WHERE bio = @bio";
        let (_, params) = preprocess_named_params(sql).unwrap();
        assert_eq!(params[0].sql_type, Some(SqlType::Text));
        assert_eq!(params[0].nullable, Some(false));
    }

    #[test]
    fn test_no_named_params_returns_none() {
        let sql = "SELECT id FROM users WHERE id = $1";
        assert!(preprocess_named_params(sql).is_none());
    }

    #[test]
    fn test_named_param_in_regular_comment_skipped() {
        // @foo in a regular SQL comment should not become a param
        let sql = "-- just a comment mentioning @foo\nSELECT id FROM users WHERE id = @real_id";
        let (_, params) = preprocess_named_params(sql).unwrap();
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].name, "real_id");
    }

    #[test]
    fn test_annotation_line_stripped_from_output_sql() {
        let sql = "-- @user_id bigint\nSELECT id FROM users WHERE id = @user_id";
        let (rewritten, _) = preprocess_named_params(sql).unwrap();
        assert!(!rewritten.contains("-- @user_id"));
        assert!(rewritten.contains("$1"));
    }

    #[test]
    fn test_apply_named_param_overrides() {
        let mut params = vec![
            Parameter { index: 1, name: "param1".into(), sql_type: SqlType::Text, nullable: false },
            Parameter { index: 2, name: "param2".into(), sql_type: SqlType::Text, nullable: false },
        ];
        let named = vec![
            NamedParam { name: "user_id".into(), sql_type: Some(SqlType::BigInt), nullable: Some(false) },
            NamedParam { name: "bio".into(), sql_type: None, nullable: Some(true) },
        ];
        apply_named_param_overrides(&mut params, &named);
        assert_eq!(params[0].name, "user_id");
        assert_eq!(params[0].sql_type, SqlType::BigInt);
        assert_eq!(params[1].name, "bio");
        assert_eq!(params[1].nullable, true);
        // sql_type not overridden when None
        assert_eq!(params[1].sql_type, SqlType::Text);
    }
}
