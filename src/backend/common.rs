use std::fmt::Write;

use crate::ir::{Query, Schema};

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
        if table.columns.len() == query.result_columns.len()
            && table.columns.iter().zip(&query.result_columns).all(|(a, b)| a.name == b.name)
        {
            return Some(&table.name);
        }
    }
    None
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
