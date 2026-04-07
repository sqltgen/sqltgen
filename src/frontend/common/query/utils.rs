use std::collections::HashMap;

use crate::ir::{Parameter, Query, SqlType};

use super::annotations::QueryAnnotation;

pub(in crate::frontend::common) fn build_params(mapping: HashMap<usize, (String, SqlType, bool)>, count: usize) -> Vec<Parameter> {
    // Track how many times each name has been used so we can deduplicate.
    // e.g. `price BETWEEN $1 AND $2` → both get name "price" from the column,
    // but we need "price" and "price_2" in the function signature.
    let mut name_counts: HashMap<String, usize> = HashMap::new();
    (1..=count)
        .map(|idx| match mapping.get(&idx) {
            Some((name, sql_type, nullable)) => {
                let count = name_counts.entry(name.clone()).or_insert(0);
                *count += 1;
                let unique_name = if *count == 1 { name.clone() } else { format!("{name}_{count}") };
                Parameter::scalar(idx, unique_name, sql_type.clone(), *nullable)
            },
            None => Parameter::scalar(idx, format!("param{idx}"), SqlType::Text, false),
        })
        .collect()
}

/// Build a fallback query with no type information for parameters or result columns.
///
/// Used when a query cannot be fully resolved against the schema (e.g. unsupported
/// syntax, unknown tables). The query still runs but parameter/result types default
/// to `SqlType::Text`.
pub(in crate::frontend::common) fn unresolved_query(ann: &QueryAnnotation, sql: &str) -> Query {
    Query::new(ann.name.clone(), ann.cmd.clone(), sql, build_params(HashMap::new(), count_params(sql)), vec![])
}

pub(in crate::frontend::common) fn count_params(sql: &str) -> usize {
    let mut max = 0usize;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '$' || c == '?' {
            let digits: String = chars.by_ref().take_while(|ch| ch.is_ascii_digit()).collect();
            if let Ok(n) = digits.parse::<usize>() {
                max = max.max(n);
            }
        }
    }
    max
}

pub(in crate::frontend::common) fn placeholder_idx(s: &str) -> Option<usize> {
    // $N (PostgreSQL) or ?N (SQLite)
    let rest = s.strip_prefix('$').or_else(|| s.strip_prefix('?'))?;
    rest.parse().ok()
}
