use std::collections::HashMap;

use crate::ir::{Column, Parameter, Query, QueryCmd, ResultColumn, Schema, SqlType, Table};

/// Parses an annotated PostgreSQL query file into a list of [Query] models.
///
/// Each query must be preceded by:
/// ```sql
/// -- name: GetUser :one
/// SELECT id, name FROM users WHERE id = $1;
/// ```
///
/// Supported commands: `:one`, `:many`, `:exec`, `:execrows`
pub fn parse_queries(sql: &str, schema: &Schema) -> anyhow::Result<Vec<Query>> {
    let blocks = split_into_blocks(sql);
    let queries = blocks
        .into_iter()
        .filter_map(|(ann, body)| build_query(&ann, body.trim().trim_end_matches(';').trim(), schema).ok())
        .collect();
    Ok(queries)
}

// ─── Block splitting ─────────────────────────────────────────────────────────

struct Annotation {
    name: String,
    cmd: QueryCmd,
}

fn split_into_blocks(sql: &str) -> Vec<(Annotation, String)> {
    let mut blocks = Vec::new();
    let mut current: Option<Annotation> = None;
    let mut body_lines: Vec<&str> = Vec::new();

    for line in sql.lines() {
        if let Some(ann) = parse_annotation(line) {
            flush_block(&mut current, &mut body_lines, &mut blocks);
            current = Some(ann);
        } else if current.is_some() {
            body_lines.push(line);
        }
    }
    flush_block(&mut current, &mut body_lines, &mut blocks);
    blocks
}

fn flush_block(
    current: &mut Option<Annotation>,
    lines: &mut Vec<&str>,
    out: &mut Vec<(Annotation, String)>,
) {
    if let Some(ann) = current.take() {
        let body = lines.join("\n");
        let body = body.trim().to_string();
        if !body.is_empty() {
            out.push((ann, body));
        }
    }
    lines.clear();
}

fn parse_annotation(line: &str) -> Option<Annotation> {
    let line = line.trim();
    // -- name: Foo :one
    let rest = line.strip_prefix("--")?.trim();
    let rest = rest.strip_prefix("name:")?.trim();
    let mut parts = rest.splitn(2, ':');
    let name = parts.next()?.trim().to_string();
    let cmd_str = parts.next()?.trim().to_lowercase();
    let cmd = match cmd_str.as_str() {
        "one"      => QueryCmd::One,
        "many"     => QueryCmd::Many,
        "exec"     => QueryCmd::Exec,
        "execrows" => QueryCmd::ExecRows,
        _          => return None,
    };
    if name.is_empty() { return None; }
    Some(Annotation { name, cmd })
}

// ─── Query building ──────────────────────────────────────────────────────────

fn build_query(ann: &Annotation, sql: &str, schema: &Schema) -> anyhow::Result<Query> {
    let upper = sql.trim_start().to_uppercase();
    let result = if upper.starts_with("SELECT") {
        build_select(ann, sql, schema)
    } else if upper.starts_with("INSERT") {
        build_insert(ann, sql, schema)
    } else if upper.starts_with("UPDATE") {
        build_update(ann, sql, schema)
    } else if upper.starts_with("DELETE") {
        build_delete(ann, sql, schema)
    } else {
        bare(ann, sql)
    };
    Ok(result)
}

// ─── SELECT ──────────────────────────────────────────────────────────────────

fn build_select(ann: &Annotation, sql: &str, schema: &Schema) -> Query {
    let query_tables = extract_query_tables(sql, schema);
    if query_tables.is_empty() { return bare(ann, sql); }

    let alias_map = build_alias_map(&query_tables);
    let select_list = extract_select_list(sql);
    let result_columns = resolve_select_cols_multi(&select_list, &alias_map, &query_tables);
    let params = resolve_params_multi(sql, &alias_map, &query_tables);

    Query {
        name: ann.name.clone(),
        cmd: ann.cmd.clone(),
        sql: sql.to_string(),
        params,
        result_columns,
    }
}

fn extract_select_list(sql: &str) -> String {
    let upper = sql.to_uppercase();
    let from_idx = find_top_level_word(&upper, "FROM").unwrap_or(upper.len());
    sql[6..from_idx].trim().to_string() // skip "SELECT"
}

// ─── Multi-table helpers ──────────────────────────────────────────────────────

/// Scan `FROM` and `JOIN` keywords and return all referenced tables with their aliases.
fn extract_query_tables<'s>(sql: &str, schema: &'s Schema) -> Vec<(&'s Table, Option<String>)> {
    let upper = sql.to_uppercase();
    let mut result: Vec<(&'s Table, Option<String>)> = Vec::new();
    let mut pos = 0;

    loop {
        // Pick whichever of FROM / JOIN appears next
        let from_hit = find_top_level_word(&upper[pos..], "FROM").map(|p| (pos + p, 4usize));
        let join_hit = find_top_level_word(&upper[pos..], "JOIN").map(|p| (pos + p, 4usize));
        let (kw_abs, kw_len) = match (from_hit, join_hit) {
            (None, None) => break,
            (Some(f), None) => f,
            (None, Some(j)) => j,
            (Some(f), Some(j)) => if f.0 <= j.0 { f } else { j },
        };
        pos = kw_abs + 1; // advance past keyword start to avoid re-matching

        let after_kw = sql[kw_abs + kw_len..].trim_start();
        if after_kw.starts_with('(') { continue; } // skip subqueries

        // Read table reference (handles schema.table)
        let table_ref: String = after_kw.chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '.')
            .collect();
        if table_ref.is_empty() { continue; }

        let table_name = table_ref.split('.').last().unwrap_or(&table_ref).to_lowercase();
        let Some(table) = schema.tables.iter().find(|t| t.name == table_name) else { continue };

        let after_table = after_kw[table_ref.len()..].trim_start();
        let alias = read_alias(after_table);
        result.push((table, alias));
    }
    result
}

/// Read an optional alias word (skipping AS), returning None for SQL keywords.
fn read_alias(s: &str) -> Option<String> {
    // Skip optional AS
    let s = if s.len() >= 2
        && s[..2].eq_ignore_ascii_case("AS")
        && s.as_bytes().get(2).map_or(true, |b| !b.is_ascii_alphanumeric() && *b != b'_')
    {
        s[2..].trim_start()
    } else {
        s
    };

    let word: String = s.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
    if word.is_empty() { return None; }

    const STOP: &[&str] = &[
        "ON", "USING", "WHERE", "SET", "GROUP", "ORDER", "HAVING", "LIMIT",
        "OFFSET", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "NATURAL", "JOIN",
        "UNION", "INTERSECT", "EXCEPT", "RETURNING", "AND", "OR", "NOT",
        "SELECT", "FROM", "AS",
    ];
    if STOP.contains(&word.to_uppercase().as_str()) { None } else { Some(word.to_lowercase()) }
}

/// Build a map from alias (and table name) to table reference.
fn build_alias_map<'s>(tables: &[(&'s Table, Option<String>)]) -> HashMap<String, &'s Table> {
    let mut map = HashMap::new();
    for (table, alias) in tables {
        map.insert(table.name.clone(), *table);
        if let Some(a) = alias { map.insert(a.clone(), *table); }
    }
    map
}

/// Resolve SELECT list columns against multiple tables.
fn resolve_select_cols_multi(
    select_list: &str,
    alias_map: &HashMap<String, &Table>,
    all_tables: &[(&Table, Option<String>)],
) -> Vec<ResultColumn> {
    let trimmed = select_list.trim();
    if trimmed == "*" {
        return all_tables.iter().flat_map(|(t, _)| t.columns.iter().map(col_to_result)).collect();
    }
    trimmed.split(',').flat_map(|expr| -> Vec<ResultColumn> {
        let col_expr = expr.trim().split_whitespace().next().unwrap_or("").trim_matches('"');
        if let Some(dot) = col_expr.find('.') {
            // Qualified: alias.col  OR  alias.*
            let qualifier = col_expr[..dot].to_lowercase();
            let col_name  = col_expr[dot + 1..].trim_matches('"').to_lowercase();
            if col_name == "*" {
                // Expand all columns of the aliased table
                alias_map.get(&qualifier)
                    .map(|t| t.columns.iter().map(col_to_result).collect())
                    .unwrap_or_default()
            } else {
                alias_map.get(&qualifier)
                    .and_then(|t| t.columns.iter().find(|c| c.name == col_name))
                    .map(|c| vec![col_to_result(c)])
                    .unwrap_or_default()
            }
        } else {
            // Unqualified: first match across tables in FROM order
            let col_name = col_expr.to_lowercase();
            all_tables.iter().flat_map(|(t, _)| t.columns.iter())
                .find(|c| c.name == col_name)
                .map(|c| vec![col_to_result(c)])
                .unwrap_or_default()
        }
    }).collect()
}

/// Resolve `$N` parameters using column references across multiple tables.
fn resolve_params_multi(
    sql: &str,
    alias_map: &HashMap<String, &Table>,
    all_tables: &[(&Table, Option<String>)],
) -> Vec<Parameter> {
    let count = count_params(sql);
    if count == 0 { return vec![]; }

    let mut mapping: HashMap<usize, (String, SqlType, bool)> = HashMap::new();
    let mut pos = 0;
    while let Some(eq_pos) = sql[pos..].find('=') {
        let abs_eq = pos + eq_pos;
        let before = sql[..abs_eq].trim_end();
        let col_ref: String = before.chars().rev()
            .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == '.')
            .collect::<String>().chars().rev().collect();

        let after = sql[abs_eq + 1..].trim_start();
        if after.starts_with('$') {
            let idx_str: String = after[1..].chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(idx) = idx_str.parse::<usize>() {
                let col = if let Some(dot) = col_ref.find('.') {
                    let qualifier = col_ref[..dot].to_lowercase();
                    let col_name  = col_ref[dot + 1..].to_lowercase();
                    alias_map.get(&qualifier)
                        .and_then(|t| t.columns.iter().find(|c| c.name == col_name))
                } else {
                    let col_name = col_ref.to_lowercase();
                    all_tables.iter().flat_map(|(t, _)| t.columns.iter())
                        .find(|c| c.name == col_name)
                };
                if let Some(c) = col {
                    mapping.entry(idx).or_insert_with(|| {
                        (c.name.clone(), c.sql_type.clone(), c.nullable)
                    });
                }
            }
        }
        pos = abs_eq + 1;
    }

    (1..=count).map(|idx| match mapping.get(&idx) {
        Some((name, sql_type, nullable)) => Parameter {
            index: idx, name: name.clone(), sql_type: sql_type.clone(), nullable: *nullable,
        },
        None => Parameter { index: idx, name: format!("param{idx}"), sql_type: SqlType::Text, nullable: false },
    }).collect()
}

fn col_to_result(col: &Column) -> ResultColumn {
    ResultColumn {
        name: col.name.clone(),
        sql_type: col.sql_type.clone(),
        nullable: col.nullable,
    }
}

fn extract_from_table(sql: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let from_pos = find_top_level_word(&upper, "FROM")?;
    let after = sql[from_pos + 4..].trim_start();
    let name: String = after.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
    if name.is_empty() { None } else { Some(name.to_lowercase()) }
}

// ─── INSERT ──────────────────────────────────────────────────────────────────

fn build_insert(ann: &Annotation, sql: &str, schema: &Schema) -> Query {
    let table_name = match extract_word_after(sql, "INTO") {
        Some(t) => t,
        None => return bare(ann, sql),
    };
    let table = match find_table(schema, &table_name) { Some(t) => t, None => return bare(ann, sql) };

    let col_list = extract_parenthesised_list(sql, 0);
    let params = build_insert_params(&col_list, sql, table);
    let returning = extract_returning(sql, table);

    Query {
        name: ann.name.clone(),
        cmd: ann.cmd.clone(),
        sql: sql.to_string(),
        params,
        result_columns: returning,
    }
}

fn build_insert_params(col_names: &[String], sql: &str, table: &Table) -> Vec<Parameter> {
    let count = count_params(sql);
    (1..=count)
        .map(|idx| {
            let col_name = col_names.get(idx - 1);
            let (name, sql_type, nullable) = match col_name.and_then(|n| find_col(table, n)) {
                Some(col) => (col.name.clone(), col.sql_type.clone(), col.nullable),
                None => (format!("param{idx}"), SqlType::Text, false),
            };
            Parameter { index: idx, name, sql_type, nullable }
        })
        .collect()
}

// ─── UPDATE ──────────────────────────────────────────────────────────────────

fn build_update(ann: &Annotation, sql: &str, schema: &Schema) -> Query {
    let table_name = match extract_word_after(sql, "UPDATE") {
        Some(t) => t,
        None => return bare(ann, sql),
    };
    let table = match find_table(schema, &table_name) { Some(t) => t, None => return bare(ann, sql) };

    let params = resolve_params(sql, table);
    let returning = extract_returning(sql, table);

    Query {
        name: ann.name.clone(),
        cmd: ann.cmd.clone(),
        sql: sql.to_string(),
        params,
        result_columns: returning,
    }
}

// ─── DELETE ──────────────────────────────────────────────────────────────────

fn build_delete(ann: &Annotation, sql: &str, schema: &Schema) -> Query {
    let table_name = match extract_from_table(sql) {
        Some(t) => t,
        None => return bare(ann, sql),
    };
    let table = match find_table(schema, &table_name) { Some(t) => t, None => return bare(ann, sql) };

    let params = resolve_params(sql, table);

    Query {
        name: ann.name.clone(),
        cmd: ann.cmd.clone(),
        sql: sql.to_string(),
        params,
        result_columns: vec![],
    }
}

// ─── Parameter resolution ────────────────────────────────────────────────────

/// Finds all `$N` placeholders and infers their column type from context.
fn resolve_params(sql: &str, table: &Table) -> Vec<Parameter> {
    let count = count_params(sql);
    if count == 0 { return vec![]; }

    // Build index → column name from `col = $N` patterns in SET and WHERE
    let mut mapping: std::collections::HashMap<usize, String> = Default::default();
    let mut pos = 0;
    while let Some(eq_pos) = sql[pos..].find('=') {
        let abs_eq = pos + eq_pos;
        // Get the word before '='
        let before = sql[..abs_eq].trim_end();
        let col_name: String = before
            .chars()
            .rev()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect::<String>()
            .chars()
            .rev()
            .collect();

        // Get token after '='
        let after = sql[abs_eq + 1..].trim_start();
        if after.starts_with('$') {
            let idx_str: String = after[1..].chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(idx) = idx_str.parse::<usize>() {
                if !col_name.is_empty() {
                    mapping.entry(idx).or_insert_with(|| col_name.to_lowercase());
                }
            }
        }
        pos = abs_eq + 1;
    }

    (1..=count)
        .map(|idx| {
            let col_name = mapping.get(&idx);
            let (name, sql_type, nullable) = match col_name.and_then(|n| find_col(table, n)) {
                Some(col) => (col.name.clone(), col.sql_type.clone(), col.nullable),
                None => (format!("param{idx}"), SqlType::Text, false),
            };
            Parameter { index: idx, name, sql_type, nullable }
        })
        .collect()
}

// ─── RETURNING ───────────────────────────────────────────────────────────────

fn extract_returning(sql: &str, table: &Table) -> Vec<ResultColumn> {
    let upper = sql.to_uppercase();
    let pos = match find_word(&upper, "RETURNING") {
        Some(p) => p,
        None => return vec![],
    };
    let list = sql[pos + 9..].trim();
    if list == "*" {
        return table.columns.iter().map(col_to_result).collect();
    }
    list.split(',')
        .filter_map(|expr| {
            let name = expr.trim().split_whitespace().next().unwrap_or("").to_lowercase();
            let name = name.trim_matches('"');
            find_col(table, name).map(col_to_result)
        })
        .collect()
}

// ─── Utilities ───────────────────────────────────────────────────────────────

fn find_table<'a>(schema: &'a Schema, name: &str) -> Option<&'a Table> {
    schema.tables.iter().find(|t| t.name == name)
}

fn find_col<'a>(table: &'a Table, name: &str) -> Option<&'a Column> {
    table.columns.iter().find(|c| c.name == name)
}

fn count_params(sql: &str) -> usize {
    let mut max = 0usize;
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '$' {
            let digits: String = chars.by_ref().take_while(|ch| ch.is_ascii_digit()).collect();
            if let Ok(n) = digits.parse::<usize>() {
                max = max.max(n);
            }
        }
    }
    max
}

fn bare(ann: &Annotation, sql: &str) -> Query {
    Query {
        name: ann.name.clone(),
        cmd: ann.cmd.clone(),
        sql: sql.to_string(),
        params: vec![],
        result_columns: vec![],
    }
}

/// Extracts comma-separated tokens from the first `(...)` in sql starting at `start`.
fn extract_parenthesised_list(sql: &str, start: usize) -> Vec<String> {
    let open = match sql[start..].find('(') {
        Some(p) => start + p,
        None => return vec![],
    };
    let mut depth = 0usize;
    let mut buf = String::new();
    for ch in sql[open..].chars() {
        match ch {
            '(' => {
                depth += 1;
                if depth > 1 { buf.push(ch); }
            }
            ')' => {
                depth -= 1;
                if depth == 0 { break; }
                buf.push(ch);
            }
            _ if depth == 1 => buf.push(ch),
            _ => {}
        }
    }
    buf.split(',').map(|s| s.trim().to_lowercase()).filter(|s| !s.is_empty()).collect()
}

/// Like [`find_word`] but skips content inside parenthesised subexpressions and
/// single-quoted string literals, so keywords inside subqueries are invisible.
fn find_top_level_word(upper: &str, keyword: &str) -> Option<usize> {
    let klen = keyword.len();
    let bytes = upper.as_bytes();
    let mut i = 0;
    let mut depth = 0usize;

    while i < upper.len() {
        match bytes[i] {
            b'\'' => {
                // Skip string literal, handling escaped '' pairs
                i += 1;
                while i < upper.len() {
                    if bytes[i] == b'\'' {
                        i += 1;
                        if bytes.get(i) == Some(&b'\'') { i += 1; } else { break; }
                    } else {
                        i += 1;
                    }
                }
            }
            b'(' => { depth += 1; i += 1; }
            b')' => { depth = depth.saturating_sub(1); i += 1; }
            _ if depth == 0 => {
                if upper[i..].starts_with(keyword) {
                    let before_ok = i == 0
                        || (!bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_');
                    let after_ok = i + klen >= upper.len()
                        || (!bytes[i + klen].is_ascii_alphanumeric() && bytes[i + klen] != b'_');
                    if before_ok && after_ok { return Some(i); }
                }
                i += 1;
            }
            _ => { i += 1; }
        }
    }
    None
}

/// Returns index of word boundary match for `keyword` in `upper` (uppercase) string.
fn find_word(upper: &str, keyword: &str) -> Option<usize> {
    let klen = keyword.len();
    let bytes = upper.as_bytes();
    let mut i = 0;
    while i + klen <= upper.len() {
        if upper[i..].starts_with(keyword) {
            let before_ok = i == 0 || !bytes[i - 1].is_ascii_alphanumeric() && bytes[i - 1] != b'_';
            let after_ok = i + klen >= upper.len()
                || !bytes[i + klen].is_ascii_alphanumeric() && bytes[i + klen] != b'_';
            if before_ok && after_ok {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

/// Extracts the next bare identifier word after a keyword.
fn extract_word_after(sql: &str, keyword: &str) -> Option<String> {
    let upper = sql.to_uppercase();
    let pos = find_top_level_word(&upper, keyword)?;
    let after = sql[pos + keyword.len()..].trim_start();
    let word: String = after.chars().take_while(|c| c.is_alphanumeric() || *c == '_').collect();
    if word.is_empty() { None } else { Some(word.to_lowercase()) }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{Column, Schema, SqlType, Table};

    fn make_schema() -> Schema {
        Schema {
            tables: vec![Table {
                name: "users".into(),
                columns: vec![
                    Column { name: "id".into(),    sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                    Column { name: "name".into(),  sql_type: SqlType::Text,   nullable: false, is_primary_key: false },
                    Column { name: "email".into(), sql_type: SqlType::Text,   nullable: false, is_primary_key: false },
                    Column { name: "bio".into(),   sql_type: SqlType::Text,   nullable: true,  is_primary_key: false },
                ],
            }],
        }
    }

    fn make_join_schema() -> Schema {
        Schema {
            tables: vec![
                Table {
                    name: "users".into(),
                    columns: vec![
                        Column { name: "id".into(),   sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                        Column { name: "name".into(), sql_type: SqlType::Text,   nullable: false, is_primary_key: false },
                    ],
                },
                Table {
                    name: "posts".into(),
                    columns: vec![
                        Column { name: "id".into(),      sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                        Column { name: "user_id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: false },
                        Column { name: "title".into(),   sql_type: SqlType::Text,   nullable: false, is_primary_key: false },
                    ],
                },
            ],
        }
    }

    #[test]
    fn parses_one_annotation() {
        let sql = "-- name: GetUser :one\nSELECT id, name, email FROM users WHERE id = $1;";
        let queries = parse_queries(sql, &make_schema()).unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0].name, "GetUser");
        assert_eq!(queries[0].cmd, QueryCmd::One);
    }

    #[test]
    fn parses_many_annotation() {
        let sql = "-- name: ListUsers :many\nSELECT id, name FROM users;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.cmd, QueryCmd::Many);
    }

    #[test]
    fn parses_exec_annotation() {
        let sql = "-- name: DeleteUser :exec\nDELETE FROM users WHERE id = $1;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.cmd, QueryCmd::Exec);
    }

    #[test]
    fn parses_execrows_annotation() {
        let sql = "-- name: UpdateName :execrows\nUPDATE users SET name = $1 WHERE id = $2;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.cmd, QueryCmd::ExecRows);
    }

    #[test]
    fn resolves_select_result_columns() {
        let sql = "-- name: GetUser :one\nSELECT id, name, email, bio FROM users WHERE id = $1;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.result_columns.len(), 4);
        let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "name", "email", "bio"]);
    }

    #[test]
    fn resolves_select_star() {
        let sql = "-- name: ListUsers :many\nSELECT * FROM users;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.result_columns.len(), 4);
    }

    #[test]
    fn preserves_nullability_in_select_result() {
        let sql = "-- name: GetUser :one\nSELECT id, bio FROM users WHERE id = $1;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert!(!q.result_columns.iter().find(|c| c.name == "id").unwrap().nullable);
        assert!(q.result_columns.iter().find(|c| c.name == "bio").unwrap().nullable);
    }

    #[test]
    fn resolves_select_param_from_where() {
        let sql = "-- name: GetUser :one\nSELECT id, name FROM users WHERE id = $1;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].index, 1);
        assert_eq!(q.params[0].name, "id");
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    }

    #[test]
    fn resolves_insert_params_from_column_list() {
        let sql = "-- name: CreateUser :exec\nINSERT INTO users (name, email, bio) VALUES ($1, $2, $3);";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 3);
        assert_eq!(q.params[0].name, "name");
        assert_eq!(q.params[1].name, "email");
        assert_eq!(q.params[2].name, "bio");
    }

    #[test]
    fn resolves_update_params_from_set_clause() {
        let sql = "-- name: UpdateUser :exec\nUPDATE users SET name = $1, email = $2 WHERE id = $3;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 3);
        assert_eq!(q.params[0].name, "name");
        assert_eq!(q.params[1].name, "email");
        assert_eq!(q.params[2].name, "id");
    }

    #[test]
    fn resolves_delete_param_from_where() {
        let sql = "-- name: DeleteUser :exec\nDELETE FROM users WHERE id = $1;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].name, "id");
        assert_eq!(q.result_columns.len(), 0);
    }

    #[test]
    fn parses_multiple_queries() {
        let sql = "-- name: GetUser :one\nSELECT id, name FROM users WHERE id = $1;\n\n-- name: ListUsers :many\nSELECT id, name FROM users;\n\n-- name: CreateUser :exec\nINSERT INTO users (name, email) VALUES ($1, $2);";
        let queries = parse_queries(sql, &make_schema()).unwrap();
        assert_eq!(queries.len(), 3);
        let names: Vec<_> = queries.iter().map(|q| q.name.as_str()).collect();
        assert_eq!(names, ["GetUser", "ListUsers", "CreateUser"]);
    }

    // ─── Subquery tests ───────────────────────────────────────────────────────

    #[test]
    fn subquery_in_where_does_not_add_inner_table_to_scope() {
        // posts is only referenced inside a subquery — it must not leak into the alias map
        let sql = "-- name: GetUsersWithPosts :many\n\
            SELECT u.id, u.name FROM users u WHERE u.id IN (SELECT user_id FROM posts);";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "name"]);
        assert_eq!(q.params.len(), 0);
    }

    #[test]
    fn scalar_subquery_in_select_does_not_truncate_outer_columns() {
        // The inner FROM must not cut off the outer select list
        let sql = "-- name: GetUserPostCount :many\n\
            SELECT u.name, (SELECT COUNT(*) FROM posts p WHERE p.user_id = u.id) \
            FROM users u;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        // u.name must be resolved; the scalar subquery result is unresolvable but
        // it must not prevent name from being in the result set
        assert!(q.result_columns.iter().any(|c| c.name == "name"));
    }

    #[test]
    fn subquery_param_in_where_resolves_from_outer_table() {
        // $1 appears in the outer WHERE, bound to the outer table
        let sql = "-- name: GetUser :one\n\
            SELECT u.id, u.name FROM users u \
            WHERE u.id = $1 AND u.id IN (SELECT user_id FROM posts);";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].name, "id");
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    }

    // ─── JOIN tests ───────────────────────────────────────────────────────────

    #[test]
    fn join_resolves_qualified_columns() {
        let sql = "-- name: GetUserPost :one\n\
            SELECT u.id, u.name, p.title FROM users u INNER JOIN posts p ON p.user_id = u.id WHERE u.id = $1;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "name", "title"]);
        assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
        assert_eq!(q.result_columns[2].sql_type, SqlType::Text);
    }

    #[test]
    fn join_resolves_unqualified_columns() {
        let sql = "-- name: ListUserPosts :many\n\
            SELECT name, title FROM users JOIN posts ON posts.user_id = users.id;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["name", "title"]);
        assert_eq!(q.params.len(), 0);
    }

    #[test]
    fn join_resolves_params_with_qualifier() {
        let sql = "-- name: GetPostsByUser :many\n\
            SELECT p.id, p.title FROM users u JOIN posts p ON p.user_id = u.id WHERE u.id = $1;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].name, "id");
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    }

    #[test]
    fn join_select_star_returns_all_columns() {
        let sql = "-- name: GetAll :many\n\
            SELECT * FROM users u JOIN posts p ON p.user_id = u.id;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        // users(2) + posts(3)
        assert_eq!(q.result_columns.len(), 5);
    }

    #[test]
    fn join_left_join_alias() {
        let sql = "-- name: GetUserWithPost :one\n\
            SELECT u.id, p.title FROM users AS u LEFT JOIN posts AS p ON p.user_id = u.id WHERE u.id = $1;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        assert_eq!(q.result_columns.len(), 2);
        assert_eq!(q.result_columns[1].name, "title");
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    }

    // ─── Qualified-wildcard tests (alias.*) ──────────────────────────────────

    #[test]
    fn qualified_star_expands_single_table() {
        // SELECT a.* should expand to all columns of `users`
        let sql = "-- name: ListUsers :many\nSELECT a.* FROM users a;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "name", "email", "bio"]);
    }

    #[test]
    fn qualified_star_expands_each_table_in_join() {
        // SELECT a.*, b.* should expand both tables independently
        let sql = "-- name: GetAll :many\n\
            SELECT a.*, b.* FROM users a INNER JOIN posts b ON b.user_id = a.id;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        // users has 2 cols, posts has 3 cols → 5 total, in order
        assert_eq!(q.result_columns.len(), 5);
        let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "name", "id", "user_id", "title"]);
    }

    #[test]
    fn qualified_star_mixed_with_regular_column() {
        // SELECT a.*, b.title — a.* expands, b.title resolves normally
        let sql = "-- name: GetUserPosts :many\n\
            SELECT a.*, b.title FROM users a INNER JOIN posts b ON b.user_id = a.id;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "name", "title"]);
    }

    #[test]
    fn strips_trailing_semicolons() {
        let sql = "-- name: ListUsers :many\nSELECT id, name FROM users;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert!(!q.sql.ends_with(';'));
    }
}
