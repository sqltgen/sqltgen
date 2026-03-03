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
    let table_name = match extract_from_table(sql) { Some(t) => t, None => return bare(ann, sql) };
    let table = match find_table(schema, &table_name) { Some(t) => t, None => return bare(ann, sql) };

    let select_list = extract_select_list(sql);
    let result_columns = resolve_select_cols(&select_list, table);
    let params = resolve_params(sql, table);

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
    let from_idx = find_word(&upper, "FROM").unwrap_or(upper.len());
    sql[6..from_idx].trim().to_string() // skip "SELECT"
}

fn resolve_select_cols(select_list: &str, table: &Table) -> Vec<ResultColumn> {
    let trimmed = select_list.trim();
    if trimmed == "*" {
        return table.columns.iter().map(col_to_result).collect();
    }
    trimmed
        .split(',')
        .filter_map(|expr| {
            // Handle table.col and aliases (col AS alias)
            let col_name = expr
                .trim()
                .split_whitespace()
                .collect::<Vec<_>>()
                .chunks(1) // just get first token before AS
                .next()
                .and_then(|c| c.first())
                .copied()
                .unwrap_or(expr.trim());
            // strip table prefix
            let col_name = col_name.rsplit('.').next().unwrap_or(col_name);
            let col_name = col_name.trim_matches('"');
            table.columns.iter().find(|c| c.name == col_name.to_lowercase())
                .map(col_to_result)
        })
        .collect()
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
    let from_pos = find_word(&upper, "FROM")?;
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
    let pos = find_word(&upper, keyword)?;
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

    #[test]
    fn strips_trailing_semicolons() {
        let sql = "-- name: ListUsers :many\nSELECT id, name FROM users;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert!(!q.sql.ends_with(';'));
    }
}
