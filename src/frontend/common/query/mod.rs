mod dml;
mod params;
mod resolve;
mod select;

use std::collections::HashMap;

use sqlparser::ast::{Delete, Insert, JoinOperator, Query as SqlQuery, Select, SelectItem, SetExpr, Statement, TableFactor, TableObject, With};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;

use crate::frontend::common::{ident_to_str, named_params, obj_name_to_str};
use crate::ir::{Column, Parameter, Query, QueryCmd, ResultColumn, Schema, SqlType, Table};

use dml::{build_delete, build_insert, build_update, collect_delete_where_params, collect_insert_value_params, collect_update_params};
use params::{collect_join_params, collect_limit_offset_params, collect_params_from_expr};
use resolve::{col_to_result, resolve_expr, resolve_projection};
use select::build_select;

/// Dialect-agnostic type inference configuration.
pub(crate) struct ResolverConfig {
    /// Return type of SUM applied to smallint/integer columns.
    /// PostgreSQL: BigInt.  MySQL: Decimal.  SQLite: BigInt.
    pub sum_integer_type: SqlType,
    /// Return type of SUM applied to bigint columns.
    /// PostgreSQL: Decimal (numeric).  MySQL: Decimal.  SQLite: BigInt.
    pub sum_bigint_type: SqlType,
    /// Return type of AVG applied to any integer column (smallint/integer/bigint).
    /// PostgreSQL: Decimal (numeric).  MySQL: Double.  SQLite: Double (real).
    pub avg_integer_type: SqlType,
    /// Maps a sqlparser `DataType` to `SqlType` using the active dialect's typemap.
    ///
    /// Used by `resolve_expr` for CAST expressions. Each dialect supplies its own
    /// mapping function (e.g. `postgres::typemap::map`).
    pub typemap: fn(&sqlparser::ast::DataType) -> SqlType,
}

impl Default for ResolverConfig {
    fn default() -> Self {
        Self {
            sum_integer_type: SqlType::BigInt,
            sum_bigint_type: SqlType::BigInt,
            avg_integer_type: SqlType::Double,
            typemap: crate::frontend::common::typemap::map_common_or_custom,
        }
    }
}

/// Groups the read-only context and mutable parameter mapping that most
/// parameter-collection functions need. Avoids threading five separate
/// arguments through every call.
pub(super) struct ResolverContext<'a> {
    pub alias_map: &'a HashMap<String, &'a Table>,
    pub all_tables: &'a [(Table, Option<String>)],
    pub schema: &'a Schema,
    pub config: &'a ResolverConfig,
    pub mapping: &'a mut HashMap<usize, (String, SqlType, bool)>,
    pub query_name: &'a str,
}

pub(super) fn insert_table_name(ins: &Insert) -> String {
    match &ins.table {
        TableObject::TableName(name) => obj_name_to_str(name),
        _ => String::new(),
    }
}

pub(super) fn delete_table_name(del: &Delete) -> Option<String> {
    let tables = match &del.from {
        sqlparser::ast::FromTable::WithFromKeyword(t) | sqlparser::ast::FromTable::WithoutKeyword(t) => t,
    };
    tables.first().and_then(|twj| match &twj.relation {
        TableFactor::Table { name, .. } => Some(obj_name_to_str(name)),
        _ => None,
    })
}

pub(crate) fn parse_queries_with_config(dialect: &dyn Dialect, sql: &str, schema: &Schema, config: &ResolverConfig) -> anyhow::Result<Vec<Query>> {
    let blocks = split_into_blocks(sql);
    let queries = blocks
        .into_iter()
        .filter_map(|(ann, body)| {
            let body = body.trim().trim_end_matches(';').trim();
            match build_query_with_dialect(dialect, &ann, body, schema, config) {
                Ok(q) => Some(q),
                Err(e) => {
                    eprintln!("warning: cannot parse query {:?}: {e}", ann.name);
                    None
                },
            }
        })
        .collect();
    Ok(queries)
}

// ─── Block splitting ─────────────────────────────────────────────────────────

pub(super) struct QueryAnnotation {
    pub name: String,
    pub cmd: QueryCmd,
}

fn split_into_blocks(sql: &str) -> Vec<(QueryAnnotation, String)> {
    let mut blocks = Vec::new();
    let mut current: Option<QueryAnnotation> = None;
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

fn flush_block(current: &mut Option<QueryAnnotation>, lines: &mut Vec<&str>, out: &mut Vec<(QueryAnnotation, String)>) {
    if let Some(ann) = current.take() {
        let body = lines.join("\n");
        let body = body.trim().to_string();
        if !body.is_empty() {
            out.push((ann, body));
        }
    }
    lines.clear();
}

fn parse_annotation(line: &str) -> Option<QueryAnnotation> {
    let line = line.trim();
    // -- name: Foo :one
    let rest = line.strip_prefix("--")?.trim();
    let rest = rest.strip_prefix("name:")?.trim();
    let mut parts = rest.splitn(2, ':');
    let name = parts.next()?.trim().to_string();
    let cmd_str = parts.next()?.trim().to_lowercase();
    let cmd = match cmd_str.as_str() {
        "one" => QueryCmd::One,
        "many" => QueryCmd::Many,
        "exec" => QueryCmd::Exec,
        "execrows" => QueryCmd::ExecRows,
        _ => return None,
    };
    if name.is_empty() {
        return None;
    }
    Some(QueryAnnotation { name, cmd })
}

// ─── Query building ──────────────────────────────────────────────────────────

fn build_query_with_dialect(dialect: &dyn Dialect, ann: &QueryAnnotation, sql: &str, schema: &Schema, config: &ResolverConfig) -> anyhow::Result<Query> {
    let (sql_buf, np) = match named_params::preprocess_named_params(sql) {
        Some((rewritten, params)) => (rewritten, params),
        // No named params: still strip comment lines so that the stored SQL can be
        // safely collapsed to a single line in codegen (-- comments would eat the rest).
        None => (named_params::strip_sql_comment_lines(sql), vec![]),
    };
    let sql = sql_buf.as_str();

    let stmts = match Parser::parse_sql(dialect, sql) {
        Ok(s) if !s.is_empty() => s,
        _ => {
            let mut query = unresolved_query(ann, sql);
            named_params::apply_named_param_overrides(&mut query.params, &np);
            return Ok(query);
        },
    };

    let mut query = match &stmts[0] {
        Statement::Query(q) => build_select(ann, sql, q, schema, config),
        Statement::Insert(ins) => build_insert(ann, sql, ins, schema, config),
        Statement::Update(u) => build_update(ann, sql, u, schema, config),
        Statement::Delete(del) => build_delete(ann, sql, del, schema, config),
        _ => unresolved_query(ann, sql),
    };

    named_params::apply_named_param_overrides(&mut query.params, &np);
    Ok(query)
}

// ─── CTE parameter collection ────────────────────────────────────────────────

/// Collect typed parameter mappings from the bodies of all CTEs in `with`.
///
/// Walks UPDATE, DELETE, and SELECT CTE bodies using schema column types for
/// inference. INSERT CTE bodies are handled via `collect_insert_value_params`.
/// This ensures parameters defined inside data-modifying CTEs receive correct
/// types even when the outer query body provides no column context.
pub(super) fn collect_cte_params(with: Option<&With>, schema: &Schema, config: &ResolverConfig, mapping: &mut HashMap<usize, (String, SqlType, bool)>, query_name: &str) {
    let Some(with) = with else { return };
    let mut local_ctes: Vec<Table> = Vec::new();
    for cte in &with.cte_tables {
        // Recurse into nested WITH clauses before processing this CTE's body.
        collect_cte_params(cte.query.with.as_ref(), schema, config, mapping, query_name);
        match cte.query.body.as_ref() {
            SetExpr::Update(Statement::Update(u)) => {
                collect_update_params(&u.table, &u.assignments, u.selection.as_ref(), schema, config, mapping, query_name);
            },
            SetExpr::Delete(Statement::Delete(del)) => {
                collect_delete_where_params(del, schema, config, mapping, query_name);
            },
            SetExpr::Insert(Statement::Insert(ins)) => {
                collect_insert_value_params(ins, schema, mapping);
            },
            SetExpr::Select(select) => {
                let all_tables = collect_from_tables(select, schema, &local_ctes, config);
                if !all_tables.is_empty() {
                    let alias_map = build_alias_map(&all_tables);
                    let ctx = &mut ResolverContext { alias_map: &alias_map, all_tables: &all_tables, schema, config, mapping, query_name };
                    if let Some(expr) = &select.selection {
                        collect_params_from_expr(expr, ctx);
                    }
                    collect_join_params(select, ctx);
                    if let Some(expr) = &select.having {
                        collect_params_from_expr(expr, ctx);
                    }
                    collect_limit_offset_params(&cte.query, ctx.mapping);
                }
            },
            _ => {},
        }
        // Register this CTE's output shape so later CTEs can reference it.
        let cols = derived_cols(&cte.query, schema, &local_ctes, config);
        if !cols.is_empty() {
            local_ctes.push(Table { name: cte.alias.name.value.clone(), columns: cols });
        }
    }
}

// ─── Table collection ─────────────────────────────────────────────────────────

pub(super) fn collect_from_tables(select: &Select, schema: &Schema, ctes: &[Table], config: &ResolverConfig) -> Vec<(Table, Option<String>)> {
    let mut tables = Vec::new();
    for twj in &select.from {
        let base_idx = tables.len();
        collect_table_factor(&twj.relation, schema, ctes, &mut tables, config);
        for join in &twj.joins {
            let nulls_left = is_right_outer(&join.join_operator);
            let nulls_right = is_left_outer(&join.join_operator);

            // RIGHT or FULL OUTER: all previously collected tables from this
            // FROM item become nullable (rows may be absent on the left side).
            if nulls_left {
                for (t, _) in &mut tables[base_idx..] {
                    make_columns_nullable(t);
                }
            }

            collect_table_factor(&join.relation, schema, ctes, &mut tables, config);

            // LEFT or FULL OUTER: the just-added table becomes nullable
            // (rows may be absent on the right side).
            if nulls_right {
                if let Some((t, _)) = tables.last_mut() {
                    make_columns_nullable(t);
                }
            }
        }
    }
    tables
}

/// Returns true if the join makes the **left** side nullable (RIGHT / FULL OUTER).
fn is_right_outer(op: &JoinOperator) -> bool {
    matches!(op, JoinOperator::Right(_) | JoinOperator::RightOuter(_) | JoinOperator::FullOuter(_))
}

/// Returns true if the join makes the **right** side nullable (LEFT / FULL OUTER).
fn is_left_outer(op: &JoinOperator) -> bool {
    matches!(op, JoinOperator::Left(_) | JoinOperator::LeftOuter(_) | JoinOperator::FullOuter(_))
}

/// Mark all columns in a table as nullable.
fn make_columns_nullable(table: &mut Table) {
    for col in &mut table.columns {
        col.nullable = true;
    }
}

fn collect_table_factor(factor: &TableFactor, schema: &Schema, ctes: &[Table], out: &mut Vec<(Table, Option<String>)>, config: &ResolverConfig) {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let table_name = obj_name_to_str(name);
            let found = ctes.iter().find(|t| t.name == table_name).or_else(|| schema.tables.iter().find(|t| t.name == table_name));
            if let Some(t) = found {
                let alias_str = alias.as_ref().map(|a| ident_to_str(&a.name));
                out.push((t.clone(), alias_str));
            }
        },
        TableFactor::Derived { subquery, alias: Some(a), .. } => {
            let alias_name = ident_to_str(&a.name);
            let cols = derived_cols(subquery, schema, ctes, config);
            if !cols.is_empty() {
                out.push((Table { name: alias_name.clone(), columns: cols }, Some(alias_name)));
            }
        },
        _ => {},
    }
}

pub(super) fn build_alias_map(tables: &[(Table, Option<String>)]) -> HashMap<String, &Table> {
    let mut map = HashMap::new();
    for (table, alias) in tables {
        map.insert(table.name.clone(), table);
        if let Some(a) = alias {
            map.insert(a.clone(), table);
        }
    }
    map
}

// ─── Derived table columns ────────────────────────────────────────────────────

/// Convert RETURNING result columns to `Column` values (no primary-key flag).
fn returning_to_columns(returning: &[SelectItem], table: &Table, config: &ResolverConfig) -> Vec<Column> {
    resolve_returning(returning, table, config)
        .into_iter()
        .map(|rc| Column { name: rc.name, sql_type: rc.sql_type, nullable: rc.nullable, is_primary_key: false })
        .collect()
}

/// Resolve RETURNING columns for an INSERT CTE body.
fn returning_cols_for_insert(ins: &Insert, schema: &Schema, config: &ResolverConfig) -> Vec<Column> {
    let Some(table) = schema.tables.iter().find(|t| t.name == insert_table_name(ins)) else { return vec![] };
    let Some(returning) = &ins.returning else { return vec![] };
    returning_to_columns(returning, table, config)
}

/// Resolve RETURNING columns for an UPDATE CTE body.
fn returning_cols_for_update(u: &sqlparser::ast::Update, schema: &Schema, config: &ResolverConfig) -> Vec<Column> {
    let TableFactor::Table { name, .. } = &u.table.relation else { return vec![] };
    let Some(table) = schema.tables.iter().find(|t| t.name == obj_name_to_str(name)) else { return vec![] };
    let Some(returning) = &u.returning else { return vec![] };
    returning_to_columns(returning, table, config)
}

/// Resolve RETURNING columns for a DELETE CTE body.
fn returning_cols_for_delete(del: &Delete, schema: &Schema, config: &ResolverConfig) -> Vec<Column> {
    let Some(table_name) = delete_table_name(del) else { return vec![] };
    let Some(table) = schema.tables.iter().find(|t| t.name == table_name) else { return vec![] };
    let Some(returning) = &del.returning else { return vec![] };
    returning_to_columns(returning, table, config)
}

pub(super) fn derived_cols(subquery: &SqlQuery, schema: &Schema, ctes: &[Table], config: &ResolverConfig) -> Vec<Column> {
    // A CTE body may be INSERT … RETURNING or UPDATE … RETURNING (data-modifying CTE).
    // In those cases the CTE output is the RETURNING clause, not a SELECT projection.
    match subquery.body.as_ref() {
        SetExpr::Insert(Statement::Insert(ins)) => return returning_cols_for_insert(ins, schema, config),
        SetExpr::Update(Statement::Update(u)) => return returning_cols_for_update(u, schema, config),
        SetExpr::Delete(Statement::Delete(del)) => return returning_cols_for_delete(del, schema, config),
        SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_) => return vec![],
        _ => {},
    }

    let SetExpr::Select(select) = subquery.body.as_ref() else {
        return vec![];
    };

    let inner_tables = collect_from_tables(select, schema, ctes, config);
    if inner_tables.is_empty() {
        return vec![];
    }
    let alias_map = build_alias_map(&inner_tables);

    // Reuse resolve_projection and convert ResultColumn → Column (no PK flag for derived tables).
    resolve_projection(select, &alias_map, &inner_tables, config, schema)
        .into_iter()
        .map(|rc| Column { name: rc.name, sql_type: rc.sql_type, nullable: rc.nullable, is_primary_key: false })
        .collect()
}

pub(super) fn build_cte_scope(with: Option<&With>, schema: &Schema, config: &ResolverConfig) -> Vec<Table> {
    let Some(with) = with else { return vec![] };
    let mut ctes: Vec<Table> = Vec::new();
    for cte in &with.cte_tables {
        let cols = derived_cols(&cte.query, schema, &ctes, config);
        if !cols.is_empty() {
            ctes.push(Table { name: ident_to_str(&cte.alias.name), columns: cols });
        }
    }
    ctes
}

pub(super) fn build_params(mapping: HashMap<usize, (String, SqlType, bool)>, count: usize) -> Vec<Parameter> {
    // Track how many times each name has been used so we can deduplicate.
    // e.g. `price BETWEEN $1 AND $2` → both get name "price" from the column,
    // but we need "price" and "price_2" in the function signature.
    let mut name_counts: HashMap<String, usize> = HashMap::new();
    (1..=count)
        .map(|idx| match mapping.get(&idx) {
            Some((name, sql_type, nullable)) => {
                let count = name_counts.entry(name.clone()).or_insert(0);
                *count += 1;
                let unique_name = if *count == 1 { name.clone() } else { format!("{}_{}", name, count) };
                Parameter::scalar(idx, unique_name, sql_type.clone(), *nullable)
            },
            None => Parameter::scalar(idx, format!("param{idx}"), SqlType::Text, false),
        })
        .collect()
}

// ─── RETURNING ────────────────────────────────────────────────────────────────

pub(super) fn resolve_returning(items: &[SelectItem], table: &Table, config: &ResolverConfig) -> Vec<ResultColumn> {
    let all_tables = [(table.clone(), None)];
    let alias_map = build_alias_map(&all_tables);
    let mut result = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard(_) => {
                result.extend(table.columns.iter().map(col_to_result));
            },
            SelectItem::UnnamedExpr(expr) => {
                if let Some(rc) = resolve_expr(expr, &alias_map, &all_tables, config) {
                    result.push(rc);
                }
            },
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Some(rc) = resolve_expr(expr, &alias_map, &all_tables, config) {
                    result.push(ResultColumn { name: ident_to_str(alias), ..rc });
                }
            },
            _ => {},
        }
    }
    result
}

// ─── Utilities ────────────────────────────────────────────────────────────────

/// Build a fallback query with no type information for parameters or result columns.
///
/// Used when a query cannot be fully resolved against the schema (e.g. unsupported
/// syntax, unknown tables). The query still runs but parameter/result types default
/// to `SqlType::Text`.
pub(super) fn unresolved_query(ann: &QueryAnnotation, sql: &str) -> Query {
    Query::new(ann.name.clone(), ann.cmd.clone(), sql, build_params(HashMap::new(), count_params(sql)), vec![])
}

pub(super) fn count_params(sql: &str) -> usize {
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

pub(super) fn placeholder_idx(s: &str) -> Option<usize> {
    // $N (PostgreSQL) or ?N (SQLite)
    let rest = s.strip_prefix('$').or_else(|| s.strip_prefix('?'))?;
    rest.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::{Column, Schema, SqlType, Table};
    use sqlparser::dialect::PostgreSqlDialect;

    fn parse_queries(sql: &str, schema: &Schema) -> anyhow::Result<Vec<Query>> {
        parse_queries_with_config(&PostgreSqlDialect {}, sql, schema, &ResolverConfig::default())
    }

    fn make_schema() -> Schema {
        Schema {
            tables: vec![Table {
                name: "users".into(),
                columns: vec![
                    Column { name: "id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                    Column { name: "name".into(), sql_type: SqlType::Text, nullable: false, is_primary_key: false },
                    Column { name: "email".into(), sql_type: SqlType::Text, nullable: false, is_primary_key: false },
                    Column { name: "bio".into(), sql_type: SqlType::Text, nullable: true, is_primary_key: false },
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
                        Column { name: "id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                        Column { name: "name".into(), sql_type: SqlType::Text, nullable: false, is_primary_key: false },
                    ],
                },
                Table {
                    name: "posts".into(),
                    columns: vec![
                        Column { name: "id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                        Column { name: "user_id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: false },
                        Column { name: "title".into(), sql_type: SqlType::Text, nullable: false, is_primary_key: false },
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
    fn correlated_scalar_subquery_resolves_to_inner_column_type() {
        // (SELECT p.title FROM posts p WHERE p.user_id = u.id) AS post_title
        // The inner column `p.title` is TEXT; the result should be Option<Text>.
        // Before the fix, the generator fell back to Custom("expr") → serde_json::Value.
        let sql = "-- name: GetUserPost :one\n\
            SELECT u.name, \
                   (SELECT p.title FROM posts p WHERE p.user_id = u.id) AS post_title \
            FROM users u WHERE u.id = $1;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        let post_col = q.result_columns.iter().find(|c| c.name == "post_title").unwrap();
        assert_eq!(post_col.sql_type, SqlType::Text, "scalar subquery should resolve to inner column type");
        assert!(post_col.nullable, "scalar subquery is always nullable");
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

    // ─── Outer join nullability ──────────────────────────────────────────────

    #[test]
    fn left_join_makes_right_side_nullable() {
        // posts columns should become nullable because posts is on the right side of a LEFT JOIN
        let sql = "-- name: GetUserWithPost :one\n\
            SELECT u.id, u.name, p.id, p.title FROM users u LEFT JOIN posts p ON p.user_id = u.id WHERE u.id = $1;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        assert_eq!(q.result_columns.len(), 4);
        // Left side (users) keeps original nullability
        assert!(!q.result_columns[0].nullable, "users.id should remain non-nullable");
        assert!(!q.result_columns[1].nullable, "users.name should remain non-nullable");
        // Right side (posts) becomes nullable
        assert!(q.result_columns[2].nullable, "posts.id should become nullable in LEFT JOIN");
        assert!(q.result_columns[3].nullable, "posts.title should become nullable in LEFT JOIN");
    }

    #[test]
    fn right_join_makes_left_side_nullable() {
        // users columns should become nullable because users is on the left side of a RIGHT JOIN
        let sql = "-- name: GetPostWithUser :one\n\
            SELECT u.name, p.title FROM users u RIGHT JOIN posts p ON p.user_id = u.id WHERE p.id = $1;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        assert_eq!(q.result_columns.len(), 2);
        // Left side (users) becomes nullable
        assert!(q.result_columns[0].nullable, "users.name should become nullable in RIGHT JOIN");
        // Right side (posts) keeps original nullability
        assert!(!q.result_columns[1].nullable, "posts.title should remain non-nullable");
    }

    #[test]
    fn full_outer_join_makes_both_sides_nullable() {
        let sql = "-- name: AllUsersAndPosts :many\n\
            SELECT u.name, p.title FROM users u FULL OUTER JOIN posts p ON p.user_id = u.id;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        assert_eq!(q.result_columns.len(), 2);
        assert!(q.result_columns[0].nullable, "users.name should become nullable in FULL OUTER JOIN");
        assert!(q.result_columns[1].nullable, "posts.title should become nullable in FULL OUTER JOIN");
    }

    #[test]
    fn inner_join_preserves_nullability() {
        // INNER JOIN should not change nullability — both sides must match
        let sql = "-- name: GetUserPost :one\n\
            SELECT u.id, u.name, p.title FROM users u INNER JOIN posts p ON p.user_id = u.id WHERE u.id = $1;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        assert!(!q.result_columns[0].nullable, "users.id should remain non-nullable in INNER JOIN");
        assert!(!q.result_columns[1].nullable, "users.name should remain non-nullable in INNER JOIN");
        assert!(!q.result_columns[2].nullable, "posts.title should remain non-nullable in INNER JOIN");
    }

    #[test]
    fn left_join_wildcard_makes_right_side_nullable() {
        // SELECT * with LEFT JOIN — right-side columns become nullable
        let sql = "-- name: AllUserPosts :many\n\
            SELECT * FROM users u LEFT JOIN posts p ON p.user_id = u.id;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        // users(2) + posts(3) = 5
        assert_eq!(q.result_columns.len(), 5);
        // users columns (first 2) stay non-nullable
        assert!(!q.result_columns[0].nullable, "users.id should remain non-nullable");
        assert!(!q.result_columns[1].nullable, "users.name should remain non-nullable");
        // posts columns (last 3) become nullable
        assert!(q.result_columns[2].nullable, "posts.id should become nullable");
        assert!(q.result_columns[3].nullable, "posts.user_id should become nullable");
        assert!(q.result_columns[4].nullable, "posts.title should become nullable");
    }

    #[test]
    fn left_join_unqualified_column_from_right_becomes_nullable() {
        // Unqualified column that resolves to the outer-joined table
        let sql = "-- name: GetUserTitle :one\n\
            SELECT u.name, title FROM users u LEFT JOIN posts p ON p.user_id = u.id WHERE u.id = $1;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        assert_eq!(q.result_columns.len(), 2);
        assert!(!q.result_columns[0].nullable, "users.name should remain non-nullable");
        assert!(q.result_columns[1].nullable, "title (from posts via LEFT JOIN) should become nullable");
    }

    #[test]
    fn left_join_derived_subquery_becomes_nullable() {
        // LEFT OUTER JOIN on a derived table (subquery) — the subquery's
        // columns should become nullable just like a regular table would.
        let sql = "-- name: GetUserPosts :many\n\
            SELECT t1.id, t1.name, t2.id, t2.title \
            FROM users t1 LEFT OUTER JOIN (SELECT id, title, user_id FROM posts) t2 ON t1.id = t2.user_id;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        assert_eq!(q.result_columns.len(), 4);
        // Left side (users) keeps original nullability
        assert!(!q.result_columns[0].nullable, "users.id should remain non-nullable");
        assert!(!q.result_columns[1].nullable, "users.name should remain non-nullable");
        // Right side (derived subquery from posts) becomes nullable
        assert!(q.result_columns[2].nullable, "derived posts.id should become nullable in LEFT OUTER JOIN");
        assert!(q.result_columns[3].nullable, "derived posts.title should become nullable in LEFT OUTER JOIN");
    }

    // ─── Derived-table tests (JOIN (SELECT …) alias) ─────────────────────────

    #[test]
    fn derived_table_join_resolves_column() {
        // b.user_id comes from the derived table — should resolve to BigInt
        let sql = "-- name: GetPosts :many\n\
            SELECT a.id, b.user_id \
            FROM users a JOIN (SELECT user_id FROM posts) b ON a.id = b.user_id;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "user_id"]);
        assert_eq!(q.result_columns[1].sql_type, SqlType::BigInt);
    }

    #[test]
    fn derived_table_column_alias_renames() {
        // title AS post_title in derived SELECT → outer sees b.post_title : Text
        let sql = "-- name: GetPosts :many\n\
            SELECT a.name, b.post_title \
            FROM users a JOIN (SELECT title AS post_title FROM posts) b ON a.id = b.user_id;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["name", "post_title"]);
        assert_eq!(q.result_columns[1].sql_type, SqlType::Text);
    }

    #[test]
    fn derived_table_star_expands() {
        // b.* should expand to the columns declared in the derived SELECT
        let sql = "-- name: GetAll :many\n\
            SELECT a.name, b.* \
            FROM users a JOIN (SELECT id, title FROM posts) b ON a.id = b.user_id;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["name", "id", "title"]);
    }

    #[test]
    fn derived_table_count_star_resolves_to_bigint() {
        // COUNT(*) AS cnt — resolves to BigInt
        let sql = "-- name: GetCounts :many\n\
            SELECT a.name, b.cnt \
            FROM users a \
            JOIN (SELECT user_id, COUNT(*) AS cnt FROM posts GROUP BY user_id) b \
            ON a.id = b.user_id;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        let cnt = q.result_columns.iter().find(|c| c.name == "cnt");
        assert!(cnt.is_some());
        assert_eq!(cnt.unwrap().sql_type, SqlType::BigInt);
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

    // ─── CTE (WITH) tests ─────────────────────────────────────────────────────

    #[test]
    fn cte_basic_resolves_columns() {
        let sql = "-- name: GetRecentPosts :many\n\
            WITH recent AS (SELECT id, title FROM posts)\n\
            SELECT id, title FROM recent;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "title"]);
        assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
        assert_eq!(q.result_columns[1].sql_type, SqlType::Text);
    }

    #[test]
    fn cte_param_in_outer_where() {
        // $1 is in the outer WHERE, bound to a column from the CTE
        let sql = "-- name: GetUserPosts :many\n\
            WITH uposts AS (SELECT id, user_id, title FROM posts)\n\
            SELECT id, title FROM uposts WHERE user_id = $1;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].name, "user_id");
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    }

    #[test]
    fn cte_chained() {
        // Second CTE references the first CTE
        let sql = "-- name: GetTitles :many\n\
            WITH base AS (SELECT id, title FROM posts),\n\
                 titled AS (SELECT title FROM base)\n\
            SELECT title FROM titled;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["title"]);
        assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
    }

    #[test]
    fn cte_joined_with_schema_table() {
        // CTE is JOINed with a real schema table
        let sql = "-- name: GetUserPostTitles :many\n\
            WITH uposts AS (SELECT user_id, title FROM posts)\n\
            SELECT u.name, p.title FROM users u JOIN uposts p ON p.user_id = u.id;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["name", "title"]);
        assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
        assert_eq!(q.result_columns[1].sql_type, SqlType::Text);
    }

    // ─── CTE DML tests ────────────────────────────────────────────────────────

    fn make_inventory_schema() -> Schema {
        Schema {
            tables: vec![Table {
                name: "inventory".into(),
                columns: vec![
                    Column { name: "sku".into(), sql_type: SqlType::Text, nullable: false, is_primary_key: true },
                    Column { name: "qty".into(), sql_type: SqlType::Integer, nullable: false, is_primary_key: false },
                ],
            }],
        }
    }

    #[test]
    fn cte_update_body_params_are_typed_from_schema() {
        // WITH up AS (UPDATE … SET qty=$1 WHERE sku=$2) INSERT …
        // $1 and $2 should be typed from the UPDATE CTE body, not fallback Text.
        let sql = "-- name: UpsertStock :one\n\
            WITH up AS ( \
                UPDATE inventory SET qty = $1 WHERE sku = $2 RETURNING sku, qty \
            ) \
            INSERT INTO inventory (sku, qty) SELECT $2, $1 \
            WHERE NOT EXISTS (SELECT 1 FROM up) \
            RETURNING sku, qty;";
        let q = &parse_queries(sql, &make_inventory_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 2);
        // $1 = qty, $2 = sku (first-appearance order from named-param rewrite / schema)
        let qty_param = q.params.iter().find(|p| p.index == 1).unwrap();
        let sku_param = q.params.iter().find(|p| p.index == 2).unwrap();
        assert_eq!(qty_param.sql_type, SqlType::Integer, "$1 should be qty (Integer)");
        assert_eq!(sku_param.sql_type, SqlType::Text, "$2 should be sku (Text)");
    }

    #[test]
    fn cte_update_body_result_columns_from_insert_returning() {
        // RETURNING on the outer INSERT should produce typed result columns.
        let sql = "-- name: UpsertStock :one\n\
            WITH up AS ( \
                UPDATE inventory SET qty = $1 WHERE sku = $2 RETURNING sku, qty \
            ) \
            INSERT INTO inventory (sku, qty) SELECT $2, $1 \
            WHERE NOT EXISTS (SELECT 1 FROM up) \
            RETURNING sku, qty;";
        let q = &parse_queries(sql, &make_inventory_schema()).unwrap()[0];
        assert_eq!(q.result_columns.len(), 2);
        assert_eq!(q.result_columns[0].name, "sku");
        assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
        assert_eq!(q.result_columns[1].name, "qty");
        assert_eq!(q.result_columns[1].sql_type, SqlType::Integer);
    }

    #[test]
    fn cte_insert_returning_columns_flow_to_outer_select() {
        // WITH inserted AS (INSERT … RETURNING …) SELECT * FROM inserted
        // The outer SELECT * should expand to the RETURNING columns.
        let sql = "-- name: CreateUser :one\n\
            WITH ins AS (\
                INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, name, email\
            )\
            SELECT * FROM ins;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.result_columns.len(), 3, "should have id, name, email from RETURNING");
        let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "name", "email"]);
        assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
        assert_eq!(q.result_columns[1].sql_type, SqlType::Text);
    }

    #[test]
    fn comparison_operator_infers_param_type() {
        // col < $1 should produce the same type inference as col = $1
        let sql = "-- name: GetRecentUsers :many\n\
            SELECT id, name FROM users WHERE id < $1;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    }

    #[test]
    fn strips_trailing_semicolons() {
        let sql = "-- name: ListUsers :many\nSELECT id, name FROM users;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert!(!q.sql.ends_with(';'));
    }

    // ─── RETURNING tests ──────────────────────────────────────────────────────

    #[test]
    fn insert_returning_star() {
        let sql = "-- name: CreateUser :one\n\
            INSERT INTO users (name, email) VALUES ($1, $2) RETURNING *;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 2);
        assert_eq!(q.params[0].name, "name");
        assert_eq!(q.params[0].sql_type, SqlType::Text);
        assert_eq!(q.params[1].name, "email");
        assert_eq!(q.params[1].sql_type, SqlType::Text);
        assert_eq!(q.result_columns.len(), 4);
        let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "name", "email", "bio"]);
    }

    #[test]
    fn insert_returning_columns() {
        let sql = "-- name: CreateUser :one\n\
            INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id, name;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.result_columns.len(), 2);
        assert_eq!(q.result_columns[0].name, "id");
        assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
        assert_eq!(q.result_columns[1].name, "name");
        assert_eq!(q.result_columns[1].sql_type, SqlType::Text);
    }

    #[test]
    fn update_returning_star() {
        let sql = "-- name: UpdateUser :one\n\
            UPDATE users SET name = $1 WHERE id = $2 RETURNING *;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 2);
        assert_eq!(q.params[0].name, "name");
        assert_eq!(q.params[1].name, "id");
        assert_eq!(q.result_columns.len(), 4);
    }

    #[test]
    fn update_returning_columns() {
        let sql = "-- name: UpdateUser :one\n\
            UPDATE users SET name = $1, email = $2 WHERE id = $3 RETURNING id, name, email;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.result_columns.len(), 3);
        let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "name", "email"]);
    }

    #[test]
    fn delete_returning_star() {
        let sql = "-- name: DeleteUser :one\n\
            DELETE FROM users WHERE id = $1 RETURNING *;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].name, "id");
        assert_eq!(q.result_columns.len(), 4);
        let names: Vec<_> = q.result_columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "name", "email", "bio"]);
    }

    #[test]
    fn delete_returning_columns() {
        let sql = "-- name: DeleteUser :one\n\
            DELETE FROM users WHERE id = $1 RETURNING id, name;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.result_columns.len(), 2);
        assert_eq!(q.result_columns[0].name, "id");
        assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
        assert_eq!(q.result_columns[1].name, "name");
        assert_eq!(q.result_columns[1].sql_type, SqlType::Text);
    }

    // ─── Named param integration tests ────────────────────────────────────────

    #[test]
    fn test_named_param_select_type_inferred() {
        let sql = "-- name: GetUser :one\nSELECT id, name FROM users WHERE id = @user_id;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].name, "user_id");
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
        assert_eq!(q.params[0].nullable, false);
        assert!(q.sql.contains("$1"));
        assert!(!q.sql.contains("@user_id"));
    }

    #[test]
    fn test_named_param_repeated_becomes_one_param() {
        let sql = "-- name: Test :exec\nUPDATE users SET name = @name WHERE name = @name;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].name, "name");
        assert_eq!(q.sql.matches("$1").count(), 2);
    }

    #[test]
    fn test_named_param_annotation_forces_nullable() {
        let sql = "-- name: Test :many\n-- @bio null\nSELECT id FROM users WHERE bio = @bio;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params[0].name, "bio");
        assert_eq!(q.params[0].nullable, true);
    }

    #[test]
    fn test_named_param_annotation_forces_type_and_not_null() {
        let sql = "-- name: Test :many\n-- @bio text not null\nSELECT id FROM users WHERE bio = @bio;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::Text);
        assert_eq!(q.params[0].nullable, false);
    }

    #[test]
    fn test_named_param_update() {
        let sql = "-- name: UpdateUser :exec\nUPDATE users SET name = @name WHERE id = @user_id;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 2);
        assert_eq!(q.params[0].name, "name");
        assert_eq!(q.params[0].sql_type, SqlType::Text);
        assert_eq!(q.params[1].name, "user_id");
        assert_eq!(q.params[1].sql_type, SqlType::BigInt);
    }

    // ─── Parameter resolution in non-WHERE clauses ─────────────────────────

    #[test]
    fn param_in_join_on_clause_is_typed() {
        // $1 in JOIN ON should be typed from the column it's compared to
        let sql = "-- name: GetPostsByUser :many\n\
            SELECT p.id, p.title FROM posts p JOIN users u ON u.id = p.user_id AND u.id = $1;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].name, "id");
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    }

    #[test]
    fn param_in_in_list_is_typed() {
        let sql = "-- name: GetUsers :many\n\
            SELECT id, name FROM users WHERE id IN ($1, $2, $3);";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 3);
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
        assert_eq!(q.params[1].sql_type, SqlType::BigInt);
        assert_eq!(q.params[2].sql_type, SqlType::BigInt);
    }

    #[test]
    fn param_in_between_is_typed() {
        let sql = "-- name: GetUsers :many\n\
            SELECT id, name FROM users WHERE id BETWEEN $1 AND $2;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 2);
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
        assert_eq!(q.params[1].sql_type, SqlType::BigInt);
    }

    #[test]
    fn param_in_like_is_typed() {
        let sql = "-- name: SearchUsers :many\n\
            SELECT id, name FROM users WHERE name LIKE $1;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].sql_type, SqlType::Text);
    }

    #[test]
    fn param_in_case_when_is_typed() {
        let sql = "-- name: GetUsers :many\n\
            SELECT id, CASE WHEN id = $1 THEN 'match' ELSE 'no' END AS label FROM users;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    }

    #[test]
    fn param_in_case_when_then_is_collected() {
        // $1 in THEN branch — no column context, but should at least be collected
        let sql = "-- name: GetUsers :many\n\
            SELECT id, CASE WHEN id > 0 THEN $1 ELSE name END AS label FROM users;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
    }

    #[test]
    fn param_in_coalesce_is_recursed() {
        // WHERE COALESCE(bio, $1) — the function body should be recursed into
        // so $1 is at least found (even without direct column type inference)
        let sql = "-- name: GetUsers :many\n\
            SELECT id FROM users WHERE COALESCE(bio, $1) = 'default';";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        // $1 is inside a function arg — no adjacent column context, but should still be found
    }

    #[test]
    fn param_in_where_function_arg_is_found() {
        // WHERE id = ABS($1) — $1 is inside a function; should be recursed into
        // so the param is at least found by count_params even though typing
        // can't infer through the function boundary
        let sql = "-- name: GetUser :one\n\
            SELECT id, name FROM users WHERE id = ABS($1);";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        // Ideally this would be BigInt (from id), but the function wrapping
        // prevents direct column inference — falls back to Text
    }

    #[test]
    fn param_in_having_clause_is_typed() {
        let schema = make_join_schema();
        let sql = "-- name: GetActiveUsers :many\n\
            SELECT u.id, u.name FROM users u JOIN posts p ON p.user_id = u.id \
            GROUP BY u.id, u.name \
            HAVING COUNT(*) > $1;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        // COUNT(*) > $1 — the param is compared to a count (BigInt)
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    }

    #[test]
    fn param_in_limit_is_typed() {
        let sql = "-- name: ListUsers :many\n\
            SELECT id, name FROM users LIMIT $1;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        // LIMIT should produce BigInt (or Integer)
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    }

    #[test]
    fn param_in_offset_is_typed() {
        let sql = "-- name: ListUsers :many\n\
            SELECT id, name FROM users LIMIT $1 OFFSET $2;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.params.len(), 2);
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
        assert_eq!(q.params[1].sql_type, SqlType::BigInt);
    }

    #[test]
    fn execrows_cte_with_params_keeps_method_params_when_type_inference_fails() {
        let sql = "-- name: ArchiveOldSessions :execrows\n\
            with moved as (\n\
              delete from sessions\n\
              where created_at < @cutoff\n\
                and (@tenant_id = -1 or tenant_id = @tenant_id)\n\
              returning id, tenant_id\n\
            )\n\
            update tenants\n\
            set active_sessions = active_sessions - 1\n\
            from moved\n\
            where tenants.id = moved.tenant_id;";

        let schema = Schema {
            tables: vec![
                Table {
                    name: "sessions".into(),
                    columns: vec![
                        Column { name: "id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                        Column { name: "tenant_id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: false },
                        Column { name: "created_at".into(), sql_type: SqlType::Timestamp, nullable: false, is_primary_key: false },
                    ],
                },
                Table {
                    name: "tenants".into(),
                    columns: vec![
                        Column { name: "id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                        Column { name: "active_sessions".into(), sql_type: SqlType::Integer, nullable: false, is_primary_key: false },
                    ],
                },
            ],
        };

        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.cmd, QueryCmd::ExecRows);
        assert_eq!(q.params.len(), 2);
        assert_eq!(q.params[0].name, "cutoff");
        assert_eq!(q.params[0].sql_type, SqlType::Timestamp, "cutoff should be typed from sessions.created_at");
        assert_eq!(q.params[1].name, "tenant_id");
        assert_eq!(q.params[1].sql_type, SqlType::BigInt, "tenant_id should be typed from sessions.tenant_id");
        assert_eq!(q.sql.matches("$1").count(), 1);
        assert_eq!(q.sql.matches("$2").count(), 2);
    }

    #[test]
    fn param_dedup_between() {
        let schema = make_schema();
        let sql = "-- name: GetByIdRange :many\nSELECT * FROM users WHERE id BETWEEN $1 AND $2;";
        let queries = parse_queries(sql, &schema).unwrap();
        let q = &queries[0];
        assert_eq!(q.params.len(), 2);
        assert_eq!(q.params[0].name, "id");
        assert_eq!(q.params[1].name, "id_2");
    }

    #[test]
    fn param_dedup_in_list() {
        let schema = make_schema();
        let sql = "-- name: GetByNames :many\nSELECT * FROM users WHERE name IN ($1, $2, $3);";
        let queries = parse_queries(sql, &schema).unwrap();
        let q = &queries[0];
        assert_eq!(q.params.len(), 3);
        assert_eq!(q.params[0].name, "name");
        assert_eq!(q.params[1].name, "name_2");
        assert_eq!(q.params[2].name, "name_3");
    }

    #[test]
    fn param_dedup_same_column_or() {
        let schema = make_schema();
        let sql = "-- name: GetByIdOr :many\nSELECT * FROM users WHERE id = $1 OR id = $2;";
        let queries = parse_queries(sql, &schema).unwrap();
        let q = &queries[0];
        assert_eq!(q.params.len(), 2);
        assert_eq!(q.params[0].name, "id");
        assert_eq!(q.params[1].name, "id_2");
    }

    #[test]
    fn param_dedup_different_columns_no_suffix() {
        let schema = make_schema();
        let sql = "-- name: GetByIdAndName :many\nSELECT * FROM users WHERE id = $1 AND name = $2;";
        let queries = parse_queries(sql, &schema).unwrap();
        let q = &queries[0];
        assert_eq!(q.params.len(), 2);
        assert_eq!(q.params[0].name, "id");
        assert_eq!(q.params[1].name, "name");
    }

    #[test]
    fn repeated_param_same_index_different_columns() {
        // WHERE col_a = $1 OR col_b = $1 — same param index used with two columns.
        // The param should get one name (from first resolution) and no dedup suffix.
        let schema = make_schema();
        let sql = "-- name: SearchByIdOrName :many\nSELECT * FROM users WHERE id = $1 OR name = $1;";
        let queries = parse_queries(sql, &schema).unwrap();
        let q = &queries[0];
        assert_eq!(q.params.len(), 1);
        // First resolution wins — id is encountered first
        assert_eq!(q.params[0].name, "id");
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    }

    // ─── ORDER BY param inference tests ──────────────────────────────────────

    #[test]
    fn param_in_order_by_case_expr() {
        // ORDER BY CASE WHEN id = $1 THEN 0 ELSE 1 END — $1 should be BigInt, not Text
        let schema = make_schema();
        let sql = "-- name: ListUsersOrderByParam :many\nSELECT id, name FROM users ORDER BY CASE WHEN id = $1 THEN 0 ELSE 1 END, id;";
        let queries = parse_queries(sql, &schema).unwrap();
        let q = &queries[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].name, "id");
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    }

    #[test]
    fn param_in_order_by_simple_comparison() {
        // ORDER BY (name = $1) DESC — $1 should be Text
        let schema = make_schema();
        let sql = "-- name: ListUsersNameFirst :many\nSELECT id, name FROM users ORDER BY name = $1 DESC;";
        let queries = parse_queries(sql, &schema).unwrap();
        let q = &queries[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].name, "name");
        assert_eq!(q.params[0].sql_type, SqlType::Text);
    }

    // ─── Set operation (UNION/INTERSECT/EXCEPT) tests ────────────────────────

    #[test]
    fn union_all_produces_typed_result_columns() {
        let schema = make_schema();
        let sql = "-- name: UnionAll :many\n\
            SELECT id, name FROM users WHERE id = $1\n\
            UNION ALL\n\
            SELECT id, name FROM users WHERE id = $2;";
        let queries = parse_queries(sql, &schema).unwrap();
        let q = &queries[0];
        // Result columns come from the left branch
        assert_eq!(q.result_columns.len(), 2);
        assert_eq!(q.result_columns[0].name, "id");
        assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
        assert_eq!(q.result_columns[1].name, "name");
        assert_eq!(q.result_columns[1].sql_type, SqlType::Text);
        // Params from both branches
        assert_eq!(q.params.len(), 2);
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
        assert_eq!(q.params[1].sql_type, SqlType::BigInt);
    }

    #[test]
    fn union_distinct_produces_typed_result_columns() {
        let schema = make_schema();
        let sql = "-- name: UnionDistinct :many\n\
            SELECT id, name FROM users WHERE name = $1\n\
            UNION\n\
            SELECT id, name FROM users WHERE name = $2;";
        let queries = parse_queries(sql, &schema).unwrap();
        let q = &queries[0];
        assert_eq!(q.result_columns.len(), 2);
        assert_eq!(q.result_columns[0].name, "id");
        assert_eq!(q.result_columns[1].name, "name");
        assert_eq!(q.params.len(), 2);
        assert_eq!(q.params[0].sql_type, SqlType::Text);
        assert_eq!(q.params[1].sql_type, SqlType::Text);
    }

    #[test]
    fn intersect_produces_typed_result_columns() {
        let schema = make_schema();
        let sql = "-- name: Intersect :many\n\
            SELECT id, name FROM users WHERE id = $1\n\
            INTERSECT\n\
            SELECT id, name FROM users WHERE id = $2;";
        let queries = parse_queries(sql, &schema).unwrap();
        let q = &queries[0];
        assert_eq!(q.result_columns.len(), 2);
        assert_eq!(q.params.len(), 2);
    }

    #[test]
    fn except_produces_typed_result_columns() {
        let schema = make_schema();
        let sql = "-- name: Except :many\n\
            SELECT id, name FROM users WHERE id = $1\n\
            EXCEPT\n\
            SELECT id, name FROM users WHERE id = $2;";
        let queries = parse_queries(sql, &schema).unwrap();
        let q = &queries[0];
        assert_eq!(q.result_columns.len(), 2);
        assert_eq!(q.params.len(), 2);
    }

    #[test]
    fn triple_union_all_collects_all_params() {
        // Three branches chained: UNION ALL of UNION ALL
        let schema = make_schema();
        let sql = "-- name: TripleUnion :many\n\
            SELECT id, name FROM users WHERE id = $1\n\
            UNION ALL\n\
            SELECT id, name FROM users WHERE id = $2\n\
            UNION ALL\n\
            SELECT id, name FROM users WHERE id = $3;";
        let queries = parse_queries(sql, &schema).unwrap();
        let q = &queries[0];
        assert_eq!(q.result_columns.len(), 2);
        assert_eq!(q.params.len(), 3);
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
        assert_eq!(q.params[1].sql_type, SqlType::BigInt);
        assert_eq!(q.params[2].sql_type, SqlType::BigInt);
    }

    #[test]
    fn union_all_with_join_infers_params() {
        let schema = make_join_schema();
        let sql = "-- name: UnionJoin :many\n\
            SELECT u.id, u.name FROM users u JOIN posts p ON p.user_id = u.id WHERE p.id = $1\n\
            UNION ALL\n\
            SELECT u.id, u.name FROM users u WHERE u.id = $2;";
        let queries = parse_queries(sql, &schema).unwrap();
        let q = &queries[0];
        assert_eq!(q.result_columns.len(), 2);
        assert_eq!(q.params.len(), 2);
        assert_eq!(q.params[0].name, "id");
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
        // Second param also resolves to "id" column, gets dedup suffix
        assert_eq!(q.params[1].name, "id_2");
        assert_eq!(q.params[1].sql_type, SqlType::BigInt);
    }

    #[test]
    fn union_all_no_params_still_typed() {
        let schema = make_schema();
        let sql = "-- name: UnionNoParams :many\n\
            SELECT id, name FROM users\n\
            UNION ALL\n\
            SELECT id, name FROM users;";
        let queries = parse_queries(sql, &schema).unwrap();
        let q = &queries[0];
        assert_eq!(q.result_columns.len(), 2);
        assert_eq!(q.result_columns[0].name, "id");
        assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
        assert_eq!(q.params.len(), 0);
    }

    // ─── Expression type inference ──────────────────────────────────────

    #[test]
    fn expr_integer_literal() {
        let schema = make_schema();
        let sql = "-- name: GetOne :one\nSELECT 1 AS n FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns.len(), 1);
        assert_eq!(q.result_columns[0].name, "n");
        assert_eq!(q.result_columns[0].sql_type, SqlType::Integer);
        assert!(!q.result_columns[0].nullable);
    }

    #[test]
    fn expr_bigint_literal() {
        let schema = make_schema();
        let sql = "-- name: GetBig :one\nSELECT 9999999999 AS n FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
    }

    #[test]
    fn expr_float_literal() {
        let schema = make_schema();
        let sql = "-- name: GetPi :one\nSELECT 3.14 AS n FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::Double);
    }

    #[test]
    fn expr_string_literal() {
        let schema = make_schema();
        let sql = "-- name: GetHello :one\nSELECT 'hello' AS s FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
        assert!(!q.result_columns[0].nullable);
    }

    #[test]
    fn expr_boolean_literal() {
        let schema = make_schema();
        let sql = "-- name: GetBool :one\nSELECT true AS b FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::Boolean);
    }

    #[test]
    fn expr_null_literal() {
        let schema = make_schema();
        let sql = "-- name: GetNull :one\nSELECT NULL AS n FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
        assert!(q.result_columns[0].nullable);
    }

    #[test]
    fn expr_arithmetic_literals() {
        let schema = make_schema();
        let sql = "-- name: Calc :one\nSELECT 1 + 2 AS sum FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::Integer);
    }

    #[test]
    fn expr_arithmetic_promotes_to_wider() {
        let schema = make_schema();
        let sql = "-- name: Calc :one\nSELECT id + 1 AS result FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        // id is BigInt, 1 is Integer → BigInt (wider)
        assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
    }

    #[test]
    fn expr_string_concat() {
        let schema = make_schema();
        let sql = "-- name: Concat :one\nSELECT name || ' ' || email AS full FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
    }

    #[test]
    fn expr_comparison_returns_boolean() {
        let schema = make_schema();
        let sql = "-- name: Check :one\nSELECT id > 5 AS is_high FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::Boolean);
    }

    #[test]
    fn expr_cast_to_text() {
        let schema = make_schema();
        let sql = "-- name: Str :one\nSELECT CAST(id AS TEXT) AS s FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
        assert!(!q.result_columns[0].nullable, "id is not nullable, so CAST should preserve that");
    }

    #[test]
    fn expr_cast_to_integer() {
        let schema = make_schema();
        let sql = "-- name: Num :one\nSELECT CAST(name AS INTEGER) AS n FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::Integer);
    }

    #[test]
    fn param_cast_infers_type() {
        let schema = make_schema();
        // $1::bigint in the WHERE clause — no column context, cast is the only signal.
        let sql = "-- name: Q :exec\nSELECT id FROM users WHERE $1::bigint > 0;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    }

    #[test]
    fn param_cast_same_type_idempotent() {
        let schema = make_schema();
        // $1::text appears twice — should still produce a single Text param without conflict.
        let sql = "-- name: Q :exec\nSELECT id FROM users WHERE $1::text IS NOT NULL AND $1::text != '';";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].sql_type, SqlType::Text);
    }

    #[test]
    fn param_cast_conflicting_types_falls_back_to_custom() {
        let schema = make_schema();
        // $1 cast to both text and bigint — conflict → Custom("unknown").
        let sql = "-- name: ConflictQ :exec\nSELECT id FROM users WHERE $1::text IS NOT NULL AND $1::bigint > 0;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::Custom("unknown".into()));
    }

    #[test]
    fn param_cast_annotation_resolves_conflict() {
        let schema = make_schema();
        // @id is cast to both text and bigint, but the annotation declares it as bigint.
        let sql = "-- name: AnnotatedQ :exec\n-- @id bigint\nSELECT id FROM users WHERE @id::text IS NOT NULL AND @id::bigint > 0;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::BigInt);
    }

    // ── cast vs. column-comparison interactions ───────────────────────────────

    #[test]
    fn param_cast_overrides_column_comparison_type() {
        let schema = make_schema();
        // id is BigInt, but the developer explicitly casts $1 to Text.
        // The cast is the authoritative signal — Text wins.
        let sql = "-- name: Q :exec\nSELECT id FROM users WHERE id = $1::text;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::Text);
    }

    #[test]
    fn cast_on_column_side_infers_param_type() {
        let schema = make_schema();
        // CAST(id AS TEXT) = $1 — the column cast resolves to Text, so $1 gets Text.
        let sql = "-- name: Q :exec\nSELECT id FROM users WHERE CAST(id AS TEXT) = $1;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::Text);
    }

    #[test]
    fn pg_cast_on_column_side_infers_param_type() {
        let schema = make_schema();
        // id::text = $1 — Postgres cast on the column side resolves to Text.
        let sql = "-- name: Q :exec\nSELECT id FROM users WHERE id::text = $1;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::Text);
    }

    #[test]
    fn param_column_comparison_then_conflicting_cast_warns_and_falls_back() {
        let schema = make_schema();
        // id = $1 gives BigInt; $1::text adds a conflicting Text cast → Custom("unknown").
        let sql = "-- name: Q :exec\nSELECT id FROM users WHERE id = $1 AND $1::text IS NOT NULL;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::Custom("unknown".into()));
    }

    #[test]
    fn param_field_comparison_no_cast_infers_type() {
        let schema = make_schema();
        // Plain comparison — no cast anywhere. Type comes entirely from the column.
        let sql = "-- name: Q :one\nSELECT id FROM users WHERE name = $1;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params[0].name, "name");
        assert_eq!(q.params[0].sql_type, SqlType::Text);
    }

    #[test]
    fn param_field_comparison_two_columns_same_type() {
        let schema = make_schema();
        // $1 compared to two Text columns — both agree, so Text (first-wins, no conflict).
        let sql = "-- name: Q :exec\nSELECT id FROM users WHERE name = $1 OR email = $1;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params.len(), 1);
        assert_eq!(q.params[0].sql_type, SqlType::Text);
    }

    #[test]
    fn expr_case_when_then_column() {
        let schema = make_schema();
        let sql = "-- name: Label :one\nSELECT CASE WHEN id > 5 THEN name ELSE 'unknown' END AS label FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
        // ELSE is present and not nullable → not nullable
        assert!(!q.result_columns[0].nullable);
    }

    #[test]
    fn expr_case_without_else_is_nullable() {
        let schema = make_schema();
        let sql = "-- name: Label :one\nSELECT CASE WHEN id > 5 THEN name END AS label FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
        assert!(q.result_columns[0].nullable, "CASE without ELSE is nullable");
    }

    #[test]
    fn expr_coalesce_non_nullable_first() {
        let schema = make_schema();
        let sql = "-- name: CoalName :one\nSELECT COALESCE(name, 'fallback') AS n FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
        // name is not nullable → COALESCE is not nullable
        assert!(!q.result_columns[0].nullable);
    }

    #[test]
    fn expr_coalesce_all_nullable() {
        let schema = make_schema();
        let sql = "-- name: CoalBio :one\nSELECT COALESCE(bio, NULL) AS b FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
        assert!(q.result_columns[0].nullable, "all args nullable → result nullable");
    }

    #[test]
    fn expr_upper_lower_return_text() {
        let schema = make_schema();
        let sql = "-- name: Upper :one\nSELECT UPPER(name) AS u, LOWER(email) AS l FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns.len(), 2);
        assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
        assert_eq!(q.result_columns[1].sql_type, SqlType::Text);
    }

    #[test]
    fn expr_length_returns_integer() {
        let schema = make_schema();
        let sql = "-- name: Len :one\nSELECT LENGTH(name) AS len FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::Integer);
    }

    #[test]
    fn expr_abs_preserves_type() {
        let schema = make_schema();
        let sql = "-- name: AbsId :one\nSELECT ABS(id) AS a FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
    }

    #[test]
    fn expr_sqrt_returns_double() {
        let schema = make_schema();
        let sql = "-- name: Root :one\nSELECT SQRT(id) AS r FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::Double);
    }

    #[test]
    fn expr_now_returns_timestamp_tz() {
        let schema = make_schema();
        let sql = "-- name: GetNow :one\nSELECT NOW() AS ts FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::TimestampTz);
        assert!(!q.result_columns[0].nullable);
    }

    #[test]
    fn expr_nullif_always_nullable() {
        let schema = make_schema();
        let sql = "-- name: NullIf :one\nSELECT NULLIF(name, 'admin') AS n FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::Text);
        assert!(q.result_columns[0].nullable, "NULLIF can always return NULL");
    }

    #[test]
    fn expr_row_number_returns_bigint() {
        let schema = make_schema();
        let sql = "-- name: WithRowNum :many\nSELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[1].name, "rn");
        assert_eq!(q.result_columns[1].sql_type, SqlType::BigInt);
    }

    #[test]
    fn expr_nested_parenthesized() {
        let schema = make_schema();
        let sql = "-- name: Parens :one\nSELECT (id + 1) AS n FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
    }

    #[test]
    fn expr_unary_minus() {
        let schema = make_schema();
        let sql = "-- name: Neg :one\nSELECT -id AS n FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::BigInt);
    }

    #[test]
    fn expr_not_returns_boolean() {
        let schema = make_schema();
        let sql = "-- name: NotCheck :one\nSELECT NOT (id > 5) AS flag FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns[0].sql_type, SqlType::Boolean);
    }

    #[test]
    fn expr_unnamed_literal_produces_result_column() {
        // Previously, `SELECT 1` (no alias) was silently skipped.
        // Now it resolves as Integer.
        let schema = make_schema();
        let sql = "-- name: Bare :one\nSELECT 1 FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.result_columns.len(), 1);
        assert_eq!(q.result_columns[0].sql_type, SqlType::Integer);
    }

    #[test]
    fn expr_literal_does_not_override_param_type_from_column() {
        // `@p = -1 or col = @p` — param type must come from col (BigInt), not from -1 (Integer).
        let schema = make_schema();
        let sql = "-- name: Filter :many\nSELECT id FROM users WHERE $1 = -1 OR id = $1;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::BigInt, "param type must come from column, not literal");
    }

    fn make_upsert_schema() -> Schema {
        Schema {
            tables: vec![Table {
                name: "item".into(),
                columns: vec![
                    Column { name: "id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                    Column { name: "count".into(), sql_type: SqlType::Integer, nullable: false, is_primary_key: false },
                ],
            }],
        }
    }

    #[test]
    fn test_param_type_inferred_from_bitwise_op() {
        // id & $mask — $mask must be typed BigInt from the bitwise-and context.
        let schema = make_schema();
        let sql = "-- name: Mask :many\nSELECT id FROM users WHERE id & $1 > 0;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::BigInt, "$1 in bitwise op must be typed from column");
    }

    #[test]
    fn test_param_type_inferred_from_string_concat() {
        // name || $1 — $1 must be Text from the string-concat context.
        let schema = make_schema();
        let sql = "-- name: Search :many\nSELECT id FROM users WHERE name = name || $1;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::Text, "$1 in string concat must be Text");
    }

    #[test]
    fn test_param_type_inferred_from_on_conflict_set() {
        // $2 (increment) is only referenced in the ON CONFLICT SET clause, not in VALUES.
        // build_insert used to map params by col_names position, which would either
        // give the wrong column type or fall through to Text.
        let schema = make_upsert_schema();
        let sql = "-- name: UpsertItem :one\n\
            INSERT INTO item (id) VALUES ($1)\n\
            ON CONFLICT (id) DO UPDATE SET count = excluded.count + $2\n\
            RETURNING id, count;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::BigInt, "$1 (id) must be BigInt");
        assert_eq!(q.params[1].sql_type, SqlType::Integer, "$2 (increment) must be Integer from count column");
    }

    #[test]
    fn test_param_type_inferred_from_exists_subquery() {
        // $1 is inside an EXISTS subquery WHERE clause — must still be typed from the column.
        let schema = make_join_schema();
        let sql = "-- name: GetActive :many\nSELECT id FROM users WHERE EXISTS (SELECT 1 FROM posts WHERE posts.user_id = users.id AND posts.id > $1);";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::BigInt, "param inside EXISTS subquery must be typed from column");
    }

    #[test]
    fn test_param_type_inferred_from_coalesce() {
        // $1 is the fallback in COALESCE(id, $1) — must be typed BigInt from the first arg's column.
        // (Text is the default for untyped params, so using a BigInt column proves inference works.)
        let schema = make_schema();
        let sql = "-- name: GetId :many\nSELECT COALESCE(id, $1) AS effective_id FROM users;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::BigInt, "COALESCE fallback param must be typed from the first arg");
    }

    fn make_json_schema() -> Schema {
        Schema {
            tables: vec![Table {
                name: "docs".into(),
                columns: vec![
                    Column { name: "id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                    Column { name: "data".into(), sql_type: SqlType::Jsonb, nullable: false, is_primary_key: false },
                ],
            }],
        }
    }

    #[test]
    fn test_param_type_inferred_from_json_arrow() {
        // data -> $1 — key param must be Text (JSON field access by name).
        let schema = make_json_schema();
        let sql = "-- name: GetField :many\nSELECT data -> $1 FROM docs;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::Text, "JSON -> key param must be Text");
    }

    #[test]
    fn test_param_type_inferred_from_json_long_arrow() {
        // data ->> $1 — key param must be Text (JSON field access as text).
        let schema = make_json_schema();
        let sql = "-- name: GetFieldText :many\nSELECT data ->> $1 FROM docs;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::Text, "JSON ->> key param must be Text");
    }

    #[test]
    fn test_param_type_inferred_from_json_path_arrow() {
        // data #> $1 — path param must be Text[] (JSON path access by key array).
        let schema = make_json_schema();
        let sql = "-- name: GetPath :many\nSELECT data #> $1 FROM docs;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::Array(Box::new(SqlType::Text)), "JSON #> path param must be Text[]");
    }

    #[test]
    fn test_param_type_inferred_from_json_contains() {
        // data @> $1 — $1 must be Jsonb (JSONB containment; symmetric types).
        let schema = make_json_schema();
        let sql = "-- name: Contains :many\nSELECT id FROM docs WHERE data @> $1;";
        let q = &parse_queries(sql, &schema).unwrap()[0];
        assert_eq!(q.params[0].sql_type, SqlType::Jsonb, "JSONB @> param must be Jsonb");
    }

    // ─── Aggregate type inference tests ──────────────────────────────────────
    //
    // Rules:
    //   MIN / MAX  — always same type as argument, always nullable (no rows case)
    //   SUM        — preserves type, except integer inputs are widened to avoid
    //                overflow (SmallInt/Integer → sum_integer_type, BigInt →
    //                sum_bigint_type; defaults: BigInt and BigInt for SQLite/PG
    //                default, Decimal for MySQL/PG bigint)
    //   AVG        — preserves type for float/decimal inputs; integer/bigint
    //                inputs are widened to a fractional type (avg_integer_type:
    //                Decimal for PG, Double for MySQL/SQLite)
    //
    // The test helpers below use explicit ResolverConfig values to document the
    // per-dialect behaviour without depending on the dialect parse functions.

    fn make_numeric_schema() -> Schema {
        Schema {
            tables: vec![Table {
                name: "metrics".into(),
                columns: vec![
                    Column { name: "id".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                    Column { name: "small_val".into(), sql_type: SqlType::SmallInt, nullable: false, is_primary_key: false },
                    Column { name: "int_val".into(), sql_type: SqlType::Integer, nullable: false, is_primary_key: false },
                    Column { name: "big_val".into(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: false },
                    Column { name: "dec_val".into(), sql_type: SqlType::Decimal, nullable: false, is_primary_key: false },
                    Column { name: "dbl_val".into(), sql_type: SqlType::Double, nullable: false, is_primary_key: false },
                    Column { name: "label".into(), sql_type: SqlType::Text, nullable: false, is_primary_key: false },
                ],
            }],
        }
    }

    fn pg_config() -> ResolverConfig {
        ResolverConfig {
            typemap: crate::frontend::postgres::typemap::map,
            sum_bigint_type: SqlType::Decimal,
            avg_integer_type: SqlType::Decimal,
            ..ResolverConfig::default()
        }
    }

    fn mysql_config() -> ResolverConfig {
        ResolverConfig {
            sum_integer_type: SqlType::Decimal,
            sum_bigint_type: SqlType::Decimal,
            avg_integer_type: SqlType::Decimal,
            typemap: crate::frontend::mysql::typemap::map,
        }
    }

    fn sqlite_config() -> ResolverConfig {
        ResolverConfig { typemap: crate::frontend::sqlite::typemap::map, ..ResolverConfig::default() }
    }

    /// Parse a single aggregate query and return the type and nullability of
    /// the named result column. The `expr` argument is the SELECT expression
    /// list (e.g. `"SUM(int_val) AS s"`); the helper wraps it in a full query.
    fn agg_col(expr: &str, schema: &Schema, config: &ResolverConfig, name: &str) -> (SqlType, bool) {
        let sql = format!("-- name: Q :one\nSELECT {expr} FROM metrics;");
        let qs = parse_queries_with_config(&PostgreSqlDialect {}, &sql, schema, config).unwrap();
        assert!(!qs.is_empty(), "query did not parse (check table/column names)");
        let col = qs[0]
            .result_columns
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("column {name} not found in {:?}", qs[0].result_columns.iter().map(|c| &c.name).collect::<Vec<_>>()));
        (col.sql_type.clone(), col.nullable)
    }

    // ── MIN / MAX ─────────────────────────────────────────────────────────────

    #[test]
    fn test_aggregate_min_max_preserve_type_and_are_nullable() {
        let schema = make_numeric_schema();
        for config in [pg_config(), mysql_config(), sqlite_config()] {
            let (t, n) = agg_col("MIN(int_val) AS mn", &schema, &config, "mn");
            assert_eq!(t, SqlType::Integer, "MIN(integer) must stay Integer");
            assert!(n, "MIN result is always nullable (no rows)");

            let (t, _) = agg_col("MAX(dec_val) AS mx", &schema, &config, "mx");
            assert_eq!(t, SqlType::Decimal, "MAX(decimal) must stay Decimal");

            let (t, _) = agg_col("MIN(label) AS ml", &schema, &config, "ml");
            assert_eq!(t, SqlType::Text, "MIN(text) must stay Text");

            let (t, _) = agg_col("MAX(dbl_val) AS md", &schema, &config, "md");
            assert_eq!(t, SqlType::Double, "MAX(double) must stay Double");
        }
    }

    // ── SUM ───────────────────────────────────────────────────────────────────

    #[test]
    fn test_aggregate_sum_smallint_integer_widened_per_dialect() {
        let schema = make_numeric_schema();

        let (t, _) = agg_col("SUM(int_val) AS s", &schema, &pg_config(), "s");
        assert_eq!(t, SqlType::BigInt, "PG SUM(integer) → bigint");

        let (t, _) = agg_col("SUM(int_val) AS s", &schema, &mysql_config(), "s");
        assert_eq!(t, SqlType::Decimal, "MySQL SUM(integer) → decimal");

        let (t, _) = agg_col("SUM(int_val) AS s", &schema, &sqlite_config(), "s");
        assert_eq!(t, SqlType::BigInt, "SQLite SUM(integer) → bigint");

        let (t, _) = agg_col("SUM(small_val) AS s", &schema, &pg_config(), "s");
        assert_eq!(t, SqlType::BigInt, "PG SUM(smallint) → bigint");
    }

    #[test]
    fn test_aggregate_sum_bigint_widened_per_dialect() {
        // PostgreSQL and MySQL widen SUM(bigint) to numeric/decimal to prevent overflow.
        // SQLite keeps it as BigInt (SQLite's integer is arbitrary precision).
        let schema = make_numeric_schema();

        let (t, _) = agg_col("SUM(big_val) AS s", &schema, &pg_config(), "s");
        assert_eq!(t, SqlType::Decimal, "PG SUM(bigint) → numeric (Decimal)");

        let (t, _) = agg_col("SUM(big_val) AS s", &schema, &mysql_config(), "s");
        assert_eq!(t, SqlType::Decimal, "MySQL SUM(bigint) → decimal");

        let (t, _) = agg_col("SUM(big_val) AS s", &schema, &sqlite_config(), "s");
        assert_eq!(t, SqlType::BigInt, "SQLite SUM(bigint) → bigint (no overflow concern)");
    }

    #[test]
    fn test_aggregate_sum_decimal_and_double_preserved() {
        let schema = make_numeric_schema();
        for config in [pg_config(), mysql_config(), sqlite_config()] {
            let (t, n) = agg_col("SUM(dec_val) AS s", &schema, &config, "s");
            assert_eq!(t, SqlType::Decimal, "SUM(decimal) must stay Decimal in all dialects");
            assert!(n, "SUM result is always nullable");

            let (t, _) = agg_col("SUM(dbl_val) AS s", &schema, &config, "s");
            assert_eq!(t, SqlType::Double, "SUM(double) must stay Double in all dialects");
        }
    }

    #[test]
    fn test_aggregate_sum_expression_uses_expression_type() {
        // SUM(int_val * dec_val): arithmetic widens int*decimal → Decimal,
        // and SUM(Decimal) stays Decimal (no extra widening for non-integers).
        let schema = make_numeric_schema();
        for config in [pg_config(), mysql_config(), sqlite_config()] {
            let (t, _) = agg_col("SUM(int_val * dec_val) AS s", &schema, &config, "s");
            assert_eq!(t, SqlType::Decimal, "SUM(int * decimal) → Decimal via arithmetic widening");
        }
    }

    // ── AVG ───────────────────────────────────────────────────────────────────

    #[test]
    fn test_aggregate_avg_integer_widened_to_fractional_per_dialect() {
        // AVG of integers always produces a fractional result, so the integer
        // type is widened. PG and MySQL return DECIMAL on the wire; SQLite returns REAL.
        let schema = make_numeric_schema();

        for expr in ["AVG(int_val) AS a", "AVG(big_val) AS a", "AVG(small_val) AS a"] {
            let (t, n) = agg_col(expr, &schema, &pg_config(), "a");
            assert_eq!(t, SqlType::Decimal, "PG {expr} → numeric (Decimal)");
            assert!(n, "AVG is always nullable");

            let (t, _) = agg_col(expr, &schema, &mysql_config(), "a");
            assert_eq!(t, SqlType::Decimal, "MySQL {expr} → decimal");

            let (t, _) = agg_col(expr, &schema, &sqlite_config(), "a");
            assert_eq!(t, SqlType::Double, "SQLite {expr} → real (Double)");
        }
    }

    #[test]
    fn test_aggregate_avg_decimal_and_double_preserved() {
        let schema = make_numeric_schema();
        for config in [pg_config(), mysql_config(), sqlite_config()] {
            let (t, _) = agg_col("AVG(dec_val) AS a", &schema, &config, "a");
            assert_eq!(t, SqlType::Decimal, "AVG(decimal) must stay Decimal");

            let (t, _) = agg_col("AVG(dbl_val) AS a", &schema, &config, "a");
            assert_eq!(t, SqlType::Double, "AVG(double) must stay Double");
        }
    }

    // ─── source_table detection (bug 022) ─────────────────────────────────────

    /// `SELECT * FROM users` — single table, bare wildcard → source_table = users.
    #[test]
    fn test_source_table_bare_star_single_table() {
        let sql = "-- name: ListUsers :many\nSELECT * FROM users;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.source_table.as_deref(), Some("users"));
    }

    /// `SELECT u.* FROM users u` — qualified wildcard → source_table = users.
    #[test]
    fn test_source_table_qualified_star() {
        let sql = "-- name: ListUsers :many\nSELECT u.* FROM users u;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.source_table.as_deref(), Some("users"));
    }

    /// `SELECT u.* FROM users u INNER JOIN posts p ON u.id = p.user_id` —
    /// qualified wildcard over non-nullable side → source_table = users.
    #[test]
    fn test_source_table_qualified_star_with_join() {
        let sql = "-- name: ListUsers :many\nSELECT u.* FROM users u INNER JOIN posts p ON u.id = p.user_id;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        assert_eq!(q.source_table.as_deref(), Some("users"));
    }

    /// `SELECT * FROM (SELECT * FROM users) AS sub` — derived table wrapping a
    /// wildcard select → source_table must resolve to "users".
    #[test]
    fn test_source_table_derived_table() {
        let sql = "-- name: GetUser :one\nSELECT * FROM (SELECT * FROM users) AS sub;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.source_table, Some("users".into()));
    }

    /// `WITH tmp AS (SELECT * FROM users) SELECT * FROM tmp` — single CTE
    /// whose body is a wildcard select → source_table must resolve to "users".
    #[test]
    fn test_source_table_cte() {
        let sql = "-- name: GetUser :one\nWITH tmp AS (SELECT * FROM users) SELECT * FROM tmp;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.source_table, Some("users".into()));
    }

    /// `WITH a AS (SELECT * FROM users), b AS (SELECT * FROM a) SELECT * FROM b` —
    /// chained CTEs where each wildcards the previous → source_table must resolve to "users".
    #[test]
    fn test_source_table_chained_ctes() {
        let sql = "-- name: GetUser :one\nWITH a AS (SELECT * FROM users), b AS (SELECT * FROM a) SELECT * FROM b;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.source_table, Some("users".into()));
    }

    /// Three levels of nested derived tables all selecting `*` → source_table must
    /// resolve to "users".
    #[test]
    fn test_source_table_triple_nested_derived() {
        let sql = "-- name: GetUser :one\nSELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT * FROM users) AS a) AS b) AS c;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.source_table, Some("users".into()));
    }

    /// CTE whose body selects explicit columns (not `*`) → the inner select has
    /// no single source_table, so the outer wildcard cannot be resolved → None.
    #[test]
    fn test_source_table_cte_non_wildcard_inner() {
        let sql = "-- name: GetUser :one\nWITH tmp AS (SELECT id, name FROM users) SELECT * FROM tmp;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.source_table, None);
    }

    /// `SELECT u.* FROM posts p LEFT JOIN users u ON u.id = p.user_id` —
    /// u is on the nullable side of the LEFT JOIN → source_table must be None.
    #[test]
    fn test_source_table_none_for_nullable_side_of_left_join() {
        let sql = "-- name: GetUser :one\nSELECT u.* FROM posts p LEFT JOIN users u ON u.id = p.user_id;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        assert_eq!(q.source_table, None);
    }

    /// `SELECT id, name FROM users` — explicit column list → source_table must be None.
    #[test]
    fn test_source_table_none_for_explicit_column_list() {
        let sql = "-- name: GetUser :one\nSELECT id, name FROM users WHERE id = $1;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.source_table, None);
    }

    /// `SELECT * FROM users u INNER JOIN posts p ON …` — bare `*` with multiple
    /// tables in scope → source_table must be None (ambiguous).
    #[test]
    fn test_source_table_none_for_bare_star_multiple_tables() {
        let sql = "-- name: GetAll :many\nSELECT * FROM users u INNER JOIN posts p ON u.id = p.user_id;";
        let q = &parse_queries(sql, &make_join_schema()).unwrap()[0];
        assert_eq!(q.source_table, None);
    }

    /// CTE as the final source → source_table must be None (CTE is not a schema table).
    #[test]
    fn test_source_table_none_for_cte_source() {
        let sql = "-- name: GetUser :one\nWITH cte AS (SELECT id, name FROM users WHERE id = $1)\nSELECT * FROM cte;";
        let q = &parse_queries(sql, &make_schema()).unwrap()[0];
        assert_eq!(q.source_table, None);
    }
}
