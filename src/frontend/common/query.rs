use std::collections::HashMap;

use sqlparser::ast::{
    Assignment, AssignmentTarget, BinaryOperator, Delete, Expr, FromTable, FunctionArg,
    FunctionArgExpr, FunctionArguments, Insert, Query as SqlQuery, Select, SelectItem, SetExpr,
    Statement, TableFactor, Value, With,
};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;

use crate::frontend::common::{ident_to_str, obj_name_to_str};
use crate::ir::{Column, Parameter, Query, QueryCmd, ResultColumn, Schema, SqlType, Table};

/// Dialect-agnostic type inference configuration.
pub(crate) struct ResolverConfig {
    /// Return type of SUM applied to integer columns.
    /// PostgreSQL: BigInt.  MySQL: Decimal.  SQLite: BigInt (same as PG).
    pub sum_integer_type: SqlType,
}

impl Default for ResolverConfig {
    fn default() -> Self {
        Self { sum_integer_type: SqlType::BigInt }
    }
}

pub(crate) fn parse_queries_with_config(
    dialect: &dyn Dialect,
    sql: &str,
    schema: &Schema,
    config: &ResolverConfig,
) -> anyhow::Result<Vec<Query>> {
    let blocks = split_into_blocks(sql);
    let queries = blocks
        .into_iter()
        .filter_map(|(ann, body)| {
            let body = body.trim().trim_end_matches(';').trim();
            build_query_with_dialect(dialect, &ann, body, schema, config).ok()
        })
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

fn build_query_with_dialect(
    dialect: &dyn Dialect,
    ann: &Annotation,
    sql: &str,
    schema: &Schema,
    config: &ResolverConfig,
) -> anyhow::Result<Query> {
    let stmts = match Parser::parse_sql(dialect, sql) {
        Ok(s) if !s.is_empty() => s,
        _ => return Ok(bare(ann, sql)),
    };

    let query = match &stmts[0] {
        Statement::Query(q) => build_select(ann, sql, q, schema, config),
        Statement::Insert(ins) => build_insert(ann, sql, ins, schema, config),
        Statement::Update { table, assignments, selection, returning, .. } => {
            build_update(ann, sql, table, assignments, selection.as_ref(), returning.as_ref(), schema, config)
        }
        Statement::Delete(del) => build_delete(ann, sql, del, schema, config),
        _ => bare(ann, sql),
    };
    Ok(query)
}

// ─── SELECT ──────────────────────────────────────────────────────────────────

fn build_select(ann: &Annotation, sql: &str, q: &SqlQuery, schema: &Schema, config: &ResolverConfig) -> Query {
    let SetExpr::Select(select) = q.body.as_ref() else {
        return bare(ann, sql);
    };

    let ctes = build_cte_scope(q.with.as_ref(), schema, config);
    let all_tables = collect_from_tables(select, schema, &ctes, config);
    if all_tables.is_empty() {
        return bare(ann, sql);
    }
    let alias_map = build_alias_map(&all_tables);

    let result_columns = resolve_projection(select, &alias_map, &all_tables, config);
    let params = {
        let mut mapping = HashMap::new();
        if let Some(expr) = &select.selection {
            collect_params_from_expr(expr, &alias_map, &all_tables, &mut mapping);
        }
        build_params(mapping, count_params(sql))
    };

    Query {
        name: ann.name.clone(),
        cmd: ann.cmd.clone(),
        sql: sql.to_string(),
        params,
        result_columns,
    }
}

// ─── INSERT ──────────────────────────────────────────────────────────────────

fn build_insert(ann: &Annotation, sql: &str, insert: &Insert, schema: &Schema, config: &ResolverConfig) -> Query {
    let table_name = obj_name_to_str(&insert.table_name);
    let Some(table) = schema.tables.iter().find(|t| t.name == table_name) else {
        return bare(ann, sql);
    };

    let col_names: Vec<String> = insert.columns.iter().map(ident_to_str).collect();
    let count = count_params(sql);

    let params = (1..=count)
        .map(|idx| {
            let (name, sql_type, nullable) =
                match col_names.get(idx - 1).and_then(|n| table.columns.iter().find(|c| &c.name == n)) {
                    Some(col) => (col.name.clone(), col.sql_type.clone(), col.nullable),
                    None => (format!("param{idx}"), SqlType::Text, false),
                };
            Parameter { index: idx, name, sql_type, nullable }
        })
        .collect();

    let result_columns = insert.returning.as_deref().map_or(vec![], |items| resolve_returning(items, table, config));

    Query {
        name: ann.name.clone(),
        cmd: ann.cmd.clone(),
        sql: sql.to_string(),
        params,
        result_columns,
    }
}

// ─── UPDATE ──────────────────────────────────────────────────────────────────

fn build_update(
    ann: &Annotation,
    sql: &str,
    table_with_joins: &sqlparser::ast::TableWithJoins,
    assignments: &[Assignment],
    selection: Option<&Expr>,
    returning: Option<&Vec<SelectItem>>,
    schema: &Schema,
    config: &ResolverConfig,
) -> Query {
    let table_name = match &table_with_joins.relation {
        TableFactor::Table { name, .. } => obj_name_to_str(name),
        _ => return bare(ann, sql),
    };
    let Some(table) = schema.tables.iter().find(|t| t.name == table_name) else {
        return bare(ann, sql);
    };

    let all_tables = vec![(table.clone(), None)];
    let alias_map = build_alias_map(&all_tables);
    let mut mapping: HashMap<usize, (String, SqlType, bool)> = HashMap::new();

    // Parameters from SET clause: col = $N
    for assignment in assignments {
        let col_name = match &assignment.target {
            AssignmentTarget::ColumnName(name) => {
                name.0.last().map(ident_to_str).unwrap_or_default()
            }
            _ => continue,
        };
        if let Expr::Value(Value::Placeholder(p)) = &assignment.value {
            if let Some(idx) = placeholder_idx(&p) {
                if let Some(col) = table.columns.iter().find(|c| c.name == col_name) {
                    mapping.entry(idx).or_insert((col.name.clone(), col.sql_type.clone(), col.nullable));
                }
            }
        }
    }

    // Parameters from WHERE
    if let Some(expr) = selection {
        collect_params_from_expr(expr, &alias_map, &all_tables, &mut mapping);
    }

    let params = build_params(mapping, count_params(sql));
    let result_columns = returning.map_or(vec![], |items| resolve_returning(items, table, config));

    Query {
        name: ann.name.clone(),
        cmd: ann.cmd.clone(),
        sql: sql.to_string(),
        params,
        result_columns,
    }
}

// ─── DELETE ──────────────────────────────────────────────────────────────────

fn build_delete(ann: &Annotation, sql: &str, delete: &Delete, schema: &Schema, config: &ResolverConfig) -> Query {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
    };
    let table_name = tables
        .first()
        .and_then(|twj| match &twj.relation {
            TableFactor::Table { name, .. } => Some(obj_name_to_str(name)),
            _ => None,
        });

    let Some(table_name) = table_name else {
        return bare(ann, sql);
    };
    let Some(table) = schema.tables.iter().find(|t| t.name == table_name) else {
        return bare(ann, sql);
    };

    let all_tables = vec![(table.clone(), None)];
    let alias_map = build_alias_map(&all_tables);
    let mut mapping = HashMap::new();

    if let Some(expr) = &delete.selection {
        collect_params_from_expr(expr, &alias_map, &all_tables, &mut mapping);
    }

    let params = build_params(mapping, count_params(sql));
    let result_columns = delete.returning.as_deref()
        .map_or(vec![], |items| resolve_returning(items, table, config));

    Query {
        name: ann.name.clone(),
        cmd: ann.cmd.clone(),
        sql: sql.to_string(),
        params,
        result_columns,
    }
}

// ─── Table collection ─────────────────────────────────────────────────────────

fn collect_from_tables(
    select: &Select,
    schema: &Schema,
    ctes: &[Table],
    config: &ResolverConfig,
) -> Vec<(Table, Option<String>)> {
    let mut tables = Vec::new();
    for twj in &select.from {
        collect_table_factor(&twj.relation, schema, ctes, &mut tables, config);
        for join in &twj.joins {
            collect_table_factor(&join.relation, schema, ctes, &mut tables, config);
        }
    }
    tables
}

fn collect_table_factor(
    factor: &TableFactor,
    schema: &Schema,
    ctes: &[Table],
    out: &mut Vec<(Table, Option<String>)>,
    config: &ResolverConfig,
) {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let table_name = obj_name_to_str(name);
            let found = ctes.iter().find(|t| t.name == table_name)
                .or_else(|| schema.tables.iter().find(|t| t.name == table_name));
            if let Some(t) = found {
                let alias_str = alias.as_ref().map(|a| ident_to_str(&a.name));
                out.push((t.clone(), alias_str));
            }
        }
        TableFactor::Derived { subquery, alias, .. } => {
            if let Some(a) = alias {
                let alias_name = ident_to_str(&a.name);
                let cols = derived_cols(subquery, schema, ctes, config);
                if !cols.is_empty() {
                    out.push((
                        Table { name: alias_name.clone(), columns: cols },
                        Some(alias_name),
                    ));
                }
            }
        }
        _ => {}
    }
}

fn build_alias_map<'a>(tables: &'a [(Table, Option<String>)]) -> HashMap<String, &'a Table> {
    let mut map = HashMap::new();
    for (table, alias) in tables {
        map.insert(table.name.clone(), table);
        if let Some(a) = alias {
            map.insert(a.clone(), table);
        }
    }
    map
}

// ─── Projection resolution ───────────────────────────────────────────────────

fn resolve_projection(
    select: &Select,
    alias_map: &HashMap<String, &Table>,
    all_tables: &[(Table, Option<String>)],
    config: &ResolverConfig,
) -> Vec<ResultColumn> {
    let mut result = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                for (t, _) in all_tables {
                    result.extend(t.columns.iter().map(col_to_result));
                }
            }
            SelectItem::QualifiedWildcard(name, _) => {
                let qualifier = name.0.last().map(ident_to_str).unwrap_or_default();
                if let Some(t) = alias_map.get(&qualifier) {
                    result.extend(t.columns.iter().map(col_to_result));
                }
            }
            SelectItem::UnnamedExpr(expr) => {
                if let Some(rc) = resolve_expr(expr, alias_map, all_tables, config) {
                    result.push(rc);
                }
                // Unresolvable expr without alias (subquery, aggregate) — skip
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let name = ident_to_str(alias);
                match resolve_expr(expr, alias_map, all_tables, config) {
                    Some(rc) => result.push(ResultColumn { name, ..rc }),
                    None => result.push(ResultColumn {
                        name,
                        sql_type: SqlType::Custom("expr".into()),
                        nullable: true,
                    }),
                }
            }
        }
    }
    result
}

/// Returns the wider of two numeric SQL types (for arithmetic result type promotion).
fn numeric_wider(a: &SqlType, b: &SqlType) -> SqlType {
    use SqlType::*;
    match (a, b) {
        (Decimal, _) | (_, Decimal) => Decimal,
        (Double, _) | (_, Double) => Double,
        (Real, _) | (_, Real) => Real,
        (BigInt, _) | (_, BigInt) => BigInt,
        (Integer, _) | (_, Integer) => Integer,
        _ => a.clone(),
    }
}

fn resolve_expr(
    expr: &Expr,
    alias_map: &HashMap<String, &Table>,
    all_tables: &[(Table, Option<String>)],
    config: &ResolverConfig,
) -> Option<ResultColumn> {
    match expr {
        Expr::Identifier(ident) => {
            let col_name = ident_to_str(ident);
            all_tables
                .iter()
                .flat_map(|(t, _)| t.columns.iter())
                .find(|c| c.name == col_name)
                .map(col_to_result)
        }
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualifier = ident_to_str(&parts[parts.len() - 2]);
            let col_name = ident_to_str(&parts[parts.len() - 1]);
            alias_map
                .get(&qualifier)
                .and_then(|t| t.columns.iter().find(|c| c.name == col_name))
                .map(col_to_result)
        }
        Expr::BinaryOp { left, op, right }
            if matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo
            ) =>
        {
            match (
                resolve_expr(left, alias_map, all_tables, config),
                resolve_expr(right, alias_map, all_tables, config),
            ) {
                (Some(l), Some(r)) => Some(ResultColumn {
                    name: l.name.clone(),
                    sql_type: numeric_wider(&l.sql_type, &r.sql_type),
                    nullable: l.nullable || r.nullable,
                }),
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (None, None) => None,
            }
        }
        Expr::Function(func) => {
            let fname = func.name.0.last().map(ident_to_str).unwrap_or_default().to_uppercase();
            match fname.as_str() {
                "COUNT" => Some(ResultColumn {
                    name: "count".into(),
                    sql_type: SqlType::BigInt,
                    nullable: false,
                }),
                "SUM" => {
                    if let FunctionArguments::List(arg_list) = &func.args {
                        if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(inner))) =
                            arg_list.args.first()
                        {
                            return resolve_expr(inner, alias_map, all_tables, config)
                                .map(|rc| {
                                    let promoted = match rc.sql_type {
                                        SqlType::SmallInt | SqlType::Integer => config.sum_integer_type.clone(),
                                        other => other,
                                    };
                                    ResultColumn { sql_type: promoted, nullable: true, ..rc }
                                });
                        }
                    }
                    None
                }
                "MIN" | "MAX" | "AVG" => {
                    // Propagate the type of the first argument; result is always nullable
                    // because aggregate functions return NULL when applied to an empty set.
                    if let FunctionArguments::List(arg_list) = &func.args {
                        if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(inner))) =
                            arg_list.args.first()
                        {
                            return resolve_expr(inner, alias_map, all_tables, config)
                                .map(|rc| ResultColumn { nullable: true, ..rc });
                        }
                    }
                    None
                }
                _ => None,
            }
        }
        _ => None,
    }
}

// ─── Derived table columns ────────────────────────────────────────────────────

fn derived_cols(subquery: &SqlQuery, schema: &Schema, ctes: &[Table], config: &ResolverConfig) -> Vec<Column> {
    let SetExpr::Select(select) = subquery.body.as_ref() else {
        return vec![];
    };

    let inner_tables = collect_from_tables(select, schema, ctes, config);
    if inner_tables.is_empty() {
        return vec![];
    }
    let alias_map = build_alias_map(&inner_tables);

    let mut cols = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                for (t, _) in &inner_tables {
                    cols.extend(t.columns.iter().cloned());
                }
            }
            SelectItem::QualifiedWildcard(name, _) => {
                let qualifier = name.0.last().map(ident_to_str).unwrap_or_default();
                if let Some(t) = alias_map.get(&qualifier) {
                    cols.extend(t.columns.iter().cloned());
                }
            }
            SelectItem::UnnamedExpr(expr) => {
                if let Some(rc) = resolve_expr(expr, &alias_map, &inner_tables, config) {
                    cols.push(Column {
                        name: rc.name,
                        sql_type: rc.sql_type,
                        nullable: rc.nullable,
                        is_primary_key: false,
                    });
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let alias_name = ident_to_str(alias);
                match resolve_expr(expr, &alias_map, &inner_tables, config) {
                    Some(rc) => cols.push(Column {
                        name: alias_name,
                        sql_type: rc.sql_type,
                        nullable: rc.nullable,
                        is_primary_key: false,
                    }),
                    None => cols.push(Column {
                        name: alias_name,
                        sql_type: SqlType::Custom("expr".into()),
                        nullable: true,
                        is_primary_key: false,
                    }),
                }
            }
        }
    }
    cols
}

fn build_cte_scope(with: Option<&With>, schema: &Schema, config: &ResolverConfig) -> Vec<Table> {
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

// ─── Parameter resolution ─────────────────────────────────────────────────────

fn collect_params_from_expr(
    expr: &Expr,
    alias_map: &HashMap<String, &Table>,
    all_tables: &[(Table, Option<String>)],
    mapping: &mut HashMap<usize, (String, SqlType, bool)>,
) {
    // Parameter resolution only looks up column types; no aggregate functions appear
    // in WHERE/SET clauses, so a default ResolverConfig is safe here.
    let cfg = ResolverConfig::default();
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if matches!(op, BinaryOperator::Eq) {
                // col = $N
                if let Expr::Value(Value::Placeholder(p)) = &**right {
                    if let Some(idx) = placeholder_idx(p) {
                        if let Some(rc) = resolve_expr(left, alias_map, all_tables, &cfg) {
                            mapping.entry(idx).or_insert((rc.name, rc.sql_type, rc.nullable));
                        }
                    }
                }
                // $N = col (unusual)
                if let Expr::Value(Value::Placeholder(p)) = &**left {
                    if let Some(idx) = placeholder_idx(p) {
                        if let Some(rc) = resolve_expr(right, alias_map, all_tables, &cfg) {
                            mapping.entry(idx).or_insert((rc.name, rc.sql_type, rc.nullable));
                        }
                    }
                }
            }
            collect_params_from_expr(left, alias_map, all_tables, mapping);
            collect_params_from_expr(right, alias_map, all_tables, mapping);
        }
        Expr::InSubquery { expr, .. } => {
            // Walk only the outer expression; do NOT recurse into the subquery
            collect_params_from_expr(expr, alias_map, all_tables, mapping);
        }
        Expr::Nested(inner) => {
            collect_params_from_expr(inner, alias_map, all_tables, mapping);
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            collect_params_from_expr(inner, alias_map, all_tables, mapping);
        }
        _ => {}
    }
}

fn build_params(mapping: HashMap<usize, (String, SqlType, bool)>, count: usize) -> Vec<Parameter> {
    (1..=count)
        .map(|idx| match mapping.get(&idx) {
            Some((name, sql_type, nullable)) => Parameter {
                index: idx,
                name: name.clone(),
                sql_type: sql_type.clone(),
                nullable: *nullable,
            },
            None => Parameter {
                index: idx,
                name: format!("param{idx}"),
                sql_type: SqlType::Text,
                nullable: false,
            },
        })
        .collect()
}

// ─── RETURNING ────────────────────────────────────────────────────────────────

fn resolve_returning(items: &[SelectItem], table: &Table, config: &ResolverConfig) -> Vec<ResultColumn> {
    let all_tables = [(table.clone(), None)];
    let alias_map = build_alias_map(&all_tables);
    let mut result = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard(_) => {
                result.extend(table.columns.iter().map(col_to_result));
            }
            SelectItem::UnnamedExpr(expr) => {
                if let Some(rc) = resolve_expr(expr, &alias_map, &all_tables, config) {
                    result.push(rc);
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Some(rc) = resolve_expr(expr, &alias_map, &all_tables, config) {
                    result.push(ResultColumn { name: ident_to_str(alias), ..rc });
                }
            }
            _ => {}
        }
    }
    result
}

// ─── Utilities ────────────────────────────────────────────────────────────────

fn col_to_result(col: &Column) -> ResultColumn {
    ResultColumn {
        name: col.name.clone(),
        sql_type: col.sql_type.clone(),
        nullable: col.nullable,
    }
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

fn count_params(sql: &str) -> usize {
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

fn placeholder_idx(s: &str) -> Option<usize> {
    // $N (PostgreSQL) or ?N (SQLite)
    let rest = s.strip_prefix('$').or_else(|| s.strip_prefix('?'))?;
    rest.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::PostgreSqlDialect;
    use crate::ir::{Column, Schema, SqlType, Table};

    fn parse_queries(sql: &str, schema: &Schema) -> anyhow::Result<Vec<Query>> {
        parse_queries_with_config(&PostgreSqlDialect {}, sql, schema, &ResolverConfig::default())
    }

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
}
