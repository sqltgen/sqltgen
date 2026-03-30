mod dml;
mod params;
mod resolve;
mod select;

use std::collections::HashMap;

use sqlparser::ast::{Delete, Insert, JoinOperator, Query as SqlQuery, Select, SelectItem, SetExpr, Statement, TableFactor, TableObject, With};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;

use crate::frontend::common::{ident_to_str, named_params, obj_name_to_str};
use crate::ir::{Column, NativeListBind, NestedColumn, NestedGroup, Parameter, Query, QueryCmd, ResultColumn, Schema, SqlType, Table};

type UserFunctions = HashMap<String, Vec<(Vec<SqlType>, SqlType)>>;

/// Dialect-specific function that rewrites list-param SQL and returns the binding method.
///
/// Takes the list parameter (with its final `sql_type` and `index`) and the current
/// query SQL (with `$N` placeholders). Returns the rewritten SQL and the
/// [`NativeListBind`] backends must use, or `None` when native expansion is unavailable.
type NativeListSqlFn = fn(&Parameter, &str) -> Option<(String, NativeListBind)>;

use dml::{
    build_delete, build_insert, build_update, collect_delete_where_params, collect_insert_value_params, collect_returning_params, collect_update_params,
};
use params::{collect_join_params, collect_limit_offset_params, collect_params_from_expr};
use resolve::{resolve_expr, resolve_projection};
use select::build_select;

/// Dialect-agnostic type inference configuration.
#[derive(Clone)]
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
    /// Compute the native list-param SQL and binding method for a given list parameter.
    ///
    /// Takes the list parameter (with its final `sql_type` and `index`) and
    /// the current query SQL (with `$N` placeholders). Returns the rewritten
    /// SQL and the [`NativeListBind`] method backends must use, or `None` when
    /// native expansion is unavailable.
    pub native_list_sql: Option<NativeListSqlFn>,
    /// User-defined scalar function overloads extracted from `CREATE FUNCTION` DDL.
    ///
    /// Key is the UPPERCASE function name. Value is a list of `(param_types, return_type)`
    /// pairs in declaration order. PostgreSQL supports overloading by param type/count;
    /// MySQL does not; SQLite has no DDL functions.
    pub user_functions: UserFunctions,
}

impl Default for ResolverConfig {
    fn default() -> Self {
        Self {
            sum_integer_type: SqlType::BigInt,
            sum_bigint_type: SqlType::BigInt,
            avg_integer_type: SqlType::Double,
            typemap: crate::frontend::common::typemap::map_common_or_custom,
            native_list_sql: None,
            user_functions: HashMap::new(),
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
    let config = build_effective_config(config, schema);
    let blocks = split_into_blocks(sql);
    let queries = blocks
        .into_iter()
        .filter_map(|(ann, body)| {
            let body = body.trim().trim_end_matches(';').trim();
            match build_query_with_dialect(dialect, &ann, body, schema, &config) {
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

/// Produce a `ResolverConfig` augmented with user-defined functions from the schema.
///
/// The caller's config is used as the base; `user_functions` entries from the
/// schema's `CREATE FUNCTION` statements are merged in, keyed by UPPERCASE name.
fn build_effective_config(config: &ResolverConfig, schema: &Schema) -> ResolverConfig {
    let mut user_functions = config.user_functions.clone();
    for f in &schema.functions {
        user_functions.entry(f.name.to_uppercase()).or_default().push((f.param_types.clone(), f.return_type.clone()));
    }
    ResolverConfig { user_functions, ..config.clone() }
}

// ─── Nest annotations ────────────────────────────────────────────────────────

/// Raw `-- nest:` annotation parsed from query body comments.
struct NestAnnotation {
    field_name: String,
    /// `(source_column, optional_explicit_alias)` pairs.
    columns: Vec<(String, Option<String>)>,
}

/// Extract `-- nest:` comment lines from a query body, returning the cleaned
/// SQL and the parsed nest annotations.
fn extract_nest_annotations(sql: &str) -> (String, Vec<NestAnnotation>) {
    let mut clean_lines = Vec::new();
    let mut nests = Vec::new();
    for line in sql.lines() {
        if let Some(nest) = parse_nest_line(line) {
            nests.push(nest);
        } else {
            clean_lines.push(line);
        }
    }
    (clean_lines.join("\n"), nests)
}

/// Parse a single `-- nest: field(col1 [as alias1], col2 [as alias2], …)` line.
fn parse_nest_line(line: &str) -> Option<NestAnnotation> {
    let trimmed = line.trim();
    let rest = trimmed.strip_prefix("--")?.trim();
    let rest = rest.strip_prefix("nest:")?.trim();
    let paren_start = rest.find('(')?;
    let paren_end = rest.rfind(')')?;
    if paren_end <= paren_start {
        return None;
    }
    let field_name = rest[..paren_start].trim();
    if field_name.is_empty() {
        return None;
    }
    let cols_str = &rest[paren_start + 1..paren_end];
    let columns: Vec<(String, Option<String>)> = cols_str
        .split(',')
        .filter_map(|s| {
            let s = s.trim();
            if s.is_empty() {
                return None;
            }
            let parts: Vec<&str> = s.split_whitespace().collect();
            if parts.len() == 3 && parts[1].eq_ignore_ascii_case("as") {
                Some((parts[0].to_string(), Some(parts[2].to_string())))
            } else {
                Some((s.to_string(), None))
            }
        })
        .collect();
    if columns.is_empty() {
        return None;
    }
    Some(NestAnnotation { field_name: field_name.to_string(), columns })
}

fn is_valid_js_identifier(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else { return false };
    if !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}

/// Strip a prefix from a column name for auto-deriving nested field names.
///
/// If `source` starts with `"{prefix}_"`, the prefix is stripped. Otherwise
/// the original name is returned unchanged.
fn strip_field_prefix<'a>(source: &'a str, prefix: &str) -> &'a str {
    let with_underscore = format!("{prefix}_");
    source.strip_prefix(&with_underscore).unwrap_or(source)
}

/// Build [`NestedGroup`]s from parsed annotations and resolved result columns.
fn apply_nest_groups(query: &mut Query, annotations: &[NestAnnotation]) {
    for ann in annotations {
        let mut columns = Vec::new();
        for (source_name, alias) in &ann.columns {
            if let Some(rc) = query.result_columns.iter().find(|c| c.name == *source_name) {
                let target_name = alias
                    .clone()
                    .unwrap_or_else(|| strip_field_prefix(source_name, &ann.field_name).to_string());
                columns.push(NestedColumn {
                    source_name: source_name.clone(),
                    target_name,
                    sql_type: rc.sql_type.clone(),
                    nullable: rc.nullable,
                });
            }
        }
        if !columns.is_empty() {
            query.nested_groups.push(NestedGroup { field_name: ann.field_name.clone(), columns });
        }
    }
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
    let (sql, nest_annotations) = extract_nest_annotations(sql);
    if !nest_annotations.is_empty() && !matches!(ann.cmd, QueryCmd::One | QueryCmd::Many) {
        anyhow::bail!(
            "query '{}' uses -- nest:, but nesting is only supported for :one and :many (found :{})",
            ann.name,
            query_cmd_name(&ann.cmd)
        );
    }
    if let Some(invalid) = nest_annotations.iter().find(|n| !is_valid_js_identifier(&n.field_name)) {
        anyhow::bail!(
            "query '{}' uses invalid -- nest: field name '{}'; expected JavaScript identifier [a-zA-Z_][a-zA-Z0-9_]*",
            ann.name,
            invalid.field_name
        );
    }
    let sql = sql.as_str();

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
            if !nest_annotations.is_empty() && query.params.iter().any(|p| p.is_list) {
                anyhow::bail!(
                    "query '{}' combines -- nest: with list parameters; this combination is not supported yet",
                    ann.name
                );
            }
            apply_nest_groups(&mut query, &nest_annotations);
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
    apply_native_list_sql(&mut query, config);
    if !nest_annotations.is_empty() && query.params.iter().any(|p| p.is_list) {
        anyhow::bail!(
            "query '{}' combines -- nest: with list parameters; this combination is not supported yet",
            ann.name
        );
    }
    apply_nest_groups(&mut query, &nest_annotations);
    Ok(query)
}

fn query_cmd_name(cmd: &QueryCmd) -> &'static str {
    match cmd {
        QueryCmd::One => "one",
        QueryCmd::Many => "many",
        QueryCmd::Exec => "exec",
        QueryCmd::ExecRows => "execrows",
    }
}

/// Populate `native_list_sql` and `native_list_bind` for each list parameter.
///
/// Called after parameter types and names are fully resolved. Only executes
/// when `config.native_list_sql` is `Some`.
fn apply_native_list_sql(query: &mut Query, config: &ResolverConfig) {
    let Some(rewrite) = config.native_list_sql else { return };
    for p in &mut query.params {
        if p.is_list {
            if let Some((sql, bind)) = rewrite(p, &query.sql) {
                p.native_list_sql = Some(sql);
                p.native_list_bind = Some(bind);
            }
        }
    }
}

// ─── CTE parameter collection ────────────────────────────────────────────────

/// Collect typed parameter mappings from the bodies of all CTEs in `with`.
///
/// Walks UPDATE, DELETE, and SELECT CTE bodies using schema column types for
/// inference. INSERT CTE bodies are handled via `collect_insert_value_params`.
/// This ensures parameters defined inside data-modifying CTEs receive correct
/// types even when the outer query body provides no column context.
pub(super) fn collect_cte_params(
    with: Option<&With>,
    schema: &Schema,
    config: &ResolverConfig,
    mapping: &mut HashMap<usize, (String, SqlType, bool)>,
    query_name: &str,
) {
    let Some(with) = with else { return };
    let mut local_ctes: Vec<Table> = Vec::new();
    for cte in &with.cte_tables {
        // Recurse into nested WITH clauses before processing this CTE's body.
        collect_cte_params(cte.query.with.as_ref(), schema, config, mapping, query_name);
        match cte.query.body.as_ref() {
            SetExpr::Update(Statement::Update(u)) => {
                collect_update_params(&u.table, &u.assignments, u.selection.as_ref(), schema, config, mapping, query_name);
                if let TableFactor::Table { name, .. } = &u.table.relation {
                    let table_name = obj_name_to_str(name);
                    if let Some(table) = schema.tables.iter().find(|t| t.name == table_name) {
                        if let Some(items) = u.returning.as_deref() {
                            collect_returning_params(items, table, config, mapping, query_name);
                        }
                    }
                }
            },
            SetExpr::Delete(Statement::Delete(del)) => {
                collect_delete_where_params(del, schema, config, mapping, query_name);
                if let Some(table_name) = delete_table_name(del) {
                    if let Some(table) = schema.tables.iter().find(|t| t.name == table_name) {
                        if let Some(items) = del.returning.as_deref() {
                            collect_returning_params(items, table, config, mapping, query_name);
                        }
                    }
                }
            },
            SetExpr::Insert(Statement::Insert(ins)) => {
                collect_insert_value_params(ins, schema, config, mapping, query_name);
                if let Some(table) = schema.tables.iter().find(|t| t.name == insert_table_name(ins)) {
                    if let Some(items) = ins.returning.as_deref() {
                        collect_returning_params(items, table, config, mapping, query_name);
                    }
                }
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
            local_ctes.push(Table::new(cte.alias.name.value.clone(), cols));
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
                out.push((Table::new(alias_name.clone(), cols), Some(alias_name)));
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
    resolve_returning(returning, table, config).into_iter().map(Column::from).collect()
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

/// Resolve the column types for a `CREATE VIEW` body.
///
/// Delegates to [`derived_cols`] with an empty CTE scope.  The schema passed
/// in must already contain all base tables the view references (i.e. this is
/// called during pass 2 of schema parsing, after all `CREATE TABLE` statements
/// have been processed).
pub(crate) fn resolve_view_columns(query: &SqlQuery, schema: &Schema, config: &ResolverConfig) -> Vec<Column> {
    derived_cols(query, schema, &[], config)
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
    resolve_projection(select, &alias_map, &inner_tables, config, schema).into_iter().map(Column::from).collect()
}

pub(super) fn build_cte_scope(with: Option<&With>, schema: &Schema, config: &ResolverConfig) -> Vec<Table> {
    let Some(with) = with else { return vec![] };
    let mut ctes: Vec<Table> = Vec::new();
    for cte in &with.cte_tables {
        let cols = derived_cols(&cte.query, schema, &ctes, config);
        if !cols.is_empty() {
            ctes.push(Table::new(ident_to_str(&cte.alias.name), cols));
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
                let unique_name = if *count == 1 { name.clone() } else { format!("{name}_{count}") };
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
                result.extend(table.columns.iter().map(ResultColumn::from));
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
mod tests;
