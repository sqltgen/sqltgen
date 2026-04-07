//! Query builders for SELECT statements and set operations (UNION/INTERSECT/EXCEPT).
//!
//! Entry point is [`build_select`], which dispatches to the appropriate sub-builder
//! based on the query body kind. Source-table provenance tracking for wildcard
//! SELECTs lives here alongside the SELECT builders that depend on it.

use std::collections::HashMap;

use sqlparser::ast::{ObjectNamePart, Query as SqlQuery, Select, SelectItem, SelectItemQualifiedWildcardKind, SetExpr, Statement, TableWithJoins, With};

use crate::frontend::common::ident_to_str;
use crate::ir::{Query, ResultColumn, Schema, Table};

use super::dml::{build_query_from_insert, build_query_from_update};
use super::params::{collect_limit_offset_params, collect_order_by_params, collect_select_params, collect_set_expr_params};
use super::resolve::resolve_projection;
use super::{
    apply_cte_alias_columns, build_alias_map, build_cte_scope, build_params, collect_cte_params, collect_from_tables, count_params, derived_cols,
    unresolved_query, QueryAnnotation, ResolverConfig, ResolverContext,
};

struct WildcardSourceScope<'a> {
    alias_map: &'a HashMap<String, &'a Table>,
    all_tables: &'a [(Table, Option<String>)],
    schema: &'a Schema,
    provenance: &'a HashMap<String, String>,
    default_schema: Option<&'a str>,
}

struct SelectBuildScope<'a> {
    schema: &'a Schema,
    config: &'a ResolverConfig,
    ctes: &'a [Table],
}

/// Build a [`Query`] from a `Statement::Query` node (SELECT, set operation, or
/// data-modifying CTE whose outer body is an INSERT/UPDATE).
pub(super) fn build_select(ann: &QueryAnnotation, sql: &str, q: &SqlQuery, schema: &Schema, config: &ResolverConfig) -> Query {
    let ctes = build_cte_scope(q.with.as_ref(), schema, config);
    let scope = SelectBuildScope { schema, config, ctes: &ctes };
    match q.body.as_ref() {
        SetExpr::Select(select) => build_select_body(ann, sql, q, select, &scope),
        SetExpr::Insert(Statement::Insert(ins)) => build_query_from_insert(ann, sql, q, ins, schema, config),
        SetExpr::Update(Statement::Update(u)) => build_query_from_update(ann, sql, q, u, schema, config),
        SetExpr::SetOperation { .. } => build_set_operation(ann, sql, q, q.body.as_ref(), &scope),
        _ => unresolved_query(ann, sql),
    }
}

/// Handle `Statement::Query` where the body is a plain `SELECT` (with optional CTEs).
fn build_select_body(ann: &QueryAnnotation, sql: &str, q: &SqlQuery, select: &Select, scope: &SelectBuildScope<'_>) -> Query {
    let schema = scope.schema;
    let config = scope.config;
    let ctes = scope.ctes;
    let all_tables = collect_from_tables(select, schema, ctes, config);
    let alias_map = build_alias_map(&all_tables);
    let result_columns = resolve_projection(select, &alias_map, &all_tables, config, schema);
    if all_tables.is_empty() && result_columns.is_empty() {
        return unresolved_query(ann, sql);
    }
    let params = {
        let mut mapping = HashMap::new();
        collect_cte_params(q.with.as_ref(), schema, config, &mut mapping, &ann.name);
        collect_select_params(select, schema, config, ctes, &mut mapping, &ann.name);
        collect_limit_offset_params(q, &mut mapping);
        collect_order_by_params(q, &mut ResolverContext::new(&alias_map, &all_tables, schema, config, &mut mapping, &ann.name));
        build_params(mapping, count_params(sql))
    };
    let provenance = build_source_provenance(q.with.as_ref(), &select.from, schema, ctes, config);
    let source_scope = WildcardSourceScope {
        alias_map: &alias_map,
        all_tables: &all_tables,
        schema,
        provenance: &provenance,
        default_schema: config.default_schema.as_deref(),
    };
    let source_table = select_wildcard_source(select, &source_scope);
    Query::new(ann.name.clone(), ann.cmd.clone(), sql, params, result_columns).with_source_table(source_table)
}

/// Detect the source schema table when the SELECT projection is an unambiguous wildcard.
///
/// Returns `Some(table_name)` when:
/// - The projection is a single `table.*` (qualified wildcard) or bare `*` (with one
///   table in scope), AND
/// - That table resolves to a schema table — either directly or via `provenance` (for
///   CTEs and derived subqueries whose rows come from a single schema table), AND
/// - The table was not made nullable by an outer join.
///
/// Returns `None` for all other projections (explicit column lists, set operations,
/// multi-table wildcards, non-trivial CTEs, or nullable-side tables).
fn select_wildcard_source(select: &Select, scope: &WildcardSourceScope<'_>) -> Option<String> {
    let WildcardSourceScope { alias_map, all_tables, schema, provenance, default_schema } = scope;
    let [item] = select.projection.as_slice() else { return None };
    let table_in_scope: &Table = match item {
        SelectItem::Wildcard(_) => {
            if all_tables.len() != 1 {
                return None;
            }
            &all_tables[0].0
        },
        SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(name), _) => {
            let qualifier = name.0.last().and_then(|p| if let ObjectNamePart::Identifier(i) = p { Some(ident_to_str(i)) } else { None })?;
            alias_map.get(&qualifier).copied()?
        },
        _ => return None,
    };

    // Direct schema table: present in schema and not made nullable by an outer join.
    if let Some(schema_table) = schema.find_table(table_in_scope.schema.as_deref(), &table_in_scope.name, *default_schema) {
        let made_nullable = schema_table.columns.iter().zip(&table_in_scope.columns).any(|(sc, rc)| !sc.nullable && rc.nullable);
        return if made_nullable { None } else { Some(schema_table.name.clone()) };
    }

    // CTE or derived table: look up the provenance chain built by build_source_provenance.
    // No nullable check here — the chain was already validated clean when the entry was created.
    provenance.get(&table_in_scope.name).cloned()
}

/// Build a map of `virtual_table_name → schema_table_name` for all CTEs and derived
/// tables in a query whose rows provably come from a single schema table through an
/// unambiguous wildcard selection chain.
///
/// Resolves recursively, so `WITH b AS (SELECT * FROM a), a AS (SELECT * FROM t)`
/// and `SELECT * FROM (SELECT * FROM (SELECT * FROM t) AS inner) AS outer` both
/// trace back to `t` correctly.
pub(super) fn build_source_provenance(
    with: Option<&With>,
    from: &[TableWithJoins],
    schema: &Schema,
    ctes: &[Table],
    config: &ResolverConfig,
) -> HashMap<String, String> {
    let mut provenance: HashMap<String, String> = HashMap::new();

    // CTE pass — process in declaration order so each CTE can see earlier ones.
    if let Some(with) = with {
        let mut cte_tables: Vec<Table> = Vec::new();
        for cte in &with.cte_tables {
            let cte_name = ident_to_str(&cte.alias.name);
            if let SetExpr::Select(select) = cte.query.body.as_ref() {
                // Recursively resolve provenance within this CTE's own body.
                let inner_prov = build_source_provenance(cte.query.with.as_ref(), &select.from, schema, &cte_tables, config);
                // Merge: outer provenance (earlier CTEs) + inner (this CTE's internals).
                let mut merged = provenance.clone();
                merged.extend(inner_prov);
                let all_tables = collect_from_tables(select, schema, &cte_tables, config);
                if !all_tables.is_empty() {
                    let alias_map = build_alias_map(&all_tables);
                    let source_scope = WildcardSourceScope {
                        alias_map: &alias_map,
                        all_tables: &all_tables,
                        schema,
                        provenance: &merged,
                        default_schema: config.default_schema.as_deref(),
                    };
                    if let Some(source) = select_wildcard_source(select, &source_scope) {
                        provenance.insert(cte_name.clone(), source);
                    }
                }
            }
            // Extend the local CTE list so subsequent CTEs can reference this one.
            let cols = apply_cte_alias_columns(derived_cols(&cte.query, schema, &cte_tables, config), &cte.alias.columns);
            if !cols.is_empty() {
                cte_tables.push(Table::new(cte_name, cols));
            }
        }
    }

    // Derived-table pass — recursively resolve each subquery before resolving the alias.
    for twj in from {
        if let sqlparser::ast::TableFactor::Derived { subquery, alias: Some(a), .. } = &twj.relation {
            let alias_name = ident_to_str(&a.name);
            if let SetExpr::Select(select) = subquery.body.as_ref() {
                let inner_prov = build_source_provenance(subquery.with.as_ref(), &select.from, schema, ctes, config);
                let mut merged = provenance.clone();
                merged.extend(inner_prov);
                let all_tables = collect_from_tables(select, schema, ctes, config);
                if !all_tables.is_empty() {
                    let alias_map = build_alias_map(&all_tables);
                    let source_scope = WildcardSourceScope {
                        alias_map: &alias_map,
                        all_tables: &all_tables,
                        schema,
                        provenance: &merged,
                        default_schema: config.default_schema.as_deref(),
                    };
                    if let Some(source) = select_wildcard_source(select, &source_scope) {
                        provenance.insert(alias_name, source);
                    }
                }
            }
        }
    }

    provenance
}

/// Handle `Statement::Query` where the body is a set operation (UNION/INTERSECT/EXCEPT).
///
/// Result columns come from the leftmost SELECT branch (per SQL standard).
/// Parameters are collected recursively from all branches.
fn build_set_operation(ann: &QueryAnnotation, sql: &str, q: &SqlQuery, body: &SetExpr, scope: &SelectBuildScope<'_>) -> Query {
    let schema = scope.schema;
    let config = scope.config;
    let ctes = scope.ctes;
    // Resolve result columns from all SELECT branches, then keep the leftmost
    // names/types while widening nullability across branches positionally.
    let branch_columns = resolve_set_branch_columns(body, scope);
    let mut result_columns = branch_columns.first().cloned().unwrap_or_default();
    for cols in branch_columns.iter().skip(1) {
        for (left, right) in result_columns.iter_mut().zip(cols.iter()) {
            left.nullable = left.nullable || right.nullable;
        }
    }

    // Collect params from CTEs + all set-operation branches + LIMIT/OFFSET + ORDER BY.
    let params = {
        let mut mapping = HashMap::new();
        collect_cte_params(q.with.as_ref(), schema, config, &mut mapping, &ann.name);
        collect_set_expr_params(body, schema, config, ctes, &mut mapping, &ann.name);
        collect_limit_offset_params(q, &mut mapping);
        // ORDER BY needs the leftmost branch's table context for column type inference.
        if let Some(select) = leftmost_select(body) {
            let all_tables = collect_from_tables(select, schema, ctes, config);
            if !all_tables.is_empty() {
                let alias_map = build_alias_map(&all_tables);
                collect_order_by_params(q, &mut ResolverContext::new(&alias_map, &all_tables, schema, config, &mut mapping, &ann.name));
            }
        }
        build_params(mapping, count_params(sql))
    };

    Query::new(ann.name.clone(), ann.cmd.clone(), sql, params, result_columns)
}

/// Recursively extract the leftmost `Select` from a `SetExpr` tree.
///
/// For `UNION`/`INTERSECT`/`EXCEPT`, the SQL standard defines result column
/// names from the first (leftmost) branch. This walks left until it finds a
/// plain `SELECT`.
fn leftmost_select(expr: &SetExpr) -> Option<&Select> {
    match expr {
        SetExpr::Select(select) => Some(select),
        SetExpr::SetOperation { left, .. } => leftmost_select(left),
        _ => None,
    }
}

/// Resolve projection columns for each SELECT branch in a set-expression tree.
fn resolve_set_branch_columns(expr: &SetExpr, scope: &SelectBuildScope<'_>) -> Vec<Vec<ResultColumn>> {
    let schema = scope.schema;
    let config = scope.config;
    let ctes = scope.ctes;
    let mut selects = Vec::new();
    collect_set_selects(expr, &mut selects);
    selects
        .into_iter()
        .map(|select| {
            let all_tables = collect_from_tables(select, schema, ctes, config);
            let alias_map = build_alias_map(&all_tables);
            resolve_projection(select, &alias_map, &all_tables, config, schema)
        })
        .collect()
}

/// Collect all SELECT leaves from a set-expression tree in left-to-right order.
fn collect_set_selects<'a>(expr: &'a SetExpr, out: &mut Vec<&'a Select>) {
    match expr {
        SetExpr::Select(select) => out.push(select),
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_selects(left, out);
            collect_set_selects(right, out);
        },
        _ => {},
    }
}
