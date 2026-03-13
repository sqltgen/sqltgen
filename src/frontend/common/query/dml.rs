//! Query builders for INSERT, UPDATE, and DELETE statements.
//!
//! Each `build_*` function takes a parsed sqlparser AST node and the active
//! schema, and returns a typed [`Query`] with inferred parameter types and
//! (for `RETURNING` queries) inferred result columns.

use std::collections::HashMap;

use sqlparser::ast::{
    Assignment, AssignmentTarget, Delete, Expr, FromTable, Insert, ObjectNamePart, OnConflictAction, OnInsert, Query as SqlQuery, SetExpr, TableFactor,
    TableWithJoins, Value, ValueWithSpan, Values,
};

use crate::frontend::common::{ident_to_str, obj_name_to_str};
use crate::ir::{Query, Schema, SqlType, Table};

use super::params::collect_params_from_expr;
use super::{
    build_alias_map, build_params, collect_cte_params, count_params, delete_table_name, insert_table_name, placeholder_idx, resolve_returning,
    unresolved_query, QueryAnnotation, ResolverConfig, ResolverContext,
};

// ─── INSERT ──────────────────────────────────────────────────────────────────

/// Build a [`Query`] from a top-level `INSERT` statement.
pub(super) fn build_insert(ann: &QueryAnnotation, sql: &str, insert: &Insert, schema: &Schema, config: &ResolverConfig) -> Query {
    let table_name = insert_table_name(insert);
    let Some(table) = schema.tables.iter().find(|t| t.name == table_name) else {
        return unresolved_query(ann, sql);
    };

    let mut mapping = HashMap::new();
    collect_insert_value_params(insert, schema, &mut mapping);
    collect_on_conflict_params(insert, table, config, &mut mapping, &ann.name);
    let params = build_params(mapping, count_params(sql));
    let result_columns = insert.returning.as_deref().map_or(vec![], |items| resolve_returning(items, table, config));

    Query::new(ann.name.clone(), ann.cmd.clone(), sql, params, result_columns)
}

/// Handle `Statement::Query` where the body is `INSERT … RETURNING` (data-modifying CTE pattern).
pub(super) fn build_query_from_insert(ann: &QueryAnnotation, sql: &str, q: &SqlQuery, ins: &Insert, schema: &Schema, config: &ResolverConfig) -> Query {
    let mut mapping = HashMap::new();
    collect_cte_params(q.with.as_ref(), schema, config, &mut mapping, &ann.name);
    collect_insert_value_params(ins, schema, &mut mapping);
    let params = build_params(mapping, count_params(sql));
    let result_columns = schema
        .tables
        .iter()
        .find(|t| t.name == insert_table_name(ins))
        .and_then(|t| ins.returning.as_deref().map(|items| resolve_returning(items, t, config)))
        .unwrap_or_default();
    Query::new(ann.name.clone(), ann.cmd.clone(), sql, params, result_columns)
}

/// Collect typed parameter mappings from an `ON CONFLICT DO UPDATE` clause.
///
/// The `excluded` pseudo-table is treated as an alias for the target table,
/// so `excluded.col + $N` correctly types `$N` from `col`'s schema type.
fn collect_on_conflict_params(
    insert: &Insert,
    table: &Table,
    config: &ResolverConfig,
    mapping: &mut HashMap<usize, (String, SqlType, bool)>,
    query_name: &str,
) {
    let Some(OnInsert::OnConflict(on_conflict)) = &insert.on else { return };
    let OnConflictAction::DoUpdate(do_update) = &on_conflict.action else { return };

    // Expose the target table under the "excluded" pseudo-alias so that
    // expressions like `excluded.col + $N` resolve correctly.
    let excluded_table = (table.clone(), Some("excluded".to_string()));
    let all_tables = [excluded_table];
    let alias_map = build_alias_map(&all_tables);
    let ctx =
        &mut ResolverContext { alias_map: &alias_map, all_tables: &all_tables, schema: &Schema { tables: vec![table.clone()] }, config, mapping, query_name };

    for assignment in &do_update.assignments {
        collect_params_from_expr(&assignment.value, ctx);
    }
    if let Some(selection) = &do_update.selection {
        collect_params_from_expr(selection, ctx);
    }
}

/// Collect typed parameter mappings from an INSERT … VALUES statement.
///
/// Maps each positional `$N` placeholder in the VALUES rows to the column type
/// it corresponds to, using the INSERT column list for position-to-column mapping.
/// SELECT-source INSERTs are skipped since position semantics are ambiguous there.
pub(super) fn collect_insert_value_params(ins: &Insert, schema: &Schema, mapping: &mut HashMap<usize, (String, SqlType, bool)>) {
    let table_name = insert_table_name(ins);
    let Some(table) = schema.tables.iter().find(|t| t.name == table_name) else { return };
    let col_names: Vec<String> = ins.columns.iter().map(ident_to_str).collect();
    let Some(source) = &ins.source else { return };
    let SetExpr::Values(Values { rows, .. }) = source.body.as_ref() else { return };
    for row in rows {
        for (pos, val_expr) in row.iter().enumerate() {
            if let Expr::Value(ValueWithSpan { value: Value::Placeholder(p), .. }) = val_expr {
                if let Some(idx) = placeholder_idx(p) {
                    if let Some(col) = col_names.get(pos).and_then(|n| table.columns.iter().find(|c| &c.name == n)) {
                        mapping.entry(idx).or_insert((col.name.clone(), col.sql_type.clone(), col.nullable));
                    }
                }
            }
        }
    }
}

// ─── UPDATE ──────────────────────────────────────────────────────────────────

/// Build a [`Query`] from a top-level `UPDATE` statement.
pub(super) fn build_update(ann: &QueryAnnotation, sql: &str, u: &sqlparser::ast::Update, schema: &Schema, config: &ResolverConfig) -> Query {
    let table_name = match &u.table.relation {
        TableFactor::Table { name, .. } => obj_name_to_str(name),
        _ => return unresolved_query(ann, sql),
    };
    let Some(table) = schema.tables.iter().find(|t| t.name == table_name) else {
        return unresolved_query(ann, sql);
    };

    let mut mapping: HashMap<usize, (String, SqlType, bool)> = HashMap::new();
    collect_update_params(&u.table, &u.assignments, u.selection.as_ref(), schema, config, &mut mapping, &ann.name);
    let params = build_params(mapping, count_params(sql));
    let result_columns = u.returning.as_deref().map_or(vec![], |items| resolve_returning(items, table, config));
    Query::new(ann.name.clone(), ann.cmd.clone(), sql, params, result_columns)
}

/// Handle `Statement::Query` where the body is `UPDATE … RETURNING` (data-modifying CTE pattern).
pub(super) fn build_query_from_update(
    ann: &QueryAnnotation,
    sql: &str,
    q: &SqlQuery,
    u: &sqlparser::ast::Update,
    schema: &Schema,
    config: &ResolverConfig,
) -> Query {
    let mut mapping = HashMap::new();
    collect_cte_params(q.with.as_ref(), schema, config, &mut mapping, &ann.name);
    collect_update_params(&u.table, &u.assignments, u.selection.as_ref(), schema, config, &mut mapping, &ann.name);
    let params = build_params(mapping, count_params(sql));
    let table_name = match &u.table.relation {
        TableFactor::Table { name, .. } => obj_name_to_str(name),
        _ => return unresolved_query(ann, sql),
    };
    let result_columns = schema
        .tables
        .iter()
        .find(|t| t.name == table_name)
        .and_then(|t| u.returning.as_deref().map(|items| resolve_returning(items, t, config)))
        .unwrap_or_default();
    Query::new(ann.name.clone(), ann.cmd.clone(), sql, params, result_columns)
}

/// Collect typed parameter mappings from an UPDATE statement's SET and WHERE clauses.
///
/// Shared by `build_update` (standalone UPDATE) and `build_query_from_update` (CTE-wrapped UPDATE).
pub(super) fn collect_update_params(
    table_with_joins: &TableWithJoins,
    assignments: &[Assignment],
    selection: Option<&Expr>,
    schema: &Schema,
    config: &ResolverConfig,
    mapping: &mut HashMap<usize, (String, SqlType, bool)>,
    query_name: &str,
) {
    let table_name = match &table_with_joins.relation {
        TableFactor::Table { name, .. } => obj_name_to_str(name),
        _ => return,
    };
    let Some(table) = schema.tables.iter().find(|t| t.name == table_name) else {
        return;
    };
    let all_tables = vec![(table.clone(), None)];
    let alias_map = build_alias_map(&all_tables);

    // Parameters from SET clause: col = $N
    for assignment in assignments {
        let col_name = match &assignment.target {
            AssignmentTarget::ColumnName(name) => {
                name.0.last().and_then(|p| if let ObjectNamePart::Identifier(i) = p { Some(ident_to_str(i)) } else { None }).unwrap_or_default()
            },
            _ => continue,
        };
        if let Expr::Value(ValueWithSpan { value: Value::Placeholder(p), .. }) = &assignment.value {
            if let Some(idx) = placeholder_idx(p) {
                if let Some(col) = table.columns.iter().find(|c| c.name == col_name) {
                    mapping.entry(idx).or_insert((col.name.clone(), col.sql_type.clone(), col.nullable));
                }
            }
        }
    }

    // Parameters from WHERE
    if let Some(expr) = selection {
        collect_params_from_expr(expr, &mut ResolverContext { alias_map: &alias_map, all_tables: &all_tables, schema, config, mapping, query_name });
    }
}

// ─── DELETE ──────────────────────────────────────────────────────────────────

/// Build a [`Query`] from a top-level `DELETE` statement.
pub(super) fn build_delete(ann: &QueryAnnotation, sql: &str, delete: &Delete, schema: &Schema, config: &ResolverConfig) -> Query {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
    };
    let table_name = tables.first().and_then(|twj| match &twj.relation {
        TableFactor::Table { name, .. } => Some(obj_name_to_str(name)),
        _ => None,
    });

    let Some(table_name) = table_name else {
        return unresolved_query(ann, sql);
    };
    let Some(table) = schema.tables.iter().find(|t| t.name == table_name) else {
        return unresolved_query(ann, sql);
    };

    let all_tables = vec![(table.clone(), None)];
    let alias_map = build_alias_map(&all_tables);
    let mut mapping = HashMap::new();

    if let Some(expr) = &delete.selection {
        collect_params_from_expr(
            expr,
            &mut ResolverContext { alias_map: &alias_map, all_tables: &all_tables, schema, config, mapping: &mut mapping, query_name: &ann.name },
        );
    }

    let params = build_params(mapping, count_params(sql));
    let result_columns = delete.returning.as_deref().map_or(vec![], |items| resolve_returning(items, table, config));

    Query::new(ann.name.clone(), ann.cmd.clone(), sql, params, result_columns)
}

/// Collect parameter mappings from a DELETE statement's WHERE clause.
pub(super) fn collect_delete_where_params(
    del: &Delete,
    schema: &Schema,
    config: &ResolverConfig,
    mapping: &mut HashMap<usize, (String, SqlType, bool)>,
    query_name: &str,
) {
    let Some(table_name) = delete_table_name(del) else { return };
    let Some(table) = schema.tables.iter().find(|t| t.name == table_name) else { return };
    let Some(expr) = &del.selection else { return };
    let all_tables = vec![(table.clone(), None)];
    let alias_map = build_alias_map(&all_tables);
    collect_params_from_expr(expr, &mut ResolverContext { alias_map: &alias_map, all_tables: &all_tables, schema, config, mapping, query_name });
}
