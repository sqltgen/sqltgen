//! Query builders for INSERT, UPDATE, and DELETE statements.
//!
//! Each `build_*` function takes a parsed sqlparser AST node and the active
//! schema, and returns a typed [`Query`] with inferred parameter types and
//! (for `RETURNING` queries) inferred result columns.

use std::collections::HashMap;

use sqlparser::ast::{
    Assignment, AssignmentTarget, Delete, Expr, FromTable, Insert, ObjectNamePart, OnConflictAction, OnInsert, Query as SqlQuery, SelectItem, SetExpr,
    TableFactor, TableWithJoins, Values,
};

use crate::frontend::common::{ident_to_str, obj_name_to_str, obj_schema_to_str};
use crate::ir::{Query, Schema, SqlType, Table};

use super::params::{collect_join_params_list, collect_params_from_expr, collect_select_params, placeholder_idx_in_expr};
use super::{
    build_alias_map, build_cte_scope, build_params, collect_cte_params, collect_table_list, count_params, delete_table_ref, insert_table_ref,
    resolve_returning, unresolved_query, update_from_tables, QueryAnnotation, ResolverConfig, ResolverContext,
};

// ─── RETURNING params ────────────────────────────────────────────────────────

/// Collect typed parameter mappings from the expressions in a `RETURNING` clause.
///
/// Expressions like `RETURNING col + $N` or `RETURNING col || $N` allow parameters
/// whose type is inferred from the sibling column reference, exactly as in a SELECT
/// projection. Without this pass, such parameters fall through to the `Text` default.
pub(super) fn collect_returning_params(
    items: &[SelectItem],
    table: &Table,
    config: &ResolverConfig,
    mapping: &mut HashMap<usize, (String, SqlType, bool)>,
    query_name: &str,
) {
    let all_tables = [(table.clone(), None)];
    let alias_map = build_alias_map(&all_tables);
    let schema = Schema::with_tables(vec![table.clone()]);
    let ctx = &mut ResolverContext { alias_map: &alias_map, all_tables: &all_tables, schema: &schema, config, mapping, query_name };
    for item in items {
        let expr = match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
            _ => continue,
        };
        collect_params_from_expr(expr, ctx);
    }
}

// ─── INSERT ──────────────────────────────────────────────────────────────────

/// Build a [`Query`] from a top-level `INSERT` statement.
pub(super) fn build_insert(ann: &QueryAnnotation, sql: &str, insert: &Insert, schema: &Schema, config: &ResolverConfig) -> Query {
    let (ins_schema, table_name) = insert_table_ref(insert);
    let Some(table) = schema.find_table(ins_schema.as_deref(), &table_name, config.default_schema.as_deref()) else {
        return unresolved_query(ann, sql);
    };

    let mut mapping = HashMap::new();
    collect_insert_value_params(insert, schema, config, &mut mapping, &ann.name);
    collect_on_conflict_params(insert, table, config, &mut mapping, &ann.name);
    if let Some(items) = insert.returning.as_deref() {
        collect_returning_params(items, table, config, &mut mapping, &ann.name);
    }
    let params = build_params(mapping, count_params(sql));
    let result_columns = insert.returning.as_deref().map_or(vec![], |items| resolve_returning(items, table, config));

    Query::new(ann.name.clone(), ann.cmd.clone(), sql, params, result_columns)
}

/// Handle `Statement::Query` where the body is `INSERT … RETURNING` (data-modifying CTE pattern).
pub(super) fn build_query_from_insert(ann: &QueryAnnotation, sql: &str, q: &SqlQuery, ins: &Insert, schema: &Schema, config: &ResolverConfig) -> Query {
    let (ins_schema, ins_name) = insert_table_ref(ins);
    let ds = config.default_schema.as_deref();
    let mut mapping = HashMap::new();
    collect_cte_params(q.with.as_ref(), schema, config, &mut mapping, &ann.name);
    collect_insert_value_params(ins, schema, config, &mut mapping, &ann.name);
    if let Some(table) = schema.find_table(ins_schema.as_deref(), &ins_name, ds) {
        if let Some(items) = ins.returning.as_deref() {
            collect_returning_params(items, table, config, &mut mapping, &ann.name);
        }
    }
    let params = build_params(mapping, count_params(sql));
    let result_columns = schema
        .find_table(ins_schema.as_deref(), &ins_name, ds)
        .and_then(|t| ins.returning.as_deref().map(|items| resolve_returning(items, t, config)))
        .unwrap_or_default();
    Query::new(ann.name.clone(), ann.cmd.clone(), sql, params, result_columns)
}

/// Collect typed parameter mappings from an `ON CONFLICT DO UPDATE` or
/// `ON DUPLICATE KEY UPDATE` clause.
///
/// The target table is exposed both directly (for unqualified column refs such as
/// `count = count + $N`) and under the `excluded` pseudo-alias used by PostgreSQL
/// and SQLite (for `excluded.col + $N`). MySQL `ON DUPLICATE KEY UPDATE` uses
/// unqualified references, so only the direct exposure matters there.
fn collect_on_conflict_params(
    insert: &Insert,
    table: &Table,
    config: &ResolverConfig,
    mapping: &mut HashMap<usize, (String, SqlType, bool)>,
    query_name: &str,
) {
    // Expose the target table both unaliased (for unqualified refs) and under
    // the "excluded" pseudo-alias (for PostgreSQL/SQLite `excluded.col` refs).
    let all_tables = [(table.clone(), None), (table.clone(), Some("excluded".to_string()))];
    let alias_map = build_alias_map(&all_tables);
    let ctx =
        &mut ResolverContext { alias_map: &alias_map, all_tables: &all_tables, schema: &Schema::with_tables(vec![table.clone()]), config, mapping, query_name };

    match &insert.on {
        Some(OnInsert::OnConflict(on_conflict)) => {
            let OnConflictAction::DoUpdate(do_update) = &on_conflict.action else { return };
            for assignment in &do_update.assignments {
                collect_params_from_expr(&assignment.value, ctx);
            }
            if let Some(selection) = &do_update.selection {
                collect_params_from_expr(selection, ctx);
            }
        },
        Some(OnInsert::DuplicateKeyUpdate(assignments)) => {
            for assignment in assignments {
                collect_params_from_expr(&assignment.value, ctx);
            }
        },
        _ => {},
    }
}

/// Collect typed parameter mappings from an INSERT statement's source.
///
/// Handles both `INSERT … VALUES` and `INSERT … SELECT` forms:
/// - VALUES: maps each positional placeholder in the value rows to the corresponding
///   INSERT target column type.
/// - SELECT: maps positional placeholders in the SELECT projection to the INSERT
///   target columns, then delegates WHERE/JOIN/HAVING inference to `collect_select_params`.
pub(super) fn collect_insert_value_params(
    ins: &Insert,
    schema: &Schema,
    config: &ResolverConfig,
    mapping: &mut HashMap<usize, (String, SqlType, bool)>,
    query_name: &str,
) {
    let (ins_schema, table_name) = insert_table_ref(ins);
    let Some(table) = schema.find_table(ins_schema.as_deref(), &table_name, config.default_schema.as_deref()) else { return };
    let col_names: Vec<String> = ins.columns.iter().map(ident_to_str).collect();
    let Some(source) = &ins.source else { return };

    match source.body.as_ref() {
        SetExpr::Values(Values { rows, .. }) => {
            for row in rows {
                for (pos, val_expr) in row.iter().enumerate() {
                    if let Some(idx) = placeholder_idx_in_expr(val_expr) {
                        if let Some(col) = col_names.get(pos).and_then(|n| table.columns.iter().find(|c| &c.name == n)) {
                            mapping.entry(idx).or_insert((col.name.clone(), col.sql_type.clone(), col.nullable));
                        }
                    }
                }
            }
        },
        SetExpr::Select(select) => {
            // Type SELECT-list placeholders from INSERT target columns (positional).
            for (pos, item) in select.projection.iter().enumerate() {
                let expr = match item {
                    SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
                    _ => continue,
                };
                if let Some(idx) = placeholder_idx_in_expr(expr) {
                    if let Some(col) = col_names.get(pos).and_then(|n| table.columns.iter().find(|c| &c.name == n)) {
                        mapping.entry(idx).or_insert((col.name.clone(), col.sql_type.clone(), col.nullable));
                    }
                }
            }
            // Type WHERE/JOIN/HAVING params from the SELECT's FROM tables.
            collect_select_params(select, schema, config, &[], mapping, query_name);
        },
        _ => {},
    }
}

// ─── UPDATE ──────────────────────────────────────────────────────────────────

/// Build a [`Query`] from a top-level `UPDATE` statement.
pub(super) fn build_update(ann: &QueryAnnotation, sql: &str, u: &sqlparser::ast::Update, schema: &Schema, config: &ResolverConfig) -> Query {
    let (table_schema, table_name) = match &u.table.relation {
        TableFactor::Table { name, .. } => (obj_schema_to_str(name), obj_name_to_str(name)),
        _ => return unresolved_query(ann, sql),
    };
    let Some(table) = schema.find_table(table_schema.as_deref(), &table_name, config.default_schema.as_deref()) else {
        return unresolved_query(ann, sql);
    };

    let mut mapping: HashMap<usize, (String, SqlType, bool)> = HashMap::new();
    collect_update_params(&u.table, &u.assignments, u.selection.as_ref(), update_from_tables(&u.from), &[], schema, config, &mut mapping, &ann.name);
    if let Some(items) = u.returning.as_deref() {
        collect_returning_params(items, table, config, &mut mapping, &ann.name);
    }
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
    let (table_schema, table_name) = match &u.table.relation {
        TableFactor::Table { name, .. } => (obj_schema_to_str(name), obj_name_to_str(name)),
        _ => return unresolved_query(ann, sql),
    };
    let ds = config.default_schema.as_deref();
    let mut mapping = HashMap::new();
    collect_cte_params(q.with.as_ref(), schema, config, &mut mapping, &ann.name);
    let ctes = build_cte_scope(q.with.as_ref(), schema, config);
    collect_update_params(&u.table, &u.assignments, u.selection.as_ref(), update_from_tables(&u.from), &ctes, schema, config, &mut mapping, &ann.name);
    if let Some(table) = schema.find_table(table_schema.as_deref(), &table_name, ds) {
        if let Some(items) = u.returning.as_deref() {
            collect_returning_params(items, table, config, &mut mapping, &ann.name);
        }
    }
    let params = build_params(mapping, count_params(sql));
    let result_columns = schema
        .find_table(table_schema.as_deref(), &table_name, ds)
        .and_then(|t| u.returning.as_deref().map(|items| resolve_returning(items, t, config)))
        .unwrap_or_default();
    Query::new(ann.name.clone(), ann.cmd.clone(), sql, params, result_columns)
}

/// Collect typed parameter mappings from an UPDATE statement's SET, FROM, and WHERE clauses.
///
/// `from` is the `UPDATE … FROM` clause (`u.from`); pass `&[]` for plain updates.
/// Tables in the FROM clause are included in the resolver context so that WHERE
/// conditions that reference them (e.g. `WHERE t.id = other.id AND other.status = $1`)
/// can type their parameters correctly. JOIN ON conditions within the FROM clause
/// are also walked.
///
/// `ctes` contains CTE tables visible at this UPDATE site so `FROM cte_name` can
/// resolve column types from CTE output shapes.
///
/// Shared by `build_update` (standalone UPDATE) and `build_query_from_update` (CTE-wrapped UPDATE).
#[allow(clippy::too_many_arguments)]
pub(super) fn collect_update_params(
    table_with_joins: &TableWithJoins,
    assignments: &[Assignment],
    selection: Option<&Expr>,
    from: &[TableWithJoins],
    ctes: &[Table],
    schema: &Schema,
    config: &ResolverConfig,
    mapping: &mut HashMap<usize, (String, SqlType, bool)>,
    query_name: &str,
) {
    let (table_schema, table_name, target_alias) = match &table_with_joins.relation {
        TableFactor::Table { name, alias, .. } => (obj_schema_to_str(name), obj_name_to_str(name), alias.as_ref().map(|a| ident_to_str(&a.name))),
        _ => return,
    };
    let Some(table) = schema.find_table(table_schema.as_deref(), &table_name, config.default_schema.as_deref()) else {
        return;
    };

    let mut all_tables = vec![(table.clone(), target_alias)];
    all_tables.extend(collect_table_list(from, schema, ctes, config));
    let alias_map = build_alias_map(&all_tables);

    // Parameters from SET clause: col = $N
    for assignment in assignments {
        let col_name = match &assignment.target {
            AssignmentTarget::ColumnName(name) => {
                name.0.last().and_then(|p| if let ObjectNamePart::Identifier(i) = p { Some(ident_to_str(i)) } else { None }).unwrap_or_default()
            },
            _ => continue,
        };
        if let Some(idx) = placeholder_idx_in_expr(&assignment.value) {
            if let Some(col) = table.columns.iter().find(|c| c.name == col_name) {
                mapping.entry(idx).or_insert((col.name.clone(), col.sql_type.clone(), col.nullable));
            }
        }
    }

    let ctx = &mut ResolverContext { alias_map: &alias_map, all_tables: &all_tables, schema, config, mapping, query_name };
    if let Some(expr) = selection {
        collect_params_from_expr(expr, ctx);
    }
    collect_join_params_list(from, ctx);
}

// ─── DELETE ──────────────────────────────────────────────────────────────────

/// Build a [`Query`] from a top-level `DELETE` statement.
pub(super) fn build_delete(ann: &QueryAnnotation, sql: &str, delete: &Delete, schema: &Schema, config: &ResolverConfig) -> Query {
    let tables = match &delete.from {
        FromTable::WithFromKeyword(t) | FromTable::WithoutKeyword(t) => t,
    };
    let table_ref = tables.first().and_then(|twj| match &twj.relation {
        TableFactor::Table { name, .. } => Some((obj_schema_to_str(name), obj_name_to_str(name))),
        _ => None,
    });

    let Some((table_schema, table_name)) = table_ref else {
        return unresolved_query(ann, sql);
    };
    let Some(table) = schema.find_table(table_schema.as_deref(), &table_name, config.default_schema.as_deref()) else {
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
    if let Some(items) = delete.returning.as_deref() {
        collect_returning_params(items, table, config, &mut mapping, &ann.name);
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
    let Some((del_schema, del_name)) = delete_table_ref(del) else { return };
    let Some(table) = schema.find_table(del_schema.as_deref(), &del_name, config.default_schema.as_deref()) else { return };
    let Some(expr) = &del.selection else { return };
    let all_tables = vec![(table.clone(), None)];
    let alias_map = build_alias_map(&all_tables);
    collect_params_from_expr(expr, &mut ResolverContext { alias_map: &alias_map, all_tables: &all_tables, schema, config, mapping, query_name });
}
