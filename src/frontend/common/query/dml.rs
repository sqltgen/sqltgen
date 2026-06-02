//! Query builders for INSERT, UPDATE, and DELETE statements.
//!
//! Each `build_*` function takes a parsed sqlparser AST node and the active
//! schema, and returns a typed [`Query`] with inferred parameter types and
//! (for `RETURNING` queries) inferred result columns.

use sqlparser::ast::{AssignmentTarget, Delete, Insert, ObjectNamePart, OnConflictAction, OnInsert, Query as SqlQuery, SelectItem, SetExpr, Values};

use crate::frontend::common::ident_to_str;
use crate::ir::{Query, Schema, Table};

use super::params::{collect_join_params_list, collect_params_from_expr, collect_select_params, placeholder_idx_in_expr};
use super::{
    build_alias_map, build_cte_scope, build_params, collect_cte_params, count_params, delete_table_scope, insert_table_ref, resolve_returning,
    unresolved_query, update_from_tables, update_table_scope, ParamMapping, QueryAnnotation, ResolverConfig, ResolverContext,
};

// ─── RETURNING params ────────────────────────────────────────────────────────

/// Collect typed parameter mappings from the expressions in a `RETURNING` clause.
///
/// Expressions like `RETURNING col + $N` or `RETURNING col || $N` allow parameters
/// whose type is inferred from the sibling column reference, exactly as in a SELECT
/// projection. Without this pass, such parameters fall through to the `Text` default.
///
/// `all_tables` is the full DML table scope (see [`update_table_scope`] /
/// [`delete_table_scope`]) so a parameter beside a `FROM`/`USING` column resolves too.
pub(super) fn collect_returning_params(
    items: &[SelectItem],
    all_tables: &[(Table, Option<String>)],
    config: &ResolverConfig,
    mapping: &mut ParamMapping,
    query_name: &str,
) {
    let schema = Schema::with_tables(all_tables.iter().map(|(t, _)| t.clone()).collect());
    let alias_map = build_alias_map(all_tables);
    let ctx = &mut ResolverContext::new(&alias_map, all_tables, &schema, config, mapping, query_name);
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

    let insert_scope = vec![(table.clone(), None)];
    let mut mapping: ParamMapping = ParamMapping::new();
    collect_insert_value_params(insert, schema, config, &mut mapping, &ann.name);
    collect_on_conflict_params(insert, table, config, &mut mapping, &ann.name);
    if let Some(items) = insert.returning.as_deref() {
        collect_returning_params(items, &insert_scope, config, &mut mapping, &ann.name);
    }
    let params = build_params(mapping, count_params(sql));
    let result_columns = insert.returning.as_deref().map_or(vec![], |items| resolve_returning(items, &insert_scope, config));

    Query::new(ann.name.clone(), ann.cmd.clone(), sql, params, result_columns)
}

/// Handle `Statement::Query` where the body is `INSERT … RETURNING` (data-modifying CTE pattern).
pub(super) fn build_query_from_insert(ann: &QueryAnnotation, sql: &str, q: &SqlQuery, ins: &Insert, schema: &Schema, config: &ResolverConfig) -> Query {
    let (ins_schema, ins_name) = insert_table_ref(ins);
    let ds = config.default_schema.as_deref();
    let mut mapping: ParamMapping = ParamMapping::new();
    collect_cte_params(q.with.as_ref(), schema, config, &mut mapping, &ann.name);
    collect_insert_value_params(ins, schema, config, &mut mapping, &ann.name);
    let insert_scope = schema.find_table(ins_schema.as_deref(), &ins_name, ds).map(|t| vec![(t.clone(), None)]).unwrap_or_default();
    if let Some(items) = ins.returning.as_deref() {
        collect_returning_params(items, &insert_scope, config, &mut mapping, &ann.name);
    }
    let params = build_params(mapping, count_params(sql));
    let result_columns = ins.returning.as_deref().map_or(vec![], |items| resolve_returning(items, &insert_scope, config));
    Query::new(ann.name.clone(), ann.cmd.clone(), sql, params, result_columns)
}

/// Collect typed parameter mappings from an `ON CONFLICT DO UPDATE` or
/// `ON DUPLICATE KEY UPDATE` clause.
///
/// The target table is exposed both directly (for unqualified column refs such as
/// `count = count + $N`) and under the `excluded` pseudo-alias used by PostgreSQL
/// and SQLite (for `excluded.col + $N`). MySQL `ON DUPLICATE KEY UPDATE` uses
/// unqualified references, so only the direct exposure matters there.
fn collect_on_conflict_params(insert: &Insert, table: &Table, config: &ResolverConfig, mapping: &mut ParamMapping, query_name: &str) {
    let schema = Schema::with_tables(vec![table.clone()]);
    collect_on_conflict_params_in(insert, table, &schema, config, mapping, query_name);
}

fn collect_on_conflict_params_in(insert: &Insert, table: &Table, schema: &Schema, config: &ResolverConfig, mapping: &mut ParamMapping, query_name: &str) {
    // Expose the target table both unaliased (for unqualified refs) and under
    // the "excluded" pseudo-alias (for PostgreSQL/SQLite `excluded.col` refs).
    let all_tables = [(table.clone(), None), (table.clone(), Some("excluded".to_string()))];
    let alias_map = build_alias_map(&all_tables);
    let resolver_ctx = &mut ResolverContext::new(&alias_map, &all_tables, schema, config, mapping, query_name);

    match &insert.on {
        Some(OnInsert::OnConflict(on_conflict)) => {
            let OnConflictAction::DoUpdate(do_update) = &on_conflict.action else { return };
            for assignment in &do_update.assignments {
                collect_params_from_expr(&assignment.value, resolver_ctx);
            }
            if let Some(selection) = &do_update.selection {
                collect_params_from_expr(selection, resolver_ctx);
            }
        },
        Some(OnInsert::DuplicateKeyUpdate(assignments)) => {
            for assignment in assignments {
                collect_params_from_expr(&assignment.value, resolver_ctx);
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
pub(super) fn collect_insert_value_params(ins: &Insert, schema: &Schema, config: &ResolverConfig, mapping: &mut ParamMapping, query_name: &str) {
    let (ins_schema, table_name) = insert_table_ref(ins);
    let Some(table) = schema.find_table(ins_schema.as_deref(), &table_name, config.default_schema.as_deref()) else { return };
    collect_insert_value_params_in(ins, table, schema, config, mapping, query_name);
}

fn collect_insert_value_params_in(ins: &Insert, table: &Table, schema: &Schema, config: &ResolverConfig, mapping: &mut ParamMapping, query_name: &str) {
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
    let scope = update_table_scope(u, schema, &[], config);
    if scope.is_empty() {
        return unresolved_query(ann, sql);
    }

    let mut mapping: ParamMapping = ParamMapping::new();
    collect_update_params(u, &[], schema, config, &mut mapping, &ann.name);
    if let Some(items) = u.returning.as_deref() {
        collect_returning_params(items, &scope, config, &mut mapping, &ann.name);
    }
    let params = build_params(mapping, count_params(sql));
    let result_columns = u.returning.as_deref().map_or(vec![], |items| resolve_returning(items, &scope, config));
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
    let mut mapping: ParamMapping = ParamMapping::new();
    collect_cte_params(q.with.as_ref(), schema, config, &mut mapping, &ann.name);
    let ctes = build_cte_scope(q.with.as_ref(), schema, config);
    collect_update_params(u, &ctes, schema, config, &mut mapping, &ann.name);
    let scope = update_table_scope(u, schema, &ctes, config);
    if let Some(items) = u.returning.as_deref() {
        collect_returning_params(items, &scope, config, &mut mapping, &ann.name);
    }
    let params = build_params(mapping, count_params(sql));
    let result_columns = u.returning.as_deref().map_or(vec![], |items| resolve_returning(items, &scope, config));
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
pub(super) fn collect_update_params(
    update: &sqlparser::ast::Update,
    ctes: &[Table],
    schema: &Schema,
    config: &ResolverConfig,
    mapping: &mut ParamMapping,
    query_name: &str,
) {
    let all_tables = update_table_scope(update, schema, ctes, config);
    let Some((target, _)) = all_tables.first() else { return };
    let alias_map = build_alias_map(&all_tables);

    // Parameters from SET clause: col = $N (typed from the target table's column).
    for assignment in &update.assignments {
        let col_name = match &assignment.target {
            AssignmentTarget::ColumnName(name) => {
                name.0.last().and_then(|p| if let ObjectNamePart::Identifier(i) = p { Some(ident_to_str(i)) } else { None }).unwrap_or_default()
            },
            _ => continue,
        };
        if let Some(idx) = placeholder_idx_in_expr(&assignment.value) {
            if let Some(col) = target.columns.iter().find(|c| c.name == col_name) {
                mapping.entry(idx).or_insert((col.name.clone(), col.sql_type.clone(), col.nullable));
            }
        }
    }

    let ctx = &mut ResolverContext::new(&alias_map, &all_tables, schema, config, mapping, query_name);
    if let Some(expr) = update.selection.as_ref() {
        collect_params_from_expr(expr, ctx);
    }
    collect_join_params_list(update_from_tables(&update.from), ctx);
}

// ─── DELETE ──────────────────────────────────────────────────────────────────

/// Build a [`Query`] from a top-level `DELETE` statement.
pub(super) fn build_delete(ann: &QueryAnnotation, sql: &str, delete: &Delete, schema: &Schema, config: &ResolverConfig) -> Query {
    let scope = delete_table_scope(delete, schema, &[], config);
    if scope.is_empty() {
        return unresolved_query(ann, sql);
    }

    let alias_map = build_alias_map(&scope);
    let mut mapping: ParamMapping = ParamMapping::new();
    collect_delete_params(delete, &mut ResolverContext::new(&alias_map, &scope, schema, config, &mut mapping, &ann.name));
    if let Some(items) = delete.returning.as_deref() {
        collect_returning_params(items, &scope, config, &mut mapping, &ann.name);
    }

    let params = build_params(mapping, count_params(sql));
    let result_columns = delete.returning.as_deref().map_or(vec![], |items| resolve_returning(items, &scope, config));

    Query::new(ann.name.clone(), ann.cmd.clone(), sql, params, result_columns)
}

/// Collect parameter mappings from a DELETE statement's WHERE clause and
/// `USING`-table join conditions.
///
/// `ctx` carries the DELETE table scope (target table with its alias plus any
/// `USING` tables; see [`delete_table_scope`]). That scope is what lets
/// parameters reference the target alias or a `USING`-joined column
/// (`WHERE i.id = $1 AND o.account_id = $2`).
pub(super) fn collect_delete_params(del: &Delete, ctx: &mut ResolverContext) {
    if let Some(expr) = &del.selection {
        collect_params_from_expr(expr, ctx);
    }
    if let Some(using) = &del.using {
        collect_join_params_list(using, ctx);
    }
}
