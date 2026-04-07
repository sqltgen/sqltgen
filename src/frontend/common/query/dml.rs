//! Query builders for INSERT, UPDATE, and DELETE statements.
//!
//! Each `build_*` function takes a parsed sqlparser AST node and the active
//! schema, and returns a typed [`Query`] with inferred parameter types and
//! (for `RETURNING` queries) inferred result columns.

use sqlparser::ast::{
    AssignmentTarget, Delete, FromTable, Insert, ObjectNamePart, OnConflictAction, OnInsert, Query as SqlQuery, SelectItem, SetExpr, TableFactor, Values,
};

use crate::frontend::common::{ident_to_str, obj_name_to_str, obj_schema_to_str};
use crate::ir::{Query, Schema, Table};

use super::params::{collect_join_params_list, collect_params_from_expr, collect_select_params, placeholder_idx_in_expr};
use super::{
    build_alias_map, build_cte_scope, build_params, collect_cte_params, collect_table_list, count_params, delete_table_ref, insert_table_ref,
    resolve_returning, unresolved_query, update_from_tables, ParamMapping, QueryAnnotation, ResolverConfig, ResolverContext,
};

pub(super) struct DmlBuildScope<'a> {
    schema: &'a Schema,
    config: &'a ResolverConfig,
    query_name: &'a str,
}

impl<'a> DmlBuildScope<'a> {
    pub(super) fn new(schema: &'a Schema, config: &'a ResolverConfig, query_name: &'a str) -> Self {
        Self { schema, config, query_name }
    }
}

// ─── RETURNING params ────────────────────────────────────────────────────────

/// Collect typed parameter mappings from the expressions in a `RETURNING` clause.
///
/// Expressions like `RETURNING col + $N` or `RETURNING col || $N` allow parameters
/// whose type is inferred from the sibling column reference, exactly as in a SELECT
/// projection. Without this pass, such parameters fall through to the `Text` default.
pub(super) fn collect_returning_params(items: &[SelectItem], table: &Table, scope: &DmlBuildScope<'_>, mapping: &mut ParamMapping) {
    let schema = Schema::with_tables(vec![table.clone()]);
    let all_tables = [(table.clone(), None)];
    let alias_map = build_alias_map(&all_tables);
    let resolver_ctx = &mut ResolverContext::new(&alias_map, &all_tables, &schema, scope.config, mapping, scope.query_name);
    for item in items {
        let expr = match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => e,
            _ => continue,
        };
        collect_params_from_expr(expr, resolver_ctx);
    }
}

// ─── INSERT ──────────────────────────────────────────────────────────────────

/// Build a [`Query`] from a top-level `INSERT` statement.
pub(super) fn build_insert(ann: &QueryAnnotation, sql: &str, insert: &Insert, schema: &Schema, config: &ResolverConfig) -> Query {
    let scope = DmlBuildScope::new(schema, config, &ann.name);
    let (ins_schema, table_name) = insert_table_ref(insert);
    let Some(table) = scope.schema.find_table(ins_schema.as_deref(), &table_name, scope.config.default_schema.as_deref()) else {
        return unresolved_query(ann, sql);
    };

    let mut mapping: ParamMapping = ParamMapping::new();
    collect_insert_value_params(insert, scope.schema, scope.config, &mut mapping, scope.query_name);
    collect_on_conflict_params(insert, table, scope.config, &mut mapping, scope.query_name);
    if let Some(items) = insert.returning.as_deref() {
        collect_returning_params(items, table, &scope, &mut mapping);
    }
    let params = build_params(mapping, count_params(sql));
    let result_columns = insert.returning.as_deref().map_or(vec![], |items| resolve_returning(items, table, scope.config));

    Query::new(ann.name.clone(), ann.cmd.clone(), sql, params, result_columns)
}

/// Handle `Statement::Query` where the body is `INSERT … RETURNING` (data-modifying CTE pattern).
pub(super) fn build_query_from_insert(ann: &QueryAnnotation, sql: &str, q: &SqlQuery, ins: &Insert, schema: &Schema, config: &ResolverConfig) -> Query {
    let scope = DmlBuildScope::new(schema, config, &ann.name);
    let (ins_schema, ins_name) = insert_table_ref(ins);
    let ds = scope.config.default_schema.as_deref();
    let mut mapping: ParamMapping = ParamMapping::new();
    collect_cte_params(q.with.as_ref(), scope.schema, scope.config, &mut mapping, scope.query_name);
    collect_insert_value_params(ins, scope.schema, scope.config, &mut mapping, scope.query_name);
    if let Some(table) = scope.schema.find_table(ins_schema.as_deref(), &ins_name, ds) {
        if let Some(items) = ins.returning.as_deref() {
            collect_returning_params(items, table, &scope, &mut mapping);
        }
    }
    let params = build_params(mapping, count_params(sql));
    let result_columns = schema
        .find_table(ins_schema.as_deref(), &ins_name, ds)
        .and_then(|t| ins.returning.as_deref().map(|items| resolve_returning(items, t, scope.config)))
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
    let scope = DmlBuildScope::new(schema, config, &ann.name);
    let (table_schema, table_name) = match &u.table.relation {
        TableFactor::Table { name, .. } => (obj_schema_to_str(name), obj_name_to_str(name)),
        _ => return unresolved_query(ann, sql),
    };
    let Some(table) = scope.schema.find_table(table_schema.as_deref(), &table_name, scope.config.default_schema.as_deref()) else {
        return unresolved_query(ann, sql);
    };

    let mut mapping: ParamMapping = ParamMapping::new();
    collect_update_params(u, &[], scope.schema, scope.config, &mut mapping, scope.query_name);
    if let Some(items) = u.returning.as_deref() {
        collect_returning_params(items, table, &scope, &mut mapping);
    }
    let params = build_params(mapping, count_params(sql));
    let result_columns = u.returning.as_deref().map_or(vec![], |items| resolve_returning(items, table, scope.config));
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
    let scope = DmlBuildScope::new(schema, config, &ann.name);
    let (table_schema, table_name) = match &u.table.relation {
        TableFactor::Table { name, .. } => (obj_schema_to_str(name), obj_name_to_str(name)),
        _ => return unresolved_query(ann, sql),
    };
    let ds = scope.config.default_schema.as_deref();
    let mut mapping: ParamMapping = ParamMapping::new();
    collect_cte_params(q.with.as_ref(), scope.schema, scope.config, &mut mapping, scope.query_name);
    let ctes = build_cte_scope(q.with.as_ref(), scope.schema, scope.config);
    collect_update_params(u, &ctes, scope.schema, scope.config, &mut mapping, scope.query_name);
    if let Some(table) = scope.schema.find_table(table_schema.as_deref(), &table_name, ds) {
        if let Some(items) = u.returning.as_deref() {
            collect_returning_params(items, table, &scope, &mut mapping);
        }
    }
    let params = build_params(mapping, count_params(sql));
    let result_columns = schema
        .find_table(table_schema.as_deref(), &table_name, ds)
        .and_then(|t| u.returning.as_deref().map(|items| resolve_returning(items, t, scope.config)))
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
pub(super) fn collect_update_params(
    update: &sqlparser::ast::Update,
    ctes: &[Table],
    schema: &Schema,
    config: &ResolverConfig,
    mapping: &mut ParamMapping,
    query_name: &str,
) {
    let table_with_joins = &update.table;
    let assignments = &update.assignments;
    let selection = update.selection.as_ref();
    let from = update_from_tables(&update.from);
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

    let ctx = &mut ResolverContext::new(&alias_map, &all_tables, schema, config, mapping, query_name);
    if let Some(expr) = selection {
        collect_params_from_expr(expr, ctx);
    }
    collect_join_params_list(from, ctx);
}

// ─── DELETE ──────────────────────────────────────────────────────────────────

/// Build a [`Query`] from a top-level `DELETE` statement.
pub(super) fn build_delete(ann: &QueryAnnotation, sql: &str, delete: &Delete, schema: &Schema, config: &ResolverConfig) -> Query {
    let scope = DmlBuildScope::new(schema, config, &ann.name);
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
    let Some(table) = scope.schema.find_table(table_schema.as_deref(), &table_name, scope.config.default_schema.as_deref()) else {
        return unresolved_query(ann, sql);
    };

    let all_tables = vec![(table.clone(), None)];
    let alias_map = build_alias_map(&all_tables);
    let mut mapping: ParamMapping = ParamMapping::new();

    if let Some(expr) = &delete.selection {
        collect_params_from_expr(expr, &mut ResolverContext::new(&alias_map, &all_tables, scope.schema, scope.config, &mut mapping, scope.query_name));
    }
    if let Some(items) = delete.returning.as_deref() {
        collect_returning_params(items, table, &scope, &mut mapping);
    }

    let params = build_params(mapping, count_params(sql));
    let result_columns = delete.returning.as_deref().map_or(vec![], |items| resolve_returning(items, table, scope.config));

    Query::new(ann.name.clone(), ann.cmd.clone(), sql, params, result_columns)
}

/// Collect parameter mappings from a DELETE statement's WHERE clause.
pub(super) fn collect_delete_where_params(del: &Delete, schema: &Schema, config: &ResolverConfig, mapping: &mut ParamMapping, query_name: &str) {
    let Some((del_schema, del_name)) = delete_table_ref(del) else { return };
    let Some(table) = schema.find_table(del_schema.as_deref(), &del_name, config.default_schema.as_deref()) else { return };
    let Some(expr) = &del.selection else { return };
    let all_tables = vec![(table.clone(), None)];
    let alias_map = build_alias_map(&all_tables);
    collect_params_from_expr(expr, &mut ResolverContext::new(&alias_map, &all_tables, schema, config, mapping, query_name));
}
