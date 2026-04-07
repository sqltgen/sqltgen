use sqlparser::ast::{Delete, Insert, Query as SqlQuery, SelectItem, SetExpr, Statement, TableFactor};

use crate::frontend::common::{obj_name_to_str, obj_schema_to_str};
use crate::ir::{Column, Schema, Table};

use super::{build_alias_map, collect_from_tables, delete_table_ref, insert_table_ref, resolve_projection, resolve_returning, ResolverConfig};

/// Convert RETURNING result columns to `Column` values (no primary-key flag).
fn returning_to_columns(returning: &[SelectItem], table: &Table, config: &ResolverConfig) -> Vec<Column> {
    resolve_returning(returning, table, config).into_iter().map(Column::from).collect()
}

/// Resolve RETURNING columns for an INSERT CTE body.
fn returning_cols_for_insert(ins: &Insert, schema: &Schema, config: &ResolverConfig) -> Vec<Column> {
    let (ins_schema, ins_name) = insert_table_ref(ins);
    let Some(table) = schema.find_table(ins_schema.as_deref(), &ins_name, config.default_schema.as_deref()) else { return vec![] };
    let Some(returning) = &ins.returning else { return vec![] };
    returning_to_columns(returning, table, config)
}

/// Resolve RETURNING columns for an UPDATE CTE body.
fn returning_cols_for_update(u: &sqlparser::ast::Update, schema: &Schema, config: &ResolverConfig) -> Vec<Column> {
    let TableFactor::Table { name, .. } = &u.table.relation else { return vec![] };
    let Some(table) = schema.find_table(obj_schema_to_str(name).as_deref(), &obj_name_to_str(name), config.default_schema.as_deref()) else { return vec![] };
    let Some(returning) = &u.returning else { return vec![] };
    returning_to_columns(returning, table, config)
}

/// Resolve RETURNING columns for a DELETE CTE body.
fn returning_cols_for_delete(del: &Delete, schema: &Schema, config: &ResolverConfig) -> Vec<Column> {
    let Some((del_schema, del_name)) = delete_table_ref(del) else { return vec![] };
    let Some(table) = schema.find_table(del_schema.as_deref(), &del_name, config.default_schema.as_deref()) else { return vec![] };
    let Some(returning) = &del.returning else { return vec![] };
    returning_to_columns(returning, table, config)
}

/// Resolve the column types for a `CREATE VIEW` body.
///
/// Delegates to [`derived_cols`] with an empty CTE scope. The schema passed
/// in must already contain all base tables the view references.
pub(in crate::frontend::common) fn resolve_view_columns(query: &SqlQuery, schema: &Schema, config: &ResolverConfig) -> Vec<Column> {
    derived_cols(query, schema, &[], config)
}

pub(in crate::frontend::common) fn derived_cols(subquery: &SqlQuery, schema: &Schema, ctes: &[Table], config: &ResolverConfig) -> Vec<Column> {
    // A CTE body may be INSERT … RETURNING or UPDATE … RETURNING (data-modifying CTE).
    // In those cases the CTE output is the RETURNING clause, not a SELECT projection.
    match subquery.body.as_ref() {
        SetExpr::Insert(Statement::Insert(ins)) => return returning_cols_for_insert(ins, schema, config),
        SetExpr::Update(Statement::Update(u)) => return returning_cols_for_update(u, schema, config),
        SetExpr::Delete(Statement::Delete(del)) => return returning_cols_for_delete(del, schema, config),
        SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_) => return vec![],
        _ => {},
    }

    // For set operations (UNION ALL in recursive CTEs), derive columns from the
    // anchor term (left branch). SQL requires that all branches have compatible types.
    let select = match subquery.body.as_ref() {
        SetExpr::Select(s) => s,
        SetExpr::SetOperation { left, .. } => {
            let mut body = left.as_ref();
            while let SetExpr::SetOperation { left, .. } = body {
                body = left.as_ref();
            }
            let SetExpr::Select(s) = body else { return vec![] };
            s
        },
        _ => return vec![],
    };

    let inner_tables = collect_from_tables(select, schema, ctes, config);
    let alias_map = build_alias_map(&inner_tables);

    // Reuse resolve_projection and convert ResultColumn → Column (no PK flag).
    resolve_projection(select, &alias_map, &inner_tables, config, schema).into_iter().map(Column::from).collect()
}
