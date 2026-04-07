use std::collections::HashMap;

use sqlparser::ast::{SetExpr, Statement, TableAliasColumnDef, TableFactor, With};

use crate::frontend::common::{ident_to_str, obj_name_to_str, obj_schema_to_str};
use crate::ir::{Column, Schema, SqlType, Table};

use super::dml::{collect_delete_where_params, collect_insert_value_params, collect_returning_params, collect_update_params};
use super::params::{collect_limit_offset_params, collect_set_expr_params};
use super::{delete_table_ref, derived_cols, insert_table_ref, update_from_tables, ResolverConfig};

/// Collect typed parameter mappings from the bodies of all CTEs in `with`.
///
/// Walks UPDATE, DELETE, and SELECT CTE bodies using schema column types for
/// inference. INSERT CTE bodies are handled via `collect_insert_value_params`.
/// This ensures parameters defined inside data-modifying CTEs receive correct
/// types even when the outer query body provides no column context.
pub(in crate::frontend::common) fn collect_cte_params(
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
                collect_update_params(
                    &u.table,
                    &u.assignments,
                    u.selection.as_ref(),
                    update_from_tables(&u.from),
                    &local_ctes,
                    schema,
                    config,
                    mapping,
                    query_name,
                );
                if let TableFactor::Table { name, .. } = &u.table.relation {
                    if let Some(table) = schema.find_table(obj_schema_to_str(name).as_deref(), &obj_name_to_str(name), config.default_schema.as_deref()) {
                        if let Some(items) = u.returning.as_deref() {
                            collect_returning_params(items, table, config, mapping, query_name);
                        }
                    }
                }
            },
            SetExpr::Delete(Statement::Delete(del)) => {
                collect_delete_where_params(del, schema, config, mapping, query_name);
                if let Some((del_schema, del_name)) = delete_table_ref(del) {
                    if let Some(table) = schema.find_table(del_schema.as_deref(), &del_name, config.default_schema.as_deref()) {
                        if let Some(items) = del.returning.as_deref() {
                            collect_returning_params(items, table, config, mapping, query_name);
                        }
                    }
                }
            },
            SetExpr::Insert(Statement::Insert(ins)) => {
                collect_insert_value_params(ins, schema, config, mapping, query_name);
                let (ins_schema, ins_name) = insert_table_ref(ins);
                if let Some(table) = schema.find_table(ins_schema.as_deref(), &ins_name, config.default_schema.as_deref()) {
                    if let Some(items) = ins.returning.as_deref() {
                        collect_returning_params(items, table, config, mapping, query_name);
                    }
                }
            },
            // SELECT bodies and set operations (UNION ALL for recursive CTEs).
            // Seed scope with this CTE's derived columns so recursive-term
            // self-references (e.g. `tree.id > $1`) can infer parameter types.
            _ => {
                let cols = apply_cte_alias_columns(derived_cols(&cte.query, schema, &local_ctes, config), &cte.alias.columns);
                let mut ctes_for_params = local_ctes.clone();
                if !cols.is_empty() {
                    ctes_for_params.push(Table::new(cte.alias.name.value.clone(), cols));
                }
                collect_set_expr_params(cte.query.body.as_ref(), schema, config, &ctes_for_params, mapping, query_name);
                collect_limit_offset_params(&cte.query, mapping);
            },
        }
        // Register this CTE's output shape so later CTEs can reference it.
        let cols = apply_cte_alias_columns(derived_cols(&cte.query, schema, &local_ctes, config), &cte.alias.columns);
        if !cols.is_empty() {
            local_ctes.push(Table::new(cte.alias.name.value.clone(), cols));
        }
    }
}

pub(in crate::frontend::common) fn apply_cte_alias_columns(mut cols: Vec<Column>, aliases: &[TableAliasColumnDef]) -> Vec<Column> {
    if aliases.is_empty() {
        return cols;
    }
    for (col, alias) in cols.iter_mut().zip(aliases.iter()) {
        col.name = alias.name.value.clone();
    }
    cols
}

pub(in crate::frontend::common) fn build_cte_scope(with: Option<&With>, schema: &Schema, config: &ResolverConfig) -> Vec<Table> {
    let Some(with) = with else { return vec![] };
    let mut ctes: Vec<Table> = Vec::new();
    for cte in &with.cte_tables {
        let cols = apply_cte_alias_columns(derived_cols(&cte.query, schema, &ctes, config), &cte.alias.columns);
        if !cols.is_empty() {
            ctes.push(Table::new(ident_to_str(&cte.alias.name), cols));
        }
    }
    ctes
}
