use std::collections::HashMap;

use sqlparser::ast::{JoinOperator, Select, TableFactor, TableWithJoins, UpdateTableFromKind};

use crate::frontend::common::{ident_to_str, obj_name_to_str, obj_schema_to_str};
use crate::ir::{Schema, Table};

use super::{derived_cols, ResolverConfig};

/// Extract the FROM tables from an `UPDATE … FROM` clause.
///
/// `UpdateTableFromKind` wraps the same `Vec<TableWithJoins>` in two variants
/// (`BeforeSet` for Snowflake-style, `AfterSet` for standard PostgreSQL-style).
/// Returns an empty slice for plain `UPDATE … SET … WHERE` with no FROM clause.
pub(in crate::frontend::common) fn update_from_tables(from: &Option<UpdateTableFromKind>) -> &[TableWithJoins] {
    match from {
        Some(UpdateTableFromKind::BeforeSet(tables)) | Some(UpdateTableFromKind::AfterSet(tables)) => tables,
        None => &[],
    }
}

/// Collect typed tables from a list of `TableWithJoins` (a FROM clause or UPDATE FROM clause).
///
/// Shared by `collect_from_tables` (SELECT) and `collect_update_params` (UPDATE … FROM).
/// Respects outer-join nullability: LEFT JOIN makes the right side nullable, RIGHT/FULL
/// makes the left side nullable.
pub(in crate::frontend::common) fn collect_table_list(
    from: &[TableWithJoins],
    schema: &Schema,
    ctes: &[Table],
    config: &ResolverConfig,
) -> Vec<(Table, Option<String>)> {
    let mut tables = Vec::new();
    for twj in from {
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

pub(in crate::frontend::common) fn collect_from_tables(
    select: &Select,
    schema: &Schema,
    ctes: &[Table],
    config: &ResolverConfig,
) -> Vec<(Table, Option<String>)> {
    collect_table_list(&select.from, schema, ctes, config)
}

pub(in crate::frontend::common) fn build_alias_map(tables: &[(Table, Option<String>)]) -> HashMap<String, &Table> {
    let mut map = HashMap::new();
    for (table, alias) in tables {
        map.insert(table.name.clone(), table);
        if let Some(a) = alias {
            map.insert(a.clone(), table);
        }
    }
    map
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
            let table_schema = obj_schema_to_str(name);
            let found = ctes
                .iter()
                .find(|t| t.name == table_name)
                .or_else(|| schema.find_table(table_schema.as_deref(), &table_name, config.default_schema.as_deref()));
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
