pub(crate) mod query;

use sqlparser::ast::{ColumnDef, ColumnOption, DataType, Ident, ObjectName, TableConstraint};

use crate::ir::{Column, SqlType, Table};

/// Removes tables named in a `DROP TABLE` statement.
pub(crate) fn apply_drop_tables(names: &[ObjectName], tables: &mut Vec<Table>) {
    for name in names {
        let table_name = obj_name_to_str(name);
        tables.retain(|t| t.name != table_name);
    }
}

// ─── Identifier helpers ───────────────────────────────────────────────────────

/// Converts an identifier to a string, preserving case for quoted identifiers
/// and lowercasing bare ones.
pub(crate) fn ident_to_str(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

/// Returns the last component of a dotted name (e.g. `schema.table` → `table`).
pub(crate) fn obj_name_to_str(name: &ObjectName) -> String {
    name.0.last().map(ident_to_str).unwrap_or_default()
}

/// Extracts PRIMARY KEY column names from a table-level constraint, if any.
pub(crate) fn pk_columns_from_constraint(tc: &TableConstraint) -> Vec<String> {
    match tc {
        TableConstraint::PrimaryKey { columns, .. } => columns.iter().map(ident_to_str).collect(),
        _ => vec![],
    }
}

// ─── Column / table builders ─────────────────────────────────────────────────

/// Builds a [`Column`] from an AST column definition.
///
/// `map_type` is the dialect-specific type mapper (e.g. `postgres::typemap::map`
/// or `sqlite::typemap::map`).
pub(crate) fn build_column(col_def: &ColumnDef, map_type: fn(&DataType) -> SqlType) -> Column {
    let name = ident_to_str(&col_def.name);
    let sql_type = map_type(&col_def.data_type);

    let mut nullable = true;
    let mut is_primary_key = false;

    for opt_def in &col_def.options {
        match &opt_def.option {
            ColumnOption::NotNull => nullable = false,
            ColumnOption::Null => nullable = true,
            ColumnOption::Unique { is_primary, .. } if *is_primary => {
                is_primary_key = true;
                nullable = false;
            },
            // GENERATED ALWAYS AS IDENTITY implies non-null (PostgreSQL; harmless on SQLite)
            ColumnOption::Generated { .. } => nullable = false,
            _ => {},
        }
    }

    Column { name, sql_type, nullable, is_primary_key }
}

/// Builds a [`Table`] from a `CREATE TABLE` AST node.
///
/// `map_type` is passed through to [`build_column`].
pub(crate) fn build_create_table(name: &ObjectName, column_defs: &[ColumnDef], constraints: &[TableConstraint], map_type: fn(&DataType) -> SqlType) -> Table {
    let table_name = obj_name_to_str(name);

    // Collect table-level PRIMARY KEY column names
    let mut pk_cols: Vec<String> = Vec::new();
    for constraint in constraints {
        pk_cols.extend(pk_columns_from_constraint(constraint));
    }

    let mut columns: Vec<Column> = column_defs.iter().map(|col_def| build_column(col_def, map_type)).collect();

    // Promote columns that appear in a table-level PRIMARY KEY
    for col in &mut columns {
        if pk_cols.contains(&col.name) {
            col.is_primary_key = true;
            col.nullable = false;
        }
    }

    Table { name: table_name, columns }
}
