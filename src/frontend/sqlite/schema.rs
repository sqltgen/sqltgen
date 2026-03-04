use sqlparser::ast::{
    AlterTableOperation, ColumnOption, Ident, ObjectName, Statement, TableConstraint,
};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

use crate::frontend::sqlite::typemap;
use crate::ir::{Column, Schema, Table};

pub fn parse_schema(ddl: &str) -> anyhow::Result<Schema> {
    let dialect = SQLiteDialect {};
    let stmts = Parser::parse_sql(&dialect, ddl)
        .map_err(|e| anyhow::anyhow!("DDL parse error: {e}"))?;

    let mut tables: Vec<Table> = Vec::new();

    for stmt in stmts {
        match stmt {
            Statement::CreateTable(ct) => {
                let table = build_create_table(&ct.name, &ct.columns, &ct.constraints);
                tables.push(table);
            }
            Statement::AlterTable { name, operations, .. } => {
                apply_alter_table(&name, &operations, &mut tables);
            }
            _ => {}
        }
    }

    Ok(Schema { tables })
}

// ─── CREATE TABLE ─────────────────────────────────────────────────────────────

fn build_create_table(
    name: &ObjectName,
    column_defs: &[sqlparser::ast::ColumnDef],
    constraints: &[TableConstraint],
) -> Table {
    let table_name = obj_name_to_str(name);

    let mut pk_cols: Vec<String> = Vec::new();
    for constraint in constraints {
        pk_cols.extend(pk_columns_from_constraint(constraint));
    }

    let mut columns: Vec<Column> = Vec::new();
    for col_def in column_defs {
        columns.push(build_column(col_def));
    }

    for col in &mut columns {
        if pk_cols.contains(&col.name) {
            col.is_primary_key = true;
            col.nullable = false;
        }
    }

    Table { name: table_name, columns }
}

fn build_column(col_def: &sqlparser::ast::ColumnDef) -> Column {
    let name = ident_to_str(&col_def.name);
    let sql_type = typemap::map(&col_def.data_type);

    let mut nullable = true;
    let mut is_primary_key = false;

    for opt_def in &col_def.options {
        match &opt_def.option {
            ColumnOption::NotNull => nullable = false,
            ColumnOption::Null => nullable = true,
            ColumnOption::Unique { is_primary, .. } if *is_primary => {
                is_primary_key = true;
                nullable = false;
            }
            _ => {}
        }
    }

    Column { name, sql_type, nullable, is_primary_key }
}

fn pk_columns_from_constraint(tc: &TableConstraint) -> Vec<String> {
    match tc {
        TableConstraint::PrimaryKey { columns, .. } => {
            columns.iter().map(ident_to_str).collect()
        }
        _ => vec![],
    }
}

// ─── ALTER TABLE ─────────────────────────────────────────────────────────────
// SQLite supports only: RENAME TO, RENAME COLUMN, ADD COLUMN.
// Everything else is silently ignored.

fn apply_alter_table(
    name: &ObjectName,
    operations: &[AlterTableOperation],
    tables: &mut Vec<Table>,
) {
    let table_name = obj_name_to_str(name);
    let Some(idx) = tables.iter().position(|t| t.name == table_name) else {
        return;
    };

    for op in operations {
        let table = &mut tables[idx];
        match op {
            AlterTableOperation::AddColumn { column_def, .. } => {
                table.columns.push(build_column(column_def));
            }
            AlterTableOperation::RenameColumn { old_column_name, new_column_name } => {
                let old = ident_to_str(old_column_name);
                let new = ident_to_str(new_column_name);
                if let Some(col) = table.columns.iter_mut().find(|c| c.name == old) {
                    col.name = new;
                }
            }
            AlterTableOperation::RenameTable { table_name: new_name } => {
                table.name = obj_name_to_str(new_name);
            }
            _ => {}
        }
    }
}

// ─── Identifier helpers ───────────────────────────────────────────────────────

pub(super) fn ident_to_str(ident: &Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

pub(super) fn obj_name_to_str(name: &ObjectName) -> String {
    name.0.last().map(ident_to_str).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::SqlType;

    #[test]
    fn parses_simple_table() {
        let ddl = "
            CREATE TABLE users (
                id    INTEGER PRIMARY KEY,
                name  TEXT    NOT NULL,
                email TEXT    NOT NULL,
                bio   TEXT
            );
        ";
        let schema = parse_schema(ddl).unwrap();
        let t = &schema.tables[0];
        assert_eq!(t.name, "users");
        assert_eq!(t.columns.len(), 4);
        assert!(t.columns[0].is_primary_key);
        assert!(!t.columns[0].nullable);
        assert!(!t.columns[1].nullable);
        assert!(t.columns[3].nullable);
    }

    #[test]
    fn parses_autoincrement_primary_key() {
        let ddl = "
            CREATE TABLE items (
                id    INTEGER PRIMARY KEY AUTOINCREMENT,
                label TEXT    NOT NULL
            );
        ";
        let schema = parse_schema(ddl).unwrap();
        let id = &schema.tables[0].columns[0];
        assert!(id.is_primary_key);
        assert!(!id.nullable);
        assert_eq!(id.sql_type, SqlType::Integer);
    }

    #[test]
    fn parses_multiple_tables() {
        let ddl = "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
            CREATE TABLE posts (
                id      INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id),
                title   TEXT    NOT NULL
            );
        ";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 2);
        assert_eq!(schema.tables[0].name, "users");
        assert_eq!(schema.tables[1].name, "posts");
    }

    #[test]
    fn parses_table_level_primary_key() {
        let ddl = "
            CREATE TABLE kv (
                key   TEXT NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY (key)
            );
        ";
        let schema = parse_schema(ddl).unwrap();
        let t = &schema.tables[0];
        assert!(t.columns[0].is_primary_key);
        assert!(!t.columns[1].is_primary_key);
    }

    #[test]
    fn parses_if_not_exists() {
        let ddl = "CREATE TABLE IF NOT EXISTS tags (id INTEGER PRIMARY KEY, name TEXT NOT NULL);";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables[0].name, "tags");
    }

    #[test]
    fn alter_add_column() {
        let ddl = "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
            ALTER TABLE users ADD COLUMN bio TEXT;
        ";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables[0].columns.len(), 3);
        assert_eq!(schema.tables[0].columns[2].name, "bio");
        assert!(schema.tables[0].columns[2].nullable);
    }

    #[test]
    fn alter_rename_column() {
        let ddl = "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
            ALTER TABLE users RENAME COLUMN name TO full_name;
        ";
        let schema = parse_schema(ddl).unwrap();
        let t = &schema.tables[0];
        assert!(t.columns.iter().any(|c| c.name == "full_name"));
        assert!(t.columns.iter().all(|c| c.name != "name"));
    }

    #[test]
    fn alter_rename_table() {
        let ddl = "
            CREATE TABLE users (id INTEGER PRIMARY KEY);
            ALTER TABLE users RENAME TO accounts;
        ";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables[0].name, "accounts");
    }

    #[test]
    fn ignores_non_create_table_statements() {
        let ddl = "
            CREATE INDEX idx_name ON users(name);
            CREATE TABLE things (id INTEGER PRIMARY KEY, label TEXT NOT NULL);
        ";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "things");
    }

    #[test]
    fn type_affinity_mapping() {
        let ddl = "
            CREATE TABLE data (
                i  INTEGER NOT NULL,
                t  TEXT    NOT NULL,
                r  REAL    NOT NULL,
                b  BLOB,
                n  NUMERIC NOT NULL
            );
        ";
        let schema = parse_schema(ddl).unwrap();
        let t = &schema.tables[0];
        let col = |name: &str| t.columns.iter().find(|c| c.name == name).unwrap();
        assert_eq!(col("i").sql_type, SqlType::Integer);
        assert_eq!(col("t").sql_type, SqlType::Text);
        assert_eq!(col("r").sql_type, SqlType::Real);
        assert_eq!(col("b").sql_type, SqlType::Bytes);
        assert_eq!(col("n").sql_type, SqlType::Decimal);
    }
}
