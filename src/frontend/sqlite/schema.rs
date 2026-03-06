use sqlparser::ast::{AlterTableOperation, ObjectName, ObjectType, Statement};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

use crate::frontend::common::{
    apply_drop_tables, build_column, build_create_table, ident_to_str, obj_name_to_str,
};
use crate::frontend::sqlite::typemap;
use crate::ir::Schema;

pub fn parse_schema(ddl: &str) -> anyhow::Result<Schema> {
    let dialect = SQLiteDialect {};
    let stmts = Parser::parse_sql(&dialect, ddl)
        .map_err(|e| anyhow::anyhow!("DDL parse error: {e}"))?;

    let mut tables = Vec::new();

    for stmt in stmts {
        match stmt {
            Statement::CreateTable(ct) => {
                tables.push(build_create_table(&ct.name, &ct.columns, &ct.constraints, typemap::map));
            }
            Statement::AlterTable { name, operations, .. } => {
                apply_alter_table(&name, &operations, &mut tables);
            }
            Statement::Drop { object_type: ObjectType::Table, names, .. } => {
                apply_drop_tables(&names, &mut tables);
            }
            _ => {}
        }
    }

    Ok(Schema { tables })
}

// ─── ALTER TABLE ─────────────────────────────────────────────────────────────
// SQLite supports only: RENAME TO, RENAME COLUMN, ADD COLUMN.
// Everything else is silently ignored.

fn apply_alter_table(
    name: &ObjectName,
    operations: &[AlterTableOperation],
    tables: &mut Vec<crate::ir::Table>,
) {
    let table_name = obj_name_to_str(name);
    let Some(idx) = tables.iter().position(|t| t.name == table_name) else {
        return;
    };

    for op in operations {
        let table = &mut tables[idx];
        match op {
            AlterTableOperation::AddColumn { column_def, .. } => {
                table.columns.push(build_column(column_def, typemap::map));
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
    fn drop_table_removes_table() {
        let ddl = "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
            CREATE TABLE posts (id INTEGER PRIMARY KEY, title TEXT NOT NULL);
            DROP TABLE users;
        ";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "posts");
    }

    #[test]
    fn drop_table_if_exists() {
        let ddl = "
            CREATE TABLE users (id INTEGER PRIMARY KEY);
            DROP TABLE IF EXISTS users;
            DROP TABLE IF EXISTS ghost;
        ";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 0);
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
