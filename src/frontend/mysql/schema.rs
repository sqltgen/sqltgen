use sqlparser::ast::{AlterColumnOperation, AlterTableOperation, ObjectName, ObjectType, Statement};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;

use crate::frontend::common::{apply_drop_tables, build_column, build_create_table, ident_to_str, obj_name_to_str, pk_columns_from_constraint};
use crate::frontend::mysql::typemap;
use crate::ir::Schema;

pub fn parse_schema(ddl: &str) -> anyhow::Result<Schema> {
    let dialect = MySqlDialect {};
    let stmts = Parser::parse_sql(&dialect, ddl).map_err(|e| anyhow::anyhow!("DDL parse error: {e}"))?;

    let mut tables = Vec::new();

    for stmt in stmts {
        match stmt {
            Statement::CreateTable(ct) => {
                tables.push(build_create_table(&ct.name, &ct.columns, &ct.constraints, typemap::map));
            },
            Statement::AlterTable { name, operations, .. } => {
                apply_alter_table(&name, &operations, &mut tables);
            },
            Statement::Drop { object_type: ObjectType::Table, names, .. } => {
                apply_drop_tables(&names, &mut tables);
            },
            _ => {},
        }
    }

    Ok(Schema { tables })
}

// ─── ALTER TABLE ─────────────────────────────────────────────────────────────
// MySQL supports: ADD COLUMN, DROP COLUMN, RENAME COLUMN (8.0+),
// CHANGE COLUMN (rename + retype), MODIFY COLUMN (retype), RENAME TO.
// Constraints and other operations are silently ignored.

fn apply_alter_table(name: &ObjectName, operations: &[AlterTableOperation], tables: &mut [crate::ir::Table]) {
    let table_name = obj_name_to_str(name);
    let Some(idx) = tables.iter().position(|t| t.name == table_name) else {
        return;
    };

    for op in operations {
        let table = &mut tables[idx];
        match op {
            AlterTableOperation::AddColumn { column_def, .. } => {
                table.columns.push(build_column(column_def, typemap::map));
            },
            AlterTableOperation::DropColumn { column_name, .. } => {
                let name = ident_to_str(column_name);
                table.columns.retain(|c| c.name != name);
            },
            AlterTableOperation::AlterColumn { column_name, op } => {
                let col_name = ident_to_str(column_name);
                if let Some(col) = table.columns.iter_mut().find(|c| c.name == col_name) {
                    match op {
                        AlterColumnOperation::SetNotNull => col.nullable = false,
                        AlterColumnOperation::DropNotNull => col.nullable = true,
                        AlterColumnOperation::SetDataType { data_type, .. } => {
                            col.sql_type = typemap::map(data_type);
                        },
                        _ => {},
                    }
                }
            },
            AlterTableOperation::RenameColumn { old_column_name, new_column_name } => {
                let old = ident_to_str(old_column_name);
                let new = ident_to_str(new_column_name);
                if let Some(col) = table.columns.iter_mut().find(|c| c.name == old) {
                    col.name = new;
                }
            },
            AlterTableOperation::RenameTable { table_name: new_name } => {
                table.name = obj_name_to_str(new_name);
            },
            AlterTableOperation::AddConstraint(constraint) => {
                let pk_cols = pk_columns_from_constraint(constraint);
                for col in table.columns.iter_mut() {
                    if pk_cols.contains(&col.name) {
                        col.is_primary_key = true;
                        col.nullable = false;
                    }
                }
            },
            _ => {},
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
                id    BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
                name  VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                bio   TEXT
            );
        ";
        let schema = parse_schema(ddl).unwrap();
        let t = &schema.tables[0];
        assert_eq!(t.name, "users");
        assert_eq!(t.columns.len(), 4);
        assert!(t.columns[0].is_primary_key);
        assert!(!t.columns[0].nullable);
        assert_eq!(t.columns[0].sql_type, SqlType::BigInt);
        assert!(!t.columns[1].nullable);
        assert!(matches!(t.columns[1].sql_type, SqlType::VarChar(_)));
        assert!(t.columns[3].nullable);
    }

    #[test]
    fn parses_auto_increment_primary_key() {
        let ddl = "
            CREATE TABLE items (
                id    INT  NOT NULL AUTO_INCREMENT,
                label TEXT NOT NULL,
                PRIMARY KEY (id)
            );
        ";
        let schema = parse_schema(ddl).unwrap();
        let id = &schema.tables[0].columns[0];
        assert!(id.is_primary_key);
        assert!(!id.nullable);
        assert_eq!(id.sql_type, SqlType::Integer);
    }

    #[test]
    fn parses_table_level_primary_key() {
        let ddl = "
            CREATE TABLE kv (
                k VARCHAR(255) NOT NULL,
                v TEXT         NOT NULL,
                PRIMARY KEY (k)
            );
        ";
        let schema = parse_schema(ddl).unwrap();
        let t = &schema.tables[0];
        assert!(t.columns[0].is_primary_key);
        assert!(!t.columns[1].is_primary_key);
    }

    #[test]
    fn parses_multiple_tables() {
        let ddl = "
            CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL);
            CREATE TABLE posts (
                id      BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
                user_id BIGINT       NOT NULL,
                title   VARCHAR(255) NOT NULL
            );
        ";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 2);
        assert_eq!(schema.tables[0].name, "users");
        assert_eq!(schema.tables[1].name, "posts");
    }

    #[test]
    fn parses_if_not_exists() {
        let ddl = "CREATE TABLE IF NOT EXISTS tags (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100) NOT NULL);";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables[0].name, "tags");
    }

    #[test]
    fn drop_table_removes_table() {
        let ddl = "
            CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL);
            CREATE TABLE posts (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, title VARCHAR(255) NOT NULL);
            DROP TABLE users;
        ";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "posts");
    }

    #[test]
    fn drop_table_if_exists() {
        let ddl = "
            CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY);
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
            CREATE TABLE things (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, label TEXT NOT NULL);
        ";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "things");
    }

    #[test]
    fn type_mapping() {
        let ddl = "
            CREATE TABLE data (
                a  TINYINT     NOT NULL,
                b  INT         NOT NULL,
                c  BIGINT      NOT NULL,
                d  FLOAT       NOT NULL,
                e  DOUBLE      NOT NULL,
                f  DECIMAL(10,2) NOT NULL,
                g  VARCHAR(50) NOT NULL,
                h  TEXT,
                i  BLOB,
                j  DATE        NOT NULL,
                k  DATETIME    NOT NULL,
                l  JSON,
                m  BOOLEAN     NOT NULL
            );
        ";
        let schema = parse_schema(ddl).unwrap();
        let t = &schema.tables[0];
        let col = |name: &str| t.columns.iter().find(|c| c.name == name).unwrap();
        assert_eq!(col("a").sql_type, SqlType::SmallInt);
        assert_eq!(col("b").sql_type, SqlType::Integer);
        assert_eq!(col("c").sql_type, SqlType::BigInt);
        assert_eq!(col("d").sql_type, SqlType::Real);
        assert_eq!(col("e").sql_type, SqlType::Double);
        assert_eq!(col("f").sql_type, SqlType::Decimal);
        assert!(matches!(col("g").sql_type, SqlType::VarChar(_)));
        assert_eq!(col("h").sql_type, SqlType::Text);
        assert_eq!(col("i").sql_type, SqlType::Bytes);
        assert_eq!(col("j").sql_type, SqlType::Date);
        assert_eq!(col("k").sql_type, SqlType::Timestamp);
        assert_eq!(col("l").sql_type, SqlType::Json);
        assert_eq!(col("m").sql_type, SqlType::Boolean);
    }

    #[test]
    fn alter_add_column() {
        let ddl = "
            CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL);
            ALTER TABLE users ADD COLUMN bio TEXT;
        ";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables[0].columns.len(), 3);
        assert_eq!(schema.tables[0].columns[2].name, "bio");
        assert!(schema.tables[0].columns[2].nullable);
    }

    #[test]
    fn alter_drop_column() {
        let ddl = "
            CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL, bio TEXT);
            ALTER TABLE users DROP COLUMN bio;
        ";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables[0].columns.len(), 2);
        assert!(schema.tables[0].columns.iter().all(|c| c.name != "bio"));
    }

    #[test]
    fn alter_rename_column() {
        let ddl = "
            CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL);
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
            CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY);
            ALTER TABLE users RENAME TO accounts;
        ";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables[0].name, "accounts");
    }

    #[test]
    fn alter_unknown_table_is_ignored() {
        let ddl = "
            CREATE TABLE users (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY);
            ALTER TABLE ghost ADD COLUMN x TEXT;
        ";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables[0].columns.len(), 1);
    }

    #[test]
    fn alter_add_primary_key_constraint() {
        let ddl = "
            CREATE TABLE orders (user_id BIGINT NOT NULL, item_id BIGINT NOT NULL);
            ALTER TABLE orders ADD CONSTRAINT orders_pkey PRIMARY KEY (user_id, item_id);
        ";
        let schema = parse_schema(ddl).unwrap();
        let t = &schema.tables[0];
        let col = |n: &str| t.columns.iter().find(|c| c.name == n).unwrap();
        assert!(col("user_id").is_primary_key);
        assert!(!col("user_id").nullable);
        assert!(col("item_id").is_primary_key);
    }
}
