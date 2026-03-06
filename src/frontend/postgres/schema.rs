use sqlparser::ast::{AlterColumnOperation, AlterTableOperation, ObjectName, ObjectType, Statement};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

use crate::frontend::common::{
    build_column, build_create_table, ident_to_str, obj_name_to_str, pk_columns_from_constraint,
};
use crate::frontend::postgres::typemap;
use crate::ir::Schema;

/// Parses PostgreSQL DDL into a [Schema].
///
/// Processes `CREATE TABLE` and `ALTER TABLE` statements in order.
/// All other statements are silently ignored.
pub fn parse_schema(ddl: &str) -> anyhow::Result<Schema> {
    let dialect = PostgreSqlDialect {};
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
                for name in &names {
                    let table_name = obj_name_to_str(name);
                    tables.retain(|t| t.name != table_name);
                }
            }
            _ => {}
        }
    }

    Ok(Schema { tables })
}

// ─── ALTER TABLE ─────────────────────────────────────────────────────────────

fn apply_alter_table(
    name: &ObjectName,
    operations: &[AlterTableOperation],
    tables: &mut Vec<crate::ir::Table>,
) {
    let table_name = obj_name_to_str(name);
    let Some(idx) = tables.iter().position(|t| t.name == table_name) else {
        return; // ALTER on unknown table — ignore
    };

    for op in operations {
        let table = &mut tables[idx];
        match op {
            AlterTableOperation::AddColumn { column_def, .. } => {
                table.columns.push(build_column(column_def, typemap::map));
            }
            AlterTableOperation::DropColumn { column_name, .. } => {
                let name = ident_to_str(column_name);
                table.columns.retain(|c| c.name != name);
            }
            AlterTableOperation::AlterColumn { column_name, op } => {
                let col_name = ident_to_str(column_name);
                if let Some(col) = table.columns.iter_mut().find(|c| c.name == col_name) {
                    match op {
                        AlterColumnOperation::SetNotNull => col.nullable = false,
                        AlterColumnOperation::DropNotNull => col.nullable = true,
                        AlterColumnOperation::SetDataType { data_type, .. } => {
                            col.sql_type = typemap::map(data_type);
                        }
                        _ => {}
                    }
                }
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
            AlterTableOperation::AddConstraint(constraint) => {
                let pk_cols = pk_columns_from_constraint(constraint);
                for col in table.columns.iter_mut() {
                    if pk_cols.contains(&col.name) {
                        col.is_primary_key = true;
                        col.nullable = false;
                    }
                }
            }
            _ => {} // OWNER TO, ENABLE/DISABLE TRIGGER, etc.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::SqlType;

    #[test]
    fn parses_simple_table_with_common_types() {
        let ddl = r#"
            CREATE TABLE users (
                id      BIGSERIAL    PRIMARY KEY,
                name    TEXT         NOT NULL,
                email   VARCHAR(255) NOT NULL,
                bio     TEXT
            );
        "#;

        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 1);

        let t = &schema.tables[0];
        assert_eq!(t.name, "users");
        assert_eq!(t.columns.len(), 4);

        let id = &t.columns[0];
        assert_eq!(id.name, "id");
        assert_eq!(id.sql_type, SqlType::BigInt);
        assert!(!id.nullable);
        assert!(id.is_primary_key);

        assert_eq!(t.columns[1].name, "name");
        assert!(!t.columns[1].nullable);

        assert_eq!(t.columns[2].name, "email");
        assert!(matches!(t.columns[2].sql_type, SqlType::VarChar(_)));
        assert!(!t.columns[2].nullable);

        assert_eq!(t.columns[3].name, "bio");
        assert!(t.columns[3].nullable);
    }

    #[test]
    fn parses_table_level_primary_key() {
        let ddl = r#"
            CREATE TABLE orders (
                user_id  BIGINT  NOT NULL,
                item_id  BIGINT  NOT NULL,
                quantity INTEGER NOT NULL,
                PRIMARY KEY (user_id, item_id)
            );
        "#;

        let schema = parse_schema(ddl).unwrap();
        let t = &schema.tables[0];

        let col = |n: &str| t.columns.iter().find(|c| c.name == n).unwrap();
        assert!(col("user_id").is_primary_key);
        assert!(col("item_id").is_primary_key);
        assert!(!col("quantity").is_primary_key);
    }

    #[test]
    fn parses_multiple_tables() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);
            CREATE TABLE posts (
                id      BIGSERIAL PRIMARY KEY,
                user_id BIGINT    NOT NULL REFERENCES users(id),
                title   TEXT      NOT NULL
            );
        "#;

        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 2);
        assert_eq!(schema.tables[0].name, "users");
        assert_eq!(schema.tables[1].name, "posts");
    }

    #[test]
    fn ignores_non_create_table_statements() {
        let ddl = r#"
            CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
            CREATE INDEX idx_users_email ON users(email);
            CREATE TABLE things (id UUID PRIMARY KEY, label TEXT NOT NULL);
            CREATE SEQUENCE things_seq;
        "#;

        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "things");
    }

    #[test]
    fn parses_if_not_exists() {
        let ddl = r#"
            CREATE TABLE IF NOT EXISTS tags (
                id   SERIAL PRIMARY KEY,
                name TEXT NOT NULL
            );
        "#;

        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "tags");
    }

    #[test]
    fn parses_array_columns() {
        let ddl = r#"
            CREATE TABLE vectors (
                id   SERIAL  PRIMARY KEY,
                tags TEXT[]  NOT NULL,
                nums INTEGER[]
            );
        "#;

        let schema = parse_schema(ddl).unwrap();
        let t = &schema.tables[0];

        let col = |n: &str| t.columns.iter().find(|c| c.name == n).unwrap();
        assert!(matches!(&col("tags").sql_type, SqlType::Array(_)));
        assert!(matches!(&col("nums").sql_type, SqlType::Array(_)));
    }

    #[test]
    fn parses_generated_always_as_identity() {
        let ddl = r#"
            CREATE TABLE items (
                id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                label TEXT   NOT NULL
            );
        "#;

        let schema = parse_schema(ddl).unwrap();
        let col = &schema.tables[0].columns[0];
        assert!(!col.nullable);
    }

    // ─── ALTER TABLE tests ───────────────────────────────────────────────────

    #[test]
    fn alter_add_column() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);
            ALTER TABLE users ADD COLUMN email TEXT NOT NULL;
        "#;
        let schema = parse_schema(ddl).unwrap();
        let t = &schema.tables[0];
        assert_eq!(t.columns.len(), 3);
        let email = &t.columns[2];
        assert_eq!(email.name, "email");
        assert_eq!(email.sql_type, SqlType::Text);
        assert!(!email.nullable);
    }

    #[test]
    fn alter_add_column_if_not_exists() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY);
            ALTER TABLE users ADD COLUMN IF NOT EXISTS bio TEXT;
        "#;
        let schema = parse_schema(ddl).unwrap();
        let t = &schema.tables[0];
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.columns[1].name, "bio");
        assert!(t.columns[1].nullable);
    }

    #[test]
    fn alter_drop_column() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, bio TEXT);
            ALTER TABLE users DROP COLUMN bio;
        "#;
        let schema = parse_schema(ddl).unwrap();
        let t = &schema.tables[0];
        assert_eq!(t.columns.len(), 2);
        assert!(t.columns.iter().all(|c| c.name != "bio"));
    }

    #[test]
    fn alter_drop_column_if_exists() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);
            ALTER TABLE users DROP COLUMN IF EXISTS ghost;
        "#;
        let schema = parse_schema(ddl).unwrap();
        // ghost never existed — table unchanged
        assert_eq!(schema.tables[0].columns.len(), 2);
    }

    #[test]
    fn alter_column_set_not_null() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, bio TEXT);
            ALTER TABLE users ALTER COLUMN bio SET NOT NULL;
        "#;
        let schema = parse_schema(ddl).unwrap();
        let col = schema.tables[0].columns.iter().find(|c| c.name == "bio").unwrap();
        assert!(!col.nullable);
    }

    #[test]
    fn alter_column_drop_not_null() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);
            ALTER TABLE users ALTER COLUMN name DROP NOT NULL;
        "#;
        let schema = parse_schema(ddl).unwrap();
        let col = schema.tables[0].columns.iter().find(|c| c.name == "name").unwrap();
        assert!(col.nullable);
    }

    #[test]
    fn alter_column_type() {
        let ddl = r#"
            CREATE TABLE events (id SERIAL PRIMARY KEY, payload TEXT NOT NULL);
            ALTER TABLE events ALTER COLUMN payload TYPE JSONB;
        "#;
        let schema = parse_schema(ddl).unwrap();
        let col = schema.tables[0].columns.iter().find(|c| c.name == "payload").unwrap();
        assert_eq!(col.sql_type, SqlType::Jsonb);
    }

    #[test]
    fn alter_column_set_data_type_with_using() {
        let ddl = r#"
            CREATE TABLE items (id SERIAL PRIMARY KEY, amount TEXT NOT NULL);
            ALTER TABLE items ALTER COLUMN amount SET DATA TYPE NUMERIC USING amount::numeric;
        "#;
        let schema = parse_schema(ddl).unwrap();
        let col = schema.tables[0].columns.iter().find(|c| c.name == "amount").unwrap();
        assert_eq!(col.sql_type, SqlType::Decimal);
    }

    #[test]
    fn alter_rename_column() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);
            ALTER TABLE users RENAME COLUMN name TO full_name;
        "#;
        let schema = parse_schema(ddl).unwrap();
        let t = &schema.tables[0];
        assert!(t.columns.iter().any(|c| c.name == "full_name"));
        assert!(t.columns.iter().all(|c| c.name != "name"));
    }

    #[test]
    fn alter_rename_table() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);
            ALTER TABLE users RENAME TO accounts;
        "#;
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "accounts");
    }

    #[test]
    fn alter_add_primary_key_constraint() {
        let ddl = r#"
            CREATE TABLE orders (user_id BIGINT NOT NULL, item_id BIGINT NOT NULL);
            ALTER TABLE orders ADD CONSTRAINT orders_pkey PRIMARY KEY (user_id, item_id);
        "#;
        let schema = parse_schema(ddl).unwrap();
        let t = &schema.tables[0];
        let col = |n: &str| t.columns.iter().find(|c| c.name == n).unwrap();
        assert!(col("user_id").is_primary_key);
        assert!(!col("user_id").nullable);
        assert!(col("item_id").is_primary_key);
    }

    #[test]
    fn alter_multiple_actions_in_one_statement() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, bio TEXT);
            ALTER TABLE users
                DROP COLUMN bio,
                ADD COLUMN email TEXT NOT NULL,
                ALTER COLUMN name SET NOT NULL;
        "#;
        let schema = parse_schema(ddl).unwrap();
        let t = &schema.tables[0];
        assert!(t.columns.iter().all(|c| c.name != "bio"));
        assert!(t.columns.iter().any(|c| c.name == "email" && !c.nullable));
    }

    #[test]
    fn alter_unknown_table_is_ignored() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY);
            ALTER TABLE ghost ADD COLUMN x TEXT;
        "#;
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables[0].columns.len(), 1);
    }

    #[test]
    fn drop_table_removes_table() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);
            CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, title TEXT NOT NULL);
            DROP TABLE users;
        "#;
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "posts");
    }

    #[test]
    fn drop_table_if_exists() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY);
            DROP TABLE IF EXISTS users;
            DROP TABLE IF EXISTS ghost;
        "#;
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 0);
    }

    #[test]
    fn drop_table_multiple_names() {
        let ddl = r#"
            CREATE TABLE a (id BIGSERIAL PRIMARY KEY);
            CREATE TABLE b (id BIGSERIAL PRIMARY KEY);
            CREATE TABLE c (id BIGSERIAL PRIMARY KEY);
            DROP TABLE a, b;
        "#;
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "c");
    }

    #[test]
    fn alter_non_schema_actions_are_ignored() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);
            ALTER TABLE users
                ADD CONSTRAINT users_name_key UNIQUE (name),
                OWNER TO admin;
        "#;
        // Should parse without error; table is unchanged
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables[0].columns.len(), 2);
    }

    #[test]
    fn parses_default_constraint() {
        let ddl = r#"
            CREATE TABLE events (
                id         BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                status     TEXT      NOT NULL DEFAULT 'active'
            );
        "#;

        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables[0].columns.len(), 3);
    }
}
