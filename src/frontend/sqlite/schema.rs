use sqlparser::dialect::SQLiteDialect;

use crate::frontend::common::query::ResolverConfig;
use crate::frontend::common::schema::parse_schema_impl;
use crate::frontend::common::{AlterCaps, DdlDialect};
use crate::frontend::sqlite::typemap;
use crate::ir::Schema;

/// Parses SQLite DDL into a [Schema].
///
/// Processes `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, and `CREATE VIEW`
/// statements. Delegates to the shared [`parse_schema_impl`] with the SQLite
/// dialect, limited `ALTER TABLE` capabilities, and the SQLite type mapper and
/// resolver config.
pub(crate) fn parse_schema(ddl: &str) -> anyhow::Result<Schema> {
    parse_schema_impl(
        ddl,
        &SQLiteDialect {},
        DdlDialect { map_type: typemap::map, alter_caps: AlterCaps::SQLITE },
        &ResolverConfig { typemap: typemap::map, ..ResolverConfig::default() },
    )
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
    fn drop_table_multiple_names() {
        let ddl = "
            CREATE TABLE a (id INTEGER PRIMARY KEY);
            CREATE TABLE b (id INTEGER PRIMARY KEY);
            CREATE TABLE c (id INTEGER PRIMARY KEY);
            DROP TABLE a, b;
        ";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "c");
    }

    #[test]
    fn alter_unknown_table_is_ignored() {
        let ddl = "
            CREATE TABLE users (id INTEGER PRIMARY KEY);
            ALTER TABLE ghost ADD COLUMN x TEXT;
        ";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables[0].columns.len(), 1);
    }

    #[test]
    fn parses_default_constraint() {
        let ddl = "
            CREATE TABLE events (
                id         INTEGER PRIMARY KEY,
                created_at TEXT    NOT NULL DEFAULT (datetime('now')),
                status     TEXT    NOT NULL DEFAULT 'active'
            );
        ";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables[0].columns.len(), 3);
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
        // SQLite has no fixed-point type; NUMERIC maps to Double (REAL affinity).
        assert_eq!(col("n").sql_type, SqlType::Double);
    }

    // ─── CREATE VIEW tests ───────────────────────────────────────────────────

    #[test]
    fn test_create_view_registers_as_view_kind() {
        let ddl = "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
            CREATE VIEW user_names AS SELECT id, name FROM users;
        ";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 2);
        let view = schema.tables.iter().find(|t| t.name == "user_names").unwrap();
        assert!(view.is_view());
        assert!(!schema.tables.iter().find(|t| t.name == "users").unwrap().is_view());
    }

    #[test]
    fn test_create_view_columns_inferred() {
        let ddl = "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
            CREATE VIEW user_names AS SELECT id, name FROM users;
        ";
        let schema = parse_schema(ddl).unwrap();
        let view = schema.tables.iter().find(|t| t.name == "user_names").unwrap();
        assert_eq!(view.columns.len(), 2);
        assert_eq!(view.columns[0].name, "id");
        assert_eq!(view.columns[0].sql_type, SqlType::Integer);
        assert_eq!(view.columns[1].name, "name");
        assert_eq!(view.columns[1].sql_type, SqlType::Text);
    }

    #[test]
    fn test_create_view_unknown_table_fallback() {
        let ddl = "CREATE VIEW orphan AS SELECT id FROM ghost;";
        let schema = parse_schema(ddl).unwrap();
        let view = schema.tables.iter().find(|t| t.name == "orphan").unwrap();
        assert!(view.is_view());
        assert!(view.columns.is_empty());
    }

    #[test]
    fn test_create_view_references_another_view() {
        let ddl = "
            CREATE TABLE items (id INTEGER PRIMARY KEY, qty INTEGER NOT NULL);
            CREATE VIEW base AS SELECT id, qty FROM items;
            CREATE VIEW derived AS SELECT id FROM base;
        ";
        let schema = parse_schema(ddl).unwrap();
        let derived = schema.tables.iter().find(|t| t.name == "derived").unwrap();
        assert!(derived.is_view());
        assert_eq!(derived.columns.len(), 1);
        assert_eq!(derived.columns[0].name, "id");
        assert_eq!(derived.columns[0].sql_type, SqlType::Integer);
    }

    #[test]
    fn test_drop_view_removes_from_schema() {
        let ddl = "
            CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL);
            CREATE VIEW user_names AS SELECT id, name FROM users;
            DROP VIEW user_names;
        ";
        let schema = parse_schema(ddl).unwrap();
        assert!(schema.tables.iter().any(|t| t.name == "users" && !t.is_view()));
        assert!(schema.tables.iter().all(|t| t.name != "user_names"));
    }
}
