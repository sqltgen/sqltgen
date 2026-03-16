use sqlparser::dialect::PostgreSqlDialect;

use crate::frontend::common::schema::parse_schema_impl;
use crate::frontend::common::AlterCaps;
use crate::frontend::postgres::typemap;
use crate::ir::Schema;

/// Parses PostgreSQL DDL into a [Schema].
///
/// Processes `CREATE TABLE`, `ALTER TABLE`, and `DROP TABLE` statements in
/// order.  All other statements are silently ignored.  Delegates to the shared
/// [`parse_schema_impl`] with the PostgreSQL dialect, full `ALTER TABLE`
/// capabilities, and the PostgreSQL type mapper.
pub(crate) fn parse_schema(ddl: &str) -> anyhow::Result<Schema> {
    parse_schema_impl(ddl, &PostgreSqlDialect {}, typemap::map, AlterCaps::ALL)
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

    // ─── Error-recovery / unsupported-statement tests ────────────────────────

    #[test]
    fn skips_create_function_with_leakproof() {
        // LEAKPROOF is a PostgreSQL function option that sqlparser-rs does not
        // support.  The schema parser should skip the function definition and
        // still produce the correct tables.
        let ddl = r#"
            CREATE OR REPLACE FUNCTION random_id()
                RETURNS bigint
                LANGUAGE plpgsql
                LEAKPROOF
                STRICT
                PARALLEL SAFE
            AS $$
            BEGIN
                RETURN ('x' || md5(random()::text))::bit(63)::bigint;
            END;
            $$;

            CREATE TABLE things (
                id   BIGINT PRIMARY KEY,
                name TEXT   NOT NULL
            );
        "#;

        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "things");
    }

    #[test]
    fn skips_unsupported_statement_between_tables() {
        // An unsupported statement in the middle should not prevent the tables
        // before and after it from being parsed.
        let ddl = r#"
            CREATE TABLE before_tbl (id BIGINT PRIMARY KEY);

            CREATE OR REPLACE FUNCTION noop()
                RETURNS void LANGUAGE plpgsql LEAKPROOF AS $$ BEGIN END; $$;

            CREATE TABLE after_tbl (id BIGINT PRIMARY KEY, val TEXT NOT NULL);
        "#;

        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 2);
        assert_eq!(schema.tables[0].name, "before_tbl");
        assert_eq!(schema.tables[1].name, "after_tbl");
        assert_eq!(schema.tables[1].columns.len(), 2);
    }

    #[test]
    fn skips_function_with_dollar_quoted_body_containing_semicolons() {
        // The function body contains semicolons — the tokenizer must treat
        // the whole $$ ... $$ as a single token so we don't split early.
        let ddl = r#"
            CREATE OR REPLACE FUNCTION multi_stmt()
                RETURNS void LANGUAGE plpgsql LEAKPROOF AS $$
            BEGIN
                INSERT INTO foo VALUES (1);
                UPDATE foo SET x = 2 WHERE id = 1;
            END;
            $$;

            CREATE TABLE real_table (id BIGINT PRIMARY KEY, data TEXT);
        "#;

        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "real_table");
    }

    #[test]
    fn test_create_function_return_type_parsed() {
        let ddl = "CREATE FUNCTION fetch_name(resource_id bigint) RETURNS text LANGUAGE sql AS $$ SELECT name FROM users WHERE id = resource_id $$;";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.functions.len(), 1);
        let f = &schema.functions[0];
        assert_eq!(f.name, "fetch_name");
        assert_eq!(f.return_type, SqlType::Text);
        assert_eq!(f.param_types, vec![SqlType::BigInt]);
    }

    #[test]
    fn test_create_function_multiple_params_parsed() {
        let ddl = "CREATE FUNCTION add_score(user_id bigint, delta integer) RETURNS bigint LANGUAGE sql AS $$ SELECT $1 $$;";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.functions.len(), 1);
        let f = &schema.functions[0];
        assert_eq!(f.param_types, vec![SqlType::BigInt, SqlType::Integer]);
        assert_eq!(f.return_type, SqlType::BigInt);
    }

    #[test]
    fn test_create_function_out_params_excluded_from_param_types() {
        let ddl = "CREATE FUNCTION stats(IN user_id bigint, OUT count bigint) RETURNS bigint LANGUAGE sql AS $$ SELECT 1 $$;";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.functions.len(), 1);
        // OUT params are return values, not inputs
        assert_eq!(schema.functions[0].param_types, vec![SqlType::BigInt]);
    }

    #[test]
    fn test_drop_function_removes_function_from_schema() {
        let ddl = "\
            CREATE FUNCTION fetch_name(resource_id bigint) RETURNS text LANGUAGE sql AS $$ SELECT '' $$;\
            DROP FUNCTION fetch_name(bigint);";
        let schema = parse_schema(ddl).unwrap();
        assert_eq!(schema.functions.len(), 0, "DROP FUNCTION should remove the function");
    }

    #[test]
    fn test_create_function_table_returning_skipped() {
        let ddl = "CREATE FUNCTION get_users() RETURNS TABLE(id bigint, name text) LANGUAGE sql AS $$ SELECT id, name FROM users $$;";
        let schema = parse_schema(ddl).unwrap();
        // Table-valued functions are skipped
        assert_eq!(schema.functions.len(), 0);
    }
}
