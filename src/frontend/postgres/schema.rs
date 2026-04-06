use sqlparser::dialect::PostgreSqlDialect;

use crate::frontend::common::query::ResolverConfig;
use crate::frontend::common::schema::parse_schema_impl;
use crate::frontend::common::{AlterCaps, DdlDialect};
use crate::frontend::postgres::typemap;
use crate::ir::{Schema, SqlType};

/// Parses PostgreSQL DDL into a [Schema].
///
/// Processes `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, `CREATE FUNCTION`,
/// `DROP FUNCTION`, and `CREATE VIEW` statements. Delegates to the shared
/// [`parse_schema_impl`] with the PostgreSQL dialect, full `ALTER TABLE`
/// capabilities, and the PostgreSQL type mapper and resolver config.
pub(crate) fn parse_schema(ddl: &str, default_schema: Option<&str>) -> anyhow::Result<Schema> {
    let ds = default_schema.unwrap_or("public");
    parse_schema_impl(
        ddl,
        &PostgreSqlDialect {},
        DdlDialect { map_type: typemap::map, alter_caps: AlterCaps::ALL },
        &ResolverConfig {
            typemap: typemap::map,
            sum_bigint_type: SqlType::Decimal,
            avg_integer_type: SqlType::Decimal,
            default_schema: Some(ds.to_string()),
            ..ResolverConfig::default()
        },
    )
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

        let schema = parse_schema(ddl, None).unwrap();
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

        let schema = parse_schema(ddl, None).unwrap();
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

        let schema = parse_schema(ddl, None).unwrap();
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

        let schema = parse_schema(ddl, None).unwrap();
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

        let schema = parse_schema(ddl, None).unwrap();
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

        let schema = parse_schema(ddl, None).unwrap();
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

        let schema = parse_schema(ddl, None).unwrap();
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
        let schema = parse_schema(ddl, None).unwrap();
        let t = &schema.tables[0];
        assert_eq!(t.columns.len(), 3);
        let email = &t.columns[2];
        assert_eq!(email.name, "email");
        assert_eq!(email.sql_type, SqlType::Text);
        assert!(!email.nullable);
    }

    #[test]
    fn alter_unqualified_matches_public_qualified_table() {
        let ddl = r#"
            CREATE TABLE public.users (id BIGSERIAL PRIMARY KEY);
            ALTER TABLE users ADD COLUMN email TEXT;
        "#;
        let schema = parse_schema(ddl, None).unwrap();
        let t = &schema.tables[0];
        assert_eq!(t.schema.as_deref(), Some("public"));
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.columns[1].name, "email");
    }

    #[test]
    fn drop_unqualified_matches_public_qualified_table() {
        let ddl = r#"
            CREATE TABLE public.users (id BIGSERIAL PRIMARY KEY);
            DROP TABLE users;
        "#;
        let schema = parse_schema(ddl, None).unwrap();
        assert!(schema.tables.is_empty());
    }

    #[test]
    fn alter_add_column_if_not_exists() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY);
            ALTER TABLE users ADD COLUMN IF NOT EXISTS bio TEXT;
        "#;
        let schema = parse_schema(ddl, None).unwrap();
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
        let schema = parse_schema(ddl, None).unwrap();
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
        let schema = parse_schema(ddl, None).unwrap();
        // ghost never existed — table unchanged
        assert_eq!(schema.tables[0].columns.len(), 2);
    }

    #[test]
    fn alter_column_set_not_null() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, bio TEXT);
            ALTER TABLE users ALTER COLUMN bio SET NOT NULL;
        "#;
        let schema = parse_schema(ddl, None).unwrap();
        let col = schema.tables[0].columns.iter().find(|c| c.name == "bio").unwrap();
        assert!(!col.nullable);
    }

    #[test]
    fn alter_column_drop_not_null() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);
            ALTER TABLE users ALTER COLUMN name DROP NOT NULL;
        "#;
        let schema = parse_schema(ddl, None).unwrap();
        let col = schema.tables[0].columns.iter().find(|c| c.name == "name").unwrap();
        assert!(col.nullable);
    }

    #[test]
    fn alter_column_type() {
        let ddl = r#"
            CREATE TABLE events (id SERIAL PRIMARY KEY, payload TEXT NOT NULL);
            ALTER TABLE events ALTER COLUMN payload TYPE JSONB;
        "#;
        let schema = parse_schema(ddl, None).unwrap();
        let col = schema.tables[0].columns.iter().find(|c| c.name == "payload").unwrap();
        assert_eq!(col.sql_type, SqlType::Jsonb);
    }

    #[test]
    fn alter_column_set_data_type_with_using() {
        let ddl = r#"
            CREATE TABLE items (id SERIAL PRIMARY KEY, amount TEXT NOT NULL);
            ALTER TABLE items ALTER COLUMN amount SET DATA TYPE NUMERIC USING amount::numeric;
        "#;
        let schema = parse_schema(ddl, None).unwrap();
        let col = schema.tables[0].columns.iter().find(|c| c.name == "amount").unwrap();
        assert_eq!(col.sql_type, SqlType::Decimal);
    }

    #[test]
    fn alter_rename_column() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);
            ALTER TABLE users RENAME COLUMN name TO full_name;
        "#;
        let schema = parse_schema(ddl, None).unwrap();
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
        let schema = parse_schema(ddl, None).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "accounts");
    }

    #[test]
    fn alter_add_primary_key_constraint() {
        let ddl = r#"
            CREATE TABLE orders (user_id BIGINT NOT NULL, item_id BIGINT NOT NULL);
            ALTER TABLE orders ADD CONSTRAINT orders_pkey PRIMARY KEY (user_id, item_id);
        "#;
        let schema = parse_schema(ddl, None).unwrap();
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
        let schema = parse_schema(ddl, None).unwrap();
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
        let schema = parse_schema(ddl, None).unwrap();
        assert_eq!(schema.tables[0].columns.len(), 1);
    }

    #[test]
    fn drop_table_removes_table() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);
            CREATE TABLE posts (id BIGSERIAL PRIMARY KEY, title TEXT NOT NULL);
            DROP TABLE users;
        "#;
        let schema = parse_schema(ddl, None).unwrap();
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
        let schema = parse_schema(ddl, None).unwrap();
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
        let schema = parse_schema(ddl, None).unwrap();
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
        let schema = parse_schema(ddl, None).unwrap();
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

        let schema = parse_schema(ddl, None).unwrap();
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

        let schema = parse_schema(ddl, None).unwrap();
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

        let schema = parse_schema(ddl, None).unwrap();
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

        let schema = parse_schema(ddl, None).unwrap();
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "real_table");
    }

    #[test]
    fn test_create_function_return_type_parsed() {
        let ddl = "CREATE FUNCTION fetch_name(resource_id bigint) RETURNS text LANGUAGE sql AS $$ SELECT name FROM users WHERE id = resource_id $$;";
        let schema = parse_schema(ddl, None).unwrap();
        assert_eq!(schema.functions.len(), 1);
        let f = &schema.functions[0];
        assert_eq!(f.name, "fetch_name");
        assert_eq!(f.return_type, SqlType::Text);
        assert_eq!(f.param_types, vec![SqlType::BigInt]);
    }

    #[test]
    fn test_create_function_multiple_params_parsed() {
        let ddl = "CREATE FUNCTION add_score(user_id bigint, delta integer) RETURNS bigint LANGUAGE sql AS $$ SELECT $1 $$;";
        let schema = parse_schema(ddl, None).unwrap();
        assert_eq!(schema.functions.len(), 1);
        let f = &schema.functions[0];
        assert_eq!(f.param_types, vec![SqlType::BigInt, SqlType::Integer]);
        assert_eq!(f.return_type, SqlType::BigInt);
    }

    #[test]
    fn test_create_function_out_params_excluded_from_param_types() {
        let ddl = "CREATE FUNCTION stats(IN user_id bigint, OUT count bigint) RETURNS bigint LANGUAGE sql AS $$ SELECT 1 $$;";
        let schema = parse_schema(ddl, None).unwrap();
        assert_eq!(schema.functions.len(), 1);
        // OUT params are return values, not inputs
        assert_eq!(schema.functions[0].param_types, vec![SqlType::BigInt]);
    }

    #[test]
    fn test_create_or_replace_function_replaces_existing() {
        let ddl = "\
            CREATE FUNCTION fetch_name(resource_id bigint) RETURNS text LANGUAGE sql AS $$ SELECT '' $$;\
            CREATE OR REPLACE FUNCTION fetch_name(resource_id bigint) RETURNS bigint LANGUAGE sql AS $$ SELECT 1 $$;";
        let schema = parse_schema(ddl, None).unwrap();
        assert_eq!(schema.functions.len(), 1, "OR REPLACE must not duplicate the function");
        assert_eq!(schema.functions[0].return_type, SqlType::BigInt, "OR REPLACE must update the return type");
    }

    #[test]
    fn test_drop_function_removes_function_from_schema() {
        let ddl = "\
            CREATE FUNCTION fetch_name(resource_id bigint) RETURNS text LANGUAGE sql AS $$ SELECT '' $$;\
            DROP FUNCTION fetch_name(bigint);";
        let schema = parse_schema(ddl, None).unwrap();
        assert_eq!(schema.functions.len(), 0, "DROP FUNCTION should remove the function");
    }

    #[test]
    fn test_create_function_table_returning_not_scalar() {
        let ddl = "CREATE FUNCTION get_users() RETURNS TABLE(id bigint, name text) LANGUAGE sql AS $$ SELECT id, name FROM users $$;";
        let schema = parse_schema(ddl, None).unwrap();
        // Table-valued functions are not scalar functions.
        assert_eq!(schema.functions.len(), 0);
    }

    // ─── Table-valued function tests ─────────────────────────────────────────

    #[test]
    fn test_tvf_returns_table_registers_as_view() {
        let ddl = "CREATE FUNCTION get_users() RETURNS TABLE(id BIGINT, name TEXT) LANGUAGE sql AS $$ SELECT id, name FROM users $$;";
        let schema = parse_schema(ddl, None).unwrap();
        assert_eq!(schema.tables.len(), 1);
        let t = &schema.tables[0];
        assert_eq!(t.name, "get_users");
        assert!(t.is_view(), "TVF must be registered as a view");
        assert_eq!(t.columns.len(), 2);
        assert_eq!(t.columns[0].name, "id");
        assert_eq!(t.columns[0].sql_type, SqlType::BigInt);
        assert_eq!(t.columns[1].name, "name");
        assert_eq!(t.columns[1].sql_type, SqlType::Text);
    }

    #[test]
    fn test_tvf_or_replace_replaces_existing() {
        let ddl = "\
            CREATE FUNCTION get_users() RETURNS TABLE(id BIGINT) LANGUAGE sql AS $$ SELECT 1 $$;\
            CREATE OR REPLACE FUNCTION get_users() RETURNS TABLE(id BIGINT, name TEXT) LANGUAGE sql AS $$ SELECT 1, '' $$;";
        let schema = parse_schema(ddl, None).unwrap();
        assert_eq!(schema.tables.len(), 1, "OR REPLACE must not duplicate the TVF");
        assert_eq!(schema.tables[0].columns.len(), 2, "OR REPLACE must update columns");
    }

    #[test]
    fn test_tvf_coexists_with_tables_and_scalar_functions() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);
            CREATE FUNCTION user_count() RETURNS bigint LANGUAGE sql AS $$ SELECT COUNT(*) FROM users $$;
            CREATE FUNCTION get_active() RETURNS TABLE(id BIGINT, name TEXT) LANGUAGE sql AS $$ SELECT id, name FROM users $$;
        "#;
        let schema = parse_schema(ddl, None).unwrap();
        // 1 base table + 1 TVF (registered as view)
        assert_eq!(schema.tables.len(), 2);
        assert!(!schema.tables[0].is_view());
        assert!(schema.tables[1].is_view());
        assert_eq!(schema.tables[1].name, "get_active");
        // 1 scalar function
        assert_eq!(schema.functions.len(), 1);
    }

    #[test]
    fn test_tvf_query_resolves_column_types() {
        // End-to-end: define a TVF, then use it in a query — columns should resolve.
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);
            CREATE FUNCTION active_users() RETURNS TABLE(id BIGINT, name TEXT) LANGUAGE sql AS $$ SELECT id, name FROM users $$;
        "#;
        let schema = parse_schema(ddl, None).unwrap();
        let tvf = schema.tables.iter().find(|t| t.name == "active_users").unwrap();
        assert_eq!(tvf.columns.len(), 2);
        assert_eq!(tvf.columns[0].sql_type, SqlType::BigInt);
        assert_eq!(tvf.columns[1].sql_type, SqlType::Text);
    }

    #[test]
    fn test_tvf_returns_setof_skipped_by_error_recovery() {
        // sqlparser 0.61 cannot parse RETURNS SETOF — the error-recovery
        // loop skips it and continues parsing subsequent statements.
        let ddl = r#"
            CREATE FUNCTION get_users() RETURNS SETOF users LANGUAGE sql AS $$ SELECT * FROM users $$;
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);
        "#;
        let schema = parse_schema(ddl, None).unwrap();
        // The RETURNS SETOF function is skipped; only the table is parsed.
        assert_eq!(schema.tables.len(), 1);
        assert_eq!(schema.tables[0].name, "users");
    }

    // ─── CREATE VIEW tests ───────────────────────────────────────────────────

    #[test]
    fn test_create_view_registers_as_view_kind() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);
            CREATE VIEW user_names AS SELECT id, name FROM users;
        "#;
        let schema = parse_schema(ddl, None).unwrap();
        // One base table, one view — both appear in schema.tables.
        assert_eq!(schema.tables.len(), 2);
        let view = schema.tables.iter().find(|t| t.name == "user_names").unwrap();
        assert!(view.is_view(), "CREATE VIEW must produce a view-kind entry");
        let table = schema.tables.iter().find(|t| t.name == "users").unwrap();
        assert!(!table.is_view(), "base table must not be flagged as view");
    }

    #[test]
    fn test_create_view_columns_inferred_from_select() {
        let ddl = r#"
            CREATE TABLE users (
                id   BIGSERIAL PRIMARY KEY,
                name TEXT      NOT NULL,
                bio  TEXT
            );
            CREATE VIEW active_users AS SELECT id, name FROM users;
        "#;
        let schema = parse_schema(ddl, None).unwrap();
        let view = schema.tables.iter().find(|t| t.name == "active_users").unwrap();
        assert_eq!(view.columns.len(), 2);
        assert_eq!(view.columns[0].name, "id");
        assert_eq!(view.columns[0].sql_type, SqlType::BigInt);
        assert_eq!(view.columns[1].name, "name");
        assert_eq!(view.columns[1].sql_type, SqlType::Text);
        // `bio` is NOT in the view
        assert!(view.columns.iter().all(|c| c.name != "bio"));
    }

    #[test]
    fn test_create_view_wildcard_expands_to_all_columns() {
        let ddl = r#"
            CREATE TABLE products (
                id    BIGSERIAL PRIMARY KEY,
                label TEXT      NOT NULL,
                price NUMERIC   NOT NULL
            );
            CREATE VIEW all_products AS SELECT * FROM products;
        "#;
        let schema = parse_schema(ddl, None).unwrap();
        let view = schema.tables.iter().find(|t| t.name == "all_products").unwrap();
        assert_eq!(view.columns.len(), 3);
        assert_eq!(view.columns[0].name, "id");
        assert_eq!(view.columns[1].name, "label");
        assert_eq!(view.columns[2].name, "price");
    }

    #[test]
    fn test_create_view_references_another_view() {
        // Second view references the first — pass-2 ordering must handle this.
        let ddl = r#"
            CREATE TABLE orders (
                id     BIGSERIAL PRIMARY KEY,
                amount NUMERIC   NOT NULL
            );
            CREATE VIEW base_view AS SELECT id, amount FROM orders;
            CREATE VIEW derived_view AS SELECT id FROM base_view;
        "#;
        let schema = parse_schema(ddl, None).unwrap();
        let derived = schema.tables.iter().find(|t| t.name == "derived_view").unwrap();
        assert!(derived.is_view());
        assert_eq!(derived.columns.len(), 1);
        assert_eq!(derived.columns[0].name, "id");
        assert_eq!(derived.columns[0].sql_type, SqlType::BigInt);
    }

    #[test]
    fn test_create_view_unknown_table_fallback_to_empty_columns() {
        // A view that references a table not in the schema falls back to
        // an empty column list — the view is registered but untyped.
        let ddl = r#"
            CREATE VIEW orphan_view AS SELECT id, name FROM nonexistent_table;
        "#;
        let schema = parse_schema(ddl, None).unwrap();
        let view = schema.tables.iter().find(|t| t.name == "orphan_view").unwrap();
        assert!(view.is_view());
        assert!(view.columns.is_empty(), "view with unknown source table must have no inferred columns");
    }

    #[test]
    fn test_create_view_coexists_with_base_tables_and_functions() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);
            CREATE FUNCTION user_count() RETURNS bigint LANGUAGE sql AS $$ SELECT COUNT(*) FROM users $$;
            CREATE VIEW user_names AS SELECT id, name FROM users;
        "#;
        let schema = parse_schema(ddl, None).unwrap();
        assert_eq!(schema.tables.iter().filter(|t| !t.is_view()).count(), 1);
        assert_eq!(schema.tables.iter().filter(|t| t.is_view()).count(), 1);
        assert_eq!(schema.functions.len(), 1);
    }

    #[test]
    fn test_drop_view_removes_from_schema() {
        let ddl = r#"
            CREATE TABLE users (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);
            CREATE VIEW user_names AS SELECT id, name FROM users;
            DROP VIEW user_names;
        "#;
        let schema = parse_schema(ddl, None).unwrap();
        assert!(schema.tables.iter().any(|t| t.name == "users" && !t.is_view()));
        assert!(schema.tables.iter().all(|t| t.name != "user_names"));
    }
}
