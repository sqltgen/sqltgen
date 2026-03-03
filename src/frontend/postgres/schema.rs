use pest::Parser;
use pest_derive::Parser;

use crate::ir::{Column, Schema, SqlType, Table};
use crate::frontend::postgres::typemap;

#[derive(Parser)]
#[grammar = "frontend/postgres/ddl.pest"]
struct DdlParser;

/// Parses PostgreSQL DDL into a [Schema].
///
/// Processes `CREATE TABLE` and `ALTER TABLE` statements in order.
/// All other statements are silently ignored.
pub fn parse_schema(ddl: &str) -> anyhow::Result<Schema> {
    let pairs = DdlParser::parse(Rule::script, ddl)
        .map_err(|e| anyhow::anyhow!("DDL parse error: {e}"))?;

    let mut tables: Vec<Table> = Vec::new();

    for pair in pairs.flatten() {
        match pair.as_rule() {
            Rule::create_table_stmt => {
                if let Some(table) = parse_create_table(pair) {
                    tables.push(table);
                }
            }
            Rule::alter_table_stmt => apply_alter_table(pair, &mut tables),
            _ => {}
        }
    }

    Ok(Schema { tables })
}

// ─── ALTER TABLE ─────────────────────────────────────────────────────────────

fn apply_alter_table(pair: pest::iterators::Pair<Rule>, tables: &mut Vec<Table>) {
    let mut inner = pair.into_inner();

    let table_ref = match inner.next() {
        Some(p) if p.as_rule() == Rule::table_ref => p,
        _ => return,
    };
    let table_name = extract_table_name(table_ref);

    let Some(idx) = tables.iter().position(|t| t.name == table_name) else {
        return; // ALTER on unknown table — ignore
    };

    for action_pair in inner {
        if action_pair.as_rule() != Rule::alter_action {
            continue;
        }
        let Some(child) = action_pair.into_inner().next() else { continue };
        let table = &mut tables[idx];
        match child.as_rule() {
            Rule::add_column_action    => action_add_column(child, table),
            Rule::drop_column_action   => action_drop_column(child, table),
            Rule::alter_column_action  => action_alter_column(child, table),
            Rule::rename_column_action => action_rename_column(child, table),
            Rule::rename_table_action  => action_rename_table(child, table),
            Rule::add_pk_action        => action_add_pk(child, table),
            _ => {}
        }
    }
}

fn action_add_column(pair: pest::iterators::Pair<Rule>, table: &mut Table) {
    for child in pair.into_inner() {
        if child.as_rule() == Rule::column_def {
            if let Some(col) = parse_column_def(child) {
                table.columns.push(col);
            }
            return;
        }
    }
}

fn action_drop_column(pair: pest::iterators::Pair<Rule>, table: &mut Table) {
    for child in pair.into_inner() {
        if child.as_rule() == Rule::identifier {
            let name = unquote(child.as_str());
            table.columns.retain(|c| c.name != name);
            return;
        }
    }
}

fn action_alter_column(pair: pest::iterators::Pair<Rule>, table: &mut Table) {
    let mut inner = pair.into_inner();

    let col_name = match inner.next() {
        Some(p) if p.as_rule() == Rule::identifier => unquote(p.as_str()),
        _ => return,
    };
    let subaction = match inner.next() {
        Some(p) if p.as_rule() == Rule::alter_column_subaction => p,
        _ => return,
    };
    let Some(child) = subaction.into_inner().next() else { return };
    let Some(col) = table.columns.iter_mut().find(|c| c.name == col_name) else { return };

    match child.as_rule() {
        Rule::set_not_null_action  => col.nullable = false,
        Rule::drop_not_null_action => col.nullable = true,
        Rule::set_type_action => {
            for sub in child.into_inner() {
                if sub.as_rule() == Rule::data_type {
                    let (new_type, is_array) = parse_data_type(sub);
                    col.sql_type = if is_array {
                        SqlType::Array(Box::new(new_type))
                    } else {
                        new_type
                    };
                    return;
                }
            }
        }
        _ => {}
    }
}

fn action_rename_column(pair: pest::iterators::Pair<Rule>, table: &mut Table) {
    let ids: Vec<_> = pair.into_inner()
        .filter(|p| p.as_rule() == Rule::identifier)
        .collect();
    if ids.len() == 2 {
        let old = unquote(ids[0].as_str());
        let new = unquote(ids[1].as_str());
        if let Some(col) = table.columns.iter_mut().find(|c| c.name == old) {
            col.name = new;
        }
    }
}

fn action_rename_table(pair: pest::iterators::Pair<Rule>, table: &mut Table) {
    for child in pair.into_inner() {
        if child.as_rule() == Rule::identifier {
            table.name = unquote(child.as_str());
            return;
        }
    }
}

fn action_add_pk(pair: pest::iterators::Pair<Rule>, table: &mut Table) {
    let pk_names: Vec<String> = pair.into_inner()
        .find(|p| p.as_rule() == Rule::identifier_list)
        .map(|id_list| {
            id_list.into_inner()
                .filter(|p| p.as_rule() == Rule::identifier)
                .map(|p| unquote(p.as_str()))
                .collect()
        })
        .unwrap_or_default();

    for col in table.columns.iter_mut() {
        if pk_names.contains(&col.name) {
            col.is_primary_key = true;
            col.nullable = false;
        }
    }
}

fn parse_create_table(pair: pest::iterators::Pair<Rule>) -> Option<Table> {
    let mut inner = pair.into_inner();

    // table_ref is first child
    let table_ref = inner.next()?;
    let table_name = extract_table_name(table_ref);

    let mut columns: Vec<Column> = Vec::new();
    let mut table_pk_cols: Vec<String> = Vec::new();

    for item in inner {
        match item.as_rule() {
            Rule::col_or_constraint_list => {
                for coc in item.into_inner() {
                    if coc.as_rule() != Rule::col_or_constraint {
                        continue;
                    }
                    if let Some(child) = coc.into_inner().next() {
                        match child.as_rule() {
                            Rule::column_def => {
                                if let Some(col) = parse_column_def(child) {
                                    columns.push(col);
                                }
                            }
                            Rule::table_constraint => {
                                collect_table_pk(child, &mut table_pk_cols);
                            }
                            _ => {}
                        }
                    }
                }
            }
            _ => {}
        }
    }

    // Promote columns that appear in a table-level PRIMARY KEY
    let columns = columns
        .into_iter()
        .map(|mut col| {
            if table_pk_cols.contains(&col.name) {
                col.is_primary_key = true;
                col.nullable = false;
            }
            col
        })
        .collect();

    Some(Table { name: table_name, columns })
}

fn extract_table_name(pair: pest::iterators::Pair<Rule>) -> String {
    // table_ref = { (identifier ~ ".")? ~ identifier }
    // last identifier is the table name
    pair.into_inner()
        .filter(|p| p.as_rule() == Rule::identifier)
        .last()
        .map(|p| unquote(p.as_str()))
        .unwrap_or_default()
}

fn parse_column_def(pair: pest::iterators::Pair<Rule>) -> Option<Column> {
    let mut inner = pair.into_inner();

    let name_pair = inner.next()?;
    let name = unquote(name_pair.as_str());

    let data_type_pair = inner.next()?;
    let (sql_type, is_array) = parse_data_type(data_type_pair);

    let mut nullable = true;
    let mut is_primary_key = false;

    for constraint in inner {
        if constraint.as_rule() != Rule::column_constraint {
            continue;
        }
        let child = match constraint.into_inner().next() {
            Some(c) => c,
            None => continue,
        };
        match child.as_rule() {
            Rule::not_null => nullable = false,
            Rule::primary_key_col => {
                is_primary_key = true;
                nullable = false;
            }
            Rule::generated_col => nullable = false,
            Rule::named_col_constraint => {
                // Recurse into the inner constraint
                if let Some(inner_c) = child.into_inner().nth(1) {
                    match inner_c.as_rule() {
                        Rule::not_null => nullable = false,
                        Rule::primary_key_col => {
                            is_primary_key = true;
                            nullable = false;
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    let final_type = if is_array {
        SqlType::Array(Box::new(sql_type))
    } else {
        sql_type
    };

    Some(Column { name, sql_type: final_type, nullable, is_primary_key })
}

fn parse_data_type(pair: pest::iterators::Pair<Rule>) -> (SqlType, bool) {
    let mut pg_type_text = String::new();
    let mut array_count = 0usize;

    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::pg_type => pg_type_text = child.as_str().to_string(),
            Rule::array_suffix => array_count += 1,
            _ => {}
        }
    }

    let base = typemap::map(&pg_type_text);
    (base, array_count > 0)
}

fn collect_table_pk(pair: pest::iterators::Pair<Rule>, out: &mut Vec<String>) {
    for child in pair.into_inner() {
        match child.as_rule() {
            Rule::table_primary_key => {
                for id_pair in child.into_inner() {
                    if id_pair.as_rule() == Rule::identifier_list {
                        for id in id_pair.into_inner() {
                            if id.as_rule() == Rule::identifier {
                                out.push(unquote(id.as_str()));
                            }
                        }
                    }
                }
            }
            Rule::named_table_constraint => {
                // CONSTRAINT name table_constraint — recurse
                if let Some(inner) = child.into_inner().nth(1) {
                    collect_table_pk(inner, out);
                }
            }
            _ => {}
        }
    }
}

/// Removes surrounding double-quotes from a quoted identifier, lowercases bare ones.
fn unquote(raw: &str) -> String {
    if raw.starts_with('"') && raw.ends_with('"') {
        raw[1..raw.len() - 1].replace("\"\"", "\"")
    } else {
        raw.to_lowercase()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
