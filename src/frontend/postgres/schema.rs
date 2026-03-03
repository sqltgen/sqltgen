use pest::Parser;
use pest_derive::Parser;

use crate::ir::{Column, Schema, SqlType, Table};
use crate::frontend::postgres::typemap;

#[derive(Parser)]
#[grammar = "frontend/postgres/ddl.pest"]
struct DdlParser;

/// Parses PostgreSQL DDL (CREATE TABLE statements) into a [Schema].
///
/// Non-CREATE TABLE statements are silently ignored.
pub fn parse_schema(ddl: &str) -> anyhow::Result<Schema> {
    let pairs = DdlParser::parse(Rule::script, ddl)
        .map_err(|e| anyhow::anyhow!("DDL parse error: {e}"))?;

    let mut tables = Vec::new();

    for pair in pairs.flatten() {
        if pair.as_rule() == Rule::create_table_stmt {
            if let Some(table) = parse_create_table(pair) {
                tables.push(table);
            }
        }
    }

    Ok(Schema { tables })
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
