use sqlparser::ast::{ObjectType, Statement};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

use super::{apply_alter_table, apply_drop_tables, build_create_table, AlterCaps};
use crate::ir::{Schema, SqlType, Table};

/// Shared schema-parsing implementation used by all dialect frontends.
///
/// Tokenizes the DDL first, then parses each statement individually so that
/// unsupported statements (e.g. `CREATE FUNCTION … LEAKPROOF`) can be skipped
/// without aborting the entire parse.
///
/// `dialect`  — the sqlparser dialect to use for tokenizing and parsing.
/// `map_type` — the dialect-specific `DataType → SqlType` mapper.
/// `caps`     — which `ALTER TABLE` operations the dialect supports.
pub(crate) fn parse_schema_impl(
    ddl: &str,
    dialect: &dyn Dialect,
    map_type: fn(&sqlparser::ast::DataType) -> SqlType,
    caps: AlterCaps,
) -> anyhow::Result<Schema> {
    let tokens = Tokenizer::new(dialect, ddl).tokenize_with_location().map_err(|e| anyhow::anyhow!("DDL tokenize error: {e}"))?;

    let mut parser = Parser::new(dialect).with_tokens_with_locations(tokens);
    let mut tables: Vec<Table> = Vec::new();

    loop {
        // Consume any inter-statement semicolons.
        while parser.consume_token(&Token::SemiColon) {}

        if matches!(parser.peek_token().token, Token::EOF) {
            break;
        }

        match parser.parse_statement() {
            Ok(stmt) => {
                process_statement(&stmt, &mut tables, map_type, caps);
            },
            Err(_) => {
                // Skip to the next semicolon so we can recover and continue.
                loop {
                    match parser.next_token().token {
                        Token::SemiColon | Token::EOF => break,
                        _ => {},
                    }
                }
            },
        }
    }

    Ok(Schema { tables })
}

/// Applies a single DDL statement to the in-memory table list.
fn process_statement(stmt: &Statement, tables: &mut Vec<Table>, map_type: fn(&sqlparser::ast::DataType) -> SqlType, caps: AlterCaps) {
    match stmt {
        Statement::CreateTable(ct) => {
            tables.push(build_create_table(&ct.name, &ct.columns, &ct.constraints, map_type));
        },
        Statement::AlterTable(a) => {
            apply_alter_table(&a.name, &a.operations, tables, map_type, caps);
        },
        Statement::Drop { object_type: ObjectType::Table, names, .. } => {
            apply_drop_tables(names, tables);
        },
        _ => {},
    }
}
