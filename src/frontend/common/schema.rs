use sqlparser::ast::{ArgMode, DataType, ObjectType, Statement};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

use super::{apply_alter_table, apply_drop_tables, build_create_table, obj_name_to_str, AlterCaps};
use crate::ir::{ScalarFunction, Schema, SqlType, Table};

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
    let mut functions: Vec<ScalarFunction> = Vec::new();

    loop {
        // Consume any inter-statement semicolons.
        while parser.consume_token(&Token::SemiColon) {}

        if matches!(parser.peek_token().token, Token::EOF) {
            break;
        }

        match parser.parse_statement() {
            Ok(stmt) => {
                process_statement(&stmt, &mut tables, &mut functions, map_type, caps);
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

    Ok(Schema { tables, functions })
}

/// Applies a single DDL statement to the in-memory table and function lists.
fn process_statement(
    stmt: &Statement,
    tables: &mut Vec<Table>,
    functions: &mut Vec<ScalarFunction>,
    map_type: fn(&sqlparser::ast::DataType) -> SqlType,
    caps: AlterCaps,
) {
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
        Statement::CreateFunction(f) => {
            // Skip table-valued functions (RETURNS TABLE(...)) — they are not scalar.
            let return_type = match &f.return_type {
                Some(dt) if !matches!(dt, DataType::Table(_)) => map_type(dt),
                _ => return,
            };
            let name = obj_name_to_str(&f.name);
            let param_types =
                f.args.as_deref().unwrap_or(&[]).iter().filter(|a| matches!(a.mode, None | Some(ArgMode::In))).map(|a| map_type(&a.data_type)).collect();
            functions.push(ScalarFunction { name, return_type, param_types });
        },
        _ => {},
    }
}
