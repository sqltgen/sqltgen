use sqlparser::ast::{ArgMode, DataType, DropFunction, ObjectType, Statement};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

use super::{apply_alter_table, apply_drop_tables, build_create_table, obj_name_to_str, AlterCaps};
use crate::ir::{ScalarFunction, Schema, SqlType};

/// Shared schema-parsing implementation used by all dialect frontends.
///
/// Tokenizes the DDL first, then parses each statement individually so that
/// unsupported statements (e.g. `CREATE FUNCTION … LEAKPROOF`) can be skipped
/// without aborting the entire parse.
///
/// `dialect`  — the sqlparser dialect to use for tokenizing and parsing.
/// `map_type` — the dialect-specific `DataType → SqlType` mapper.
/// `caps`     — which `ALTER TABLE` operations the dialect supports.
pub(crate) fn parse_schema_impl(ddl: &str, dialect: &dyn Dialect, map_type: fn(&DataType) -> SqlType, caps: AlterCaps) -> anyhow::Result<Schema> {
    let tokens = Tokenizer::new(dialect, ddl).tokenize_with_location().map_err(|e| anyhow::anyhow!("DDL tokenize error: {e}"))?;

    let mut parser = Parser::new(dialect).with_tokens_with_locations(tokens);
    let mut schema = Schema::default();

    loop {
        // Consume any inter-statement semicolons.
        while parser.consume_token(&Token::SemiColon) {}

        if matches!(parser.peek_token().token, Token::EOF) {
            break;
        }

        match parser.parse_statement() {
            Ok(stmt) => process_statement(&stmt, &mut schema, map_type, caps),
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

    Ok(schema)
}

/// Applies a single DDL statement to the in-progress schema.
fn process_statement(stmt: &Statement, schema: &mut Schema, map_type: fn(&DataType) -> SqlType, caps: AlterCaps) {
    match stmt {
        Statement::CreateTable(ct) => schema.tables.push(build_create_table(&ct.name, &ct.columns, &ct.constraints, map_type)),
        Statement::AlterTable(a) => apply_alter_table(&a.name, &a.operations, &mut schema.tables, map_type, caps),
        Statement::Drop { object_type: ObjectType::Table, names, .. } => apply_drop_tables(names, &mut schema.tables),
        Statement::CreateFunction(f) => {
            // Skip table-valued functions (RETURNS TABLE(...)) — they are not scalar.
            let return_type = match &f.return_type {
                Some(dt) if !matches!(dt, DataType::Table(_)) => map_type(dt),
                _ => return,
            };
            let name = obj_name_to_str(&f.name);
            let param_types =
                f.args.as_deref().unwrap_or(&[]).iter().filter(|a| matches!(a.mode, None | Some(ArgMode::In))).map(|a| map_type(&a.data_type)).collect();
            schema.functions.push(ScalarFunction { name, return_type, param_types });
        },
        Statement::DropFunction(DropFunction { func_desc, .. }) => apply_drop_functions(func_desc, &mut schema.functions),
        _ => {},
    }
}

/// Remove every function named in `func_desc` from the in-progress function list.
fn apply_drop_functions(func_desc: &[sqlparser::ast::FunctionDesc], functions: &mut Vec<ScalarFunction>) {
    for desc in func_desc {
        let name = obj_name_to_str(&desc.name);
        functions.retain(|f| f.name != name);
    }
}
