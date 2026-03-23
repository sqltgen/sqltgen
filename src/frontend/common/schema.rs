use sqlparser::ast::{ArgMode, DataType, DropFunction, ObjectType, Statement};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

use super::{apply_alter_table, apply_drop_tables, build_column, build_create_table, obj_name_to_str, DdlDialect};
use crate::frontend::common::query::{resolve_view_columns, ResolverConfig};
use crate::ir::{ScalarFunction, Schema, SqlType, Table};

/// A `CREATE VIEW` statement collected during pass 1 for resolution in pass 2.
struct PendingView {
    name: String,
    query: Box<sqlparser::ast::Query>,
}

/// Shared schema-parsing implementation used by all dialect frontends.
///
/// Tokenizes the DDL first, then parses each statement individually so that
/// unsupported statements (e.g. `CREATE FUNCTION … LEAKPROOF`) can be skipped
/// without aborting the entire parse.
///
/// Pass 1 processes `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`,
/// `CREATE FUNCTION`, and `DROP FUNCTION` in order. `CREATE VIEW` statements
/// are collected into a pending list.
///
/// Pass 2 resolves each pending view against the completed table list, in
/// declaration order, so that a view defined after the tables it references
/// (and after earlier views it references) is typed correctly.
///
/// `dialect`         — the sqlparser dialect to use for tokenizing and parsing.
/// `ddl_dialect`     — the dialect-specific type mapper and `ALTER TABLE` caps.
/// `resolver_config` — the dialect-specific expression resolver config, used to
///                     infer view column types from the SELECT body.
pub(crate) fn parse_schema_impl(ddl: &str, sql_dialect: &dyn Dialect, ddl_dialect: DdlDialect, resolver_config: &ResolverConfig) -> anyhow::Result<Schema> {
    let tokens = Tokenizer::new(sql_dialect, ddl).tokenize_with_location().map_err(|e| anyhow::anyhow!("DDL tokenize error: {e}"))?;

    let mut parser = Parser::new(sql_dialect).with_tokens_with_locations(tokens);
    let mut schema = Schema::default();
    let mut pending_views: Vec<PendingView> = Vec::new();

    // ── Pass 1: tables, functions, and collecting views ───────────────────────
    loop {
        // Consume any inter-statement semicolons.
        while parser.consume_token(&Token::SemiColon) {}

        if matches!(parser.peek_token().token, Token::EOF) {
            break;
        }

        match parser.parse_statement() {
            Ok(stmt) => process_statement(&stmt, &mut schema, ddl_dialect, &mut pending_views),
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

    // ── Pass 2: resolve views in declaration order ────────────────────────────
    // Each resolved view is pushed to schema.tables immediately so that a later
    // view can reference an earlier one.
    for view in pending_views {
        let columns = resolve_view_columns(&view.query, &schema, resolver_config);
        schema.tables.push(Table::view(view.name, columns));
    }

    Ok(schema)
}

/// Applies a single DDL statement to the in-progress schema.
///
/// `CREATE VIEW` statements are not applied here; they are stored in
/// `pending_views` for resolution in pass 2.
fn process_statement(stmt: &Statement, schema: &mut Schema, dialect: DdlDialect, pending_views: &mut Vec<PendingView>) {
    match stmt {
        Statement::CreateTable(ct) => schema.tables.push(build_create_table(&ct.name, &ct.columns, &ct.constraints, dialect)),
        Statement::AlterTable(a) => apply_alter_table(&a.name, &a.operations, &mut schema.tables, dialect),
        Statement::Drop { object_type: ObjectType::Table, names, .. } => apply_drop_tables(names, &mut schema.tables),
        Statement::CreateView(v) => {
            let name = obj_name_to_str(&v.name);
            if v.or_replace {
                pending_views.retain(|view| view.name != name);
            }
            pending_views.push(PendingView { name, query: v.query.clone() });
        },
        Statement::Drop { object_type: ObjectType::View, names, .. } => apply_drop_views(names, pending_views),
        Statement::CreateFunction(f) => {
            let name = obj_name_to_str(&f.name);
            match &f.return_type {
                // Table-valued function: RETURNS TABLE(col1 type, col2 type, ...)
                Some(DataType::Table(Some(col_defs))) => {
                    let columns = col_defs.iter().map(|cd| build_column(cd, dialect.map_type)).collect();
                    if f.or_replace {
                        schema.tables.retain(|t| t.name != name);
                    }
                    schema.tables.push(Table::view(name, columns));
                },
                // RETURNS TABLE without column list — skip.
                Some(DataType::Table(None)) => {},
                // Scalar function.
                Some(dt) => {
                    let return_type = (dialect.map_type)(dt);
                    let param_types: Vec<SqlType> = f
                        .args
                        .as_deref()
                        .unwrap_or(&[])
                        .iter()
                        .filter(|a| matches!(a.mode, None | Some(ArgMode::In)))
                        .map(|a| (dialect.map_type)(&a.data_type))
                        .collect();
                    if f.or_replace {
                        schema.functions.retain(|g| g.name != name || g.param_types.len() != param_types.len());
                    }
                    schema.functions.push(ScalarFunction { name, return_type, param_types });
                },
                // No return type — skip.
                None => {},
            }
        },
        Statement::DropFunction(DropFunction { func_desc, .. }) => apply_drop_functions(func_desc, &mut schema.functions),
        _ => {},
    }
}

/// Remove every pending view named in `names` from the pass-1 view list.
fn apply_drop_views(names: &[sqlparser::ast::ObjectName], pending_views: &mut Vec<PendingView>) {
    for name in names {
        let name = obj_name_to_str(name);
        pending_views.retain(|view| view.name != name);
    }
}

/// Remove every function named in `func_desc` from the in-progress function list.
fn apply_drop_functions(func_desc: &[sqlparser::ast::FunctionDesc], functions: &mut Vec<ScalarFunction>) {
    for desc in func_desc {
        let name = obj_name_to_str(&desc.name);
        functions.retain(|f| f.name != name);
    }
}
