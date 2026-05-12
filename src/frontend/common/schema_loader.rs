//! Pass-1 DDL statement processing with strict collision detection.
//!
//! `schema.rs` drives the overall parse (pass 1 → enum resolution → view
//! resolution); the per-statement logic and origin tracking lives here so
//! both files stay within the quality-ratchet thresholds.

use std::collections::HashMap;
use std::path::PathBuf;

use sqlparser::ast::{ArgMode, DataType, DropFunction, ObjectType, Statement, UserDefinedTypeRepresentation};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

use super::schema::{apply_drop_functions, apply_drop_views};
use super::{apply_alter_table, apply_drop_tables, build_column, build_create_table, obj_name_to_str, obj_schema_to_str, DdlDialect};
use crate::frontend::SchemaFile;
use crate::ir::schema_matches;
use crate::ir::{EnumType, ScalarFunction, Schema, SqlType, Table};

/// A `CREATE VIEW` statement collected during pass 1 for resolution in pass 2.
pub(super) struct PendingView {
    pub(super) name: String,
    pub(super) schema: Option<String>,
    pub(super) query: Box<sqlparser::ast::Query>,
}

/// Location of a DDL statement in its source file. Used only for error
/// reporting; never stored on the IR.
#[derive(Debug, Clone)]
struct SourceLoc {
    file: PathBuf,
    line: u64,
}

impl std::fmt::Display for SourceLoc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.file.display(), self.line)
    }
}

/// Tracks where each top-level relation/type was first registered, so that a
/// later collision can be reported with both source locations.
#[derive(Default)]
pub(super) struct SchemaOrigins {
    /// Tables, views, and TVFs share the postgres "relation" namespace.
    relations: HashMap<RelationKey, SourceLoc>,
    enums: HashMap<RelationKey, SourceLoc>,
}

/// Normalized lookup key for collision detection. `schema` carries the
/// *effective* name — unqualified names are resolved against `default_schema`
/// so `users` and `public.users` collide when default is `public`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RelationKey {
    schema: Option<String>,
    name: String,
}

impl RelationKey {
    fn new(schema: Option<&str>, name: &str, default_schema: Option<&str>) -> Self {
        let effective = schema.map(|s| s.to_string()).or_else(|| default_schema.map(|s| s.to_string()));
        Self { schema: effective, name: name.to_string() }
    }

    fn qualified_display(&self) -> String {
        match &self.schema {
            Some(s) => format!("{s}.{}", self.name),
            None => self.name.clone(),
        }
    }
}

fn collision_error(kind: &str, key: &RelationKey, first: &SourceLoc, second: &SourceLoc) -> anyhow::Error {
    anyhow::anyhow!("{kind} \"{}\" defined at {first} and redefined at {second}", key.qualified_display())
}

/// Immutable parsing configuration shared across all source files.
pub(super) struct ParseConfig<'a> {
    pub(super) sql_dialect: &'a dyn Dialect,
    pub(super) ddl_dialect: DdlDialect,
    pub(super) default_schema: Option<&'a str>,
}

/// Mutable schema-build state passed to every statement handler.
pub(super) struct SchemaState<'a> {
    pub(super) schema: &'a mut Schema,
    pub(super) pending_views: &'a mut Vec<PendingView>,
    pub(super) origins: &'a mut SchemaOrigins,
}

/// Cross-cutting context for a single statement (dialect + location).
struct DdlContext<'a> {
    dialect: DdlDialect,
    default_schema: Option<&'a str>,
    origins: &'a mut SchemaOrigins,
    loc: &'a SourceLoc,
}

/// Tokenize and apply every statement from one source file. Unparseable
/// statements are skipped to the next semicolon so a single bad statement
/// does not abort the entire schema parse.
pub(super) fn parse_one_file(file: &SchemaFile, cfg: &ParseConfig, state: &mut SchemaState) -> anyhow::Result<()> {
    let tokens = Tokenizer::new(cfg.sql_dialect, &file.content)
        .tokenize_with_location()
        .map_err(|e| anyhow::anyhow!("DDL tokenize error in {}: {e}", file.path.display()))?;
    let mut parser = Parser::new(cfg.sql_dialect).with_tokens_with_locations(tokens);

    loop {
        while parser.consume_token(&Token::SemiColon) {}
        if matches!(parser.peek_token().token, Token::EOF) {
            break;
        }
        let loc = SourceLoc { file: file.path.clone(), line: parser.peek_token().span.start.line };
        match parser.parse_statement() {
            Ok(stmt) => {
                let mut ctx = DdlContext { dialect: cfg.ddl_dialect, default_schema: cfg.default_schema, origins: state.origins, loc: &loc };
                process_statement(&stmt, state.schema, state.pending_views, &mut ctx)?;
            },
            Err(_) => while !matches!(parser.next_token().token, Token::SemiColon | Token::EOF) {},
        }
    }
    Ok(())
}

/// Apply a single DDL statement. `CREATE VIEW` is deferred to pass 2 via
/// `pending_views`. Bare CREATEs that re-define an existing object are
/// reported as collisions; `IF NOT EXISTS` and `OR REPLACE` keep their
/// PostgreSQL semantics.
fn process_statement(stmt: &Statement, schema: &mut Schema, pending_views: &mut Vec<PendingView>, ctx: &mut DdlContext) -> anyhow::Result<()> {
    match stmt {
        Statement::CreateTable(ct) => apply_create_table(ct, schema, ctx),
        Statement::AlterTable(a) => {
            apply_alter_table(&a.name, &a.operations, &mut schema.tables, ctx.dialect, ctx.default_schema);
            Ok(())
        },
        Statement::Drop { object_type: ObjectType::Table, names, .. } => {
            apply_drop_tables(names, &mut schema.tables, ctx.default_schema);
            for n in names {
                ctx.origins.relations.remove(&RelationKey::new(obj_schema_to_str(n).as_deref(), &obj_name_to_str(n), ctx.default_schema));
            }
            Ok(())
        },
        Statement::CreateView(v) => apply_create_view(v, pending_views, ctx),
        Statement::Drop { object_type: ObjectType::View, names, .. } => {
            apply_drop_views(names, pending_views, ctx.default_schema);
            for n in names {
                ctx.origins.relations.remove(&RelationKey::new(obj_schema_to_str(n).as_deref(), &obj_name_to_str(n), ctx.default_schema));
            }
            Ok(())
        },
        Statement::CreateFunction(f) => apply_create_function(f, schema, ctx),
        Statement::DropFunction(DropFunction { func_desc, .. }) => {
            apply_drop_functions(func_desc, &mut schema.functions, ctx.dialect, ctx.default_schema);
            Ok(())
        },
        Statement::CreateType { name, representation: Some(UserDefinedTypeRepresentation::Enum { labels }) } => {
            apply_create_enum_type(name, labels, schema, ctx)
        },
        _ => Ok(()),
    }
}

fn apply_create_table(ct: &sqlparser::ast::CreateTable, schema: &mut Schema, ctx: &mut DdlContext) -> anyhow::Result<()> {
    let table = build_create_table(&ct.name, &ct.columns, &ct.constraints, ctx.dialect);
    let key = RelationKey::new(table.schema.as_deref(), &table.name, ctx.default_schema);
    if let Some(first) = ctx.origins.relations.get(&key) {
        if ct.if_not_exists {
            return Ok(());
        }
        return Err(collision_error("table", &key, first, ctx.loc));
    }
    ctx.origins.relations.insert(key, ctx.loc.clone());
    schema.tables.push(table);
    Ok(())
}

fn apply_create_view(v: &sqlparser::ast::CreateView, pending_views: &mut Vec<PendingView>, ctx: &mut DdlContext) -> anyhow::Result<()> {
    let name = obj_name_to_str(&v.name);
    let schema_name = obj_schema_to_str(&v.name);
    let key = RelationKey::new(schema_name.as_deref(), &name, ctx.default_schema);
    if v.or_replace {
        pending_views.retain(|view| !(view.name == name && schema_matches(schema_name.as_deref(), view.schema.as_deref(), ctx.default_schema)));
        ctx.origins.relations.remove(&key);
    } else if let Some(first) = ctx.origins.relations.get(&key) {
        return Err(collision_error("view", &key, first, ctx.loc));
    }
    ctx.origins.relations.insert(key, ctx.loc.clone());
    pending_views.push(PendingView { name, schema: schema_name, query: v.query.clone() });
    Ok(())
}

fn apply_create_function(f: &sqlparser::ast::CreateFunction, schema: &mut Schema, ctx: &mut DdlContext) -> anyhow::Result<()> {
    let name = obj_name_to_str(&f.name);
    let schema_name = obj_schema_to_str(&f.name);
    match &f.return_type {
        // Table-valued function — shares the relation namespace; collisions reported.
        Some(DataType::Table(Some(col_defs))) => {
            let columns = col_defs.iter().map(|cd| build_column(cd, ctx.dialect.map_type)).collect();
            let key = RelationKey::new(schema_name.as_deref(), &name, ctx.default_schema);
            if f.or_replace {
                schema.tables.retain(|t| !(t.is_view() && t.name == name && schema_matches(schema_name.as_deref(), t.schema.as_deref(), ctx.default_schema)));
                ctx.origins.relations.remove(&key);
            } else if let Some(first) = ctx.origins.relations.get(&key) {
                return Err(collision_error("function", &key, first, ctx.loc));
            }
            ctx.origins.relations.insert(key, ctx.loc.clone());
            let mut tvf = Table::view(name, columns);
            tvf.schema = schema_name;
            schema.tables.push(tvf);
        },
        // RETURNS TABLE without column list, or no return type — skip.
        Some(DataType::Table(None)) | None => {},
        // Scalar function — overload-aware (existing logic).
        Some(dt) => {
            let return_type = (ctx.dialect.map_type)(dt);
            let param_types = collect_in_arg_types(f.args.as_deref().unwrap_or(&[]), ctx.dialect);
            if f.or_replace {
                schema.functions.retain(|g| {
                    g.name != name
                        || g.param_types.len() != param_types.len()
                        || !schema_matches(schema_name.as_deref(), g.schema.as_deref(), ctx.default_schema)
                });
            }
            schema.functions.push(ScalarFunction { name, schema: schema_name, return_type, param_types });
        },
    }
    Ok(())
}

fn apply_create_enum_type(
    name: &sqlparser::ast::ObjectName,
    labels: &[sqlparser::ast::Ident],
    schema: &mut Schema,
    ctx: &mut DdlContext,
) -> anyhow::Result<()> {
    let enum_name = obj_name_to_str(name);
    let enum_schema = obj_schema_to_str(name);
    let key = RelationKey::new(enum_schema.as_deref(), &enum_name, ctx.default_schema);
    if let Some(first) = ctx.origins.enums.get(&key) {
        return Err(collision_error("type", &key, first, ctx.loc));
    }
    ctx.origins.enums.insert(key, ctx.loc.clone());
    let mut variants = Vec::with_capacity(labels.len());
    for l in labels {
        variants.push(l.value.clone());
    }
    schema.enums.push(EnumType { name: enum_name, schema: enum_schema, variants });
    Ok(())
}

pub(super) fn collect_in_arg_types(args: &[sqlparser::ast::OperateFunctionArg], dialect: DdlDialect) -> Vec<SqlType> {
    let mut out = Vec::new();
    for a in args {
        if matches!(a.mode, None | Some(ArgMode::In)) {
            out.push((dialect.map_type)(&a.data_type));
        }
    }
    out
}
