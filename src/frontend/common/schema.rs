use std::collections::HashMap;
use std::path::PathBuf;

use sqlparser::ast::{ArgMode, DataType, DropFunction, ObjectType, Statement, UserDefinedTypeRepresentation};
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

use super::{apply_alter_table, apply_drop_tables, build_column, build_create_table, obj_name_to_str, obj_schema_to_str, DdlDialect};
use crate::frontend::common::query::{resolve_view_columns, ResolverConfig};
use crate::frontend::SchemaFile;
use crate::ir::schema_matches;
use crate::ir::{EnumType, ScalarFunction, Schema, SqlType, Table};

/// A `CREATE VIEW` statement collected during pass 1 for resolution in pass 2.
struct PendingView {
    name: String,
    schema: Option<String>,
    query: Box<sqlparser::ast::Query>,
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
struct SchemaOrigins {
    /// Tables, views, and TVFs share the postgres "relation" namespace.
    relations: HashMap<RelationKey, SourceLoc>,
    enums: HashMap<RelationKey, SourceLoc>,
}

/// Normalized lookup key for collision detection.
///
/// `schema` carries the *effective* schema name — unqualified names are
/// resolved against `default_schema` before keying so that `users` and
/// `public.users` collide when `default_schema = "public"`.
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
pub(crate) fn parse_schema_impl(
    files: &[SchemaFile],
    sql_dialect: &dyn Dialect,
    ddl_dialect: DdlDialect,
    resolver_config: &ResolverConfig,
) -> anyhow::Result<Schema> {
    let mut schema = Schema::default();
    let mut pending_views: Vec<PendingView> = Vec::new();
    let mut origins = SchemaOrigins::default();

    // ── Pass 1: tables, functions, and collecting views ───────────────────────
    for file in files {
        let tokens = Tokenizer::new(sql_dialect, &file.content)
            .tokenize_with_location()
            .map_err(|e| anyhow::anyhow!("DDL tokenize error in {}: {e}", file.path.display()))?;
        let mut parser = Parser::new(sql_dialect).with_tokens_with_locations(tokens);

        loop {
            while parser.consume_token(&Token::SemiColon) {}

            if matches!(parser.peek_token().token, Token::EOF) {
                break;
            }

            let stmt_line = parser.peek_token().span.start.line;
            let loc = SourceLoc { file: file.path.clone(), line: stmt_line };

            match parser.parse_statement() {
                Ok(stmt) => {
                    process_statement(&stmt, &mut schema, ddl_dialect, &mut pending_views, resolver_config.default_schema.as_deref(), &mut origins, &loc)?
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
    }

    // ── Pass 1.5: resolve enum column types ────────────────────────────────────
    // Replace Custom(name) → Enum(name) for columns referencing known enum types.
    schema.resolve_enum_columns();

    // ── Pass 2: resolve views in declaration order ────────────────────────────
    // Each resolved view is pushed to schema.tables immediately so that a later
    // view can reference an earlier one.
    for view in pending_views {
        let columns = resolve_view_columns(&view.query, &schema, resolver_config);
        let mut table = Table::view(view.name, columns);
        table.schema = view.schema;
        schema.tables.push(table);
    }

    Ok(schema)
}

/// Applies a single DDL statement to the in-progress schema.
///
/// `CREATE VIEW` statements are not applied here; they are stored in
/// `pending_views` for resolution in pass 2.
///
/// Bare `CREATE TABLE` / `CREATE VIEW` / `CREATE TYPE` / `CREATE FUNCTION`
/// statements that re-define an existing object are reported as collisions.
/// `IF NOT EXISTS` and `OR REPLACE` keep their PostgreSQL semantics.
fn process_statement(
    stmt: &Statement,
    schema: &mut Schema,
    dialect: DdlDialect,
    pending_views: &mut Vec<PendingView>,
    default_schema: Option<&str>,
    origins: &mut SchemaOrigins,
    loc: &SourceLoc,
) -> anyhow::Result<()> {
    match stmt {
        Statement::CreateTable(ct) => {
            let table = build_create_table(&ct.name, &ct.columns, &ct.constraints, dialect);
            let key = RelationKey::new(table.schema.as_deref(), &table.name, default_schema);
            if let Some(first) = origins.relations.get(&key) {
                if ct.if_not_exists {
                    return Ok(());
                }
                return Err(collision_error("table", &key, first, loc));
            }
            origins.relations.insert(key, loc.clone());
            schema.tables.push(table);
        },
        Statement::AlterTable(a) => apply_alter_table(&a.name, &a.operations, &mut schema.tables, dialect, default_schema),
        Statement::Drop { object_type: ObjectType::Table, names, .. } => {
            apply_drop_tables(names, &mut schema.tables, default_schema);
            for name in names {
                let key = RelationKey::new(obj_schema_to_str(name).as_deref(), &obj_name_to_str(name), default_schema);
                origins.relations.remove(&key);
            }
        },
        Statement::CreateView(v) => {
            let name = obj_name_to_str(&v.name);
            let schema_name = obj_schema_to_str(&v.name);
            let key = RelationKey::new(schema_name.as_deref(), &name, default_schema);
            if v.or_replace {
                pending_views.retain(|view| !(view.name == name && schema_matches(schema_name.as_deref(), view.schema.as_deref(), default_schema)));
                origins.relations.remove(&key);
            } else if let Some(first) = origins.relations.get(&key) {
                return Err(collision_error("view", &key, first, loc));
            }
            origins.relations.insert(key, loc.clone());
            pending_views.push(PendingView { name, schema: schema_name, query: v.query.clone() });
        },
        Statement::Drop { object_type: ObjectType::View, names, .. } => {
            apply_drop_views(names, pending_views, default_schema);
            for name in names {
                let key = RelationKey::new(obj_schema_to_str(name).as_deref(), &obj_name_to_str(name), default_schema);
                origins.relations.remove(&key);
            }
        },
        Statement::CreateFunction(f) => {
            let name = obj_name_to_str(&f.name);
            let schema_name = obj_schema_to_str(&f.name);
            match &f.return_type {
                // Table-valued function: RETURNS TABLE(col1 type, col2 type, ...)
                Some(DataType::Table(Some(col_defs))) => {
                    let columns = col_defs.iter().map(|cd| build_column(cd, dialect.map_type)).collect();
                    let key = RelationKey::new(schema_name.as_deref(), &name, default_schema);
                    if f.or_replace {
                        schema
                            .tables
                            .retain(|t| !(t.is_view() && t.name == name && schema_matches(schema_name.as_deref(), t.schema.as_deref(), default_schema)));
                        origins.relations.remove(&key);
                    } else if let Some(first) = origins.relations.get(&key) {
                        return Err(collision_error("function", &key, first, loc));
                    }
                    origins.relations.insert(key, loc.clone());
                    let mut tvf = Table::view(name, columns);
                    tvf.schema = schema_name;
                    schema.tables.push(tvf);
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
                        schema.functions.retain(|g| {
                            g.name != name
                                || g.param_types.len() != param_types.len()
                                || !schema_matches(schema_name.as_deref(), g.schema.as_deref(), default_schema)
                        });
                    }
                    schema.functions.push(ScalarFunction { name, schema: schema_name, return_type, param_types });
                },
                // No return type — skip.
                None => {},
            }
        },
        Statement::DropFunction(DropFunction { func_desc, .. }) => apply_drop_functions(func_desc, &mut schema.functions, dialect, default_schema),
        Statement::CreateType { name, representation: Some(UserDefinedTypeRepresentation::Enum { labels }) } => {
            let enum_name = obj_name_to_str(name);
            let enum_schema = obj_schema_to_str(name);
            let key = RelationKey::new(enum_schema.as_deref(), &enum_name, default_schema);
            if let Some(first) = origins.enums.get(&key) {
                return Err(collision_error("type", &key, first, loc));
            }
            origins.enums.insert(key, loc.clone());
            let variants: Vec<String> = labels.iter().map(|l| l.value.clone()).collect();
            schema.enums.push(EnumType { name: enum_name, schema: enum_schema, variants });
        },
        _ => {},
    }
    Ok(())
}

/// Remove every pending view named in `names` from the pass-1 view list.
fn apply_drop_views(names: &[sqlparser::ast::ObjectName], pending_views: &mut Vec<PendingView>, default_schema: Option<&str>) {
    for name in names {
        let view_name = obj_name_to_str(name);
        let view_schema = obj_schema_to_str(name);
        pending_views.retain(|view| !(view.name == view_name && schema_matches(view_schema.as_deref(), view.schema.as_deref(), default_schema)));
    }
}

/// Remove every function named in `func_desc` from the in-progress function list.
///
/// PostgreSQL overloads functions by argument signature, so a single name can
/// resolve to multiple distinct functions. Matching rules:
/// - `DROP FUNCTION fn(t1, t2, …)` — match name + schema + IN-arg type list.
///   Only the exact overload is removed; sibling overloads with a different
///   signature are preserved.
/// - `DROP FUNCTION fn` — no arg list given; match name + schema only. This
///   mirrors the existing limitation around `CREATE OR REPLACE FUNCTION`,
///   which matches by arity rather than full signature.
///
/// `OUT` parameters are excluded because PostgreSQL uses only IN/INOUT/VARIADIC
/// for overload resolution. (`INOUT` is currently treated as not-IN by
/// [`process_statement`]'s `CreateFunction` branch; that asymmetry is a known
/// limitation tracked separately and should not be silently fixed here.)
fn apply_drop_functions(func_desc: &[sqlparser::ast::FunctionDesc], functions: &mut Vec<ScalarFunction>, dialect: DdlDialect, default_schema: Option<&str>) {
    for desc in func_desc {
        let name = obj_name_to_str(&desc.name);
        let schema = obj_schema_to_str(&desc.name);
        let arg_types: Option<Vec<SqlType>> = desc
            .args
            .as_ref()
            .map(|args| args.iter().filter(|a| matches!(a.mode, None | Some(ArgMode::In))).map(|a| (dialect.map_type)(&a.data_type)).collect());
        functions.retain(|f| {
            let same_name = f.name == name && schema_matches(schema.as_deref(), f.schema.as_deref(), default_schema);
            if !same_name {
                return true;
            }
            match &arg_types {
                Some(types) => f.param_types != *types,
                None => false,
            }
        });
    }
}
