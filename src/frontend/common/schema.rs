use sqlparser::dialect::Dialect;

use super::schema_loader::{collect_in_arg_types, parse_one_file, ParseConfig, PendingView, SchemaOrigins, SchemaState};
use super::{obj_name_to_str, obj_schema_to_str, DdlDialect};
use crate::frontend::common::query::{resolve_view_columns, ResolverConfig};
use crate::frontend::SchemaFile;
use crate::ir::{schema_matches, ScalarFunction, Schema, SqlType, Table};

/// Shared schema-parsing implementation used by all dialect frontends.
///
/// Tokenizes each input file individually so that unsupported statements
/// (e.g. `CREATE FUNCTION … LEAKPROOF`) can be skipped without aborting
/// the entire parse, and so collision errors point at the original
/// `file:line` location.
///
/// Pass 1 processes `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`,
/// `CREATE FUNCTION`, and `DROP FUNCTION` in order, with strict
/// postgres-like collision detection on relations and types.
/// `CREATE VIEW` statements are collected into a pending list.
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
    let cfg = ParseConfig { sql_dialect, ddl_dialect, default_schema: resolver_config.default_schema.as_deref() };
    for file in files {
        let mut state = SchemaState { schema: &mut schema, pending_views: &mut pending_views, origins: &mut origins };
        parse_one_file(file, &cfg, &mut state)?;
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

/// Remove every pending view named in `names` from the pass-1 view list.
pub(super) fn apply_drop_views(names: &[sqlparser::ast::ObjectName], pending_views: &mut Vec<PendingView>, default_schema: Option<&str>) {
    for name in names {
        let view_name = obj_name_to_str(name);
        let view_schema = obj_schema_to_str(name);
        pending_views.retain(|view| !(view.name == view_name && schema_matches(view_schema.as_deref(), view.schema.as_deref(), default_schema)));
    }
}

/// Remove every function named in `func_desc` from the in-progress function list.
///
/// PostgreSQL overloads functions by argument signature. Matching rules:
/// - `DROP FUNCTION fn(t1, t2, …)` matches name + schema + IN-arg type list,
///   so sibling overloads with a different signature are preserved.
/// - `DROP FUNCTION fn` (no arg list) matches name + schema only.
pub(super) fn apply_drop_functions(
    func_desc: &[sqlparser::ast::FunctionDesc],
    functions: &mut Vec<ScalarFunction>,
    dialect: DdlDialect,
    default_schema: Option<&str>,
) {
    for desc in func_desc {
        let name = obj_name_to_str(&desc.name);
        let schema = obj_schema_to_str(&desc.name);
        let arg_types: Option<Vec<SqlType>> = desc.args.as_ref().map(|args| collect_in_arg_types(args, dialect));
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
