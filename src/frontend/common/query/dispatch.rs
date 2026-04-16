use sqlparser::ast::Statement;
use sqlparser::dialect::Dialect;
use sqlparser::parser::Parser;

use crate::frontend::common::named_params;
use crate::ir::{Query, Schema};

use super::annotations::split_into_blocks;
use super::dml::{build_delete, build_insert, build_update};
use super::select::build_select;
use super::{unresolved_query, ResolverConfig};

pub(crate) fn parse_queries_with_config(dialect: &dyn Dialect, sql: &str, schema: &Schema, config: &ResolverConfig) -> anyhow::Result<Vec<Query>> {
    let config = build_effective_config(config, schema);
    let blocks = split_into_blocks(sql);
    let queries = blocks
        .into_iter()
        .filter_map(|(ann, body)| {
            let body = body.trim().trim_end_matches(';').trim();
            match build_query_with_dialect(dialect, &ann, body, schema, &config) {
                Ok(q) => Some(q),
                Err(e) => {
                    eprintln!("warning: cannot parse query {:?}: {e}", ann.name);
                    None
                },
            }
        })
        .collect();
    Ok(queries)
}

/// Produce a `ResolverConfig` augmented with user-defined functions from the schema.
///
/// The caller's config is used as the base; `user_functions` entries from the
/// schema's `CREATE FUNCTION` statements are merged in, keyed by UPPERCASE name.
fn build_effective_config(config: &ResolverConfig, schema: &Schema) -> ResolverConfig {
    let mut user_functions = config.user_functions.clone();
    for f in &schema.functions {
        user_functions.entry(f.name.to_uppercase()).or_default().push((f.param_types.clone(), f.return_type.clone()));
    }
    ResolverConfig { user_functions, ..config.clone() }
}

fn build_query_with_dialect(
    dialect: &dyn Dialect,
    ann: &super::annotations::QueryAnnotation,
    sql: &str,
    schema: &Schema,
    config: &ResolverConfig,
) -> anyhow::Result<Query> {
    let enum_names: Vec<String> = schema.enums.iter().map(|e| e.name.clone()).collect();
    let (sql_buf, np) = match named_params::preprocess_named_params(sql, &enum_names) {
        Some((rewritten, params)) => (rewritten, params),
        // No named params: still strip comment lines so that the stored SQL can be
        // safely collapsed to a single line in codegen (-- comments would eat the rest).
        None => (named_params::strip_sql_comment_lines(sql), vec![]),
    };
    let sql = sql_buf.as_str();

    let stmts = match Parser::parse_sql(dialect, sql) {
        Ok(s) if !s.is_empty() => s,
        _ => {
            let mut query = unresolved_query(ann, sql);
            named_params::apply_named_param_overrides(&mut query.params, &np);
            return Ok(query);
        },
    };

    let mut query = match &stmts[0] {
        Statement::Query(q) => build_select(ann, sql, q, schema, config),
        Statement::Insert(ins) => build_insert(ann, sql, ins, schema, config),
        Statement::Update(u) => build_update(ann, sql, u, schema, config),
        Statement::Delete(del) => build_delete(ann, sql, del, schema, config),
        _ => unresolved_query(ann, sql),
    };

    named_params::apply_named_param_overrides(&mut query.params, &np);
    apply_native_list_sql(&mut query, config);
    Ok(query)
}

/// Populate `native_list_sql` and `native_list_bind` for each list parameter.
///
/// Called after parameter types and names are fully resolved. Only executes
/// when `config.native_list_sql` is `Some`.
fn apply_native_list_sql(query: &mut Query, config: &ResolverConfig) {
    let Some(rewrite) = config.native_list_sql else { return };
    for p in &mut query.params {
        if p.is_list {
            if let Some((sql, bind)) = rewrite(p, &query.sql) {
                p.native_list_sql = Some(sql);
                p.native_list_bind = Some(bind);
            }
        }
    }
}
