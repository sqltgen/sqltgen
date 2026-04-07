mod annotations;
mod ctes;
mod derived;
mod dispatch;
mod dml;
mod params;
mod resolve;
mod returning;
mod select;
mod tables;
mod utils;

use std::collections::HashMap;

use crate::frontend::common::{obj_name_to_str, obj_schema_to_str};
use crate::ir::{NativeListBind, Parameter, Schema, SqlType, Table};
#[cfg(test)]
use crate::ir::{Query, QueryCmd};
use sqlparser::ast::{Delete, Insert, TableFactor, TableObject};

type UserFunctions = HashMap<String, Vec<(Vec<SqlType>, SqlType)>>;

/// Dialect-specific function that rewrites list-param SQL and returns the binding method.
///
/// Takes the list parameter (with its final `sql_type` and `index`) and the current
/// query SQL (with `$N` placeholders). Returns the rewritten SQL and the
/// [`NativeListBind`] backends must use, or `None` when native expansion is unavailable.
type NativeListSqlFn = fn(&Parameter, &str) -> Option<(String, NativeListBind)>;

use annotations::QueryAnnotation;
pub(super) use ctes::{apply_cte_alias_columns, build_cte_scope, collect_cte_params};
pub(super) use derived::{derived_cols, resolve_view_columns};
pub(crate) use dispatch::parse_queries_with_config;
use resolve::{resolve_expr, resolve_projection};
pub(super) use returning::resolve_returning;
pub(super) use tables::{build_alias_map, collect_from_tables, collect_table_list, update_from_tables};
pub(super) use utils::{build_params, count_params, placeholder_idx, unresolved_query};

/// Dialect-agnostic type inference configuration.
#[derive(Clone)]
pub(crate) struct ResolverConfig {
    /// Return type of SUM applied to smallint/integer columns.
    /// PostgreSQL: BigInt.  MySQL: Decimal.  SQLite: BigInt.
    pub sum_integer_type: SqlType,
    /// Return type of SUM applied to bigint columns.
    /// PostgreSQL: Decimal (numeric).  MySQL: Decimal.  SQLite: BigInt.
    pub sum_bigint_type: SqlType,
    /// Return type of AVG applied to any integer column (smallint/integer/bigint).
    /// PostgreSQL: Decimal (numeric).  MySQL: Double.  SQLite: Double (real).
    pub avg_integer_type: SqlType,
    /// Maps a sqlparser `DataType` to `SqlType` using the active dialect's typemap.
    ///
    /// Used by `resolve_expr` for CAST expressions. Each dialect supplies its own
    /// mapping function (e.g. `postgres::typemap::map`).
    pub typemap: fn(&sqlparser::ast::DataType) -> SqlType,
    /// Compute the native list-param SQL and binding method for a given list parameter.
    ///
    /// Takes the list parameter (with its final `sql_type` and `index`) and
    /// the current query SQL (with `$N` placeholders). Returns the rewritten
    /// SQL and the [`NativeListBind`] method backends must use, or `None` when
    /// native expansion is unavailable.
    pub native_list_sql: Option<NativeListSqlFn>,
    /// User-defined scalar function overloads extracted from `CREATE FUNCTION` DDL.
    ///
    /// Key is the UPPERCASE function name. Value is a list of `(param_types, return_type)`
    /// pairs in declaration order. PostgreSQL supports overloading by param type/count;
    /// MySQL does not; SQLite has no DDL functions.
    pub user_functions: UserFunctions,
    /// Schema name to use when matching unqualified table references against
    /// schema-qualified tables. Set from config, falling back to engine default.
    pub default_schema: Option<String>,
}

impl Default for ResolverConfig {
    fn default() -> Self {
        Self {
            sum_integer_type: SqlType::BigInt,
            sum_bigint_type: SqlType::BigInt,
            avg_integer_type: SqlType::Double,
            typemap: crate::frontend::common::typemap::map_common_or_custom,
            native_list_sql: None,
            user_functions: HashMap::new(),
            default_schema: None,
        }
    }
}

/// Groups the read-only context and mutable parameter mapping that most
/// parameter-collection functions need. Avoids threading five separate
/// arguments through every call.
pub(super) struct ResolverContext<'a> {
    pub alias_map: &'a HashMap<String, &'a Table>,
    pub all_tables: &'a [(Table, Option<String>)],
    pub schema: &'a Schema,
    pub config: &'a ResolverConfig,
    pub mapping: &'a mut HashMap<usize, (String, SqlType, bool)>,
    pub query_name: &'a str,
}

/// Returns `(schema, table_name)` for an INSERT statement's target table.
pub(super) fn insert_table_ref(ins: &Insert) -> (Option<String>, String) {
    match &ins.table {
        TableObject::TableName(name) => (obj_schema_to_str(name), obj_name_to_str(name)),
        _ => (None, String::new()),
    }
}

/// Returns `(schema, table_name)` for a DELETE statement's target table.
pub(super) fn delete_table_ref(del: &Delete) -> Option<(Option<String>, String)> {
    let tables = match &del.from {
        sqlparser::ast::FromTable::WithFromKeyword(t) | sqlparser::ast::FromTable::WithoutKeyword(t) => t,
    };
    tables.first().and_then(|twj| match &twj.relation {
        TableFactor::Table { name, .. } => Some((obj_schema_to_str(name), obj_name_to_str(name))),
        _ => None,
    })
}

#[cfg(test)]
mod tests;
