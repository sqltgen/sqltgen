//! Resolve the list-parameter binding action for a query parameter.
//!
//! Every backend faces the same decision when a query has a list (`@name :type[]`)
//! parameter: do we expand `IN (?,?,?)` at runtime (Dynamic), or use an
//! engine-native binding (a SQL array, or a JSON-encoded string)?
//!
//! The decision is invariant across backends because it depends only on:
//!   - the user-configured [`ListParamStrategy`] (Native vs Dynamic)
//!   - the IR's `native_list_sql` and `native_list_bind` fields, which are set
//!     by the dialect frontend
//!
//! Each backend then dispatches on the resulting [`ListAction`] using its own
//! language-specific emission code. The SQL contained in the action variants is
//! the **raw** native SQL (with `$N` placeholders); each backend applies its own
//! placeholder normalization before use.

use crate::config::ListParamStrategy;
use crate::ir::{NativeListBind, Parameter};

/// The resolved list-param action for a single list parameter.
///
/// Variants are named by *bind shape*, not by engine — `SqlArrayBind` is
/// currently produced only by Postgres but the name reflects the binding
/// mechanism (a single SQL array placeholder), not the engine.
pub enum ListAction {
    /// Bind the list as a single SQL array argument (e.g. `= ANY(?)`).
    /// Contains the raw native SQL, with `$N` placeholders.
    SqlArrayBind(String),
    /// Build `IN (?,?,…,?)` at runtime by expanding placeholders for each element.
    Dynamic,
    /// Bind the list as a JSON-encoded string consumed by an SQL function
    /// (e.g. SQLite `json_each`, MySQL `JSON_TABLE`).
    /// Contains the raw native SQL, with `$N` placeholders.
    JsonStringBind(String),
}

/// Resolve the list-param action for a given strategy and parameter.
///
/// Returns [`ListAction::Dynamic`] when the strategy is Dynamic, or when Native
/// was requested but the IR has no `native_list_sql` for this parameter (i.e. the
/// dialect frontend did not produce one).
pub fn resolve(strategy: &ListParamStrategy, lp: &Parameter) -> ListAction {
    if *strategy == ListParamStrategy::Native {
        if let (Some(native_sql), Some(bind)) = (&lp.native_list_sql, &lp.native_list_bind) {
            return match bind {
                NativeListBind::Array => ListAction::SqlArrayBind(native_sql.clone()),
                NativeListBind::Json => ListAction::JsonStringBind(native_sql.clone()),
            };
        }
    }
    ListAction::Dynamic
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::SqlType;

    fn make_param(native_sql: Option<&str>, bind: Option<NativeListBind>) -> Parameter {
        let mut p = Parameter::list(1, "ids", SqlType::BigInt, false);
        if let (Some(sql), Some(b)) = (native_sql, bind) {
            p = p.with_native_list(sql, b);
        }
        p
    }

    #[test]
    fn test_native_with_array_returns_sql_array_bind() {
        let lp = make_param(Some("SELECT * FROM users WHERE id = ANY($1)"), Some(NativeListBind::Array));
        match resolve(&ListParamStrategy::Native, &lp) {
            ListAction::SqlArrayBind(sql) => assert!(sql.contains("ANY($1)")),
            _ => panic!("expected SqlArrayBind"),
        }
    }

    #[test]
    fn test_native_with_json_returns_json_string_bind() {
        let lp = make_param(Some("SELECT * FROM users WHERE id IN (SELECT value FROM json_each(?1))"), Some(NativeListBind::Json));
        match resolve(&ListParamStrategy::Native, &lp) {
            ListAction::JsonStringBind(sql) => assert!(sql.contains("json_each")),
            _ => panic!("expected JsonStringBind"),
        }
    }

    #[test]
    fn test_dynamic_strategy_returns_dynamic() {
        let lp = make_param(Some("SELECT ..."), Some(NativeListBind::Array));
        assert!(matches!(resolve(&ListParamStrategy::Dynamic, &lp), ListAction::Dynamic));
    }

    #[test]
    fn test_native_without_native_sql_falls_back_to_dynamic() {
        let lp = make_param(None, None);
        assert!(matches!(resolve(&ListParamStrategy::Native, &lp), ListAction::Dynamic));
    }

    #[test]
    fn test_returned_sql_is_raw_not_rewritten() {
        // The resolver must not rewrite placeholders — backends do their own normalization.
        let lp = make_param(Some("SELECT $1, $2 WHERE id = ANY($3)"), Some(NativeListBind::Array));
        match resolve(&ListParamStrategy::Native, &lp) {
            ListAction::SqlArrayBind(sql) => {
                assert!(sql.contains("$1"), "must preserve $N placeholders, got: {sql}");
                assert!(sql.contains("$3"), "must preserve $N placeholders, got: {sql}");
            },
            _ => panic!("expected SqlArrayBind"),
        }
    }
}
