use sqlparser::ast::SelectItem;

use crate::frontend::common::ident_to_str;
use crate::ir::{ResultColumn, Table};

use super::resolve::resolve_expr;
use super::{build_alias_map, ParamMapping, ResolverConfig, ResolverContext};

/// Resolve a `RETURNING` clause into typed result columns.
///
/// `all_tables` is the full table scope of the DML statement: the target table
/// (carrying its alias) followed by any `UPDATE … FROM` / `DELETE … USING`
/// tables. Passing the whole scope — not just the bare target — is what lets
/// qualified references such as `RETURNING i.owner_id` or `RETURNING o.id AS x`
/// resolve their column types. The first entry is treated as the target table
/// for `RETURNING *` expansion.
pub(in crate::frontend::common) fn resolve_returning(
    items: &[SelectItem],
    all_tables: &[(Table, Option<String>)],
    config: &ResolverConfig,
) -> Vec<ResultColumn> {
    let Some((target, _)) = all_tables.first() else { return Vec::new() };
    let alias_map = build_alias_map(all_tables);
    let schema = Default::default();
    let mut mapping = ParamMapping::new();
    let ctx = ResolverContext::new(&alias_map, all_tables, &schema, config, &mut mapping, "");
    let mut result = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard(_) => {
                result.extend(target.columns.iter().map(ResultColumn::from));
            },
            SelectItem::UnnamedExpr(expr) => {
                if let Some(rc) = resolve_expr(expr, &ctx) {
                    result.push(rc);
                }
            },
            SelectItem::ExprWithAlias { expr, alias } => {
                if let Some(rc) = resolve_expr(expr, &ctx) {
                    result.push(ResultColumn { name: ident_to_str(alias), ..rc });
                }
            },
            _ => {},
        }
    }
    result
}
