use sqlparser::ast::SelectItem;

use crate::frontend::common::ident_to_str;
use crate::ir::{ResultColumn, Table};

use super::resolve::resolve_expr;
use super::{build_alias_map, ParamMapping, ResolverConfig, ResolverContext};

pub(in crate::frontend::common) fn resolve_returning(items: &[SelectItem], table: &Table, config: &ResolverConfig) -> Vec<ResultColumn> {
    let all_tables = [(table.clone(), None)];
    let alias_map = build_alias_map(&all_tables);
    let schema = Default::default();
    let mut mapping = ParamMapping::new();
    let ctx = ResolverContext::new(&alias_map, &all_tables, &schema, config, &mut mapping, "");
    let mut result = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard(_) => {
                result.extend(table.columns.iter().map(ResultColumn::from));
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
