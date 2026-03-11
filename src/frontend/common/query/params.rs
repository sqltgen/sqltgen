//! Parameter collection from SQL AST expressions.
//!
//! Walks SQL expressions to find positional placeholders (`$N`, `?N`) and
//! infer their types from surrounding column references and operators.

use std::collections::HashMap;

use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, JoinConstraint, JoinOperator, LimitClause, OrderByKind, Query as SqlQuery, Select,
    SelectItem, SetExpr, UnaryOperator, Value, ValueWithSpan,
};

use crate::ir::{Schema, SqlType, Table};

use super::resolve::resolve_expr;
use super::{build_alias_map, collect_cte_params, collect_from_tables, placeholder_idx, ResolverConfig, ResolverContext};

/// Collect typed parameter mappings from a single `SELECT` clause.
///
/// Covers WHERE, JOIN ON, HAVING, and projection expressions. Shared by
/// `build_select_body` (plain queries) and `collect_set_expr_params` (UNION branches).
pub(super) fn collect_select_params(
    select: &Select,
    schema: &Schema,
    config: &ResolverConfig,
    ctes: &[Table],
    mapping: &mut HashMap<usize, (String, SqlType, bool)>,
) {
    let all_tables = collect_from_tables(select, schema, ctes, config);
    if all_tables.is_empty() {
        return;
    }
    let alias_map = build_alias_map(&all_tables);
    let ctx = &mut ResolverContext { alias_map: &alias_map, all_tables: &all_tables, schema, config, mapping };
    if let Some(expr) = &select.selection {
        collect_params_from_expr(expr, ctx);
    }
    collect_join_params(select, ctx);
    if let Some(expr) = &select.having {
        collect_params_from_expr(expr, ctx);
    }
    collect_projection_params(select, ctx);
}

/// Recursively collect typed parameter mappings from all branches of a set operation.
///
/// Each `SELECT` branch gets its own table context for inference. Set operation
/// nodes recurse into both left and right operands.
pub(super) fn collect_set_expr_params(
    expr: &SetExpr,
    schema: &Schema,
    config: &ResolverConfig,
    ctes: &[Table],
    mapping: &mut HashMap<usize, (String, SqlType, bool)>,
) {
    match expr {
        SetExpr::Select(select) => {
            collect_select_params(select, schema, config, ctes, mapping);
        },
        SetExpr::SetOperation { left, right, .. } => {
            collect_set_expr_params(left, schema, config, ctes, mapping);
            collect_set_expr_params(right, schema, config, ctes, mapping);
        },
        _ => {},
    }
}

/// Collect typed parameter mappings from an expression tree.
///
/// Walks the AST recursively to find `$N`/`?N` placeholders and infer
/// their types from comparison contexts (e.g. `col = $1`), IN lists,
/// BETWEEN, LIKE, and function arguments.
pub(super) fn collect_params_from_expr(expr: &Expr, ctx: &mut ResolverContext) {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            // Infer param type from any comparison: col = $N, col < $N, col > $N, etc.
            // Only use column-based types for parameter inference, not bare literals
            // (a literal like `-1` would give Integer, masking a BigInt column type).
            if matches!(
                op,
                BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Lt
                    | BinaryOperator::LtEq
                    | BinaryOperator::Gt
                    | BinaryOperator::GtEq
                    | BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo
            ) {
                // col OP $N
                if let Expr::Value(ValueWithSpan { value: Value::Placeholder(p), .. }) = &**right {
                    if let Some(idx) = placeholder_idx(p) {
                        if !is_literal_expr(left) {
                            if let Some(rc) = resolve_expr(left, ctx.alias_map, ctx.all_tables, ctx.config) {
                                ctx.mapping.entry(idx).or_insert((rc.name, rc.sql_type, rc.nullable));
                            }
                        }
                    }
                }
                // $N OP col
                if let Expr::Value(ValueWithSpan { value: Value::Placeholder(p), .. }) = &**left {
                    if let Some(idx) = placeholder_idx(p) {
                        if !is_literal_expr(right) {
                            if let Some(rc) = resolve_expr(right, ctx.alias_map, ctx.all_tables, ctx.config) {
                                ctx.mapping.entry(idx).or_insert((rc.name, rc.sql_type, rc.nullable));
                            }
                        }
                    }
                }
            }
            collect_params_from_expr(left, ctx);
            collect_params_from_expr(right, ctx);
        },
        Expr::InSubquery { expr, subquery, .. } => {
            collect_params_from_expr(expr, ctx);
            collect_params_from_subquery(subquery, ctx.schema, ctx.config, ctx.mapping);
        },
        Expr::Subquery(q) => {
            collect_params_from_subquery(q, ctx.schema, ctx.config, ctx.mapping);
        },
        Expr::Nested(inner) => {
            collect_params_from_expr(inner, ctx);
        },
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            collect_params_from_expr(inner, ctx);
        },
        Expr::Exists { subquery, .. } => {
            collect_params_from_subquery(subquery, ctx.schema, ctx.config, ctx.mapping);
        },
        Expr::InList { expr, list, .. } => {
            // col IN ($1, $2, …) — infer param type from the expression being tested
            let resolved = resolve_expr(expr, ctx.alias_map, ctx.all_tables, ctx.config);
            for item in list {
                if let Expr::Value(ValueWithSpan { value: Value::Placeholder(p), .. }) = item {
                    if let Some(idx) = placeholder_idx(p) {
                        if let Some(rc) = &resolved {
                            ctx.mapping.entry(idx).or_insert((rc.name.clone(), rc.sql_type.clone(), rc.nullable));
                        }
                    }
                }
                collect_params_from_expr(item, ctx);
            }
            collect_params_from_expr(expr, ctx);
        },
        Expr::Between { expr, low, high, .. } => {
            // col BETWEEN $1 AND $2 — infer both param types from the tested expression
            let resolved = resolve_expr(expr, ctx.alias_map, ctx.all_tables, ctx.config);
            for bound in [low.as_ref(), high.as_ref()] {
                if let Expr::Value(ValueWithSpan { value: Value::Placeholder(p), .. }) = bound {
                    if let Some(idx) = placeholder_idx(p) {
                        if let Some(rc) = &resolved {
                            ctx.mapping.entry(idx).or_insert((rc.name.clone(), rc.sql_type.clone(), rc.nullable));
                        }
                    }
                }
                collect_params_from_expr(bound, ctx);
            }
            collect_params_from_expr(expr, ctx);
        },
        Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
            // col LIKE $1 — infer param type from the column
            if let Expr::Value(ValueWithSpan { value: Value::Placeholder(p), .. }) = pattern.as_ref() {
                if let Some(idx) = placeholder_idx(p) {
                    if let Some(rc) = resolve_expr(expr, ctx.alias_map, ctx.all_tables, ctx.config) {
                        ctx.mapping.entry(idx).or_insert((rc.name, rc.sql_type, rc.nullable));
                    }
                }
            }
            collect_params_from_expr(expr, ctx);
            collect_params_from_expr(pattern, ctx);
        },
        Expr::UnaryOp { expr, .. } | Expr::Cast { expr, .. } => {
            collect_params_from_expr(expr, ctx);
        },
        Expr::Case { operand, conditions, else_result, .. } => {
            if let Some(op) = operand {
                collect_params_from_expr(op, ctx);
            }
            for cw in conditions {
                collect_params_from_expr(&cw.condition, ctx);
                collect_params_from_expr(&cw.result, ctx);
            }
            if let Some(el) = else_result {
                collect_params_from_expr(el, ctx);
            }
        },
        Expr::Function(func) => {
            if let FunctionArguments::List(arg_list) = &func.args {
                let func_name =
                    func.name.0.last().and_then(|p| if let sqlparser::ast::ObjectNamePart::Identifier(i) = p { Some(i.value.to_uppercase()) } else { None });
                let is_coalesce = matches!(func_name.as_deref(), Some("COALESCE" | "IFNULL" | "NULLIF" | "NVL"));
                // For COALESCE-family functions, infer placeholder types from the first
                // resolvable non-placeholder argument (the column that determines the type).
                let coalesce_type = if is_coalesce {
                    arg_list.args.iter().find_map(|a| {
                        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = a {
                            if !matches!(inner, Expr::Value(ValueWithSpan { value: Value::Placeholder(_), .. })) {
                                return resolve_expr(inner, ctx.alias_map, ctx.all_tables, ctx.config);
                            }
                        }
                        None
                    })
                } else {
                    None
                };
                for arg in &arg_list.args {
                    if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = arg {
                        if let Some(ref rc) = coalesce_type {
                            if let Expr::Value(ValueWithSpan { value: Value::Placeholder(p), .. }) = inner {
                                if let Some(idx) = placeholder_idx(p) {
                                    ctx.mapping.entry(idx).or_insert((rc.name.clone(), rc.sql_type.clone(), true));
                                }
                            }
                        }
                        collect_params_from_expr(inner, ctx);
                    }
                }
            }
        },
        _ => {},
    }
}

/// Returns true if the expression is a bare literal (number, string, boolean,
/// null, or unary +/- on a literal). Used to skip literal-based inference in
/// parameter type resolution — column-based types are always more accurate.
fn is_literal_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Value(ValueWithSpan { value, .. }) => !matches!(value, Value::Placeholder(_)),
        Expr::UnaryOp { op: UnaryOperator::Minus | UnaryOperator::Plus, expr } => is_literal_expr(expr),
        Expr::Nested(inner) => is_literal_expr(inner),
        _ => false,
    }
}

/// Recursively collect typed parameter mappings from a subquery.
///
/// Builds the subquery's FROM scope, recurses into any nested WITH clauses,
/// and collects parameters from the WHERE clause. Handles both scalar
/// subqueries (`Expr::Subquery`) and IN-subquery expressions.
fn collect_params_from_subquery(q: &SqlQuery, schema: &Schema, config: &ResolverConfig, mapping: &mut HashMap<usize, (String, SqlType, bool)>) {
    collect_cte_params(q.with.as_ref(), schema, config, mapping);
    let SetExpr::Select(select) = q.body.as_ref() else { return };
    let all_tables = collect_from_tables(select, schema, &[], config);
    if all_tables.is_empty() {
        return;
    }
    let alias_map = build_alias_map(&all_tables);
    let ctx = &mut ResolverContext { alias_map: &alias_map, all_tables: &all_tables, schema, config, mapping };
    if let Some(expr) = &select.selection {
        collect_params_from_expr(expr, ctx);
    }
    collect_join_params(select, ctx);
    if let Some(expr) = &select.having {
        collect_params_from_expr(expr, ctx);
    }
    collect_limit_offset_params(q, ctx.mapping);
}

/// Collect typed parameter mappings from JOIN ON conditions.
pub(super) fn collect_join_params(select: &Select, ctx: &mut ResolverContext) {
    for twj in &select.from {
        for join in &twj.joins {
            let constraint = match &join.join_operator {
                JoinOperator::Join(c)
                | JoinOperator::Inner(c)
                | JoinOperator::Left(c)
                | JoinOperator::LeftOuter(c)
                | JoinOperator::Right(c)
                | JoinOperator::RightOuter(c)
                | JoinOperator::FullOuter(c)
                | JoinOperator::CrossJoin(c)
                | JoinOperator::Semi(c)
                | JoinOperator::LeftSemi(c)
                | JoinOperator::RightSemi(c)
                | JoinOperator::LeftAnti(c)
                | JoinOperator::RightAnti(c)
                | JoinOperator::Anti(c)
                | JoinOperator::StraightJoin(c) => c,
                JoinOperator::CrossApply | JoinOperator::OuterApply => return,
                JoinOperator::AsOf { constraint, .. } => constraint,
            };
            if let JoinConstraint::On(expr) = constraint {
                collect_params_from_expr(expr, ctx);
            }
        }
    }
}

/// Collect parameter mappings from LIMIT and OFFSET expressions.
///
/// LIMIT/OFFSET params are always typed as `BigInt` since they control row counts.
pub(super) fn collect_limit_offset_params(q: &SqlQuery, mapping: &mut HashMap<usize, (String, SqlType, bool)>) {
    if let Some(LimitClause::LimitOffset { limit, offset, .. }) = &q.limit_clause {
        if let Some(Expr::Value(ValueWithSpan { value: Value::Placeholder(p), .. })) = limit {
            if let Some(idx) = placeholder_idx(p) {
                mapping.entry(idx).or_insert(("limit".into(), SqlType::BigInt, false));
            }
        }
        if let Some(off) = offset {
            if let Expr::Value(ValueWithSpan { value: Value::Placeholder(p), .. }) = &off.value {
                if let Some(idx) = placeholder_idx(p) {
                    mapping.entry(idx).or_insert(("offset".into(), SqlType::BigInt, false));
                }
            }
        }
    }
}

/// Collect typed parameter mappings from ORDER BY expressions.
///
/// Walks each ORDER BY expression to find placeholders inside CASE, function
/// calls, and other complex expressions used for sorting.
pub(super) fn collect_order_by_params(q: &SqlQuery, ctx: &mut ResolverContext) {
    if let Some(order_by) = &q.order_by {
        if let OrderByKind::Expressions(exprs) = &order_by.kind {
            for obe in exprs {
                collect_params_from_expr(&obe.expr, ctx);
            }
        }
    }
}

/// Collect typed parameter mappings from projection (SELECT list) expressions.
///
/// Walks each select item's expression to find placeholders inside CASE, function
/// calls, and other complex expressions that appear in the SELECT list.
fn collect_projection_params(select: &Select, ctx: &mut ResolverContext) {
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                collect_params_from_expr(expr, ctx);
            },
            _ => {},
        }
    }
}
