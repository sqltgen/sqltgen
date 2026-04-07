//! Expression and projection type inference for SQL queries.
//!
//! Resolves `sqlparser` AST expressions and SELECT projections to typed
//! [`ResultColumn`] values. Handles column references, literals, arithmetic,
//! string concatenation, comparisons, logical/unary operators, CAST, CASE,
//! and a comprehensive set of SQL functions (aggregates, string, math,
//! date/time, JSON, window, conditional).

use std::collections::HashMap;

use sqlparser::ast::{
    BinaryOperator, DataType, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, ObjectNamePart, Query as SqlQuery, Select, SelectItem,
    SelectItemQualifiedWildcardKind, SetExpr, UnaryOperator, Value, ValueWithSpan,
};

use crate::frontend::common::ident_to_str;
use crate::ir::{ResultColumn, Schema, SqlType, Table};

use super::ResolverConfig;

struct ResolveScope<'a> {
    alias_map: &'a HashMap<String, &'a Table>,
    all_tables: &'a [(Table, Option<String>)],
    config: &'a ResolverConfig,
}

/// Resolve a SELECT projection into typed result columns.
pub(super) fn resolve_projection(
    select: &Select,
    alias_map: &HashMap<String, &Table>,
    all_tables: &[(Table, Option<String>)],
    config: &ResolverConfig,
    schema: &Schema,
) -> Vec<ResultColumn> {
    let mut result = Vec::new();
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) => {
                for (t, _) in all_tables {
                    result.extend(t.columns.iter().map(ResultColumn::from));
                }
            },
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::ObjectName(name), _) => {
                let qualifier =
                    name.0.last().and_then(|p| if let ObjectNamePart::Identifier(i) = p { Some(ident_to_str(i)) } else { None }).unwrap_or_default();
                if let Some(t) = alias_map.get(&qualifier) {
                    result.extend(t.columns.iter().map(ResultColumn::from));
                }
            },
            SelectItem::QualifiedWildcard(SelectItemQualifiedWildcardKind::Expr(_), _) => {},
            SelectItem::UnnamedExpr(expr) => {
                if let Some(rc) = resolve_expr(expr, alias_map, all_tables, config) {
                    result.push(rc);
                }
                // Unresolvable expr without alias (subquery, aggregate) — skip
            },
            SelectItem::ExprWithAlias { expr, alias } => {
                let name = ident_to_str(alias);
                let resolved = resolve_expr(expr, alias_map, all_tables, config).or_else(|| resolve_scalar_subquery_expr(expr, all_tables, schema, config));
                match resolved {
                    Some(rc) => result.push(ResultColumn { name, ..rc }),
                    // Unknown expression (unrecognized function, complex expr): default
                    // to nullable Text. This is far more useful than Custom("expr"), which
                    // backends render as Object/Any? — an unresolvable function almost
                    // always returns a string-like value at the JDBC/driver level.
                    None => result.push(ResultColumn { name, sql_type: SqlType::Text, nullable: true }),
                }
            },
        }
    }
    result
}

/// Resolve a scalar subquery expression (`(SELECT col FROM t WHERE …) AS alias`)
/// to a typed [`ResultColumn`]. Returns `None` if the expression is not a
/// subquery or the inner projection cannot be resolved.
///
/// Scalar subqueries are always nullable because they return `NULL` when the
/// inner query produces no rows.
fn resolve_scalar_subquery_expr(expr: &Expr, outer_tables: &[(Table, Option<String>)], schema: &Schema, config: &ResolverConfig) -> Option<ResultColumn> {
    let Expr::Subquery(inner_query) = expr else { return None };
    resolve_scalar_subquery(inner_query, outer_tables, schema, config)
}

/// Resolve the first result column of a scalar subquery against the combined
/// inner and outer table scopes.
fn resolve_scalar_subquery(inner_query: &SqlQuery, outer_tables: &[(Table, Option<String>)], schema: &Schema, config: &ResolverConfig) -> Option<ResultColumn> {
    let SetExpr::Select(inner_select) = inner_query.body.as_ref() else { return None };
    // Collect the subquery's own FROM tables and merge with outer scope so that
    // correlated references (e.g. `b.author_id` from the outer query) resolve.
    let mut combined = super::collect_from_tables(inner_select, schema, &[], config);
    combined.extend(outer_tables.iter().cloned());
    let alias_map = super::build_alias_map(&combined);
    let cols = resolve_projection(inner_select, &alias_map, &combined, config, schema);
    // Scalar subquery returns NULL when the inner query has no rows.
    cols.into_iter().next().map(|rc| ResultColumn { nullable: true, ..rc })
}

/// Returns the wider of two numeric SQL types (for arithmetic result type promotion).
fn numeric_wider(a: &SqlType, b: &SqlType) -> SqlType {
    use SqlType::*;
    match (a, b) {
        (Decimal, _) | (_, Decimal) => Decimal,
        (Double, _) | (_, Double) => Double,
        (Real, _) | (_, Real) => Real,
        (BigInt, _) | (_, BigInt) => BigInt,
        (Integer, _) | (_, Integer) => Integer,
        _ => a.clone(),
    }
}

/// Resolve a SQL expression to a typed [`ResultColumn`].
///
/// Returns `None` for expressions that cannot be resolved (e.g. unknown
/// references, unsupported syntax). Handles column references, literals,
/// arithmetic, string concatenation, comparisons, logical/unary operators,
/// CAST, CASE, and function calls.
pub(super) fn resolve_expr(
    expr: &Expr,
    alias_map: &HashMap<String, &Table>,
    all_tables: &[(Table, Option<String>)],
    config: &ResolverConfig,
) -> Option<ResultColumn> {
    let scope = ResolveScope { alias_map, all_tables, config };
    resolve_expr_in(expr, &scope)
}

fn resolve_expr_in(expr: &Expr, scope: &ResolveScope<'_>) -> Option<ResultColumn> {
    match expr {
        // ── Column references ────────────────────────────────────────────
        Expr::Identifier(ident) => {
            let col_name = ident_to_str(ident);
            scope.all_tables.iter().flat_map(|(t, _)| t.columns.iter()).find(|c| c.name == col_name).map(ResultColumn::from)
        },
        Expr::CompoundIdentifier(parts) if parts.len() >= 2 => {
            let qualifier = ident_to_str(&parts[parts.len() - 2]);
            let col_name = ident_to_str(&parts[parts.len() - 1]);
            scope.alias_map.get(&qualifier).and_then(|t| t.columns.iter().find(|c| c.name == col_name)).map(ResultColumn::from)
        },

        // ── Literals ─────────────────────────────────────────────────────
        Expr::Value(ValueWithSpan { value, .. }) => resolve_literal(value),

        // ── Parenthesized expression ─────────────────────────────────────
        Expr::Nested(inner) => resolve_expr_in(inner, scope),

        // ── Arithmetic operators ─────────────────────────────────────────
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Plus | BinaryOperator::Minus | BinaryOperator::Multiply | BinaryOperator::Divide | BinaryOperator::Modulo,
            right,
        } => resolve_binary_arithmetic(left, right, scope),

        // ── String concatenation (||) ────────────────────────────────────
        Expr::BinaryOp { op: BinaryOperator::StringConcat, .. } => Some(ResultColumn::nullable("concat", SqlType::Text)),

        // ── Comparison operators → Boolean ───────────────────────────────
        Expr::BinaryOp {
            op: BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            // Null-safe equality: MySQL `<=>`, equivalent to IS NOT DISTINCT FROM
            | BinaryOperator::Spaceship,
            ..
        }
        | Expr::IsNull(_)
        | Expr::IsNotNull(_)
        // Null-aware comparisons: standard SQL and MySQL equivalents
        | Expr::IsDistinctFrom(_, _)
        | Expr::IsNotDistinctFrom(_, _)
        | Expr::IsTrue(_)
        | Expr::IsFalse(_)
        | Expr::IsNotTrue(_)
        | Expr::IsNotFalse(_)
        | Expr::InList { .. }
        | Expr::InSubquery { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::ILike { .. }
        | Expr::Exists { .. } => Some(ResultColumn::not_nullable("bool", SqlType::Boolean)),

        // ── Logical operators → Boolean ──────────────────────────────────
        Expr::BinaryOp { op: BinaryOperator::And | BinaryOperator::Or | BinaryOperator::Xor, .. } => Some(ResultColumn::not_nullable("bool", SqlType::Boolean)),

        // ── Unary operators ──────────────────────────────────────────────
        Expr::UnaryOp { op: UnaryOperator::Not, .. } => Some(ResultColumn::not_nullable("not", SqlType::Boolean)),
        Expr::UnaryOp { op: UnaryOperator::Minus, expr } => resolve_expr_in(expr, scope),
        Expr::UnaryOp { op: UnaryOperator::Plus, expr } => resolve_expr_in(expr, scope),

        // ── CAST(expr AS type) ───────────────────────────────────────────
        Expr::Cast { data_type, expr, .. } => {
            let sql_type = (scope.config.typemap)(data_type);
            let inner = resolve_expr_in(expr, scope);
            let nullable = inner.as_ref().is_none_or(|rc| rc.nullable);
            let name = inner.map_or_else(|| cast_name(data_type), |rc| rc.name);
            Some(ResultColumn { name, sql_type, nullable })
        },

        // ── CASE WHEN … THEN … END ──────────────────────────────────────
        Expr::Case { conditions, else_result, .. } => resolve_case(conditions, else_result.as_deref(), scope),

        // ── Functions ────────────────────────────────────────────────────
        Expr::Function(func) => resolve_function(func, scope),

        _ => None,
    }
}

/// Infer a result column from a SQL literal value.
fn resolve_literal(value: &Value) -> Option<ResultColumn> {
    match value {
        Value::Number(s, _) => {
            let sql_type = if s.contains('.') {
                SqlType::Double
            } else if s.parse::<i32>().is_ok() {
                SqlType::Integer
            } else {
                SqlType::BigInt
            };
            Some(ResultColumn::not_nullable("literal", sql_type))
        },
        Value::SingleQuotedString(_) | Value::DoubleQuotedString(_) | Value::DollarQuotedString(_) => {
            Some(ResultColumn::not_nullable("literal", SqlType::Text))
        },
        Value::Boolean(_) => Some(ResultColumn::not_nullable("literal", SqlType::Boolean)),
        Value::Null => Some(ResultColumn::nullable("literal", SqlType::Text)),
        _ => None,
    }
}

/// Resolve arithmetic binary ops with numeric widening.
fn resolve_binary_arithmetic(left: &Expr, right: &Expr, scope: &ResolveScope<'_>) -> Option<ResultColumn> {
    match (resolve_expr_in(left, scope), resolve_expr_in(right, scope)) {
        (Some(l), Some(r)) => {
            Some(ResultColumn { name: l.name.clone(), sql_type: numeric_wider(&l.sql_type, &r.sql_type), nullable: l.nullable || r.nullable })
        },
        (Some(l), None) => Some(l),
        (None, Some(r)) => Some(r),
        (None, None) => None,
    }
}

/// Resolve CASE expressions: type comes from first THEN branch (or ELSE), nullable
/// if any branch is nullable or an ELSE is absent.
fn resolve_case(conditions: &[sqlparser::ast::CaseWhen], else_result: Option<&Expr>, scope: &ResolveScope<'_>) -> Option<ResultColumn> {
    // Try each THEN branch, then ELSE; use the first resolvable one.
    let mut resolved: Option<ResultColumn> = None;
    let mut any_nullable = else_result.is_none(); // no ELSE means NULL is possible

    for cw in conditions {
        if let Some(rc) = resolve_expr_in(&cw.result, scope) {
            any_nullable = any_nullable || rc.nullable;
            if resolved.is_none() {
                resolved = Some(rc);
            }
        }
    }
    if let Some(el) = else_result {
        if let Some(rc) = resolve_expr_in(el, scope) {
            any_nullable = any_nullable || rc.nullable;
            if resolved.is_none() {
                resolved = Some(rc);
            }
        }
    }
    resolved.map(|rc| ResultColumn { nullable: any_nullable, ..rc })
}

/// Generate a default column name from a CAST target type.
pub(super) fn cast_name(dt: &DataType) -> String {
    format!("{dt}").to_lowercase().replace(' ', "_")
}

pub(super) fn function_name_upper(func: &sqlparser::ast::Function) -> Option<String> {
    func.name.0.last().and_then(|p| if let ObjectNamePart::Identifier(i) = p { Some(ident_to_str(i).to_uppercase()) } else { None })
}

/// Resolve function calls: aggregates, string, math, date, and conditional functions.
fn resolve_function(func: &sqlparser::ast::Function, scope: &ResolveScope<'_>) -> Option<ResultColumn> {
    let fname = function_name_upper(func).unwrap_or_default();

    resolve_aggregate_function(func, &fname, scope)
        .or_else(|| resolve_string_function(&fname))
        .or_else(|| resolve_length_function(&fname))
        .or_else(|| resolve_math_function(func, &fname, scope))
        .or_else(|| resolve_datetime_function(&fname))
        .or_else(|| resolve_conditional_function(func, &fname, scope))
        .or_else(|| resolve_misc_function(&fname))
        .or_else(|| resolve_json_function(&fname))
        .or_else(|| resolve_boolean_function(&fname))
        .or_else(|| resolve_window_function(func, &fname, scope))
        .or_else(|| resolve_udf(func, &fname, scope.config))
}

fn resolve_aggregate_function(func: &sqlparser::ast::Function, fname: &str, scope: &ResolveScope<'_>) -> Option<ResultColumn> {
    match fname {
        "COUNT" => Some(ResultColumn::not_nullable("count", SqlType::BigInt)),
        "SUM" => resolve_func_first_arg(func, scope).map(|rc| {
            // Integer inputs are widened to avoid overflow; other types are preserved.
            let promoted = match rc.sql_type {
                SqlType::SmallInt | SqlType::Integer => scope.config.sum_integer_type.clone(),
                SqlType::BigInt => scope.config.sum_bigint_type.clone(),
                other => other,
            };
            ResultColumn { sql_type: promoted, nullable: true, ..rc }
        }),
        "MIN" | "MAX" => resolve_func_first_arg(func, scope).map(|rc| ResultColumn { nullable: true, ..rc }),
        "AVG" => resolve_func_first_arg(func, scope).map(|rc| {
            // Averaging integers produces a fractional result; widen to the
            // dialect-specific fractional type (Decimal for PG, Double for MySQL/SQLite).
            let promoted = match rc.sql_type {
                SqlType::SmallInt | SqlType::Integer | SqlType::BigInt => scope.config.avg_integer_type.clone(),
                other => other,
            };
            ResultColumn { sql_type: promoted, nullable: true, ..rc }
        }),
        _ => None,
    }
}

fn resolve_string_function(fname: &str) -> Option<ResultColumn> {
    match fname {
        "UPPER" | "LOWER" | "TRIM" | "LTRIM" | "RTRIM" | "REPLACE" | "SUBSTR" | "SUBSTRING" | "CONCAT" | "LEFT" | "RIGHT" | "LPAD" | "RPAD" | "REVERSE"
        | "REPEAT" | "TRANSLATE" | "INITCAP" | "MD5" | "ENCODE" | "DECODE" | "FORMAT" | "TO_CHAR" | "STRING_AGG" | "GROUP_CONCAT" => {
            Some(ResultColumn { name: fname.to_lowercase(), sql_type: SqlType::Text, nullable: true })
        },
        _ => None,
    }
}

fn resolve_length_function(fname: &str) -> Option<ResultColumn> {
    match fname {
        "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" | "OCTET_LENGTH" | "BIT_LENGTH" | "POSITION" | "STRPOS" => {
            Some(ResultColumn { name: fname.to_lowercase(), sql_type: SqlType::Integer, nullable: true })
        },
        _ => None,
    }
}

fn resolve_math_function(func: &sqlparser::ast::Function, fname: &str, scope: &ResolveScope<'_>) -> Option<ResultColumn> {
    match fname {
        "ABS" | "CEIL" | "CEILING" | "FLOOR" | "ROUND" | "TRUNC" | "TRUNCATE" | "SIGN" => {
            resolve_func_first_arg(func, scope).map(|rc| ResultColumn { nullable: true, ..rc })
        },
        "SQRT" | "CBRT" | "EXP" | "LN" | "LOG" | "LOG2" | "LOG10" | "POWER" | "POW" | "RANDOM" | "PI" | "DEGREES" | "RADIANS" | "SIN" | "COS" | "TAN"
        | "ASIN" | "ACOS" | "ATAN" | "ATAN2" => Some(ResultColumn { name: fname.to_lowercase(), sql_type: SqlType::Double, nullable: true }),
        "MOD" => Some(ResultColumn::nullable("mod", SqlType::Integer)),
        _ => None,
    }
}

fn resolve_datetime_function(fname: &str) -> Option<ResultColumn> {
    match fname {
        "NOW" | "CURRENT_TIMESTAMP" | "LOCALTIMESTAMP" | "STATEMENT_TIMESTAMP" | "TRANSACTION_TIMESTAMP" | "CLOCK_TIMESTAMP" => {
            Some(ResultColumn { name: fname.to_lowercase(), sql_type: SqlType::TimestampTz, nullable: false })
        },
        "CURRENT_DATE" | "DATE" => Some(ResultColumn::not_nullable("date", SqlType::Date)),
        "CURRENT_TIME" | "LOCALTIME" => Some(ResultColumn::not_nullable("time", SqlType::Time)),
        _ => None,
    }
}

fn resolve_conditional_function(func: &sqlparser::ast::Function, fname: &str, scope: &ResolveScope<'_>) -> Option<ResultColumn> {
    match fname {
        "COALESCE" => resolve_coalesce(func, scope),
        "NULLIF" => resolve_func_first_arg(func, scope).map(|rc| ResultColumn { nullable: true, ..rc }),
        "IFNULL" => resolve_func_first_arg(func, scope).map(|rc| ResultColumn { nullable: false, ..rc }),
        "GREATEST" | "LEAST" => resolve_func_first_arg(func, scope).map(|rc| ResultColumn { nullable: true, ..rc }),
        _ => None,
    }
}

fn resolve_misc_function(fname: &str) -> Option<ResultColumn> {
    match fname {
        "TYPEOF" => Some(ResultColumn::not_nullable("typeof", SqlType::Text)),
        _ => None,
    }
}

fn resolve_json_function(fname: &str) -> Option<ResultColumn> {
    match fname {
        "JSON_EXTRACT" | "JSON_EXTRACT_PATH_TEXT" | "JSONB_EXTRACT_PATH_TEXT" | "JSON_VALUE" => {
            Some(ResultColumn { name: fname.to_lowercase(), sql_type: SqlType::Text, nullable: true })
        },
        "JSON_OBJECT" | "JSON_BUILD_OBJECT" | "JSONB_BUILD_OBJECT" | "JSON_ARRAY" | "JSON_BUILD_ARRAY" | "JSONB_BUILD_ARRAY" | "JSON_AGG" | "JSONB_AGG"
        | "JSON_ARRAYAGG" | "JSON_OBJECTAGG" => Some(ResultColumn { name: fname.to_lowercase(), sql_type: SqlType::Json, nullable: true }),
        _ => None,
    }
}

fn resolve_boolean_function(fname: &str) -> Option<ResultColumn> {
    match fname {
        "BOOL_AND" | "BOOL_OR" | "EVERY" => Some(ResultColumn { name: fname.to_lowercase(), sql_type: SqlType::Boolean, nullable: true }),
        _ => None,
    }
}

fn resolve_window_function(func: &sqlparser::ast::Function, fname: &str, scope: &ResolveScope<'_>) -> Option<ResultColumn> {
    match fname {
        "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" => Some(ResultColumn { name: fname.to_lowercase(), sql_type: SqlType::BigInt, nullable: false }),
        "CUME_DIST" | "PERCENT_RANK" => Some(ResultColumn { name: fname.to_lowercase(), sql_type: SqlType::Double, nullable: false }),
        "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" => resolve_func_first_arg(func, scope).map(|rc| ResultColumn { nullable: true, ..rc }),
        _ => None,
    }
}

/// Look up a user-defined function by name and argument count, returning a typed
/// [`ResultColumn`] if a matching overload is found in `config.user_functions`.
///
/// UDF return values are always nullable — no SQL engine has DDL syntax to
/// declare a function's return as non-null.
fn resolve_udf(func: &sqlparser::ast::Function, fname: &str, config: &ResolverConfig) -> Option<ResultColumn> {
    let arg_count = if let FunctionArguments::List(al) = &func.args { al.args.len() } else { 0 };
    let overloads = config.user_functions.get(fname)?;
    let (_, return_type) = overloads.iter().find(|(pt, _)| pt.len() == arg_count).or_else(|| overloads.first())?;
    Some(ResultColumn { name: fname.to_lowercase(), sql_type: return_type.clone(), nullable: true })
}

/// Extract the first argument from a function and resolve its type.
fn resolve_func_first_arg(func: &sqlparser::ast::Function, scope: &ResolveScope<'_>) -> Option<ResultColumn> {
    if let FunctionArguments::List(arg_list) = &func.args {
        if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(inner))) = arg_list.args.first() {
            return resolve_expr_in(inner, scope);
        }
    }
    None
}

/// Resolve COALESCE: type from first argument, nullable only if all arguments are nullable.
fn resolve_coalesce(func: &sqlparser::ast::Function, scope: &ResolveScope<'_>) -> Option<ResultColumn> {
    let FunctionArguments::List(arg_list) = &func.args else { return None };
    let mut first: Option<ResultColumn> = None;
    let mut all_nullable = true;
    for arg in &arg_list.args {
        if let FunctionArg::Unnamed(FunctionArgExpr::Expr(inner)) = arg {
            if let Some(rc) = resolve_expr_in(inner, scope) {
                if !rc.nullable {
                    all_nullable = false;
                }
                if first.is_none() {
                    first = Some(rc);
                }
            }
        }
    }
    first.map(|rc| ResultColumn { nullable: all_nullable, ..rc })
}
