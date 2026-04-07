use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};

use crate::ir::{ResultColumn, SqlType};

use super::resolve::{resolve_expr_in, ResolveScope};
use super::ResolverConfig;

fn named_result(fname: &str, sql_type: SqlType, nullable: bool) -> ResultColumn {
    ResultColumn { name: fname.to_lowercase(), sql_type, nullable }
}

pub(super) fn resolve_function(func: &sqlparser::ast::Function, scope: &ResolveScope<'_>) -> Option<ResultColumn> {
    let fname = func
        .name
        .0
        .last()
        .and_then(|p| if let sqlparser::ast::ObjectNamePart::Identifier(i) = p { Some(i.value.to_uppercase()) } else { None })
        .unwrap_or_default();

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
            let promoted = match rc.sql_type {
                SqlType::SmallInt | SqlType::Integer => scope.config.sum_integer_type.clone(),
                SqlType::BigInt => scope.config.sum_bigint_type.clone(),
                other => other,
            };
            ResultColumn { sql_type: promoted, nullable: true, ..rc }
        }),
        "MIN" | "MAX" => resolve_func_first_arg(func, scope).map(|rc| ResultColumn { nullable: true, ..rc }),
        "AVG" => resolve_func_first_arg(func, scope).map(|rc| {
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
            Some(named_result(fname, SqlType::Text, true))
        },
        _ => None,
    }
}

fn resolve_length_function(fname: &str) -> Option<ResultColumn> {
    match fname {
        "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" | "OCTET_LENGTH" | "BIT_LENGTH" | "POSITION" | "STRPOS" => {
            Some(named_result(fname, SqlType::Integer, true))
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
        | "ASIN" | "ACOS" | "ATAN" | "ATAN2" => Some(named_result(fname, SqlType::Double, true)),
        "MOD" => Some(ResultColumn::nullable("mod", SqlType::Integer)),
        _ => None,
    }
}

fn resolve_datetime_function(fname: &str) -> Option<ResultColumn> {
    match fname {
        "NOW" | "CURRENT_TIMESTAMP" | "LOCALTIMESTAMP" | "STATEMENT_TIMESTAMP" | "TRANSACTION_TIMESTAMP" | "CLOCK_TIMESTAMP" => {
            Some(named_result(fname, SqlType::TimestampTz, false))
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
        "JSON_EXTRACT" | "JSON_EXTRACT_PATH_TEXT" | "JSONB_EXTRACT_PATH_TEXT" | "JSON_VALUE" => Some(named_result(fname, SqlType::Text, true)),
        "JSON_OBJECT" | "JSON_BUILD_OBJECT" | "JSONB_BUILD_OBJECT" | "JSON_ARRAY" | "JSON_BUILD_ARRAY" | "JSONB_BUILD_ARRAY" | "JSON_AGG" | "JSONB_AGG"
        | "JSON_ARRAYAGG" | "JSON_OBJECTAGG" => Some(named_result(fname, SqlType::Json, true)),
        _ => None,
    }
}

fn resolve_boolean_function(fname: &str) -> Option<ResultColumn> {
    match fname {
        "BOOL_AND" | "BOOL_OR" | "EVERY" => Some(named_result(fname, SqlType::Boolean, true)),
        _ => None,
    }
}

fn resolve_window_function(func: &sqlparser::ast::Function, fname: &str, scope: &ResolveScope<'_>) -> Option<ResultColumn> {
    match fname {
        "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "NTILE" => Some(named_result(fname, SqlType::BigInt, false)),
        "CUME_DIST" | "PERCENT_RANK" => Some(named_result(fname, SqlType::Double, false)),
        "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" => resolve_func_first_arg(func, scope).map(|rc| ResultColumn { nullable: true, ..rc }),
        _ => None,
    }
}

fn resolve_udf(func: &sqlparser::ast::Function, fname: &str, config: &ResolverConfig) -> Option<ResultColumn> {
    let arg_count = if let FunctionArguments::List(al) = &func.args { al.args.len() } else { 0 };
    let overloads = config.user_functions.get(fname)?;
    let (_, return_type) = overloads.iter().find(|(pt, _)| pt.len() == arg_count).or_else(|| overloads.first())?;
    Some(named_result(fname, return_type.clone(), true))
}

fn resolve_func_first_arg(func: &sqlparser::ast::Function, scope: &ResolveScope<'_>) -> Option<ResultColumn> {
    if let FunctionArguments::List(arg_list) = &func.args {
        if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(inner))) = arg_list.args.first() {
            return resolve_expr_in(inner, scope);
        }
    }
    None
}

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
