use std::collections::HashMap;

use crate::backend::common::{canonical_sql_types, sql_type_key, SqlTypeKey};
use crate::backend::naming::to_pascal_case;
use crate::config::{resolve_type_override, Language, OutputConfig, TypeVariant};
use crate::ir::{Parameter, Query, SqlType};

use super::adapter::JsDriverAdapter;
use super::JsOutput;

/// Fully resolved code-generation entry for one SQL type in the TypeScript/JavaScript backend.
///
/// Built once before codegen by [`build_js_type_map`]; all override and preset
/// logic runs during map construction and is never repeated at emit time.
pub(super) struct JsTypeEntry {
    /// Base JS/TS type name for result columns (without `| null`).
    pub(super) field_type: String,
    /// Base JS/TS type name for query parameters. May differ on split overrides.
    pub(super) param_type: String,
    /// Read transformation template with `{raw}` placeholder, or `None` when the
    /// driver value can be used directly.
    pub(super) read_expr: Option<String>,
    /// Write transformation template with `{value}` placeholder, or `None` when the
    /// parameter value can be used directly.
    pub(super) write_expr: Option<String>,
}

/// Pre-resolved map from SQL type to JS/TS code-generation entry.
///
/// Constructed once by [`build_js_type_map`]; consumed by the emitters in `core.rs`
/// without any further override or dispatch logic.
pub(super) struct JsTypeMap {
    entries: HashMap<SqlTypeKey, JsTypeEntry>,
}

impl JsTypeMap {
    /// Return the resolved entry for `sql_type`.
    pub(super) fn get(&self, sql_type: &SqlType) -> &JsTypeEntry {
        &self.entries[&sql_type_key(sql_type)]
    }

    /// Return the JS/TS field type for `sql_type`, with nullability applied.
    ///
    /// `Array(inner)` maps to `InnerType[]` (or `InnerType[] | null` when nullable).
    pub(super) fn field_type(&self, sql_type: &SqlType, nullable: bool) -> String {
        if let SqlType::Enum(name) = sql_type {
            let ty = to_pascal_case(name);
            return if nullable { format!("{ty} | null") } else { ty };
        }
        if let SqlType::Array(inner) = sql_type {
            let inner_ty = self.field_type(inner, false);
            let arr_ty = format!("{inner_ty}[]");
            return if nullable { format!("{arr_ty} | null") } else { arr_ty };
        }
        let base = &self.get(sql_type).field_type;
        if nullable {
            format!("{base} | null")
        } else {
            base.clone()
        }
    }

    /// Return the JS/TS parameter type for `sql_type`, with nullability applied.
    pub(super) fn param_type(&self, sql_type: &SqlType, nullable: bool) -> String {
        if let SqlType::Enum(name) = sql_type {
            let ty = to_pascal_case(name);
            return if nullable { format!("{ty} | null") } else { ty };
        }
        if let SqlType::Array(inner) = sql_type {
            let inner_ty = self.param_type(inner, false);
            let arr_ty = format!("{inner_ty}[]");
            return if nullable { format!("{arr_ty} | null") } else { arr_ty };
        }
        let base = &self.get(sql_type).param_type;
        if nullable {
            format!("{base} | null")
        } else {
            base.clone()
        }
    }

    /// Return the JS expression that binds a parameter, applying any write transformation.
    ///
    /// When a `write_expr` is configured for the type, `{value}` is substituted with the
    /// camelCase parameter name. Otherwise the name is returned as-is.
    pub(super) fn write_expr(&self, p: &Parameter) -> String {
        let name = crate::backend::naming::to_camel_case(&p.name);
        if let Some(expr) = &self.get(&p.sql_type).write_expr {
            expr.replace("{value}", &name)
        } else {
            name
        }
    }

    /// Build the `JSON.stringify(...)` expression for a list parameter.
    ///
    /// `bigint[]` is not JSON-serializable with the default replacer, so BigInt list
    /// params use a replacer that converts each element to a string before serialization.
    /// All other element types use plain `JSON.stringify`.
    pub(super) fn list_json_stringify(&self, sql_type: &SqlType, name: &str) -> String {
        if matches!(sql_type, SqlType::BigInt | SqlType::BigIntUnsigned) {
            format!("JSON.stringify({name}, (_, v) => typeof v === 'bigint' ? String(v) : v)")
        } else {
            format!("JSON.stringify({name})")
        }
    }

    /// Build a row-transform expression if any result column has a `read_expr` override.
    ///
    /// Returns `None` when no columns need transformation. Returns `Some(expr)` that
    /// spreads `raw_var` and overrides the affected columns, e.g.:
    /// `{ ...raw, col: JSON.parse(raw.col as string) }`.
    pub(super) fn row_transform_expr(&self, query: &Query, raw_var: &str) -> Option<String> {
        let transforms: Vec<String> = query
            .result_columns
            .iter()
            .filter_map(|col| {
                // Type-override read expressions.
                if let Some(read_expr) = self.get(&col.sql_type).read_expr.as_ref() {
                    let raw_access = format!("{raw_var}.{}", col.name);
                    return Some(format!("{}: {}", col.name, read_expr.replace("{raw}", &raw_access)));
                }
                // Enum array columns: pg returns them as raw text ('{a,b,c}').
                // Parse into a proper JS string array.
                if let SqlType::Array(inner) = &col.sql_type {
                    if matches!(inner.as_ref(), SqlType::Enum(_)) {
                        let raw_access = format!("{raw_var}.{}", col.name);
                        let parse =
                            format!("typeof {raw_access} === 'string' ? {raw_access}.replace(/[{{}}]/g, '').split(',').filter(Boolean) : ({raw_access} ?? [])");
                        return Some(format!("{}: {parse}", col.name));
                    }
                }
                None
            })
            .collect();
        if transforms.is_empty() {
            return None;
        }
        Some(format!("{{ ...{raw_var}, {} }}", transforms.join(", ")))
    }
}

/// Build the fully-resolved type map for the TypeScript/JavaScript backend.
///
/// Delegates all driver-specific type behavior to `adapter` — no branching on flags.
pub(super) fn build_js_type_map(config: &OutputConfig, adapter: &dyn JsDriverAdapter) -> JsTypeMap {
    let output = adapter.output();
    let language = match output {
        JsOutput::TypeScript => Language::TypeScript,
        JsOutput::JavaScript => Language::JavaScript,
    };

    let types = canonical_sql_types();
    let mut entries = HashMap::with_capacity(types.len());
    for sql_type in &types {
        let preset = |s: &str| adapter.resolve_preset(s);
        let field_ov = resolve_type_override(sql_type, TypeVariant::Field, config, language, preset);
        let param_ov = resolve_type_override(sql_type, TypeVariant::Param, config, language, preset);
        let default_field = js_default_type(sql_type, adapter);
        let field_type = field_ov.as_ref().map(|o| o.name.clone()).unwrap_or(default_field.clone());
        let param_type = param_ov.as_ref().map(|o| o.name.clone()).or_else(|| field_ov.as_ref().map(|o| o.name.clone())).unwrap_or(default_field);
        let read_expr = field_ov.as_ref().and_then(|o| o.read_expr.clone()).or_else(|| bigint_default_read_expr(sql_type));
        let write_expr = param_ov
            .and_then(|o| o.write_expr)
            .or_else(|| field_ov.and_then(|o| o.write_expr))
            .or_else(|| adapter.bigint_write_expr().filter(|_| matches!(sql_type, SqlType::BigInt)).map(String::from));
        entries.insert(sql_type_key(sql_type), JsTypeEntry { field_type, param_type, read_expr, write_expr });
    }
    JsTypeMap { entries }
}

/// Default read expression for BigInt: all drivers return BIGINT as a non-bigint primitive.
fn bigint_default_read_expr(sql_type: &SqlType) -> Option<String> {
    if matches!(sql_type, SqlType::BigInt | SqlType::BigIntUnsigned) {
        Some("BigInt({raw})".to_string())
    } else {
        None
    }
}

fn js_default_type(sql_type: &SqlType, adapter: &dyn JsDriverAdapter) -> String {
    match sql_type {
        SqlType::Boolean => "boolean",
        SqlType::SmallInt | SqlType::Integer => "number",
        SqlType::BigInt => "bigint",
        // MySQL UNSIGNED variants. Up through INT UNSIGNED (2^32-1) the value
        // fits in JS `number` (safe integer range goes to 2^53-1). BIGINT
        // UNSIGNED (2^64-1) exceeds that, so it must use `bigint`.
        SqlType::TinyIntUnsigned | SqlType::SmallIntUnsigned | SqlType::IntegerUnsigned => "number",
        SqlType::BigIntUnsigned => "bigint",
        SqlType::Real | SqlType::Double => "number",
        SqlType::Decimal => "string",
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => "string",
        SqlType::Interval | SqlType::Uuid => "string",
        SqlType::Bytes => "Uint8Array",
        SqlType::Date => adapter.date_field_type(),
        SqlType::Time => "string",
        SqlType::Timestamp | SqlType::TimestampTz => "Date",
        SqlType::Json | SqlType::Jsonb => "unknown",
        SqlType::Custom(_) => "unknown",
        SqlType::Enum(_) | SqlType::Array(_) => unreachable!("enums and arrays are not in the canonical type list"),
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::typescript::{adapter::build_adapter, JsOutput, JsTarget};

    #[test]
    fn unsigned_integers_widen_only_for_64bit() {
        // Up through INT UNSIGNED (2^32-1) the value fits safely in JS `number`.
        // BIGINT UNSIGNED (2^64-1) exceeds Number.MAX_SAFE_INTEGER and must use bigint.
        let adapter = build_adapter(JsTarget::Mysql2, JsOutput::TypeScript);
        assert_eq!(js_default_type(&SqlType::TinyIntUnsigned, adapter.as_ref()), "number");
        assert_eq!(js_default_type(&SqlType::SmallIntUnsigned, adapter.as_ref()), "number");
        assert_eq!(js_default_type(&SqlType::IntegerUnsigned, adapter.as_ref()), "number");
        assert_eq!(js_default_type(&SqlType::BigIntUnsigned, adapter.as_ref()), "bigint");
    }
}
