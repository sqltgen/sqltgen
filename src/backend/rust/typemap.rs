use std::collections::HashMap;

use crate::backend::common::{canonical_sql_types, sql_type_key, SqlTypeKey};
use crate::backend::naming::to_pascal_case;
use crate::config::{resolve_type_override, Language, OutputConfig, ResolvedType, TypeVariant};
use crate::ir::SqlType;

fn try_preset_rust(name: &str) -> Option<ResolvedType> {
    match name {
        // sqlx implements Decode/Encode for serde_json::Value natively —
        // no read_expr/write_expr needed; the type name alone is sufficient.
        "serde_json" => Some(ResolvedType::simple("serde_json::Value")),
        _ => None,
    }
}

/// Fully resolved code-generation entry for one SQL type in the Rust backend.
///
/// Built once before codegen by [`build_rust_type_map`]; all override and default
/// logic runs during map construction and is never repeated at emit time.
///
/// Rust uses fully qualified paths (e.g. `time::Date`, `uuid::Uuid`) so no import
/// tracking is needed at codegen time. User overrides may include an import path for
/// future `use` statement emission, but that is not yet implemented.
pub(super) struct RustTypeEntry {
    /// Rust type name for result columns and model fields (base, without `Option<>`).
    pub(super) field_type: String,
    /// Rust type name for query parameters (base, without `Option<>`).
    pub(super) param_type: String,
}

/// Pre-resolved map from SQL type to Rust code-generation entry.
///
/// Constructed once by [`build_rust_type_map`]; consumed by the emitters in `core.rs`
/// without any further override or dispatch logic.
pub(super) struct RustTypeMap(HashMap<SqlTypeKey, RustTypeEntry>);

impl RustTypeMap {
    /// Return the resolved entry for `sql_type`.
    pub(super) fn get(&self, sql_type: &SqlType) -> &RustTypeEntry {
        &self.0[&sql_type_key(sql_type)]
    }

    /// Return the Rust field type string for `sql_type`, with nullability applied.
    ///
    /// `Array(inner)` maps to `Vec<InnerType>` (or `Option<Vec<InnerType>>` when nullable).
    pub(super) fn field_type(&self, sql_type: &SqlType, nullable: bool) -> String {
        if let SqlType::Enum(name) = sql_type {
            let ty = to_pascal_case(name);
            return if nullable { format!("Option<{ty}>") } else { ty };
        }
        if let SqlType::Array(inner) = sql_type {
            let inner_ty = self.field_type(inner, false);
            let vec_ty = format!("Vec<{inner_ty}>");
            return if nullable { format!("Option<{vec_ty}>") } else { vec_ty };
        }
        let base = &self.get(sql_type).field_type;
        if nullable {
            format!("Option<{base}>")
        } else {
            base.clone()
        }
    }

    /// Return the Rust parameter type string for `sql_type`, with nullability applied.
    pub(super) fn param_type(&self, sql_type: &SqlType, nullable: bool) -> String {
        if let SqlType::Enum(name) = sql_type {
            let ty = to_pascal_case(name);
            return if nullable { format!("Option<{ty}>") } else { ty };
        }
        if let SqlType::Array(inner) = sql_type {
            let inner_ty = self.param_type(inner, false);
            let vec_ty = format!("Vec<{inner_ty}>");
            return if nullable { format!("Option<{vec_ty}>") } else { vec_ty };
        }
        let base = &self.get(sql_type).param_type;
        if nullable {
            format!("Option<{base}>")
        } else {
            base.clone()
        }
    }
}

/// Build the fully-resolved type map for the Rust backend.
///
/// Iterates over all canonical SQL types, applies any configured overrides on top of the
/// defaults, and stores the result. The rest of the code generator consumes the map without
/// any further override checks.
pub(super) fn build_rust_type_map(config: &OutputConfig) -> RustTypeMap {
    let types = canonical_sql_types();
    let mut map = HashMap::with_capacity(types.len());
    for sql_type in &types {
        let field_ov = resolve_type_override(sql_type, TypeVariant::Field, config, Language::Rust, try_preset_rust);
        let param_ov = resolve_type_override(sql_type, TypeVariant::Param, config, Language::Rust, try_preset_rust);
        let default = rust_default_type_name(sql_type);
        let field_type = field_ov.as_ref().map(|o| o.name.clone()).unwrap_or_else(|| default.to_string());
        let param_type = param_ov.as_ref().map(|o| o.name.clone()).unwrap_or_else(|| default.to_string());
        map.insert(sql_type_key(sql_type), RustTypeEntry { field_type, param_type });
    }
    RustTypeMap(map)
}

fn rust_default_type_name(sql_type: &SqlType) -> &'static str {
    match sql_type {
        SqlType::Boolean => "bool",
        SqlType::SmallInt => "i16",
        SqlType::Integer => "i32",
        SqlType::BigInt => "i64",
        SqlType::Real => "f32",
        SqlType::Double => "f64",
        SqlType::Decimal => "rust_decimal::Decimal",
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) | SqlType::Interval => "String",
        SqlType::Bytes => "Vec<u8>",
        SqlType::Date => "time::Date",
        SqlType::Time => "time::Time",
        SqlType::Timestamp => "time::PrimitiveDateTime",
        SqlType::TimestampTz => "time::OffsetDateTime",
        SqlType::Uuid => "uuid::Uuid",
        SqlType::Json | SqlType::Jsonb => "serde_json::Value",
        SqlType::Custom(_) => "serde_json::Value",
        SqlType::Enum(_) | SqlType::Array(_) => unreachable!("enums and arrays are not in the canonical type list"),
    }
}
