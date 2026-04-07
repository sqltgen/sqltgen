use std::collections::HashMap;

use crate::backend::common::{canonical_sql_types, sql_type_key, SqlTypeKey};
use crate::config::{resolve_type_override, Language, OutputConfig, TypeVariant};
use crate::ir::SqlType;

use super::adapter::PythonJsonMode;

fn try_preset_python(_name: &str) -> Option<crate::config::ResolvedType> {
    None
}

/// Fully resolved code-generation entry for one SQL type in the Python backend.
///
/// Built once before codegen by [`build_python_type_map`]; all override and default
/// logic runs during map construction and is never repeated at emit time.
pub(super) struct PythonTypeEntry {
    /// Python type annotation for result columns and model fields (base, without `| None`).
    pub(super) field_type: String,
    /// Python type annotation for query parameters (base, without `| None`).
    /// May differ from `field_type` when a split override is configured.
    pub(super) param_type: String,
    /// Optional import line (e.g. `"import datetime"`) for this type.
    /// Prefers the field override import; falls back to the param override import.
    pub(super) import: Option<String>,
}

/// Pre-resolved map from SQL type to Python code-generation entry.
///
/// Constructed once by [`build_python_type_map`]; consumed by the emitters in `core.rs`
/// without any further override or dispatch logic.
pub(super) struct PythonTypeMap(HashMap<SqlTypeKey, PythonTypeEntry>);

impl PythonTypeMap {
    /// Return the resolved entry for `sql_type`.
    pub(super) fn get(&self, sql_type: &SqlType) -> &PythonTypeEntry {
        &self.0[&sql_type_key(sql_type)]
    }

    /// Return the Python field type annotation for `sql_type`, with nullability applied.
    ///
    /// `Array(inner)` maps to `list[InnerType]` (or `list[InnerType] | None` when nullable).
    pub(super) fn field_type(&self, sql_type: &SqlType, nullable: bool) -> String {
        if let SqlType::Array(inner) = sql_type {
            let inner_ty = self.field_type(inner, false);
            let list_ty = format!("list[{inner_ty}]");
            return if nullable { format!("{list_ty} | None") } else { list_ty };
        }
        let base = &self.get(sql_type).field_type;
        if nullable {
            format!("{base} | None")
        } else {
            base.clone()
        }
    }

    /// Return the Python parameter type annotation for `sql_type`, with nullability applied.
    pub(super) fn param_type(&self, sql_type: &SqlType, nullable: bool) -> String {
        if let SqlType::Array(inner) = sql_type {
            let inner_ty = self.param_type(inner, false);
            let list_ty = format!("list[{inner_ty}]");
            return if nullable { format!("{list_ty} | None") } else { list_ty };
        }
        let base = &self.get(sql_type).param_type;
        if nullable {
            format!("{base} | None")
        } else {
            base.clone()
        }
    }

    /// Return the import line for `sql_type`, if one is needed.
    ///
    /// For `Array(inner)`, returns the import for the inner element type.
    pub(super) fn import_for(&self, sql_type: &SqlType) -> Option<String> {
        if let SqlType::Array(inner) = sql_type {
            return self.import_for(inner);
        }
        self.get(sql_type).import.clone()
    }
}

/// Build the fully-resolved type map for the Python backend.
///
/// `json_mode` determines how `Json`/`Jsonb` SQL types are represented in Python
/// (as `str` or `object`). It comes from the engine contract, not from user config.
pub(super) fn build_python_type_map(config: &OutputConfig, json_mode: PythonJsonMode) -> PythonTypeMap {
    let types = canonical_sql_types();
    let mut map = HashMap::with_capacity(types.len());
    for sql_type in &types {
        let field_ov = resolve_type_override(sql_type, TypeVariant::Field, config, Language::Python, try_preset_python);
        let param_ov = resolve_type_override(sql_type, TypeVariant::Param, config, Language::Python, try_preset_python);
        let default = python_default_entry(sql_type, json_mode);
        let field_type = field_ov.as_ref().map(|o| o.name.clone()).unwrap_or(default.field_type);
        let param_type = param_ov.as_ref().map(|o| o.name.clone()).or_else(|| field_ov.as_ref().map(|o| o.name.clone())).unwrap_or(default.param_type);
        let import = field_ov.as_ref().and_then(|o| o.import.clone()).or_else(|| param_ov.as_ref().and_then(|o| o.import.clone())).or(default.import);
        map.insert(sql_type_key(sql_type), PythonTypeEntry { field_type, param_type, import });
    }
    PythonTypeMap(map)
}

struct PythonDefaultEntry {
    field_type: String,
    param_type: String,
    import: Option<String>,
}

impl PythonDefaultEntry {
    fn simple(ty: &'static str) -> Self {
        PythonDefaultEntry { field_type: ty.to_string(), param_type: ty.to_string(), import: None }
    }

    fn with_import(ty: &'static str, import: &'static str) -> Self {
        PythonDefaultEntry { field_type: ty.to_string(), param_type: ty.to_string(), import: Some(import.to_string()) }
    }
}

fn python_default_entry(sql_type: &SqlType, json_mode: PythonJsonMode) -> PythonDefaultEntry {
    match sql_type {
        SqlType::Boolean => PythonDefaultEntry::simple("bool"),
        SqlType::SmallInt | SqlType::Integer | SqlType::BigInt => PythonDefaultEntry::simple("int"),
        SqlType::Real | SqlType::Double => PythonDefaultEntry::simple("float"),
        SqlType::Decimal => PythonDefaultEntry::with_import("decimal.Decimal", "import decimal"),
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => PythonDefaultEntry::simple("str"),
        SqlType::Bytes => PythonDefaultEntry::simple("bytes"),
        SqlType::Date => PythonDefaultEntry::with_import("datetime.date", "import datetime"),
        SqlType::Time => PythonDefaultEntry::with_import("datetime.time", "import datetime"),
        SqlType::Timestamp | SqlType::TimestampTz => PythonDefaultEntry::with_import("datetime.datetime", "import datetime"),
        SqlType::Interval => PythonDefaultEntry::with_import("datetime.timedelta", "import datetime"),
        SqlType::Uuid => PythonDefaultEntry::with_import("uuid.UUID", "import uuid"),
        SqlType::Json | SqlType::Jsonb => match json_mode {
            PythonJsonMode::Object => PythonDefaultEntry::simple("object"),
            PythonJsonMode::Text => PythonDefaultEntry::simple("str"),
        },
        SqlType::Custom(_) => PythonDefaultEntry::with_import("Any", "from typing import Any"),
        SqlType::Enum(_) | SqlType::Array(_) => unreachable!("enums and arrays are not in the canonical type list"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{OutputConfig, TypeOverride, TypeRef};
    use crate::ir::SqlType;

    fn map_default() -> PythonTypeMap {
        build_python_type_map(&OutputConfig::default(), PythonJsonMode::Object)
    }

    fn map_with_override(sql_name: &str, py_type: &str) -> PythonTypeMap {
        let mut config = OutputConfig::default();
        config.type_overrides.insert(sql_name.to_string(), TypeOverride::Same(crate::config::TypeRef::String(py_type.to_string())));
        build_python_type_map(&config, PythonJsonMode::Object)
    }

    #[test]
    fn test_array_timestamp_default_annotation() {
        let map = map_default();
        assert_eq!(map.field_type(&SqlType::Array(Box::new(SqlType::Timestamp)), false), "list[datetime.datetime]");
    }

    #[test]
    fn test_array_uuid_default_annotation() {
        let map = map_default();
        assert_eq!(map.field_type(&SqlType::Array(Box::new(SqlType::Uuid)), false), "list[uuid.UUID]");
    }

    #[test]
    fn test_array_text_default_annotation() {
        let map = map_default();
        assert_eq!(map.field_type(&SqlType::Array(Box::new(SqlType::Text)), false), "list[str]");
    }

    #[test]
    fn test_array_nullable_annotation() {
        let map = map_default();
        assert_eq!(map.field_type(&SqlType::Array(Box::new(SqlType::Text)), true), "list[str] | None");
    }

    #[test]
    fn test_array_element_type_respects_override() {
        let map = map_with_override("timestamp", "MyTimestamp");
        assert_eq!(map.field_type(&SqlType::Array(Box::new(SqlType::Timestamp)), false), "list[MyTimestamp]");
    }
}
