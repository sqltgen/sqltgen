use std::collections::HashMap;

use crate::backend::common::{canonical_sql_types, sql_type_key, SqlTypeKey};
use crate::backend::naming::to_pascal_case;
use crate::config::{resolve_type_override, Language, OutputConfig, TypeVariant};
use crate::ir::SqlType;

use super::adapter::GoJsonMode;

fn try_preset_go(_name: &str) -> Option<crate::config::ResolvedType> {
    None
}

/// Fully resolved code-generation entry for one SQL type in the Go backend.
///
/// Built once before codegen by [`build_go_type_map`]; all override and default
/// logic runs during map construction and is never repeated at emit time.
///
/// Go uses distinct nullable types (e.g. `sql.NullBool` vs `bool`) rather than a
/// simple wrapper, so both forms are stored pre-resolved.
pub(super) struct GoTypeEntry {
    /// Non-nullable Go type name (e.g. `"bool"`, `"time.Time"`).
    pub(super) field_type: String,
    /// Nullable Go type name (e.g. `"sql.NullBool"`, `"sql.NullTime"`).
    pub(super) field_type_nullable: String,
    /// Non-nullable Go type name for parameters. May differ from `field_type` on split overrides.
    pub(super) param_type: String,
    /// Nullable Go parameter type.
    pub(super) param_type_nullable: String,
    /// Import path string (quoted, e.g. `"\"time\""`) needed for the non-nullable type.
    pub(super) import: Option<String>,
    /// Import path string needed for the nullable type. Often `"\"database/sql\""`.
    pub(super) import_nullable: Option<String>,
}

/// Pre-resolved map from SQL type to Go code-generation entry.
///
/// Constructed once by [`build_go_type_map`]; consumed by the emitters in `core.rs`
/// without any further override or dispatch logic.
pub(super) struct GoTypeMap(HashMap<SqlTypeKey, GoTypeEntry>);

impl GoTypeMap {
    /// Return the resolved entry for `sql_type`.
    pub(super) fn get(&self, sql_type: &SqlType) -> &GoTypeEntry {
        &self.0[&sql_type_key(sql_type)]
    }

    /// Return the Go field type string for `sql_type`, with nullability applied.
    ///
    /// `Array(inner)` maps to `[]InnerType` (or `*[]InnerType` when nullable).
    pub(super) fn field_type(&self, sql_type: &SqlType, nullable: bool) -> String {
        if let SqlType::Enum(name) = sql_type {
            let ty = to_pascal_case(name);
            return if nullable { format!("*{ty}") } else { ty };
        }
        if let SqlType::Array(inner) = sql_type {
            let inner_ty = self.field_type(inner, false);
            let slice = format!("[]{inner_ty}");
            return if nullable { format!("*{slice}") } else { slice };
        }
        let entry = self.get(sql_type);
        if nullable {
            entry.field_type_nullable.clone()
        } else {
            entry.field_type.clone()
        }
    }

    /// Return the Go parameter type string for `sql_type`, with nullability applied.
    pub(super) fn param_type(&self, sql_type: &SqlType, nullable: bool) -> String {
        if let SqlType::Enum(name) = sql_type {
            let ty = to_pascal_case(name);
            return if nullable { format!("*{ty}") } else { ty };
        }
        if let SqlType::Array(inner) = sql_type {
            let inner_ty = self.param_type(inner, false);
            let slice = format!("[]{inner_ty}");
            return if nullable { format!("*{slice}") } else { slice };
        }
        let entry = self.get(sql_type);
        if nullable {
            entry.param_type_nullable.clone()
        } else {
            entry.param_type.clone()
        }
    }

    /// Return the import path string needed when `sql_type` is used with the given nullability.
    ///
    /// For `Array(inner)`, returns the import for the inner element type.
    pub(super) fn import_for(&self, sql_type: &SqlType, nullable: bool) -> Option<String> {
        if matches!(sql_type, SqlType::Enum(_)) {
            return None;
        }
        if let SqlType::Array(inner) = sql_type {
            return self.import_for(inner, false);
        }
        let entry = self.get(sql_type);
        if nullable {
            entry.import_nullable.clone()
        } else {
            entry.import.clone()
        }
    }
}

/// Build the fully-resolved type map for the Go backend.
///
/// `json_mode` determines how `Json`/`Jsonb` SQL types are represented in Go
/// (`[]byte` or `string`). It comes from the engine contract, not from user config.
pub(super) fn build_go_type_map(config: &OutputConfig, json_mode: GoJsonMode) -> GoTypeMap {
    let types = canonical_sql_types();
    let mut map = HashMap::with_capacity(types.len());
    for sql_type in &types {
        let field_ov = resolve_type_override(sql_type, TypeVariant::Field, config, Language::Go, try_preset_go);
        let param_ov = resolve_type_override(sql_type, TypeVariant::Param, config, Language::Go, try_preset_go);
        let default = go_default_entry(sql_type, json_mode);
        let entry = apply_overrides(default, field_ov.as_ref(), param_ov.as_ref());
        map.insert(sql_type_key(sql_type), entry);
    }
    GoTypeMap(map)
}

fn apply_overrides(default: GoTypeEntry, field_ov: Option<&crate::config::ResolvedType>, param_ov: Option<&crate::config::ResolvedType>) -> GoTypeEntry {
    let (field_type, field_type_nullable, import, import_nullable) = if let Some(ov) = field_ov {
        let nullable = format!("*{}", ov.name);
        let imp = ov.import.as_ref().map(|i| format!("\"{i}\""));
        (ov.name.clone(), nullable, imp.clone(), imp)
    } else {
        (default.field_type, default.field_type_nullable, default.import, default.import_nullable)
    };
    let (param_type, param_type_nullable) = if let Some(ov) = param_ov {
        (ov.name.clone(), format!("*{}", ov.name))
    } else if field_ov.is_some() {
        (field_type.clone(), field_type_nullable.clone())
    } else {
        (default.param_type, default.param_type_nullable)
    };
    GoTypeEntry { field_type, field_type_nullable, param_type, param_type_nullable, import, import_nullable }
}

/// Default entry where the nullable form is one of the `sql.NullX` helpers from
/// `database/sql`. The non-nullable field and param types are both `go_type`.
fn sql_null_entry(go_type: &str, null_type: &str) -> GoTypeEntry {
    GoTypeEntry {
        field_type: go_type.into(),
        field_type_nullable: null_type.into(),
        param_type: go_type.into(),
        param_type_nullable: null_type.into(),
        import: None,
        import_nullable: Some("\"database/sql\"".to_string()),
    }
}

/// Default entry where the nullable form is a Go pointer (`*T`). Used for types
/// without a `sql.NullX` counterpart (unsigned integers, `float32`, `[]byte`).
fn pointer_null_entry(go_type: &str) -> GoTypeEntry {
    GoTypeEntry {
        field_type: go_type.into(),
        field_type_nullable: format!("*{go_type}"),
        param_type: go_type.into(),
        param_type_nullable: format!("*{go_type}"),
        import: None,
        import_nullable: None,
    }
}

fn go_default_entry(sql_type: &SqlType, json_mode: GoJsonMode) -> GoTypeEntry {
    match sql_type {
        SqlType::Boolean => sql_null_entry("bool", "sql.NullBool"),
        SqlType::SmallInt => sql_null_entry("int16", "sql.NullInt16"),
        SqlType::Integer => sql_null_entry("int32", "sql.NullInt32"),
        SqlType::BigInt => sql_null_entry("int64", "sql.NullInt64"),
        // MySQL UNSIGNED integers map to native Go unsigned widths. database/sql
        // has no NullUintN helpers, so nullable forms use pointer types (the
        // same pattern used for `Real`/`float32`).
        SqlType::TinyIntUnsigned => pointer_null_entry("uint8"),
        SqlType::SmallIntUnsigned => pointer_null_entry("uint16"),
        SqlType::IntegerUnsigned => pointer_null_entry("uint32"),
        SqlType::BigIntUnsigned => pointer_null_entry("uint64"),
        SqlType::Real => pointer_null_entry("float32"),
        SqlType::Double => sql_null_entry("float64", "sql.NullFloat64"),
        SqlType::Decimal | SqlType::Interval | SqlType::Uuid => sql_null_entry("string", "sql.NullString"),
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => sql_null_entry("string", "sql.NullString"),
        SqlType::Bytes => GoTypeEntry {
            field_type: "[]byte".into(),
            field_type_nullable: "[]byte".into(),
            param_type: "[]byte".into(),
            param_type_nullable: "[]byte".into(),
            import: None,
            import_nullable: None, // nil slice represents NULL
        },
        SqlType::Date | SqlType::Time | SqlType::Timestamp | SqlType::TimestampTz => {
            GoTypeEntry { import: Some("\"time\"".to_string()), ..sql_null_entry("time.Time", "sql.NullTime") }
        },
        SqlType::Json | SqlType::Jsonb => match json_mode {
            GoJsonMode::Bytes => pointer_null_entry("[]byte"),
            GoJsonMode::String => sql_null_entry("string", "sql.NullString"),
        },
        SqlType::Custom(_) => GoTypeEntry {
            field_type: "any".into(),
            field_type_nullable: "any".into(),
            param_type: "any".into(),
            param_type_nullable: "any".into(),
            import: None,
            import_nullable: None,
        },
        SqlType::Enum(_) | SqlType::Array(_) => unreachable!("enums and arrays are not in the canonical type list"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsigned_integers_map_to_native_unsigned_widths() {
        let entry = go_default_entry(&SqlType::TinyIntUnsigned, GoJsonMode::String);
        assert_eq!(entry.field_type, "uint8");
        assert_eq!(entry.field_type_nullable, "*uint8");

        let entry = go_default_entry(&SqlType::SmallIntUnsigned, GoJsonMode::String);
        assert_eq!(entry.field_type, "uint16");
        assert_eq!(entry.field_type_nullable, "*uint16");

        let entry = go_default_entry(&SqlType::IntegerUnsigned, GoJsonMode::String);
        assert_eq!(entry.field_type, "uint32");
        assert_eq!(entry.field_type_nullable, "*uint32");

        let entry = go_default_entry(&SqlType::BigIntUnsigned, GoJsonMode::String);
        assert_eq!(entry.field_type, "uint64");
        assert_eq!(entry.field_type_nullable, "*uint64");
    }
}
