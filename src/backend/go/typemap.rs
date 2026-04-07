use std::collections::HashMap;

use crate::backend::common::{canonical_sql_types, sql_type_key, SqlTypeKey};
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

fn go_default_entry(sql_type: &SqlType, json_mode: GoJsonMode) -> GoTypeEntry {
    let db_sql = Some("\"database/sql\"".to_string());
    let time_imp = Some("\"time\"".to_string());
    match sql_type {
        SqlType::Boolean => GoTypeEntry {
            field_type: "bool".into(),
            field_type_nullable: "sql.NullBool".into(),
            param_type: "bool".into(),
            param_type_nullable: "sql.NullBool".into(),
            import: None,
            import_nullable: db_sql,
        },
        SqlType::SmallInt => GoTypeEntry {
            field_type: "int16".into(),
            field_type_nullable: "sql.NullInt16".into(),
            param_type: "int16".into(),
            param_type_nullable: "sql.NullInt16".into(),
            import: None,
            import_nullable: db_sql,
        },
        SqlType::Integer => GoTypeEntry {
            field_type: "int32".into(),
            field_type_nullable: "sql.NullInt32".into(),
            param_type: "int32".into(),
            param_type_nullable: "sql.NullInt32".into(),
            import: None,
            import_nullable: db_sql,
        },
        SqlType::BigInt => GoTypeEntry {
            field_type: "int64".into(),
            field_type_nullable: "sql.NullInt64".into(),
            param_type: "int64".into(),
            param_type_nullable: "sql.NullInt64".into(),
            import: None,
            import_nullable: db_sql,
        },
        SqlType::Real => GoTypeEntry {
            field_type: "float32".into(),
            field_type_nullable: "*float32".into(),
            param_type: "float32".into(),
            param_type_nullable: "*float32".into(),
            import: None,
            import_nullable: None,
        },
        SqlType::Double => GoTypeEntry {
            field_type: "float64".into(),
            field_type_nullable: "sql.NullFloat64".into(),
            param_type: "float64".into(),
            param_type_nullable: "sql.NullFloat64".into(),
            import: None,
            import_nullable: db_sql,
        },
        SqlType::Decimal | SqlType::Interval | SqlType::Uuid => GoTypeEntry {
            field_type: "string".into(),
            field_type_nullable: "sql.NullString".into(),
            param_type: "string".into(),
            param_type_nullable: "sql.NullString".into(),
            import: None,
            import_nullable: db_sql,
        },
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) => GoTypeEntry {
            field_type: "string".into(),
            field_type_nullable: "sql.NullString".into(),
            param_type: "string".into(),
            param_type_nullable: "sql.NullString".into(),
            import: None,
            import_nullable: db_sql,
        },
        SqlType::Bytes => GoTypeEntry {
            field_type: "[]byte".into(),
            field_type_nullable: "[]byte".into(),
            param_type: "[]byte".into(),
            param_type_nullable: "[]byte".into(),
            import: None,
            import_nullable: None, // nil slice represents NULL
        },
        SqlType::Date | SqlType::Time | SqlType::Timestamp | SqlType::TimestampTz => GoTypeEntry {
            field_type: "time.Time".into(),
            field_type_nullable: "sql.NullTime".into(),
            param_type: "time.Time".into(),
            param_type_nullable: "sql.NullTime".into(),
            import: time_imp,
            import_nullable: db_sql,
        },
        SqlType::Json | SqlType::Jsonb => match json_mode {
            GoJsonMode::Bytes => GoTypeEntry {
                field_type: "[]byte".into(),
                field_type_nullable: "*[]byte".into(),
                param_type: "[]byte".into(),
                param_type_nullable: "*[]byte".into(),
                import: None,
                import_nullable: None,
            },
            GoJsonMode::String => GoTypeEntry {
                field_type: "string".into(),
                field_type_nullable: "sql.NullString".into(),
                param_type: "string".into(),
                param_type_nullable: "sql.NullString".into(),
                import: None,
                import_nullable: db_sql,
            },
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
