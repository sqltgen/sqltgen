use std::collections::HashMap;

use crate::backend::common::{canonical_sql_types, sql_type_key, SqlTypeKey};
use crate::backend::jdbc::{preset_gson, preset_jackson};
use crate::config::{resolve_type_override, ExtraField, Language, OutputConfig, ResolvedType, TypeVariant};
use crate::ir::SqlType;

/// Fully resolved code-generation entry for one SQL type in the Kotlin backend.
///
/// Built once before codegen by [`build_kotlin_type_map`]; all override and default
/// logic runs during map construction and is never repeated at emit time.
pub(super) struct KotlinTypeEntry {
    /// Kotlin type name for result columns and model fields.
    pub(super) field_type: String,
    /// Kotlin type name for query parameters.
    pub(super) param_type: String,
    /// Non-nullable ResultSet read expression with `{idx}` placeholder.
    pub(super) read: String,
    /// Nullable ResultSet read expression with `{idx}` placeholder.
    pub(super) read_nullable: String,
    /// Write expression for param binding with `{value}` placeholder, or `None` for raw value.
    pub(super) write: Option<String>,
    /// Per-element lambda body for JDBC ARRAY reads (`it` = raw JDBC element),
    /// or `None` when the driver delivers the correct type and `jdbcArrayToList` is safe.
    pub(super) array_elem: Option<String>,
    /// Import path to add when this type is used, if any.
    pub(super) import: Option<String>,
    /// Extra class-level declarations (e.g. `ObjectMapper` for jackson preset).
    pub(super) extra_fields: Vec<ExtraField>,
}

/// Pre-resolved map from SQL type to Kotlin code-generation entry.
///
/// Constructed once by [`build_kotlin_type_map`]; consumed by the emitters in `core.rs`
/// without any further override or dispatch logic.
pub(super) struct KotlinTypeMap(HashMap<SqlTypeKey, KotlinTypeEntry>);

impl KotlinTypeMap {
    /// Return the resolved entry for `sql_type`.
    pub(super) fn get(&self, sql_type: &SqlType) -> &KotlinTypeEntry {
        &self.0[&sql_type_key(sql_type)]
    }

    /// Return the Kotlin field type string for `sql_type`, with nullability applied.
    ///
    /// `Array(inner)` maps to `List<InnerType>` (or `List<InnerType>?` when nullable).
    pub(super) fn kotlin_type(&self, sql_type: &SqlType, nullable: bool) -> String {
        if let SqlType::Array(inner) = sql_type {
            let t = format!("List<{}>", self.kotlin_type(inner, false));
            return if nullable { format!("{t}?") } else { t };
        }
        let name = &self.get(sql_type).field_type;
        if nullable {
            format!("{name}?")
        } else {
            name.clone()
        }
    }

    /// Return the Kotlin parameter type string for `sql_type`, with nullability applied.
    ///
    /// Uses the param-specific type mapping (which may differ from the field type when a
    /// param-variant override is configured). `Array(inner)` maps to `List<InnerParamType>`.
    pub(super) fn kotlin_param_type(&self, sql_type: &SqlType, nullable: bool) -> String {
        if let SqlType::Array(inner) = sql_type {
            let t = format!("List<{}>", self.get(inner).param_type);
            return if nullable { format!("{t}?") } else { t };
        }
        let name = &self.get(sql_type).param_type;
        if nullable {
            format!("{name}?")
        } else {
            name.clone()
        }
    }
}

// ─── Public entry point ───────────────────────────────────────────────────────

/// Build the fully-resolved type map for the Kotlin backend.
///
/// Iterates over all canonical SQL types, applies any configured overrides on top of the
/// defaults, and stores the result. The rest of the code generator consumes the map without
/// any further override checks.
pub(super) fn build_kotlin_type_map(config: &OutputConfig) -> KotlinTypeMap {
    let types = canonical_sql_types();
    let mut map = HashMap::with_capacity(types.len());
    for sql_type in &types {
        let defaults = kotlin_type_info(sql_type);
        let field_ov = get_type_override_kotlin(sql_type, TypeVariant::Field, config);
        let param_ov = get_type_override_kotlin(sql_type, TypeVariant::Param, config);
        map.insert(sql_type_key(sql_type), build_entry(&defaults, field_ov.as_ref(), param_ov.as_ref()));
    }
    KotlinTypeMap(map)
}

// ─── Override resolution ──────────────────────────────────────────────────────

fn get_type_override_kotlin(sql_type: &SqlType, variant: TypeVariant, config: &OutputConfig) -> Option<ResolvedType> {
    resolve_type_override(sql_type, variant, config, Language::Kotlin, try_preset_kotlin)
}

fn try_preset_kotlin(name: &str) -> Option<ResolvedType> {
    match name {
        "jackson" => {
            let mut rt = preset_jackson("JsonNode::class.java", "private val objectMapper = ObjectMapper()");
            rt.read_expr = Some("parseJson({raw})".to_string());
            rt.write_expr = Some("toJson({value})".to_string());
            rt.extra_fields.push(crate::config::ExtraField {
                declaration: "private fun parseJson(raw: String): com.fasterxml.jackson.databind.JsonNode = objectMapper.readValue(raw, com.fasterxml.jackson.databind.JsonNode::class.java)".to_string(),
                import: None,
            });
            rt.extra_fields.push(crate::config::ExtraField {
                declaration: "private fun toJson(value: com.fasterxml.jackson.databind.JsonNode?): String? = if (value == null) null else objectMapper.writeValueAsString(value)".to_string(),
                import: None,
            });
            Some(rt)
        },
        "gson" => {
            let mut rt = preset_gson("JsonElement::class.java", "private val gson = Gson()");
            rt.read_expr = Some("parseJson({raw})".to_string());
            rt.write_expr = Some("toJson({value})".to_string());
            rt.extra_fields.push(crate::config::ExtraField {
                declaration: "private fun parseJson(raw: String): com.google.gson.JsonElement = gson.fromJson(raw, com.google.gson.JsonElement::class.java)"
                    .to_string(),
                import: None,
            });
            rt.extra_fields.push(crate::config::ExtraField {
                declaration: "private fun toJson(value: com.google.gson.JsonElement?): String? = if (value == null) null else gson.toJson(value)".to_string(),
                import: None,
            });
            Some(rt)
        },
        _ => None,
    }
}

// ─── Entry construction ───────────────────────────────────────────────────────

fn build_entry(defaults: &KotlinTypeInfo, field_ov: Option<&ResolvedType>, param_ov: Option<&ResolvedType>) -> KotlinTypeEntry {
    let field_type = field_ov.map(|o| o.name.clone()).unwrap_or_else(|| defaults.name.to_string());
    let param_type = param_ov.map(|o| o.name.clone()).unwrap_or_else(|| defaults.name.to_string());
    let (read, read_nullable, array_elem) = resolve_read_exprs(defaults, field_ov);
    let write = param_ov.and_then(|o| o.write_expr.clone());
    let import = field_ov.and_then(|o| o.import.clone()).or_else(|| param_ov.and_then(|o| o.import.clone()));
    let extra_fields = field_ov.map(|o| o.extra_fields.clone()).unwrap_or_default();
    KotlinTypeEntry { field_type, param_type, read, read_nullable, write, array_elem, import, extra_fields }
}

/// Compute the `read`, `read_nullable`, and `array_elem` strings for a type entry.
///
/// The logic runs once at map-build time so emitters never need to inspect override state.
fn resolve_read_exprs(defaults: &KotlinTypeInfo, field_ov: Option<&ResolvedType>) -> (String, String, Option<String>) {
    if let Some(resolved) = field_ov {
        if let Some(read_expr) = &resolved.read_expr {
            // Override supplies a read_expr: substitute {raw} with the string-based getter.
            let read = read_expr.replace("{raw}", "rs.getString({idx})");
            let read_nullable = format!("rs.getString({{idx}})?.let {{ {}  }}", read_expr.replace("{raw}", "it"));
            let array_elem = Some(read_expr.replace("{raw}", defaults.array_raw));
            return (read, read_nullable, array_elem);
        }
        if defaults.uses_get_object {
            // Override without read_expr on a getObject type: use the override class name.
            let read = format!("rs.getObject({{idx}}, {}::class.java)", resolved.name);
            return (read.clone(), read, defaults.array_elem.map(|s| s.to_string()));
        }
    }
    let read = defaults.scalar_read.to_string();
    let read_nullable = if let Some(helper) = defaults.nullable_helper {
        // Primitive types: use wasNull()-based helper compatible with all JDBC drivers.
        format!("{helper}(rs, {{idx}})")
    } else {
        read.clone() // Reference types: getter already returns null for SQL NULL.
    };
    (read, read_nullable, defaults.array_elem.map(|s| s.to_string()))
}

// ─── Build-time defaults ──────────────────────────────────────────────────────
//
// `KotlinTypeInfo` captures the per-type defaults that feed into `build_kotlin_type_map`.
// This struct is private to this module; nothing outside it reads it.

struct KotlinTypeInfo {
    name: &'static str,
    scalar_read: &'static str, // with {idx} placeholder
    nullable_helper: Option<&'static str>,
    uses_get_object: bool,
    array_elem: Option<&'static str>,
    array_raw: &'static str, // {raw} substitution for read_expr overrides on array elements
}

fn kotlin_type_info(sql_type: &SqlType) -> KotlinTypeInfo {
    match sql_type {
        SqlType::Boolean => KotlinTypeInfo {
            name: "Boolean",
            scalar_read: "rs.getBoolean({idx})",
            nullable_helper: Some("getNullableBoolean"),
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
        SqlType::SmallInt => KotlinTypeInfo {
            name: "Short",
            scalar_read: "rs.getShort({idx})",
            nullable_helper: Some("getNullableShort"),
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
        SqlType::Integer => KotlinTypeInfo {
            name: "Int",
            scalar_read: "rs.getInt({idx})",
            nullable_helper: Some("getNullableInt"),
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
        SqlType::BigInt => KotlinTypeInfo {
            name: "Long",
            scalar_read: "rs.getLong({idx})",
            nullable_helper: Some("getNullableLong"),
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
        SqlType::Real => KotlinTypeInfo {
            name: "Float",
            scalar_read: "rs.getFloat({idx})",
            nullable_helper: Some("getNullableFloat"),
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
        SqlType::Double => KotlinTypeInfo {
            name: "Double",
            scalar_read: "rs.getDouble({idx})",
            nullable_helper: Some("getNullableDouble"),
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
        SqlType::Decimal => KotlinTypeInfo {
            name: "java.math.BigDecimal",
            scalar_read: "rs.getBigDecimal({idx})",
            nullable_helper: None,
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) | SqlType::Interval => KotlinTypeInfo {
            name: "String",
            scalar_read: "rs.getString({idx})",
            nullable_helper: None,
            uses_get_object: false,
            array_elem: None,
            array_raw: "it as String",
        },
        SqlType::Bytes => KotlinTypeInfo {
            name: "ByteArray",
            scalar_read: "rs.getBytes({idx})",
            nullable_helper: None,
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
        SqlType::Date => KotlinTypeInfo {
            name: "java.time.LocalDate",
            scalar_read: "rs.getObject({idx}, java.time.LocalDate::class.java)",
            nullable_helper: None,
            uses_get_object: true,
            array_elem: Some("(it as java.sql.Date).toLocalDate()"),
            array_raw: "(it as java.sql.Date).toString()",
        },
        SqlType::Time => KotlinTypeInfo {
            name: "java.time.LocalTime",
            scalar_read: "rs.getObject({idx}, java.time.LocalTime::class.java)",
            nullable_helper: None,
            uses_get_object: true,
            array_elem: Some("(it as java.sql.Time).toLocalTime()"),
            array_raw: "(it as java.sql.Time).toString()",
        },
        SqlType::Timestamp => KotlinTypeInfo {
            name: "java.time.LocalDateTime",
            scalar_read: "rs.getObject({idx}, java.time.LocalDateTime::class.java)",
            nullable_helper: None,
            uses_get_object: true,
            array_elem: Some("(it as java.sql.Timestamp).toLocalDateTime()"),
            array_raw: "(it as java.sql.Timestamp).toString()",
        },
        SqlType::TimestampTz => KotlinTypeInfo {
            name: "java.time.OffsetDateTime",
            scalar_read: "rs.getObject({idx}, java.time.OffsetDateTime::class.java)",
            nullable_helper: None,
            uses_get_object: true,
            array_elem: Some("(it as java.sql.Timestamp).toInstant().atOffset(java.time.ZoneOffset.UTC)"),
            array_raw: "(it as java.sql.Timestamp).toString()",
        },
        SqlType::Uuid => KotlinTypeInfo {
            name: "java.util.UUID",
            scalar_read: "rs.getObject({idx}, java.util.UUID::class.java)",
            nullable_helper: None,
            uses_get_object: true,
            array_elem: Some("(it as java.util.UUID)"),
            array_raw: "(it as java.util.UUID).toString()",
        },
        SqlType::Json | SqlType::Jsonb => KotlinTypeInfo {
            name: "String",
            scalar_read: "rs.getString({idx})",
            nullable_helper: None,
            uses_get_object: false,
            array_elem: None,
            array_raw: "it as String",
        },
        SqlType::Array(_) | SqlType::Enum(_) | SqlType::Custom(_) => KotlinTypeInfo {
            name: "Any",
            scalar_read: "rs.getObject({idx})",
            nullable_helper: None,
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
    }
}
