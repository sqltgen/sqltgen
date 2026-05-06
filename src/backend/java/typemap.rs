use std::collections::{BTreeSet, HashMap};

use crate::backend::common::{canonical_sql_types, sql_type_key, SqlTypeKey};
use crate::backend::jdbc::{preset_gson, preset_jackson};
use crate::backend::naming::to_pascal_case;
use crate::config::{resolve_type_override, ExtraField, Language, OutputConfig, ResolvedType, TypeVariant};
use crate::ir::{Parameter, Query, SqlType, Table};

fn try_preset_java(name: &str) -> Option<ResolvedType> {
    match name {
        "jackson" => {
            let mut rt = preset_jackson("JsonNode.class", "private static final ObjectMapper objectMapper = new ObjectMapper();");
            rt.read_expr = Some("parseJson({raw})".to_string());
            rt.write_expr = Some("toJson({value})".to_string());
            rt.extra_fields.push(ExtraField {
                declaration: concat!(
                    "private static com.fasterxml.jackson.databind.JsonNode parseJson(String raw) {",
                    " try { return raw == null ? null : objectMapper.readValue(raw, com.fasterxml.jackson.databind.JsonNode.class); }",
                    " catch (com.fasterxml.jackson.core.JsonProcessingException e) { throw new RuntimeException(e); } }"
                )
                .to_string(),
                import: None,
            });
            rt.extra_fields.push(ExtraField {
                declaration: concat!(
                    "private static String toJson(com.fasterxml.jackson.databind.JsonNode value) {",
                    " if (value == null) return null;",
                    " try { return objectMapper.writeValueAsString(value); }",
                    " catch (com.fasterxml.jackson.core.JsonProcessingException e) { throw new RuntimeException(e); } }"
                )
                .to_string(),
                import: None,
            });
            Some(rt)
        },
        "gson" => {
            let mut rt = preset_gson("JsonElement.class", "private static final Gson gson = new Gson();");
            rt.read_expr = Some("parseJson({raw})".to_string());
            rt.write_expr = Some("toJson({value})".to_string());
            rt.extra_fields.push(ExtraField {
                declaration: concat!(
                    "private static com.google.gson.JsonElement parseJson(String raw) {",
                    " return raw == null ? null : gson.fromJson(raw, com.google.gson.JsonElement.class); }"
                )
                .to_string(),
                import: None,
            });
            rt.extra_fields.push(ExtraField {
                declaration: concat!(
                    "private static String toJson(com.google.gson.JsonElement value) {",
                    " return value == null ? null : gson.toJson(value); }"
                )
                .to_string(),
                import: None,
            });
            Some(rt)
        },
        _ => None,
    }
}

/// Fully resolved code-generation entry for one SQL type in the Java backend.
///
/// Built once before codegen by [`build_java_type_map`]; all override and default
/// logic runs during map construction and is never repeated at emit time.
pub(super) struct JavaTypeEntry {
    /// Java type name for non-nullable result columns and fields (e.g. `"int"`).
    pub(super) field_type: String,
    /// Boxed or nullable Java type name (e.g. `"Integer"`). Used for nullable fields
    /// and as the element type in `List<T>`.
    pub(super) field_type_boxed: String,
    /// Java type name for non-nullable query parameters. May differ from `field_type`
    /// when a split override is configured (e.g. field=`LocalDate`, param=`String`).
    pub(super) param_type: String,
    /// Boxed Java parameter type. Used for nullable params and as the element type in
    /// `List<T>` parameter positions.
    pub(super) param_type_boxed: String,
    /// Non-nullable ResultSet read expression template with `{idx}` placeholder.
    pub(super) read: String,
    /// Nullable ResultSet read expression template with `{idx}` placeholder.
    pub(super) read_nullable: String,
    /// Write expression template with `{value}` placeholder, or `None` for raw value.
    pub(super) write: Option<String>,
    /// Import path to add when this type is used, if any.
    pub(super) import: Option<String>,
    /// Extra class-level declarations (e.g. `ObjectMapper` for jackson preset).
    pub(super) extra_fields: Vec<ExtraField>,
    /// Per-element lambda body for array conversion, or `None` for types where a direct
    /// cast suffices (e.g. `((java.sql.Timestamp) it).toLocalDateTime()`).
    pub(super) array_elem: Option<String>,
}

/// Pre-resolved map from SQL type to Java code-generation entry.
///
/// Constructed once by [`build_java_type_map`]; consumed by the emitters in `core.rs`
/// without any further override or dispatch logic.
pub(super) struct JavaTypeMap(HashMap<SqlTypeKey, JavaTypeEntry>);

impl JavaTypeMap {
    /// Return the resolved entry for `sql_type`.
    pub(super) fn get(&self, sql_type: &SqlType) -> &JavaTypeEntry {
        &self.0[&sql_type_key(sql_type)]
    }

    /// Return the Java field type string for `sql_type`, with nullability applied.
    ///
    /// For `Array(inner)`, returns `java.util.List<BoxedInner>` (with `@Nullable` prefix when nullable).
    pub(super) fn java_type(&self, sql_type: &SqlType, nullable: bool) -> String {
        if let SqlType::Enum(name) = sql_type {
            return to_pascal_case(name);
        }
        if let SqlType::Array(inner) = sql_type {
            let boxed = self.java_type_boxed(inner);
            let t = format!("java.util.List<{boxed}>");
            return if nullable { format!("@Nullable {t}") } else { t };
        }
        let entry = self.get(sql_type);
        if nullable {
            entry.field_type_boxed.clone()
        } else {
            entry.field_type.clone()
        }
    }

    /// Return the boxed Java type for `sql_type`, used for `List<T>` and JDBC ARRAY casts.
    pub(super) fn java_type_boxed(&self, sql_type: &SqlType) -> String {
        if let SqlType::Enum(name) = sql_type {
            return to_pascal_case(name);
        }
        if let SqlType::Array(inner) = sql_type {
            return format!("java.util.List<{}>", self.java_type_boxed(inner));
        }
        self.get(sql_type).field_type_boxed.clone()
    }

    /// Return the ResultSet read expression for `sql_type` at the given column index.
    ///
    /// For `Array(inner)`, generates a stream-map expression when the inner type needs
    /// per-element conversion, or `Arrays.asList` with a direct cast otherwise.
    pub(super) fn read_expr(&self, sql_type: &SqlType, nullable: bool, idx: usize) -> String {
        if let SqlType::Enum(name) = sql_type {
            let ty = to_pascal_case(name);
            return if nullable {
                format!("rs.getString({idx}) != null ? {ty}.fromValue(rs.getString({idx})) : null")
            } else {
                format!("{ty}.fromValue(rs.getString({idx}))")
            };
        }
        if let SqlType::Array(inner) = sql_type {
            if let SqlType::Enum(name) = inner.as_ref() {
                let ty = to_pascal_case(name);
                let list_expr = format!("java.util.Arrays.stream((Object[]) rs.getArray({idx}).getArray()).map(it -> {ty}.fromValue((String) it)).collect(java.util.stream.Collectors.toList())");
                return if nullable { format!("rs.getArray({idx}) == null ? null : {list_expr}") } else { list_expr };
            }
            let inner_entry = self.get(inner);
            let boxed = self.java_type_boxed(inner);
            return jdbc_array_read_expr(inner_entry, &boxed, nullable, idx);
        }
        let template = if nullable { &self.get(sql_type).read_nullable } else { &self.get(sql_type).read };
        template.replace("{idx}", &idx.to_string())
    }

    /// Return the JDBC bind value expression for a parameter.
    ///
    /// When the type has a `write` expression, substitutes `{value}` with the camelCase param name.
    pub(super) fn write_expr(&self, p: &Parameter) -> String {
        let name = crate::backend::naming::to_camel_case(&p.name);
        if matches!(&p.sql_type, SqlType::Enum(_)) {
            return if p.nullable { format!("{name} != null ? {name}.getValue() : null") } else { format!("{name}.getValue()") };
        }
        if let Some(expr) = &self.get(&p.sql_type).write {
            expr.replace("{value}", &name)
        } else {
            name
        }
    }

    /// Return the Java param type for a parameter, applying list and override logic.
    pub(super) fn java_param_type(&self, p: &Parameter) -> String {
        if let SqlType::Enum(name) = &p.sql_type {
            let ty = to_pascal_case(name);
            return if p.is_list { format!("List<{ty}>") } else { ty };
        }
        if let SqlType::Array(inner) = &p.sql_type {
            return format!("java.util.List<{}>", self.java_type_boxed(inner));
        }
        if p.is_list {
            return format!("List<{}>", self.get(&p.sql_type).param_type_boxed.clone());
        }
        let entry = self.get(&p.sql_type);
        if p.nullable {
            entry.param_type_boxed.clone()
        } else {
            entry.param_type.clone()
        }
    }

    /// Collect import paths needed by a table's columns.
    pub(super) fn table_imports(&self, table: &Table) -> BTreeSet<String> {
        table.columns.iter().filter_map(|col| self.get(&col.sql_type).import.clone()).collect()
    }

    /// Collect override imports and extra fields needed by a query group.
    pub(super) fn query_metadata(&self, queries: &[Query]) -> (BTreeSet<String>, Vec<ExtraField>) {
        let mut imports = BTreeSet::new();
        let mut extra_fields: Vec<ExtraField> = Vec::new();
        for query in queries {
            for col in &query.result_columns {
                absorb_entry_metadata(self.get(&col.sql_type), &mut imports, &mut extra_fields);
            }
            for p in &query.params {
                absorb_entry_metadata(self.get(&p.sql_type), &mut imports, &mut extra_fields);
            }
        }
        (imports, extra_fields)
    }
}

fn absorb_entry_metadata(entry: &JavaTypeEntry, imports: &mut BTreeSet<String>, extra_fields: &mut Vec<ExtraField>) {
    if let Some(imp) = &entry.import {
        imports.insert(imp.clone());
    }
    for ef in &entry.extra_fields {
        if let Some(imp) = &ef.import {
            imports.insert(imp.clone());
        }
        if !extra_fields.iter().any(|e| e.declaration == ef.declaration) {
            extra_fields.push(ef.clone());
        }
    }
}

fn jdbc_array_read_expr(inner_entry: &JavaTypeEntry, boxed_type: &str, nullable: bool, idx: usize) -> String {
    let list_expr = if let Some(elem_expr) = &inner_entry.array_elem {
        format!("java.util.Arrays.stream((Object[]) rs.getArray({idx}).getArray()).map(it -> {elem_expr}).collect(java.util.stream.Collectors.toList())")
    } else {
        format!("java.util.Arrays.asList(({boxed_type}[]) rs.getArray({idx}).getArray())")
    };
    if nullable {
        format!("rs.getArray({idx}) == null ? null : {list_expr}")
    } else {
        list_expr
    }
}

// ─── Build-time defaults ──────────────────────────────────────────────────────

struct JavaTypeInfo {
    field_type: &'static str,
    field_type_boxed: &'static str,
    scalar_read: &'static str,             // with {idx} placeholder
    nullable_helper: Option<&'static str>, // for primitive types (uses wasNull idiom)
    uses_get_object: bool,
    /// Per-element lambda body for JDBC array conversion; `None` for direct-cast types.
    array_elem: Option<&'static str>,
    /// Per-element raw-value expression substituted for `{raw}` in `read_expr` overrides.
    array_raw: &'static str,
}

/// Build the fully-resolved type map for the Java backend.
///
/// Iterates over all canonical SQL types, applies any configured overrides on top of the
/// defaults, and stores the result. The rest of the code generator consumes the map without
/// any further override checks.
pub(super) fn build_java_type_map(config: &OutputConfig) -> JavaTypeMap {
    let types = canonical_sql_types();
    let mut map = HashMap::with_capacity(types.len());
    for sql_type in &types {
        let defaults = java_type_info(sql_type);
        let field_ov = resolve_type_override(sql_type, TypeVariant::Field, config, Language::Java, try_preset_java);
        let param_ov = resolve_type_override(sql_type, TypeVariant::Param, config, Language::Java, try_preset_java);
        map.insert(sql_type_key(sql_type), build_entry(&defaults, field_ov.as_ref(), param_ov.as_ref()));
    }
    JavaTypeMap(map)
}

fn build_entry(defaults: &JavaTypeInfo, field_ov: Option<&ResolvedType>, param_ov: Option<&ResolvedType>) -> JavaTypeEntry {
    let field_type = field_ov.map(|o| o.name.clone()).unwrap_or_else(|| defaults.field_type.to_string());
    let field_type_boxed = field_ov.map(|o| o.name.clone()).unwrap_or_else(|| defaults.field_type_boxed.to_string());
    // For param type, prefer the explicit param override, then fall back to the field override,
    // then the default. This correctly handles Split overrides (field ≠ param).
    let param_type = param_ov.map(|o| o.name.clone()).or_else(|| field_ov.map(|o| o.name.clone())).unwrap_or_else(|| defaults.field_type.to_string());
    let param_type_boxed =
        param_ov.map(|o| o.name.clone()).or_else(|| field_ov.map(|o| o.name.clone())).unwrap_or_else(|| defaults.field_type_boxed.to_string());
    let (read, read_nullable, array_elem) = resolve_read_exprs(defaults, field_ov);
    let write = param_ov.and_then(|o| o.write_expr.clone());
    let import = field_ov.and_then(|o| o.import.clone()).or_else(|| param_ov.and_then(|o| o.import.clone()));
    let extra_fields = field_ov.map(|o| o.extra_fields.clone()).unwrap_or_default();
    JavaTypeEntry { field_type, field_type_boxed, param_type, param_type_boxed, read, read_nullable, write, import, extra_fields, array_elem }
}

/// Compute the `read`, `read_nullable`, and `array_elem` values for a type entry.
///
/// `array_elem` is the per-element lambda body used when the type appears as an array
/// element that requires conversion from the raw JDBC type to the target Java type.
fn resolve_read_exprs(defaults: &JavaTypeInfo, field_ov: Option<&ResolvedType>) -> (String, String, Option<String>) {
    let array_elem = defaults.array_elem.map(|s| s.to_string());
    if let Some(resolved) = field_ov {
        if let Some(read_expr) = &resolved.read_expr {
            // Override supplies a read_expr: substitute {raw} with the string-based getter.
            // For arrays, substitute {raw} with the per-element raw cast expression instead.
            let read = read_expr.replace("{raw}", "rs.getString({idx})");
            let array_elem_override = Some(read_expr.replace("{raw}", defaults.array_raw));
            return (read.clone(), read, array_elem_override);
        }
        if defaults.uses_get_object {
            // Override without read_expr on a getObject type: use the override class name.
            let read = format!("rs.getObject({{idx}}, {}.class)", resolved.name);
            return (read.clone(), read, array_elem);
        }
    }
    let read = defaults.scalar_read.to_string();
    let read_nullable = if let Some(helper) = defaults.nullable_helper { format!("{helper}(rs, {{idx}})") } else { read.clone() };
    (read, read_nullable, array_elem)
}

fn java_type_info(sql_type: &SqlType) -> JavaTypeInfo {
    match sql_type {
        SqlType::Boolean => JavaTypeInfo {
            field_type: "boolean",
            field_type_boxed: "Boolean",
            scalar_read: "rs.getBoolean({idx})",
            nullable_helper: Some("getNullableBoolean"),
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
        SqlType::SmallInt => JavaTypeInfo {
            field_type: "short",
            field_type_boxed: "Short",
            scalar_read: "rs.getShort({idx})",
            nullable_helper: Some("getNullableShort"),
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
        SqlType::Integer => JavaTypeInfo {
            field_type: "int",
            field_type_boxed: "Integer",
            scalar_read: "rs.getInt({idx})",
            nullable_helper: Some("getNullableInt"),
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
        SqlType::BigInt => JavaTypeInfo {
            field_type: "long",
            field_type_boxed: "Long",
            scalar_read: "rs.getLong({idx})",
            nullable_helper: Some("getNullableLong"),
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
        // MySQL UNSIGNED integers are widened to the next signed Java integer that
        // fits the entire unsigned range. BIGINT UNSIGNED has no Java primitive
        // wide enough, so it maps to java.math.BigInteger; users who know their
        // values fit in long can opt into it via a type override (lossy).
        SqlType::TinyIntUnsigned => JavaTypeInfo {
            field_type: "short",
            field_type_boxed: "Short",
            scalar_read: "rs.getShort({idx})",
            nullable_helper: Some("getNullableShort"),
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
        SqlType::SmallIntUnsigned => JavaTypeInfo {
            field_type: "int",
            field_type_boxed: "Integer",
            scalar_read: "rs.getInt({idx})",
            nullable_helper: Some("getNullableInt"),
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
        SqlType::IntegerUnsigned => JavaTypeInfo {
            field_type: "long",
            field_type_boxed: "Long",
            scalar_read: "rs.getLong({idx})",
            nullable_helper: Some("getNullableLong"),
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
        SqlType::BigIntUnsigned => JavaTypeInfo {
            field_type: "java.math.BigInteger",
            field_type_boxed: "java.math.BigInteger",
            scalar_read: "rs.getObject({idx}, java.math.BigInteger.class)",
            nullable_helper: None,
            uses_get_object: true,
            array_elem: None,
            array_raw: "it.toString()",
        },
        SqlType::Real => JavaTypeInfo {
            field_type: "float",
            field_type_boxed: "Float",
            scalar_read: "rs.getFloat({idx})",
            nullable_helper: Some("getNullableFloat"),
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
        SqlType::Double => JavaTypeInfo {
            field_type: "double",
            field_type_boxed: "Double",
            scalar_read: "rs.getDouble({idx})",
            nullable_helper: Some("getNullableDouble"),
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
        SqlType::Decimal => JavaTypeInfo {
            field_type: "java.math.BigDecimal",
            field_type_boxed: "java.math.BigDecimal",
            scalar_read: "rs.getBigDecimal({idx})",
            nullable_helper: None,
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
        SqlType::Text | SqlType::Char(_) | SqlType::VarChar(_) | SqlType::Interval => JavaTypeInfo {
            field_type: "String",
            field_type_boxed: "String",
            scalar_read: "rs.getString({idx})",
            nullable_helper: None,
            uses_get_object: false,
            array_elem: None,
            array_raw: "(String) it",
        },
        SqlType::Bytes => JavaTypeInfo {
            field_type: "byte[]",
            field_type_boxed: "byte[]",
            scalar_read: "rs.getBytes({idx})",
            nullable_helper: None,
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
        SqlType::Date => JavaTypeInfo {
            field_type: "java.time.LocalDate",
            field_type_boxed: "java.time.LocalDate",
            scalar_read: "rs.getObject({idx}, java.time.LocalDate.class)",
            nullable_helper: None,
            uses_get_object: true,
            array_elem: Some("((java.sql.Date) it).toLocalDate()"),
            array_raw: "((java.sql.Date) it).toString()",
        },
        SqlType::Time => JavaTypeInfo {
            field_type: "java.time.LocalTime",
            field_type_boxed: "java.time.LocalTime",
            scalar_read: "rs.getObject({idx}, java.time.LocalTime.class)",
            nullable_helper: None,
            uses_get_object: true,
            array_elem: Some("((java.sql.Time) it).toLocalTime()"),
            array_raw: "((java.sql.Time) it).toString()",
        },
        SqlType::Timestamp => JavaTypeInfo {
            field_type: "java.time.LocalDateTime",
            field_type_boxed: "java.time.LocalDateTime",
            scalar_read: "rs.getObject({idx}, java.time.LocalDateTime.class)",
            nullable_helper: None,
            uses_get_object: true,
            array_elem: Some("((java.sql.Timestamp) it).toLocalDateTime()"),
            array_raw: "((java.sql.Timestamp) it).toString()",
        },
        SqlType::TimestampTz => JavaTypeInfo {
            field_type: "java.time.OffsetDateTime",
            field_type_boxed: "java.time.OffsetDateTime",
            scalar_read: "rs.getObject({idx}, java.time.OffsetDateTime.class)",
            nullable_helper: None,
            uses_get_object: true,
            array_elem: Some("((java.sql.Timestamp) it).toInstant().atOffset(java.time.ZoneOffset.UTC)"),
            array_raw: "((java.sql.Timestamp) it).toString()",
        },
        SqlType::Uuid => JavaTypeInfo {
            field_type: "java.util.UUID",
            field_type_boxed: "java.util.UUID",
            scalar_read: "rs.getObject({idx}, java.util.UUID.class)",
            nullable_helper: None,
            uses_get_object: true,
            array_elem: Some("(java.util.UUID) it"),
            array_raw: "((java.util.UUID) it).toString()",
        },
        SqlType::Json | SqlType::Jsonb => JavaTypeInfo {
            field_type: "String",
            field_type_boxed: "String",
            scalar_read: "rs.getString({idx})",
            nullable_helper: None,
            uses_get_object: false,
            array_elem: None,
            array_raw: "(String) it",
        },
        SqlType::Array(_) | SqlType::Enum(_) | SqlType::Custom(_) => JavaTypeInfo {
            field_type: "Object",
            field_type_boxed: "Object",
            scalar_read: "rs.getObject({idx})",
            nullable_helper: None,
            uses_get_object: false,
            array_elem: None,
            array_raw: "it.toString()",
        },
    }
}

/// Test shim: check whether the given SQL type uses `getObject` by default.
///
/// Matches the logic in `uses_get_object` from `jdbc.rs` for the Java backend.
#[cfg(test)]
pub(super) fn java_type_pub(sql_type: &SqlType, nullable: bool) -> String {
    let map = build_java_type_map(&crate::config::OutputConfig::default());
    map.java_type(sql_type, nullable)
}

#[cfg(test)]
pub(super) fn resultset_read_expr_pub(sql_type: &SqlType, nullable: bool, idx: usize) -> String {
    let map = build_java_type_map(&crate::config::OutputConfig::default());
    map.read_expr(sql_type, nullable, idx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::SqlType;

    fn read(sql_type: SqlType, nullable: bool) -> String {
        resultset_read_expr_pub(&sql_type, nullable, 1)
    }

    #[test]
    fn test_unsigned_integer_widening() {
        // TINYINT UNSIGNED (0..255) does not fit in Java byte, widens to short.
        assert_eq!(java_type_pub(&SqlType::TinyIntUnsigned, false), "short");
        assert_eq!(java_type_pub(&SqlType::TinyIntUnsigned, true), "Short");
        // SMALLINT UNSIGNED (0..65535) does not fit in Java short, widens to int.
        assert_eq!(java_type_pub(&SqlType::SmallIntUnsigned, false), "int");
        assert_eq!(java_type_pub(&SqlType::SmallIntUnsigned, true), "Integer");
        // INT UNSIGNED (0..2^32-1) does not fit in Java int, widens to long.
        assert_eq!(java_type_pub(&SqlType::IntegerUnsigned, false), "long");
        assert_eq!(java_type_pub(&SqlType::IntegerUnsigned, true), "Long");
        // BIGINT UNSIGNED (0..2^64-1) exceeds Java long; default to BigInteger.
        assert_eq!(java_type_pub(&SqlType::BigIntUnsigned, false), "java.math.BigInteger");
        assert_eq!(java_type_pub(&SqlType::BigIntUnsigned, true), "java.math.BigInteger");
    }

    #[test]
    fn test_array_text_uses_direct_cast() {
        let expr = read(SqlType::Array(Box::new(SqlType::Text)), false);
        assert_eq!(expr, "java.util.Arrays.asList((String[]) rs.getArray(1).getArray())");
    }

    #[test]
    fn test_array_timestamp_uses_stream_map() {
        let expr = read(SqlType::Array(Box::new(SqlType::Timestamp)), false);
        assert_eq!(
            expr,
            "java.util.Arrays.stream((Object[]) rs.getArray(1).getArray()).map(it -> ((java.sql.Timestamp) it).toLocalDateTime()).collect(java.util.stream.Collectors.toList())"
        );
    }

    #[test]
    fn test_array_date_uses_stream_map() {
        let expr = read(SqlType::Array(Box::new(SqlType::Date)), false);
        assert_eq!(
            expr,
            "java.util.Arrays.stream((Object[]) rs.getArray(1).getArray()).map(it -> ((java.sql.Date) it).toLocalDate()).collect(java.util.stream.Collectors.toList())"
        );
    }

    #[test]
    fn test_array_uuid_uses_stream_map() {
        let expr = read(SqlType::Array(Box::new(SqlType::Uuid)), false);
        assert_eq!(
            expr,
            "java.util.Arrays.stream((Object[]) rs.getArray(1).getArray()).map(it -> (java.util.UUID) it).collect(java.util.stream.Collectors.toList())"
        );
    }

    #[test]
    fn test_array_timestamp_nullable_has_null_guard() {
        let expr = read(SqlType::Array(Box::new(SqlType::Timestamp)), true);
        assert!(expr.starts_with("rs.getArray(1) == null ? null : "));
        assert!(expr.contains("((java.sql.Timestamp) it).toLocalDateTime()"));
    }

    #[test]
    fn test_array_timestamptz_uses_stream_map() {
        let expr = read(SqlType::Array(Box::new(SqlType::TimestampTz)), false);
        assert_eq!(
            expr,
            "java.util.Arrays.stream((Object[]) rs.getArray(1).getArray()).map(it -> ((java.sql.Timestamp) it).toInstant().atOffset(java.time.ZoneOffset.UTC)).collect(java.util.stream.Collectors.toList())"
        );
    }

    #[test]
    fn test_array_enum_uses_from_value() {
        let expr = read(SqlType::Array(Box::new(SqlType::Enum("status".to_string()))), false);
        assert_eq!(
            expr,
            "java.util.Arrays.stream((Object[]) rs.getArray(1).getArray()).map(it -> Status.fromValue((String) it)).collect(java.util.stream.Collectors.toList())"
        );
    }

    #[test]
    fn test_array_enum_nullable_has_null_guard() {
        let expr = read(SqlType::Array(Box::new(SqlType::Enum("status".to_string()))), true);
        assert!(expr.starts_with("rs.getArray(1) == null ? null : "), "should have null guard: {expr}");
        assert!(expr.contains("Status.fromValue((String) it)"), "should use fromValue: {expr}");
    }

    #[test]
    fn test_array_enum_type_name() {
        let map = build_java_type_map(&crate::config::OutputConfig::default());
        assert_eq!(map.java_type(&SqlType::Array(Box::new(SqlType::Enum("priority".to_string()))), false), "java.util.List<Priority>");
    }
}
