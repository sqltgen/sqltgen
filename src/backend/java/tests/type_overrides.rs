use super::*;
use crate::config::{TypeOverride, TypeRef};

fn cfg_with_overrides(overrides: Vec<(&str, TypeOverride)>) -> OutputConfig {
    let mut cfg = cfg_pkg();
    for (key, ov) in overrides {
        cfg.type_overrides.insert(key.to_string(), ov);
    }
    cfg
}

// ─── jackson preset ───────────────────────────────────────────────────────────

#[test]
fn test_jackson_preset_json_column_type() {
    let schema = Schema { tables: vec![] };
    let query = Query::one(
        "GetDoc",
        "SELECT data FROM docs WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn { name: "data".to_string(), sql_type: SqlType::Json, nullable: false }],
    );
    let cfg = cfg_with_overrides(vec![("json", TypeOverride::Same(TypeRef::String("jackson".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.java");

    // Type must be JsonNode, not String
    assert!(src.contains("JsonNode data"), "expected JsonNode field, got:\n{src}");
    // Import for JsonNode
    assert!(src.contains("import com.fasterxml.jackson.databind.JsonNode;"), "missing JsonNode import:\n{src}");
    // ObjectMapper import
    assert!(src.contains("import com.fasterxml.jackson.databind.ObjectMapper;"), "missing ObjectMapper import:\n{src}");
    // Static ObjectMapper field
    assert!(src.contains("private static final ObjectMapper objectMapper = new ObjectMapper();"), "missing objectMapper field:\n{src}");
    // read_expr applied in row constructor
    assert!(src.contains("objectMapper.readValue("), "missing readValue call:\n{src}");
}

#[test]
fn test_jackson_preset_jsonb_column() {
    let schema = Schema { tables: vec![] };
    let query = Query::one(
        "GetMeta",
        "SELECT meta FROM docs WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn { name: "meta".to_string(), sql_type: SqlType::Jsonb, nullable: false }],
    );
    let cfg = cfg_with_overrides(vec![("jsonb", TypeOverride::Same(TypeRef::String("jackson".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.java");
    assert!(src.contains("JsonNode meta"), "expected JsonNode:\n{src}");
}

// ─── FQN string override ──────────────────────────────────────────────────────

#[test]
fn test_fqn_date_override() {
    let schema = Schema { tables: vec![] };
    let query = Query::one(
        "GetEvent",
        "SELECT event_date FROM events WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn { name: "event_date".to_string(), sql_type: SqlType::Date, nullable: false }],
    );
    let cfg = cfg_with_overrides(vec![("date", TypeOverride::Same(TypeRef::String("java.time.LocalDate".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.java");

    assert!(src.contains("LocalDate eventDate"), "expected LocalDate field:\n{src}");
    assert!(src.contains("import java.time.LocalDate;"), "missing LocalDate import:\n{src}");
}

// ─── plain string override ────────────────────────────────────────────────────

#[test]
fn test_plain_string_uuid_override() {
    let schema = Schema { tables: vec![] };
    let query = Query::one(
        "GetUser",
        "SELECT user_id FROM users WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn { name: "user_id".to_string(), sql_type: SqlType::Uuid, nullable: false }],
    );
    let cfg = cfg_with_overrides(vec![("uuid", TypeOverride::Same(TypeRef::String("String".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.java");

    assert!(src.contains("String userId"), "expected String field:\n{src}");
    // No extra import for plain "String"
    assert!(!src.contains("import String"), "must not emit 'import String':\n{src}");
}

// ─── split field/param override ───────────────────────────────────────────────

#[test]
fn test_split_date_field_string_param() {
    let schema = Schema { tables: vec![] };
    let query = Query::one(
        "GetEvent",
        "SELECT event_date FROM events WHERE raw_date = $1",
        vec![Parameter::scalar(1, "raw_date".to_string(), SqlType::Date, false)],
        vec![ResultColumn { name: "event_date".to_string(), sql_type: SqlType::Date, nullable: false }],
    );
    let cfg = cfg_with_overrides(vec![(
        "date",
        TypeOverride::Split { field: TypeRef::String("java.time.LocalDate".to_string()), param: Some(TypeRef::String("String".to_string())) },
    )]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.java");

    // Result column uses LocalDate
    assert!(src.contains("LocalDate eventDate"), "expected LocalDate result field:\n{src}");
    // Param uses String
    assert!(src.contains("String rawDate"), "expected String param:\n{src}");
}

// ─── no override — existing output unchanged ──────────────────────────────────

#[test]
fn test_no_override_json_stays_string() {
    let schema = Schema { tables: vec![] };
    let query = Query::one(
        "GetDoc",
        "SELECT data FROM docs WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn { name: "data".to_string(), sql_type: SqlType::Json, nullable: false }],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.java");

    // Default: JSON maps to String, no objectMapper
    assert!(src.contains("String data"), "expected String field without override:\n{src}");
    assert!(!src.contains("objectMapper"), "must not emit objectMapper without override:\n{src}");
}
