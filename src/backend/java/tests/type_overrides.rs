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
    let schema = Schema::default();
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
    let schema = Schema::default();
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
    let schema = Schema::default();
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
    let schema = Schema::default();
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
    let schema = Schema::default();
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

// ─── read expression behaviour ────────────────────────────────────────────────

#[test]
fn test_read_expr_uses_get_string_as_raw() {
    // When read_expr is present, {raw} must expand to rs.getString — not rs.getObject —
    // so the conversion works on any JDBC driver regardless of type registration.
    let schema = Schema::default();
    let query = Query::one(
        "GetEvent",
        "SELECT event_date FROM events WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn { name: "event_date".to_string(), sql_type: SqlType::Date, nullable: false }],
    );
    let cfg = cfg_with_overrides(vec![(
        "date",
        TypeOverride::Same(TypeRef::Explicit {
            name: "org.joda.time.LocalDate".to_string(),
            import: Some("org.joda.time.LocalDate".to_string()),
            read_expr: Some("LocalDate.parse({raw})".to_string()),
            write_expr: None,
        }),
    )]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.java");

    assert!(src.contains("LocalDate.parse(rs.getString(1))"), "expected getString-based read:\n{src}");
    assert!(!src.contains("getObject"), "must not use getObject when read_expr is present:\n{src}");
}

#[test]
fn test_fqn_override_uses_get_object_with_override_class() {
    // When no read_expr is given for a getObject type, the generated read must use
    // the override class name — not the hardcoded default.
    let schema = Schema::default();
    let query = Query::one(
        "GetEvent",
        "SELECT event_date FROM events WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn { name: "event_date".to_string(), sql_type: SqlType::Date, nullable: false }],
    );
    let cfg = cfg_with_overrides(vec![("date", TypeOverride::Same(TypeRef::String("org.joda.time.LocalDate".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.java");

    // resolved.name is the short name (last segment of the FQN); import brings it into scope
    assert!(src.contains("rs.getObject(1, LocalDate.class)"), "expected override class in getObject:\n{src}");
    assert!(!src.contains("java.time.LocalDate.class"), "must not use old hardcoded class:\n{src}");
}

#[test]
fn test_uuid_fqn_override_uses_get_object_with_override_class() {
    let schema = Schema::default();
    let query = Query::one(
        "GetUser",
        "SELECT user_id FROM users WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn { name: "user_id".to_string(), sql_type: SqlType::Uuid, nullable: false }],
    );
    let cfg = cfg_with_overrides(vec![("uuid", TypeOverride::Same(TypeRef::String("com.example.MyUuid".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.java");

    assert!(src.contains("rs.getObject(1, MyUuid.class)"), "expected override class in getObject:\n{src}");
    assert!(!src.contains("java.util.UUID.class"), "must not use old hardcoded class:\n{src}");
}

#[test]
fn test_jackson_read_expr_uses_get_string() {
    // Jackson preset uses read_expr — verify {raw} expands to rs.getString.
    let schema = Schema::default();
    let query = Query::one(
        "GetDoc",
        "SELECT data FROM docs WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn { name: "data".to_string(), sql_type: SqlType::Json, nullable: false }],
    );
    let cfg = cfg_with_overrides(vec![("json", TypeOverride::Same(TypeRef::String("jackson".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.java");

    assert!(src.contains("objectMapper.readValue(rs.getString(1), JsonNode.class)"), "expected getString in readValue:\n{src}");
}

// ─── timestamp/timestamptz override ──────────────────────────────────────────

#[test]
fn test_timestamp_fqn_override_uses_get_object_with_override_class() {
    let schema = Schema::default();
    let query = Query::one(
        "GetEvent",
        "SELECT created_at FROM events WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn { name: "created_at".to_string(), sql_type: SqlType::Timestamp, nullable: false }],
    );
    let cfg = cfg_with_overrides(vec![("timestamp", TypeOverride::Same(TypeRef::String("org.joda.time.DateTime".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.java");

    assert!(src.contains("rs.getObject(1, DateTime.class)"), "expected override class in getObject:\n{src}");
    assert!(!src.contains("java.time.LocalDateTime.class"), "must not use old hardcoded class:\n{src}");
}

// ─── nullable column with override ───────────────────────────────────────────

#[test]
fn test_nullable_column_with_override() {
    let schema = Schema::default();
    let query = Query::one(
        "GetDoc",
        "SELECT data FROM docs WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn { name: "data".to_string(), sql_type: SqlType::Json, nullable: true }],
    );
    let cfg = cfg_with_overrides(vec![("json", TypeOverride::Same(TypeRef::String("jackson".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.java");

    // Nullable reference type — no boxing suffix, just the type name
    assert!(src.contains("JsonNode data"), "expected JsonNode field for nullable:\n{src}");
}

// ─── table model file ─────────────────────────────────────────────────────────

#[test]
fn test_table_model_with_override() {
    use crate::ir::{Column, Table};
    let schema = Schema {
        tables: vec![Table::new(
            "events".to_string(),
            vec![
                Column { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false, is_primary_key: true },
                Column { name: "payload".to_string(), sql_type: SqlType::Json, nullable: false, is_primary_key: false },
            ],
        )],
        ..Default::default()
    };
    let cfg = cfg_with_overrides(vec![("json", TypeOverride::Same(TypeRef::String("jackson".to_string())))]);
    let files = pg().generate(&schema, &[], &cfg).unwrap();
    let src = get_file(&files, "Events.java");

    assert!(src.contains("JsonNode payload"), "expected JsonNode in table record:\n{src}");
    assert!(src.contains("import com.fasterxml.jackson.databind.JsonNode;"), "missing import in table record:\n{src}");
}

// ─── multiple overrides in the same file ─────────────────────────────────────

#[test]
fn test_multiple_overrides_collect_all_imports() {
    let schema = Schema::default();
    let query = Query::one(
        "GetDoc",
        "SELECT payload, doc_id FROM docs WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![
            ResultColumn { name: "payload".to_string(), sql_type: SqlType::Json, nullable: false },
            ResultColumn { name: "doc_id".to_string(), sql_type: SqlType::Uuid, nullable: false },
        ],
    );
    let cfg = cfg_with_overrides(vec![
        ("json", TypeOverride::Same(TypeRef::String("jackson".to_string()))),
        ("uuid", TypeOverride::Same(TypeRef::String("java.util.UUID".to_string()))),
    ]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.java");

    assert!(src.contains("import com.fasterxml.jackson.databind.JsonNode;"), "missing JsonNode import:\n{src}");
    assert!(src.contains("import com.fasterxml.jackson.databind.ObjectMapper;"), "missing ObjectMapper import:\n{src}");
    assert!(src.contains("import java.util.UUID;"), "missing UUID import:\n{src}");
    assert!(src.contains("JsonNode payload"), "expected JsonNode field:\n{src}");
    assert!(src.contains("UUID docId"), "expected UUID field:\n{src}");
}

// ─── write_expr for param binding ────────────────────────────────────────────

#[test]
fn test_write_expr_applied_to_param_binding() {
    // When write_expr is configured, the param binding should apply the expression.
    // e.g. jackson: ps.setObject(1, objectMapper.writeValueAsString(payload), Types.OTHER)
    // rather than ps.setObject(1, payload, Types.OTHER)
    let schema = Schema::default();
    let query = Query::exec("InsertDoc", "INSERT INTO docs (payload) VALUES ($1)", vec![Parameter::scalar(1, "payload".to_string(), SqlType::Json, false)]);
    let cfg = cfg_with_overrides(vec![("json", TypeOverride::Same(TypeRef::String("jackson".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.java");

    assert!(src.contains("objectMapper.writeValueAsString(payload)"), "expected write_expr applied to param binding:\n{src}");
}

// ─── no override — existing output unchanged ──────────────────────────────────

#[test]
fn test_no_override_json_stays_string() {
    let schema = Schema::default();
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
