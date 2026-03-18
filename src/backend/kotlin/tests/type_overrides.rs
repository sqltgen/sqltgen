use super::*;
use crate::config::{TypeOverride, TypeRef};

fn cfg_with_overrides(overrides: Vec<(&str, TypeOverride)>) -> OutputConfig {
    let mut cfg = cfg_pkg();
    for (key, ov) in overrides {
        cfg.type_overrides.insert(key.to_string(), ov);
    }
    cfg
}

// ─── read expression behaviour ────────────────────────────────────────────────

#[test]
fn test_read_expr_uses_get_string_as_raw() {
    // When read_expr is present, {raw} must expand to rs.getString — not rs.getObject.
    let schema = Schema::default();
    let query = Query::one(
        "GetEvent",
        "SELECT event_date FROM events WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("event_date", SqlType::Date)],
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
    let src = get_file(&files, "Queries.kt");

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
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("event_date", SqlType::Date)],
    );
    let cfg = cfg_with_overrides(vec![("date", TypeOverride::Same(TypeRef::String("org.joda.time.LocalDate".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.kt");

    assert!(src.contains("rs.getObject(1, LocalDate::class.java)"), "expected override class in getObject:\n{src}");
    assert!(!src.contains("java.time.LocalDate::class.java"), "must not use old hardcoded class:\n{src}");
}

#[test]
fn test_uuid_fqn_override_uses_get_object_with_override_class() {
    let schema = Schema::default();
    let query = Query::one(
        "GetUser",
        "SELECT user_id FROM users WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("user_id", SqlType::Uuid)],
    );
    let cfg = cfg_with_overrides(vec![("uuid", TypeOverride::Same(TypeRef::String("com.example.MyUuid".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.kt");

    assert!(src.contains("rs.getObject(1, MyUuid::class.java)"), "expected override class in getObject:\n{src}");
    assert!(!src.contains("java.util.UUID::class.java"), "must not use old hardcoded class:\n{src}");
}

// ─── timestamp override ───────────────────────────────────────────────────────

#[test]
fn test_timestamp_fqn_override_uses_get_object_with_override_class() {
    let schema = Schema::default();
    let query = Query::one(
        "GetEvent",
        "SELECT created_at FROM events WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("created_at", SqlType::Timestamp)],
    );
    let cfg = cfg_with_overrides(vec![("timestamp", TypeOverride::Same(TypeRef::String("org.joda.time.DateTime".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.kt");

    assert!(src.contains("rs.getObject(1, DateTime::class.java)"), "expected override class in getObject:\n{src}");
    assert!(!src.contains("java.time.LocalDateTime::class.java"), "must not use old hardcoded class:\n{src}");
}

// ─── nullable column with override ───────────────────────────────────────────

#[test]
fn test_nullable_column_with_override() {
    let schema = Schema::default();
    let query = Query::one(
        "GetDoc",
        "SELECT data FROM docs WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::nullable("data", SqlType::Json)],
    );
    let cfg = cfg_with_overrides(vec![("json", TypeOverride::Same(TypeRef::String("jackson".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.kt");

    assert!(src.contains("JsonNode?"), "expected nullable JsonNode for nullable column:\n{src}");
}

// ─── table model file ─────────────────────────────────────────────────────────

#[test]
fn test_table_model_with_override() {
    use crate::ir::{Column, Table};
    let schema = Schema {
        tables: vec![Table::new("events", vec![Column::new_primary_key("id", SqlType::BigInt), Column::new_not_nullable("payload", SqlType::Json)])],
        ..Default::default()
    };
    let cfg = cfg_with_overrides(vec![("json", TypeOverride::Same(TypeRef::String("jackson".to_string())))]);
    let files = pg().generate(&schema, &[], &cfg).unwrap();
    let src = get_file(&files, "Events.kt");

    assert!(src.contains("val payload: JsonNode"), "expected JsonNode in data class:\n{src}");
    assert!(src.contains("import com.fasterxml.jackson.databind.JsonNode"), "missing import in table model:\n{src}");
}

// ─── multiple overrides in the same file ─────────────────────────────────────

#[test]
fn test_multiple_overrides_collect_all_imports() {
    let schema = Schema::default();
    let query = Query::one(
        "GetDoc",
        "SELECT payload, doc_id FROM docs WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("payload", SqlType::Json), ResultColumn::not_nullable("doc_id", SqlType::Uuid)],
    );
    let cfg = cfg_with_overrides(vec![
        ("json", TypeOverride::Same(TypeRef::String("jackson".to_string()))),
        ("uuid", TypeOverride::Same(TypeRef::String("java.util.UUID".to_string()))),
    ]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.kt");

    assert!(src.contains("import com.fasterxml.jackson.databind.JsonNode"), "missing JsonNode import:\n{src}");
    assert!(src.contains("import com.fasterxml.jackson.databind.ObjectMapper"), "missing ObjectMapper import:\n{src}");
    assert!(src.contains("import java.util.UUID"), "missing UUID import:\n{src}");
    assert!(src.contains("val payload: JsonNode"), "expected JsonNode field:\n{src}");
    assert!(src.contains("val docId: UUID"), "expected UUID field:\n{src}");
}

// ─── jackson preset ───────────────────────────────────────────────────────────

#[test]
fn test_jackson_preset_json_column() {
    let schema = Schema::default();
    let query = Query::one(
        "GetDoc",
        "SELECT data FROM docs WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("data", SqlType::Json)],
    );
    let cfg = cfg_with_overrides(vec![("json", TypeOverride::Same(TypeRef::String("jackson".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.kt");

    assert!(src.contains("val data: JsonNode"), "expected JsonNode field:\n{src}");
    assert!(src.contains("import com.fasterxml.jackson.databind.JsonNode"), "missing JsonNode import:\n{src}");
    assert!(src.contains("import com.fasterxml.jackson.databind.ObjectMapper"), "missing ObjectMapper import:\n{src}");
    assert!(src.contains("objectMapper.readValue(rs.getString(1), JsonNode::class.java)"), "expected readValue call:\n{src}");
}

// ─── write_expr for param binding ────────────────────────────────────────────

#[test]
fn test_write_expr_applied_to_param_binding() {
    let schema = Schema::default();
    let query = Query::exec("InsertDoc", "INSERT INTO docs (payload) VALUES ($1)", vec![Parameter::scalar(1, "payload", SqlType::Json, false)]);
    let cfg = cfg_with_overrides(vec![("json", TypeOverride::Same(TypeRef::String("jackson".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.kt");

    assert!(src.contains("objectMapper.writeValueAsString(payload)"), "expected write_expr applied to param binding:\n{src}");
}

// ─── type name + import ───────────────────────────────────────────────────────

#[test]
fn test_fqn_date_override_type_and_import() {
    let schema = Schema::default();
    let query = Query::one(
        "GetEvent",
        "SELECT event_date FROM events WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("event_date", SqlType::Date)],
    );
    let cfg = cfg_with_overrides(vec![("date", TypeOverride::Same(TypeRef::String("java.time.LocalDate".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "Queries.kt");

    assert!(src.contains("val eventDate: LocalDate"), "expected LocalDate field:\n{src}");
    assert!(src.contains("import java.time.LocalDate"), "missing LocalDate import:\n{src}");
}

// ─── no override — existing output unchanged ──────────────────────────────────

#[test]
fn test_no_override_date_stays_local_date() {
    let schema = Schema::default();
    let query = Query::one(
        "GetEvent",
        "SELECT event_date FROM events WHERE id = $1",
        vec![Parameter::scalar(1, "id", SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("event_date", SqlType::Date)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "Queries.kt");

    assert!(src.contains("java.time.LocalDate"), "expected java.time.LocalDate without override:\n{src}");
}
