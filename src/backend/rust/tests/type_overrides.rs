use super::*;
use crate::config::{TypeOverride, TypeRef};

fn cfg_with_overrides(overrides: Vec<(&str, TypeOverride)>) -> OutputConfig {
    let mut cfg = cfg();
    for (key, ov) in overrides {
        cfg.type_overrides.insert(key.to_string(), ov);
    }
    cfg
}

// ─── serde_json preset ────────────────────────────────────────────────────────

#[test]
fn test_serde_json_preset_json_column() {
    let schema = Schema::default();
    let query = Query::one(
        "GetDoc",
        "SELECT data FROM docs WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("data", SqlType::Json)],
    );
    let cfg = cfg_with_overrides(vec![("json", TypeOverride::Same(TypeRef::String("serde_json".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.rs");

    // Field type must be serde_json::Value
    assert!(src.contains("serde_json::Value"), "expected serde_json::Value:\n{src}");
    // No sqlx::types::Json wrapper
    assert!(!src.contains("sqlx::types::Json"), "must not wrap in sqlx::types::Json:\n{src}");
}

// ─── FQN string override ──────────────────────────────────────────────────────

#[test]
fn test_fqn_date_override() {
    let schema = Schema::default();
    let query = Query::one(
        "GetEvent",
        "SELECT event_date FROM events WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("event_date", SqlType::Date)],
    );
    let cfg = cfg_with_overrides(vec![("date", TypeOverride::Same(TypeRef::String("time::Date".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.rs");

    assert!(src.contains("time::Date"), "expected time::Date:\n{src}");
}

// ─── plain string override ────────────────────────────────────────────────────

#[test]
fn test_plain_string_uuid_override() {
    let schema = Schema::default();
    let query = Query::one(
        "GetUser",
        "SELECT user_id FROM users WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("user_id", SqlType::Uuid)],
    );
    let cfg = cfg_with_overrides(vec![("uuid", TypeOverride::Same(TypeRef::String("String".to_string())))]);
    let files = pg().generate(&schema, &[query], &cfg).unwrap();
    let src = get_file(&files, "queries.rs");

    assert!(src.contains("user_id: String"), "expected String field:\n{src}");
}

// ─── no override — existing output unchanged ──────────────────────────────────

#[test]
fn test_no_override_json_stays_serde_value() {
    let schema = Schema::default();
    let query = Query::one(
        "GetDoc",
        "SELECT data FROM docs WHERE id = $1",
        vec![Parameter::scalar(1, "id".to_string(), SqlType::BigInt, false)],
        vec![ResultColumn::not_nullable("data", SqlType::Json)],
    );
    let files = pg().generate(&schema, &[query], &cfg()).unwrap();
    let src = get_file(&files, "queries.rs");

    // Default: JSON stays as serde_json::Value (built-in sqlx mapping)
    assert!(src.contains("serde_json::Value"), "expected serde_json::Value without override:\n{src}");
}
