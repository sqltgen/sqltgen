use super::*;

// ─── js_type ─────────────────────────────────────────────────────────────

#[test]
fn test_js_type_primitives() {
    let pg = JsTarget::Postgres;
    assert_eq!(js_type(&SqlType::Boolean, false, &pg), "boolean");
    assert_eq!(js_type(&SqlType::Integer, false, &pg), "number");
    assert_eq!(js_type(&SqlType::BigInt, false, &pg), "number");
    assert_eq!(js_type(&SqlType::Text, false, &pg), "string");
    assert_eq!(js_type(&SqlType::Uuid, false, &pg), "string");
    assert_eq!(js_type(&SqlType::Bytes, false, &pg), "Buffer");
    assert_eq!(js_type(&SqlType::Date, false, &pg), "Date");
    assert_eq!(js_type(&SqlType::Timestamp, false, &pg), "Date");
    assert_eq!(js_type(&SqlType::Json, false, &pg), "unknown");
    // MySQL DATE maps to string (mysql2 returns/expects date strings to avoid timezone issues)
    assert_eq!(js_type(&SqlType::Date, false, &JsTarget::Mysql), "string");
    assert_eq!(js_type(&SqlType::Date, false, &JsTarget::Sqlite), "Date");
}

#[test]
fn test_js_type_nullable() {
    let pg = JsTarget::Postgres;
    assert_eq!(js_type(&SqlType::Text, true, &pg), "string | null");
    assert_eq!(js_type(&SqlType::BigInt, true, &pg), "number | null");
}

#[test]
fn test_js_type_array() {
    let pg = JsTarget::Postgres;
    assert_eq!(js_type(&SqlType::Array(Box::new(SqlType::Integer)), false, &pg), "number[]");
    assert_eq!(js_type(&SqlType::Array(Box::new(SqlType::Text)), true, &pg), "string[] | null");
}
