use super::*;

// ─── rust_type mapping ───────────────────────────────────────────────────

#[test]
fn test_rust_type_primitives_non_nullable() {
    let t = &RustTarget::Postgres;
    assert_eq!(rust_type(&SqlType::Boolean, false, t), "bool");
    assert_eq!(rust_type(&SqlType::SmallInt, false, t), "i16");
    assert_eq!(rust_type(&SqlType::Integer, false, t), "i32");
    assert_eq!(rust_type(&SqlType::BigInt, false, t), "i64");
    assert_eq!(rust_type(&SqlType::Real, false, t), "f32");
    assert_eq!(rust_type(&SqlType::Double, false, t), "f64");
}

#[test]
fn test_rust_type_primitives_nullable() {
    let t = &RustTarget::Postgres;
    assert_eq!(rust_type(&SqlType::Boolean, true, t), "Option<bool>");
    assert_eq!(rust_type(&SqlType::BigInt, true, t), "Option<i64>");
    assert_eq!(rust_type(&SqlType::Double, true, t), "Option<f64>");
}

#[test]
fn test_rust_type_text_types() {
    let t = &RustTarget::Postgres;
    assert_eq!(rust_type(&SqlType::Text, false, t), "String");
    assert_eq!(rust_type(&SqlType::Char(Some(10)), false, t), "String");
    assert_eq!(rust_type(&SqlType::VarChar(Some(255)), false, t), "String");
    assert_eq!(rust_type(&SqlType::Text, true, t), "Option<String>");
}

#[test]
fn test_rust_type_temporal() {
    let t = &RustTarget::Postgres;
    assert_eq!(rust_type(&SqlType::Date, false, t), "time::Date");
    assert_eq!(rust_type(&SqlType::Time, false, t), "time::Time");
    assert_eq!(rust_type(&SqlType::Timestamp, false, t), "time::PrimitiveDateTime");
    assert_eq!(rust_type(&SqlType::TimestampTz, false, t), "time::OffsetDateTime");
}

#[test]
fn test_rust_type_uuid_and_json() {
    let t = &RustTarget::Postgres;
    assert_eq!(rust_type(&SqlType::Uuid, false, t), "uuid::Uuid");
    assert_eq!(rust_type(&SqlType::Json, false, t), "serde_json::Value");
    assert_eq!(rust_type(&SqlType::Custom("geometry".to_string()), false, t), "serde_json::Value");
}

#[test]
fn test_rust_type_decimal_sqlite_vs_pg() {
    // SQLite stores DECIMAL as REAL; PG/MySQL use rust_decimal::Decimal
    assert_eq!(rust_type(&SqlType::Decimal, false, &RustTarget::Sqlite), "f64");
    assert_eq!(rust_type(&SqlType::Decimal, false, &RustTarget::Postgres), "rust_decimal::Decimal");
    assert_eq!(rust_type(&SqlType::Decimal, false, &RustTarget::Mysql), "rust_decimal::Decimal");
}

#[test]
fn test_rust_type_array_non_nullable() {
    let t = &RustTarget::Postgres;
    assert_eq!(rust_type(&SqlType::Array(Box::new(SqlType::BigInt)), false, t), "Vec<i64>");
    assert_eq!(rust_type(&SqlType::Array(Box::new(SqlType::Text)), false, t), "Vec<String>");
}

#[test]
fn test_rust_type_array_nullable() {
    let t = &RustTarget::Postgres;
    assert_eq!(rust_type(&SqlType::Array(Box::new(SqlType::BigInt)), true, t), "Option<Vec<i64>>");
}
