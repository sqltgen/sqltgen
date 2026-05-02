use super::*;

// ─── rust_type mapping ───────────────────────────────────────────────────

#[test]
fn test_rust_type_primitives_non_nullable() {
    assert_eq!(rust_type(&SqlType::Boolean, false), "bool");
    assert_eq!(rust_type(&SqlType::SmallInt, false), "i16");
    assert_eq!(rust_type(&SqlType::Integer, false), "i32");
    assert_eq!(rust_type(&SqlType::BigInt, false), "i64");
    assert_eq!(rust_type(&SqlType::Real, false), "f32");
    assert_eq!(rust_type(&SqlType::Double, false), "f64");
}

#[test]
fn test_rust_type_primitives_nullable() {
    assert_eq!(rust_type(&SqlType::Boolean, true), "Option<bool>");
    assert_eq!(rust_type(&SqlType::BigInt, true), "Option<i64>");
    assert_eq!(rust_type(&SqlType::Double, true), "Option<f64>");
}

#[test]
fn test_rust_type_text_types() {
    assert_eq!(rust_type(&SqlType::Text, false), "String");
    assert_eq!(rust_type(&SqlType::Char(Some(10)), false), "String");
    assert_eq!(rust_type(&SqlType::VarChar(Some(255)), false), "String");
    assert_eq!(rust_type(&SqlType::Text, true), "Option<String>");
}

#[test]
fn test_rust_type_temporal() {
    assert_eq!(rust_type(&SqlType::Date, false), "time::Date");
    assert_eq!(rust_type(&SqlType::Time, false), "time::Time");
    assert_eq!(rust_type(&SqlType::Timestamp, false), "time::PrimitiveDateTime");
    assert_eq!(rust_type(&SqlType::TimestampTz, false), "time::OffsetDateTime");
}

#[test]
fn test_rust_type_uuid_and_json() {
    assert_eq!(rust_type(&SqlType::Uuid, false), "uuid::Uuid");
    assert_eq!(rust_type(&SqlType::Json, false), "serde_json::Value");
    assert_eq!(rust_type(&SqlType::Custom("geometry".to_string()), false), "String");
}

#[test]
fn test_rust_type_decimal() {
    // SqlType::Decimal always maps to rust_decimal::Decimal; SQLite avoids
    // this variant entirely by mapping NUMERIC/DECIMAL to SqlType::Double.
    assert_eq!(rust_type(&SqlType::Decimal, false), "rust_decimal::Decimal");
    assert_eq!(rust_type(&SqlType::Decimal, true), "Option<rust_decimal::Decimal>");
}

#[test]
fn test_rust_type_array_non_nullable() {
    assert_eq!(rust_type(&SqlType::Array(Box::new(SqlType::BigInt)), false), "Vec<i64>");
    assert_eq!(rust_type(&SqlType::Array(Box::new(SqlType::Text)), false), "Vec<String>");
}

#[test]
fn test_rust_type_array_nullable() {
    assert_eq!(rust_type(&SqlType::Array(Box::new(SqlType::BigInt)), true), "Option<Vec<i64>>");
}
