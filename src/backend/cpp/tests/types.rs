use super::*;

// ─── cpp_type: primitives ────────────────────────────────────────────────

#[test]
fn test_cpp_type_primitives_non_nullable() {
    let t = CppTarget::Postgres;
    assert_eq!(cpp_type(&SqlType::Boolean, false, &t), "bool");
    assert_eq!(cpp_type(&SqlType::SmallInt, false, &t), "std::int16_t");
    assert_eq!(cpp_type(&SqlType::Integer, false, &t), "std::int32_t");
    assert_eq!(cpp_type(&SqlType::BigInt, false, &t), "std::int64_t");
    assert_eq!(cpp_type(&SqlType::Real, false, &t), "float");
    assert_eq!(cpp_type(&SqlType::Double, false, &t), "double");
}

#[test]
fn test_cpp_type_primitives_nullable() {
    let t = CppTarget::Postgres;
    assert_eq!(cpp_type(&SqlType::Boolean, true, &t), "std::optional<bool>");
    assert_eq!(cpp_type(&SqlType::BigInt, true, &t), "std::optional<std::int64_t>");
    assert_eq!(cpp_type(&SqlType::Double, true, &t), "std::optional<double>");
}

// ─── cpp_type: text types ────────────────────────────────────────────────

#[test]
fn test_cpp_type_text_types() {
    let t = CppTarget::Postgres;
    assert_eq!(cpp_type(&SqlType::Text, false, &t), "std::string");
    assert_eq!(cpp_type(&SqlType::Char(Some(10)), false, &t), "std::string");
    assert_eq!(cpp_type(&SqlType::VarChar(Some(255)), false, &t), "std::string");
    assert_eq!(cpp_type(&SqlType::Text, true, &t), "std::optional<std::string>");
}

// ─── cpp_type: bytes ─────────────────────────────────────────────────────

#[test]
fn test_cpp_type_bytes() {
    let t = CppTarget::Postgres;
    assert_eq!(cpp_type(&SqlType::Bytes, false, &t), "std::vector<std::uint8_t>");
    assert_eq!(cpp_type(&SqlType::Bytes, true, &t), "std::optional<std::vector<std::uint8_t>>");
}

// ─── cpp_type: temporal ──────────────────────────────────────────────────

#[test]
fn test_cpp_type_temporal() {
    let t = CppTarget::Postgres;
    assert_eq!(cpp_type(&SqlType::Date, false, &t), "std::chrono::year_month_day");
    assert_eq!(cpp_type(&SqlType::Time, false, &t), "std::chrono::seconds");
    assert_eq!(cpp_type(&SqlType::Timestamp, false, &t), "std::chrono::system_clock::time_point");
    assert_eq!(cpp_type(&SqlType::TimestampTz, false, &t), "std::chrono::system_clock::time_point");
    assert_eq!(cpp_type(&SqlType::Interval, false, &t), "std::chrono::microseconds");
}

#[test]
fn test_cpp_type_temporal_nullable() {
    let t = CppTarget::Postgres;
    assert_eq!(cpp_type(&SqlType::Date, true, &t), "std::optional<std::chrono::year_month_day>");
    assert_eq!(cpp_type(&SqlType::Timestamp, true, &t), "std::optional<std::chrono::system_clock::time_point>");
}

// ─── cpp_type: uuid, json, custom ────────────────────────────────────────

#[test]
fn test_cpp_type_uuid_json_custom() {
    let t = CppTarget::Postgres;
    assert_eq!(cpp_type(&SqlType::Uuid, false, &t), "std::string");
    assert_eq!(cpp_type(&SqlType::Json, false, &t), "std::string");
    assert_eq!(cpp_type(&SqlType::Jsonb, false, &t), "std::string");
    assert_eq!(cpp_type(&SqlType::Custom("geometry".to_string()), false, &t), "std::string");
}

// ─── cpp_type: decimal ───────────────────────────────────────────────────

#[test]
fn test_cpp_type_decimal() {
    let t = CppTarget::Postgres;
    assert_eq!(cpp_type(&SqlType::Decimal, false, &t), "std::string");
    assert_eq!(cpp_type(&SqlType::Decimal, true, &t), "std::optional<std::string>");
}

// ─── cpp_type: arrays ────────────────────────────────────────────────────

#[test]
fn test_cpp_type_array_non_nullable() {
    let t = CppTarget::Postgres;
    assert_eq!(cpp_type(&SqlType::Array(Box::new(SqlType::BigInt)), false, &t), "std::vector<std::int64_t>");
    assert_eq!(cpp_type(&SqlType::Array(Box::new(SqlType::Text)), false, &t), "std::vector<std::string>");
}

#[test]
fn test_cpp_type_array_nullable() {
    let t = CppTarget::Postgres;
    assert_eq!(cpp_type(&SqlType::Array(Box::new(SqlType::BigInt)), true, &t), "std::optional<std::vector<std::int64_t>>");
}
