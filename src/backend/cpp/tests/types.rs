use super::*;
use crate::backend::cpp::core::cpp_type;

#[test]
fn test_cpp_type_primitives_non_nullable() {
    assert_eq!(cpp_type(&SqlType::Boolean, false), "bool");
    assert_eq!(cpp_type(&SqlType::SmallInt, false), "std::int16_t");
    assert_eq!(cpp_type(&SqlType::Integer, false), "std::int32_t");
    assert_eq!(cpp_type(&SqlType::BigInt, false), "std::int64_t");
    assert_eq!(cpp_type(&SqlType::Real, false), "float");
    assert_eq!(cpp_type(&SqlType::Double, false), "double");
}

#[test]
fn test_cpp_type_primitives_nullable() {
    assert_eq!(cpp_type(&SqlType::Boolean, true), "std::optional<bool>");
    assert_eq!(cpp_type(&SqlType::BigInt, true), "std::optional<std::int64_t>");
    assert_eq!(cpp_type(&SqlType::Double, true), "std::optional<double>");
}

#[test]
fn test_cpp_type_text_and_decimal() {
    assert_eq!(cpp_type(&SqlType::Text, false), "std::string");
    assert_eq!(cpp_type(&SqlType::VarChar(Some(255)), false), "std::string");
    assert_eq!(cpp_type(&SqlType::Decimal, false), "std::string");
    assert_eq!(cpp_type(&SqlType::Text, true), "std::optional<std::string>");
}

#[test]
fn test_cpp_type_temporal_uuid_json_custom() {
    assert_eq!(cpp_type(&SqlType::Date, false), "std::string");
    assert_eq!(cpp_type(&SqlType::Timestamp, false), "std::string");
    assert_eq!(cpp_type(&SqlType::Uuid, false), "std::string");
    assert_eq!(cpp_type(&SqlType::Json, false), "std::string");
    assert_eq!(cpp_type(&SqlType::Custom("geometry".to_string()), false), "std::string");
}

#[test]
fn test_cpp_type_bytes_and_arrays() {
    assert_eq!(cpp_type(&SqlType::Bytes, false), "std::vector<std::uint8_t>");
    assert_eq!(cpp_type(&SqlType::Array(Box::new(SqlType::BigInt)), false), "std::vector<std::int64_t>");
    assert_eq!(cpp_type(&SqlType::Array(Box::new(SqlType::Text)), true), "std::optional<std::vector<std::string>>");
}
