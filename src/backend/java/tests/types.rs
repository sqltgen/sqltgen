use super::*;

// ─── java_type ─────────────────────────────────────────────────────────

#[test]
fn test_java_type_boolean_non_nullable() {
    assert_eq!(java_type(&SqlType::Boolean, false), "boolean");
}

#[test]
fn test_java_type_boolean_nullable() {
    assert_eq!(java_type(&SqlType::Boolean, true), "Boolean");
}

#[test]
fn test_java_type_integer_non_nullable() {
    assert_eq!(java_type(&SqlType::Integer, false), "int");
}

#[test]
fn test_java_type_integer_nullable() {
    assert_eq!(java_type(&SqlType::Integer, true), "Integer");
}

#[test]
fn test_java_type_bigint_non_nullable() {
    assert_eq!(java_type(&SqlType::BigInt, false), "long");
}

#[test]
fn test_java_type_bigint_nullable() {
    assert_eq!(java_type(&SqlType::BigInt, true), "Long");
}

#[test]
fn test_java_type_text_ignores_nullability() {
    // String is a reference type — same in both cases
    assert_eq!(java_type(&SqlType::Text, false), "String");
    assert_eq!(java_type(&SqlType::Text, true), "String");
}

#[test]
fn test_java_type_decimal() {
    assert_eq!(java_type(&SqlType::Decimal, false), "java.math.BigDecimal");
}

#[test]
fn test_java_type_temporal() {
    assert_eq!(java_type(&SqlType::Date, false), "java.time.LocalDate");
    assert_eq!(java_type(&SqlType::Time, false), "java.time.LocalTime");
    assert_eq!(java_type(&SqlType::Timestamp, false), "java.time.LocalDateTime");
    assert_eq!(java_type(&SqlType::TimestampTz, false), "java.time.OffsetDateTime");
}

#[test]
fn test_java_type_uuid() {
    assert_eq!(java_type(&SqlType::Uuid, false), "java.util.UUID");
}

#[test]
fn test_java_type_json() {
    assert_eq!(java_type(&SqlType::Json, false), "String");
    assert_eq!(java_type(&SqlType::Jsonb, false), "String");
}

#[test]
fn test_java_type_array_non_nullable() {
    assert_eq!(java_type(&SqlType::Array(Box::new(SqlType::Text)), false), "java.util.List<String>");
}

#[test]
fn test_java_type_array_nullable() {
    assert_eq!(java_type(&SqlType::Array(Box::new(SqlType::Text)), true), "@Nullable java.util.List<String>");
}

#[test]
fn test_java_type_array_of_integers_uses_boxed_type() {
    // Array elements must be boxed — List<int> is invalid Java
    assert_eq!(java_type(&SqlType::Array(Box::new(SqlType::Integer)), false), "java.util.List<Integer>");
}

#[test]
fn test_resultset_read_array_text() {
    let expr = resultset_read_expr(&SqlType::Array(Box::new(SqlType::Text)), false, 3);
    assert_eq!(expr, "java.util.Arrays.asList((String[]) rs.getArray(3).getArray())");
}

#[test]
fn test_resultset_read_array_integer_nullable() {
    let expr = resultset_read_expr(&SqlType::Array(Box::new(SqlType::Integer)), true, 5);
    assert!(expr.contains("rs.getArray(5) == null ? null :"));
    assert!(expr.contains("(Integer[]) rs.getArray(5).getArray()"));
}

#[test]
fn test_java_type_custom() {
    assert_eq!(java_type(&SqlType::Custom("citext".to_string()), false), "Object");
}
