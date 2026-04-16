use super::*;

// ─── kotlin_type ───────────────────────────────────────────────────────

#[test]
fn test_kotlin_type_boolean_non_nullable() {
    // Kotlin has no primitive/boxed split — Boolean is always Boolean
    assert_eq!(kotlin_type(&SqlType::Boolean, false), "Boolean");
}

#[test]
fn test_kotlin_type_boolean_nullable() {
    assert_eq!(kotlin_type(&SqlType::Boolean, true), "Boolean?");
}

#[test]
fn test_kotlin_type_integer_non_nullable() {
    assert_eq!(kotlin_type(&SqlType::Integer, false), "Int");
}

#[test]
fn test_kotlin_type_integer_nullable() {
    assert_eq!(kotlin_type(&SqlType::Integer, true), "Int?");
}

#[test]
fn test_kotlin_type_bigint_non_nullable() {
    assert_eq!(kotlin_type(&SqlType::BigInt, false), "Long");
}

#[test]
fn test_kotlin_type_bigint_nullable() {
    assert_eq!(kotlin_type(&SqlType::BigInt, true), "Long?");
}

#[test]
fn test_kotlin_type_text_non_nullable() {
    assert_eq!(kotlin_type(&SqlType::Text, false), "String");
}

#[test]
fn test_kotlin_type_text_nullable() {
    assert_eq!(kotlin_type(&SqlType::Text, true), "String?");
}

#[test]
fn test_kotlin_type_decimal() {
    assert_eq!(kotlin_type(&SqlType::Decimal, false), "java.math.BigDecimal");
}

#[test]
fn test_kotlin_type_temporal() {
    assert_eq!(kotlin_type(&SqlType::Date, false), "java.time.LocalDate");
    assert_eq!(kotlin_type(&SqlType::Time, false), "java.time.LocalTime");
    assert_eq!(kotlin_type(&SqlType::Timestamp, false), "java.time.LocalDateTime");
    assert_eq!(kotlin_type(&SqlType::TimestampTz, false), "java.time.OffsetDateTime");
}

#[test]
fn test_kotlin_type_uuid() {
    assert_eq!(kotlin_type(&SqlType::Uuid, false), "java.util.UUID");
}

#[test]
fn test_kotlin_type_json() {
    assert_eq!(kotlin_type(&SqlType::Json, false), "String");
    assert_eq!(kotlin_type(&SqlType::Jsonb, false), "String");
}

#[test]
fn test_kotlin_type_array_non_nullable() {
    assert_eq!(kotlin_type(&SqlType::Array(Box::new(SqlType::Text)), false), "List<String>");
}

#[test]
fn test_kotlin_type_array_nullable() {
    assert_eq!(kotlin_type(&SqlType::Array(Box::new(SqlType::Text)), true), "List<String>?");
}

#[test]
fn test_kotlin_type_array_of_integers() {
    // Inner type is non-nullable (List element, not the List itself)
    assert_eq!(kotlin_type(&SqlType::Array(Box::new(SqlType::Integer)), false), "List<Int>");
}

#[test]
fn test_resultset_read_array_text() {
    let cfg = OutputConfig::default();
    let expr = resultset_read_expr(&SqlType::Array(Box::new(SqlType::Text)), false, 3, &cfg);
    assert_eq!(expr, "jdbcArrayToList(rs.getArray(3))");
}

#[test]
fn test_resultset_read_array_nullable() {
    let cfg = OutputConfig::default();
    let expr = resultset_read_expr(&SqlType::Array(Box::new(SqlType::Text)), true, 5, &cfg);
    assert_eq!(expr, "rs.getArray(5)?.let { jdbcArrayToList(it) }");
}

#[test]
fn test_resultset_read_array_timestamp_default() {
    // TIMESTAMP[] with no override: emit per-element Timestamp→LocalDateTime conversion.
    let cfg = OutputConfig::default();
    let expr = resultset_read_expr(&SqlType::Array(Box::new(SqlType::Timestamp)), false, 3, &cfg);
    assert_eq!(expr, "(rs.getArray(3).array as Array<*>).map { (it as java.sql.Timestamp).toLocalDateTime() }.toList()");
}

#[test]
fn test_resultset_read_array_timestamp_nullable_default() {
    let cfg = OutputConfig::default();
    let expr = resultset_read_expr(&SqlType::Array(Box::new(SqlType::Timestamp)), true, 4, &cfg);
    assert_eq!(expr, "rs.getArray(4)?.let { a -> (a.array as Array<*>).map { (it as java.sql.Timestamp).toLocalDateTime() }.toList() }");
}

#[test]
fn test_resultset_read_array_timestamp_with_override() {
    // TIMESTAMP[] with an override to java.time.LocalDateTime: same conversion as default.
    let mut cfg = OutputConfig::default();
    cfg.type_overrides
        .insert("timestamp".to_string(), crate::config::TypeOverride::Same(crate::config::TypeRef::String("java.time.LocalDateTime".to_string())));
    let expr = resultset_read_expr(&SqlType::Array(Box::new(SqlType::Timestamp)), false, 2, &cfg);
    assert_eq!(expr, "(rs.getArray(2).array as Array<*>).map { (it as java.sql.Timestamp).toLocalDateTime() }.toList()");
}

#[test]
fn test_resultset_read_array_uuid_default() {
    // UUID[] with no override: emit explicit cast for type clarity.
    let cfg = OutputConfig::default();
    let expr = resultset_read_expr(&SqlType::Array(Box::new(SqlType::Uuid)), false, 1, &cfg);
    assert_eq!(expr, "(rs.getArray(1).array as Array<*>).map { (it as java.util.UUID) }.toList()");
}

#[test]
fn test_kotlin_type_custom() {
    assert_eq!(kotlin_type(&SqlType::Custom("citext".to_string()), false), "Any");
}

#[test]
fn test_kotlin_type_array_of_enum() {
    assert_eq!(kotlin_type(&SqlType::Array(Box::new(SqlType::Enum("status".to_string()))), false), "List<Status>");
}

#[test]
fn test_kotlin_type_array_of_enum_nullable() {
    assert_eq!(kotlin_type(&SqlType::Array(Box::new(SqlType::Enum("status".to_string()))), true), "List<Status>?");
}

#[test]
fn test_resultset_read_array_enum() {
    let cfg = OutputConfig::default();
    let expr = resultset_read_expr(&SqlType::Array(Box::new(SqlType::Enum("priority".to_string()))), false, 2, &cfg);
    assert_eq!(expr, "(rs.getArray(2).array as Array<*>).map { Priority.fromValue(it as String) }.toList()");
}

#[test]
fn test_resultset_read_array_enum_nullable() {
    let cfg = OutputConfig::default();
    let expr = resultset_read_expr(&SqlType::Array(Box::new(SqlType::Enum("status".to_string()))), true, 3, &cfg);
    assert_eq!(expr, "rs.getArray(3)?.let { a -> (a.array as Array<*>).map { Status.fromValue(it as String) }.toList() }");
}
