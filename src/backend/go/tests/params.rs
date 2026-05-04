use super::*;

// ─── Type mapping tests ─────────────────────────────────────────────────────

#[test]
fn test_go_type_boolean() {
    assert_eq!(go_type(&SqlType::Boolean, false, &GoTarget::Postgres), "bool");
    assert_eq!(go_type(&SqlType::Boolean, true, &GoTarget::Postgres), "sql.NullBool");
}

#[test]
fn test_go_type_smallint() {
    assert_eq!(go_type(&SqlType::SmallInt, false, &GoTarget::Postgres), "int16");
    assert_eq!(go_type(&SqlType::SmallInt, true, &GoTarget::Postgres), "sql.NullInt16");
}

#[test]
fn test_go_type_integer() {
    assert_eq!(go_type(&SqlType::Integer, false, &GoTarget::Postgres), "int32");
    assert_eq!(go_type(&SqlType::Integer, true, &GoTarget::Postgres), "sql.NullInt32");
}

#[test]
fn test_go_type_bigint() {
    assert_eq!(go_type(&SqlType::BigInt, false, &GoTarget::Postgres), "int64");
    assert_eq!(go_type(&SqlType::BigInt, true, &GoTarget::Postgres), "sql.NullInt64");
}

#[test]
fn test_go_type_real() {
    assert_eq!(go_type(&SqlType::Real, false, &GoTarget::Postgres), "float32");
    assert_eq!(go_type(&SqlType::Real, true, &GoTarget::Postgres), "*float32");
}

#[test]
fn test_go_type_double() {
    assert_eq!(go_type(&SqlType::Double, false, &GoTarget::Postgres), "float64");
    assert_eq!(go_type(&SqlType::Double, true, &GoTarget::Postgres), "sql.NullFloat64");
}

#[test]
fn test_go_type_decimal() {
    assert_eq!(go_type(&SqlType::Decimal, false, &GoTarget::Postgres), "string");
    assert_eq!(go_type(&SqlType::Decimal, true, &GoTarget::Postgres), "sql.NullString");
}

#[test]
fn test_go_type_text() {
    assert_eq!(go_type(&SqlType::Text, false, &GoTarget::Postgres), "string");
    assert_eq!(go_type(&SqlType::Text, true, &GoTarget::Postgres), "sql.NullString");
}

#[test]
fn test_go_type_varchar() {
    assert_eq!(go_type(&SqlType::VarChar(Some(255)), false, &GoTarget::Postgres), "string");
}

#[test]
fn test_go_type_char() {
    assert_eq!(go_type(&SqlType::Char(Some(10)), false, &GoTarget::Postgres), "string");
}

#[test]
fn test_go_type_bytes() {
    assert_eq!(go_type(&SqlType::Bytes, false, &GoTarget::Postgres), "[]byte");
    assert_eq!(go_type(&SqlType::Bytes, true, &GoTarget::Postgres), "[]byte");
}

#[test]
fn test_go_type_date() {
    assert_eq!(go_type(&SqlType::Date, false, &GoTarget::Postgres), "time.Time");
    assert_eq!(go_type(&SqlType::Date, true, &GoTarget::Postgres), "sql.NullTime");
}

#[test]
fn test_go_type_time() {
    assert_eq!(go_type(&SqlType::Time, false, &GoTarget::Postgres), "time.Time");
    // Nullable TIME still uses sql.NullTime in the struct — the scan wrapper is
    // in the generated query code, not the type map.
    assert_eq!(go_type(&SqlType::Time, true, &GoTarget::Postgres), "sql.NullTime");
    assert_eq!(go_type(&SqlType::Time, true, &GoTarget::Sqlite), "sql.NullTime");
}

#[test]
fn test_go_type_timestamp() {
    assert_eq!(go_type(&SqlType::Timestamp, false, &GoTarget::Postgres), "time.Time");
    assert_eq!(go_type(&SqlType::Timestamp, true, &GoTarget::Postgres), "sql.NullTime");
}

#[test]
fn test_go_type_timestamptz() {
    assert_eq!(go_type(&SqlType::TimestampTz, false, &GoTarget::Postgres), "time.Time");
    assert_eq!(go_type(&SqlType::TimestampTz, true, &GoTarget::Postgres), "sql.NullTime");
}

#[test]
fn test_go_type_interval() {
    assert_eq!(go_type(&SqlType::Interval, false, &GoTarget::Postgres), "string");
    assert_eq!(go_type(&SqlType::Interval, true, &GoTarget::Postgres), "sql.NullString");
}

#[test]
fn test_go_type_uuid() {
    assert_eq!(go_type(&SqlType::Uuid, false, &GoTarget::Postgres), "string");
    assert_eq!(go_type(&SqlType::Uuid, true, &GoTarget::Postgres), "sql.NullString");
}

#[test]
fn test_go_type_json_postgres() {
    assert_eq!(go_type(&SqlType::Json, false, &GoTarget::Postgres), "[]byte");
    assert_eq!(go_type(&SqlType::Jsonb, false, &GoTarget::Postgres), "[]byte");
    assert_eq!(go_type(&SqlType::Json, true, &GoTarget::Postgres), "*[]byte");
}

#[test]
fn test_go_type_json_sqlite() {
    assert_eq!(go_type(&SqlType::Json, false, &GoTarget::Sqlite), "string");
    assert_eq!(go_type(&SqlType::Json, true, &GoTarget::Sqlite), "sql.NullString");
}

#[test]
fn test_go_type_json_mysql() {
    assert_eq!(go_type(&SqlType::Json, false, &GoTarget::Mysql), "string");
    assert_eq!(go_type(&SqlType::Jsonb, false, &GoTarget::Mysql), "string");
}

#[test]
fn test_go_type_array() {
    let arr = SqlType::Array(Box::new(SqlType::BigInt));
    assert_eq!(go_type(&arr, false, &GoTarget::Postgres), "[]int64");
    assert_eq!(go_type(&arr, true, &GoTarget::Postgres), "*[]int64");
}

#[test]
fn test_go_type_custom() {
    assert_eq!(go_type(&SqlType::Custom("hstore".into()), false, &GoTarget::Postgres), "any");
    assert_eq!(go_type(&SqlType::Custom("hstore".into()), true, &GoTarget::Postgres), "any");
}
