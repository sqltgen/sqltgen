use sqlparser::ast::{DataType, ObjectName};

use crate::ir::SqlType;

/// Maps a sqlparser [`DataType`] to [`SqlType`] using SQLite type affinity rules.
pub fn map(dt: &DataType) -> SqlType {
    match dt {
        DataType::Boolean | DataType::Bool => SqlType::Boolean,

        DataType::Int2(_) | DataType::SmallInt(_) => SqlType::SmallInt,

        DataType::Int(_) | DataType::Int4(_) | DataType::Integer(_) | DataType::Unsigned | DataType::UnsignedInteger => SqlType::Integer,

        DataType::Int8(_) | DataType::BigInt(_) | DataType::BigIntUnsigned(_) => SqlType::BigInt,

        DataType::Real | DataType::Float4 => SqlType::Real,

        DataType::Double(_) | DataType::DoublePrecision | DataType::Float8 | DataType::Float64 => SqlType::Double,

        DataType::Float(_) => SqlType::Double,

        DataType::Numeric(_) | DataType::Decimal(_) => SqlType::Decimal,

        DataType::Text | DataType::Clob(_) => SqlType::Text,

        DataType::Varchar(_) | DataType::CharacterVarying(_) => SqlType::VarChar(None),

        DataType::Char(_) | DataType::Character(_) => SqlType::Char(None),

        // SQLite uses BLOB for binary data
        DataType::Blob(_) | DataType::Bytea | DataType::Varbinary(_) | DataType::Binary(_) => SqlType::Bytes,

        DataType::Date => SqlType::Date,

        DataType::Time(_, _) => SqlType::Time,

        DataType::Timestamp(_, _) => SqlType::Timestamp,

        DataType::Custom(name, _) => map_custom(name),

        _ => SqlType::Custom(format!("{dt}").to_lowercase()),
    }
}

/// Maps a SQLite custom type name string to [`SqlType`].
/// Exposed for testing; callers should use [`map`] with a parsed [`DataType`].
#[cfg(test)]
fn map_custom_str(s: &str) -> SqlType {
    use sqlparser::ast::Ident;
    let name = ObjectName::from(vec![Ident::new(s)]);
    map_custom(&name)
}

fn map_custom(name: &ObjectName) -> SqlType {
    use sqlparser::ast::ObjectNamePart;
    let upper =
        name.0.iter().filter_map(|p| if let ObjectNamePart::Identifier(i) = p { Some(i.value.to_uppercase()) } else { None }).collect::<Vec<_>>().join(".");
    match upper.as_str() {
        "INT" | "INTEGER" | "INT4" => SqlType::Integer,
        "INT2" | "SMALLINT" | "TINYINT" => SqlType::SmallInt,
        "INT8" | "BIGINT" => SqlType::BigInt,
        "REAL" | "FLOAT" | "FLOAT4" => SqlType::Real,
        "DOUBLE" | "FLOAT8" => SqlType::Double,
        "NUMERIC" | "DECIMAL" | "NUMBER" => SqlType::Decimal,
        "TEXT" | "CLOB" | "VARCHAR" | "NVARCHAR" | "NCHAR" | "VARYING CHARACTER" => SqlType::Text,
        "BLOB" | "NONE" => SqlType::Bytes,
        "BOOLEAN" | "BOOL" => SqlType::Boolean,
        "DATE" => SqlType::Date,
        "TIME" => SqlType::Time,
        "DATETIME" | "TIMESTAMP" => SqlType::Timestamp,
        "JSON" => SqlType::Json,
        _ => SqlType::Custom(upper.to_lowercase()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── map() tests (DataType → SqlType) ────────────────────────────────

    #[test]
    fn test_map_boolean() {
        assert_eq!(map(&DataType::Boolean), SqlType::Boolean);
        assert_eq!(map(&DataType::Bool), SqlType::Boolean);
    }

    #[test]
    fn test_map_integer_types() {
        assert_eq!(map(&DataType::SmallInt(None)), SqlType::SmallInt);
        assert_eq!(map(&DataType::Int2(None)), SqlType::SmallInt);
        assert_eq!(map(&DataType::Integer(None)), SqlType::Integer);
        assert_eq!(map(&DataType::Int(None)), SqlType::Integer);
        assert_eq!(map(&DataType::Int4(None)), SqlType::Integer);
        assert_eq!(map(&DataType::BigInt(None)), SqlType::BigInt);
        assert_eq!(map(&DataType::Int8(None)), SqlType::BigInt);
    }

    #[test]
    fn test_map_float_types() {
        assert_eq!(map(&DataType::Real), SqlType::Real);
        assert_eq!(map(&DataType::Float4), SqlType::Real);
        assert_eq!(map(&DataType::DoublePrecision), SqlType::Double);
        assert_eq!(map(&DataType::Float8), SqlType::Double);
        assert_eq!(map(&DataType::Float64), SqlType::Double);
        // FLOAT maps to Double (SQLite affinity)
        assert_eq!(map(&DataType::Float(sqlparser::ast::ExactNumberInfo::None)), SqlType::Double);
    }

    #[test]
    fn test_map_numeric() {
        assert_eq!(map(&DataType::Numeric(sqlparser::ast::ExactNumberInfo::None)), SqlType::Decimal);
        assert_eq!(map(&DataType::Decimal(sqlparser::ast::ExactNumberInfo::PrecisionAndScale(10, 2))), SqlType::Decimal);
    }

    #[test]
    fn test_map_text_types() {
        assert_eq!(map(&DataType::Text), SqlType::Text);
        assert!(matches!(map(&DataType::Varchar(None)), SqlType::VarChar(_)));
        assert!(matches!(map(&DataType::CharacterVarying(None)), SqlType::VarChar(_)));
        assert!(matches!(map(&DataType::Char(None)), SqlType::Char(_)));
    }

    #[test]
    fn test_map_blob_and_binary() {
        assert_eq!(map(&DataType::Blob(None)), SqlType::Bytes);
        assert_eq!(map(&DataType::Bytea), SqlType::Bytes);
        assert_eq!(map(&DataType::Varbinary(None)), SqlType::Bytes);
        assert_eq!(map(&DataType::Binary(None)), SqlType::Bytes);
    }

    #[test]
    fn test_map_date_time() {
        assert_eq!(map(&DataType::Date), SqlType::Date);
        use sqlparser::ast::TimezoneInfo;
        assert_eq!(map(&DataType::Time(None, TimezoneInfo::None)), SqlType::Time);
        assert_eq!(map(&DataType::Timestamp(None, TimezoneInfo::None)), SqlType::Timestamp);
        // SQLite ignores timezone info — always maps to plain Timestamp/Time
        assert_eq!(map(&DataType::Timestamp(None, TimezoneInfo::WithTimeZone)), SqlType::Timestamp);
    }

    #[test]
    fn test_map_unknown_falls_to_custom() {
        // DataType variant not in our match → lowercased string
        assert!(matches!(map(&DataType::Uuid), SqlType::Custom(_)));
    }

    // ─── map_custom() tests (custom type name string) ────────────────────

    #[test]
    fn test_map_custom_json_returns_json_type() {
        assert_eq!(map_custom_str("JSON"), SqlType::Json);
        assert_eq!(map_custom_str("json"), SqlType::Json);
    }

    #[test]
    fn test_map_custom_integer_affinity() {
        assert_eq!(map_custom_str("INTEGER"), SqlType::Integer);
        assert_eq!(map_custom_str("INT"), SqlType::Integer);
        assert_eq!(map_custom_str("INT4"), SqlType::Integer);
        assert_eq!(map_custom_str("BIGINT"), SqlType::BigInt);
        assert_eq!(map_custom_str("INT8"), SqlType::BigInt);
        assert_eq!(map_custom_str("SMALLINT"), SqlType::SmallInt);
        assert_eq!(map_custom_str("INT2"), SqlType::SmallInt);
        assert_eq!(map_custom_str("TINYINT"), SqlType::SmallInt);
    }

    #[test]
    fn test_map_custom_float_affinity() {
        assert_eq!(map_custom_str("REAL"), SqlType::Real);
        assert_eq!(map_custom_str("FLOAT"), SqlType::Real);
        assert_eq!(map_custom_str("FLOAT4"), SqlType::Real);
        assert_eq!(map_custom_str("DOUBLE"), SqlType::Double);
        assert_eq!(map_custom_str("FLOAT8"), SqlType::Double);
    }

    #[test]
    fn test_map_custom_text_affinity() {
        assert_eq!(map_custom_str("TEXT"), SqlType::Text);
        assert_eq!(map_custom_str("CLOB"), SqlType::Text);
        assert_eq!(map_custom_str("VARCHAR"), SqlType::Text);
        assert_eq!(map_custom_str("NVARCHAR"), SqlType::Text);
        assert_eq!(map_custom_str("NCHAR"), SqlType::Text);
    }

    #[test]
    fn test_map_custom_blob_affinity() {
        assert_eq!(map_custom_str("BLOB"), SqlType::Bytes);
        assert_eq!(map_custom_str("NONE"), SqlType::Bytes);
    }

    #[test]
    fn test_map_custom_date_time() {
        assert_eq!(map_custom_str("DATE"), SqlType::Date);
        assert_eq!(map_custom_str("TIME"), SqlType::Time);
        assert_eq!(map_custom_str("DATETIME"), SqlType::Timestamp);
        assert_eq!(map_custom_str("TIMESTAMP"), SqlType::Timestamp);
    }

    #[test]
    fn test_map_custom_boolean() {
        assert_eq!(map_custom_str("BOOLEAN"), SqlType::Boolean);
        assert_eq!(map_custom_str("BOOL"), SqlType::Boolean);
    }

    #[test]
    fn test_map_custom_numeric() {
        assert_eq!(map_custom_str("NUMERIC"), SqlType::Decimal);
        assert_eq!(map_custom_str("DECIMAL"), SqlType::Decimal);
        assert_eq!(map_custom_str("NUMBER"), SqlType::Decimal);
    }

    #[test]
    fn test_map_custom_unknown_falls_through() {
        assert_eq!(map_custom_str("MYTYPE"), SqlType::Custom("mytype".to_string()));
    }
}
