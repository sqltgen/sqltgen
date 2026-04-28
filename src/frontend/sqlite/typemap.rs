use sqlparser::ast::{DataType, ObjectName};

use crate::frontend::common::typemap::{custom_name_upper, fallback_custom, fallback_custom_name, map_common, map_custom_common};
use crate::ir::SqlType;

/// Maps a sqlparser [`DataType`] to [`SqlType`] using SQLite type affinity rules.
pub(crate) fn map(dt: &DataType) -> SqlType {
    // Dialect-specific arms first
    match dt {
        DataType::Varchar(_) | DataType::CharacterVarying(_) => SqlType::VarChar(None),

        DataType::Real | DataType::Float4 => SqlType::Real,

        DataType::Float(_) => SqlType::Double,

        // SQLite uses BLOB for binary data
        DataType::Blob(_) | DataType::Bytea | DataType::Varbinary(_) | DataType::Binary(_) => SqlType::Bytes,

        // SQLite has no native date/time types — these are stored as TEXT.
        // No driver can guarantee the stored value is a valid date/time string.
        DataType::Time(_, _)
        | DataType::Date
        | DataType::Timestamp(_, _)
        | DataType::Datetime(_) => SqlType::Text,

        // SQLite stores NUMERIC/DECIMAL as REAL (floating-point affinity).
        // Mapping to Double avoids exposing `rust_decimal::Decimal` in Rust backends
        // where SQLite cannot actually provide decimal precision.
        DataType::Numeric(_) | DataType::Decimal(_) => SqlType::Double,

        DataType::Custom(name, _) => map_custom(name),

        // Fall through to common mappings, then dialect fallback
        other => map_common(other).unwrap_or_else(|| fallback_custom(dt)),
    }
}

/// Exposed for testing; callers should use [`map`] with a parsed [`DataType`].
#[cfg(test)]
fn map_custom_str(s: &str) -> SqlType {
    use sqlparser::ast::Ident;
    let name = ObjectName::from(vec![Ident::new(s)]);
    map_custom(&name)
}

/// Maps SQLite-specific custom type names to [`SqlType`].
fn map_custom(name: &ObjectName) -> SqlType {
    let upper = custom_name_upper(name);
    match upper.as_str() {
        "INT" | "INTEGER" => SqlType::Integer,
        "SMALLINT" | "TINYINT" => SqlType::SmallInt,
        "BIGINT" => SqlType::BigInt,
        "REAL" | "FLOAT" => SqlType::Real,
        "DOUBLE" => SqlType::Double,
        "NUMBER" | "DECIMAL" | "NUMERIC" => SqlType::Double,
        "TEXT" | "CLOB" | "VARCHAR" | "NVARCHAR" | "NCHAR" | "VARYING CHARACTER" => SqlType::Text,
        "BLOB" | "NONE" => SqlType::Bytes,
        "DATETIME" | "DATE" | "TIME" | "TIMESTAMP" => SqlType::Text,
        _ => map_custom_common(&upper).unwrap_or_else(|| fallback_custom_name(&upper)),
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
        // SQLite has no fixed-point type; NUMERIC/DECIMAL map to Double (REAL affinity).
        assert_eq!(map(&DataType::Numeric(sqlparser::ast::ExactNumberInfo::None)), SqlType::Double);
        assert_eq!(map(&DataType::Decimal(sqlparser::ast::ExactNumberInfo::PrecisionAndScale(10, 2))), SqlType::Double);
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
    fn test_map_date_time_to_text() {
        // SQLite has no native date/time types — all map to Text.
        assert_eq!(map(&DataType::Date), SqlType::Text);
        use sqlparser::ast::TimezoneInfo;
        assert_eq!(map(&DataType::Time(None, TimezoneInfo::None)), SqlType::Text);
        assert_eq!(map(&DataType::Timestamp(None, TimezoneInfo::None)), SqlType::Text);
        assert_eq!(map(&DataType::Timestamp(None, TimezoneInfo::WithTimeZone)), SqlType::Text);
        assert_eq!(map(&DataType::Datetime(None)), SqlType::Text);
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
    fn test_map_custom_date_time_to_text() {
        assert_eq!(map_custom_str("DATE"), SqlType::Text);
        assert_eq!(map_custom_str("TIME"), SqlType::Text);
        assert_eq!(map_custom_str("DATETIME"), SqlType::Text);
        assert_eq!(map_custom_str("TIMESTAMP"), SqlType::Text);
    }

    #[test]
    fn test_map_custom_boolean() {
        assert_eq!(map_custom_str("BOOLEAN"), SqlType::Boolean);
        assert_eq!(map_custom_str("BOOL"), SqlType::Boolean);
    }

    #[test]
    fn test_map_custom_numeric() {
        // SQLite has no fixed-point type; NUMERIC/DECIMAL/NUMBER map to Double.
        assert_eq!(map_custom_str("NUMERIC"), SqlType::Double);
        assert_eq!(map_custom_str("DECIMAL"), SqlType::Double);
        assert_eq!(map_custom_str("NUMBER"), SqlType::Double);
    }

    #[test]
    fn test_map_custom_unknown_falls_through() {
        assert_eq!(map_custom_str("MYTYPE"), SqlType::Custom("mytype".to_string()));
    }
}
