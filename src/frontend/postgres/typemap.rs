use sqlparser::ast::{ArrayElemTypeDef, DataType, ObjectName, TimezoneInfo};

use crate::frontend::common::typemap::{custom_name_upper, fallback_custom, fallback_custom_name, map_common, map_custom_common};
use crate::ir::SqlType;

/// Maps a sqlparser [`DataType`] to the canonical [`SqlType`].
pub(crate) fn map(dt: &DataType) -> SqlType {
    // Dialect-specific arms first
    match dt {
        DataType::Varchar(_) | DataType::CharacterVarying(_) | DataType::Nvarchar(_) => SqlType::VarChar(None),

        DataType::Real | DataType::Float4 => SqlType::Real,

        DataType::Float(_) => SqlType::Double,

        DataType::Bytea => SqlType::Bytes,

        DataType::Time(_, TimezoneInfo::None) | DataType::Time(_, TimezoneInfo::WithoutTimeZone) => SqlType::Time,
        DataType::Time(_, _) => SqlType::TimestampTz,

        DataType::Timestamp(_, TimezoneInfo::None) | DataType::Timestamp(_, TimezoneInfo::WithoutTimeZone) => SqlType::Timestamp,
        DataType::Timestamp(_, _) => SqlType::TimestampTz,

        DataType::Interval { .. } => SqlType::Interval,

        DataType::Uuid => SqlType::Uuid,

        DataType::JSON => SqlType::Json,
        DataType::JSONB => SqlType::Jsonb,

        DataType::Array(ArrayElemTypeDef::SquareBracket(inner, _)) | DataType::Array(ArrayElemTypeDef::AngleBracket(inner)) => {
            SqlType::Array(Box::new(map(inner)))
        },
        DataType::Array(_) => SqlType::Array(Box::new(SqlType::Text)),

        DataType::Custom(obj_name, _) => map_custom(obj_name),

        // Fall through to common mappings, then dialect fallback
        other => map_common(other).unwrap_or_else(|| fallback_custom(dt)),
    }
}

/// Maps PostgreSQL-specific custom type names to [`SqlType`].
fn map_custom(name: &ObjectName) -> SqlType {
    let upper = custom_name_upper(name);
    match upper.as_str() {
        "BIGSERIAL" | "SERIAL8" => SqlType::BigInt,
        "SERIAL" | "SERIAL4" => SqlType::Integer,
        "SMALLSERIAL" | "SERIAL2" => SqlType::SmallInt,
        "OID" => SqlType::BigInt,
        "MONEY" => SqlType::Decimal,
        "BIT" | "VARBIT" => SqlType::Boolean,
        "BPCHAR" | "NAME" => SqlType::Text,
        "TIMESTAMPTZ" => SqlType::TimestampTz,
        "TIMETZ" => SqlType::TimestampTz,
        "FLOAT" => SqlType::Double,
        _ => map_custom_common(&upper).unwrap_or_else(|| fallback_custom_name(&upper)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{CharacterLength, ExactNumberInfo, ObjectName};

    fn custom(name: &str) -> DataType {
        DataType::Custom(ObjectName::from(vec![sqlparser::ast::Ident::new(name)]), vec![])
    }

    #[test]
    fn integer_types() {
        assert_eq!(map(&DataType::Integer(None)), SqlType::Integer);
        assert_eq!(map(&DataType::Int(None)), SqlType::Integer);
        assert_eq!(map(&custom("INT4")), SqlType::Integer);
        assert_eq!(map(&custom("SERIAL")), SqlType::Integer);
    }

    #[test]
    fn bigint_types() {
        assert_eq!(map(&DataType::BigInt(None)), SqlType::BigInt);
        assert_eq!(map(&custom("INT8")), SqlType::BigInt);
        assert_eq!(map(&custom("BIGSERIAL")), SqlType::BigInt);
        assert_eq!(map(&custom("SERIAL8")), SqlType::BigInt);
    }

    #[test]
    fn smallint_types() {
        assert_eq!(map(&DataType::SmallInt(None)), SqlType::SmallInt);
        assert_eq!(map(&custom("INT2")), SqlType::SmallInt);
        assert_eq!(map(&custom("SMALLSERIAL")), SqlType::SmallInt);
        assert_eq!(map(&custom("SERIAL2")), SqlType::SmallInt);
    }

    #[test]
    fn text_types() {
        assert_eq!(map(&DataType::Text), SqlType::Text);
        assert!(matches!(map(&DataType::Varchar(Some(CharacterLength::IntegerLength { length: 255, unit: None }))), SqlType::VarChar(_)));
        assert!(matches!(map(&DataType::CharacterVarying(None)), SqlType::VarChar(_)));
        assert!(matches!(map(&DataType::Char(None)), SqlType::Char(_)));
    }

    #[test]
    fn boolean_type() {
        assert_eq!(map(&DataType::Boolean), SqlType::Boolean);
    }

    #[test]
    fn timestamp_types() {
        assert_eq!(map(&DataType::Timestamp(None, TimezoneInfo::None)), SqlType::Timestamp);
        assert_eq!(map(&DataType::Timestamp(None, TimezoneInfo::WithoutTimeZone)), SqlType::Timestamp);
        assert_eq!(map(&DataType::Timestamp(None, TimezoneInfo::WithTimeZone)), SqlType::TimestampTz);
        assert_eq!(map(&custom("TIMESTAMPTZ")), SqlType::TimestampTz);
    }

    #[test]
    fn date_type() {
        assert_eq!(map(&DataType::Date), SqlType::Date);
    }

    #[test]
    fn uuid_type() {
        assert_eq!(map(&DataType::Uuid), SqlType::Uuid);
    }

    #[test]
    fn json_types() {
        assert_eq!(map(&DataType::JSON), SqlType::Json);
        assert_eq!(map(&DataType::JSONB), SqlType::Jsonb);
    }

    #[test]
    fn bytes_type() {
        assert_eq!(map(&DataType::Bytea), SqlType::Bytes);
    }

    #[test]
    fn numeric_type() {
        assert_eq!(map(&DataType::Numeric(ExactNumberInfo::PrecisionAndScale(10, 2))), SqlType::Decimal);
        assert_eq!(map(&DataType::Numeric(ExactNumberInfo::None)), SqlType::Decimal);
    }

    #[test]
    fn array_types() {
        assert!(matches!(map(&DataType::Array(ArrayElemTypeDef::SquareBracket(Box::new(DataType::Text), None))), SqlType::Array(_)));
        if let SqlType::Array(inner) = map(&DataType::Array(ArrayElemTypeDef::SquareBracket(Box::new(DataType::Integer(None)), None))) {
            assert_eq!(*inner, SqlType::Integer);
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn double_precision() {
        assert_eq!(map(&DataType::DoublePrecision), SqlType::Double);
        assert_eq!(map(&DataType::Float(sqlparser::ast::ExactNumberInfo::None)), SqlType::Double);
    }

    #[test]
    fn unknown_type_becomes_custom() {
        assert!(matches!(map(&custom("GEOMETRY")), SqlType::Custom(_)));
        assert!(matches!(map(&custom("citext")), SqlType::Custom(_)));
    }
}
