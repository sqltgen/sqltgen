use sqlparser::ast::{DataType, ObjectName};

use crate::frontend::common::typemap::{custom_name_upper, fallback_custom, fallback_custom_name, map_common, map_custom_common};
use crate::ir::SqlType;

/// Maps a sqlparser [`DataType`] to [`SqlType`] using MySQL type rules.
///
/// Note: MySQL query files currently use `$N` positional placeholders (same as PostgreSQL).
/// Bare `?` and named parameter support are planned for a future release.
pub(crate) fn map(dt: &DataType) -> SqlType {
    // Dialect-specific arms first
    match dt {
        // TINYINT(1) is MySQL's boolean convention; any other width is a small integer
        DataType::TinyInt(Some(1)) => SqlType::Boolean,
        DataType::TinyInt(_) => SqlType::SmallInt,

        DataType::MediumInt(_) => SqlType::Integer,

        DataType::Varchar(_) | DataType::CharacterVarying(_) | DataType::Nvarchar(_) => SqlType::VarChar(None),

        // MySQL FLOAT is 32-bit single-precision
        DataType::Float(_) | DataType::Real | DataType::Float4 => SqlType::Real,

        DataType::Blob(_) | DataType::Bytea | DataType::Binary(_) | DataType::Varbinary(_) => SqlType::Bytes,

        DataType::Time(_, _) => SqlType::Time,

        // MySQL DATETIME and TIMESTAMP are both timezone-naive
        DataType::Datetime(_) => SqlType::Timestamp,
        DataType::Timestamp(_, _) => SqlType::Timestamp,

        DataType::JSON => SqlType::Json,

        // ENUM and SET are opaque strings at the application level
        DataType::Enum(_, _) => SqlType::Text,
        DataType::Set(_) => SqlType::Text,

        DataType::Custom(name, _) => map_custom(name),

        // Fall through to common mappings, then dialect fallback
        other => map_common(other).unwrap_or_else(|| fallback_custom(dt)),
    }
}

/// Maps MySQL-specific custom type names to [`SqlType`].
fn map_custom(name: &ObjectName) -> SqlType {
    let upper = custom_name_upper(name);
    match upper.as_str() {
        "TINYINT" | "INT1" => SqlType::SmallInt,
        "SMALLINT" => SqlType::SmallInt,
        "MEDIUMINT" | "INT3" | "MIDDLEINT" => SqlType::Integer,
        "INT" | "INTEGER" => SqlType::Integer,
        "BIGINT" => SqlType::BigInt,
        "FLOAT" => SqlType::Real,
        "DOUBLE" | "REAL" => SqlType::Double,
        "DEC" | "FIXED" => SqlType::Decimal,
        // MySQL extended text types (may arrive as Custom depending on parser version)
        "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" | "CLOB" => SqlType::Text,
        "VARCHAR" | "NVARCHAR" | "NCHAR" => SqlType::Text,
        // MySQL extended blob types
        "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BINARY" | "VARBINARY" => SqlType::Bytes,
        "DATETIME" => SqlType::Timestamp,
        "YEAR" => SqlType::SmallInt,
        _ => map_custom_common(&upper).unwrap_or_else(|| fallback_custom_name(&upper)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{CharacterLength, ExactNumberInfo, TimezoneInfo};

    fn custom(name: &str) -> DataType {
        DataType::Custom(ObjectName::from(vec![sqlparser::ast::Ident::new(name)]), vec![])
    }

    #[test]
    fn integer_types() {
        assert_eq!(map(&DataType::Integer(None)), SqlType::Integer);
        assert_eq!(map(&DataType::Int(None)), SqlType::Integer);
        assert_eq!(map(&DataType::BigInt(None)), SqlType::BigInt);
        assert_eq!(map(&DataType::SmallInt(None)), SqlType::SmallInt);
        assert_eq!(map(&DataType::TinyInt(None)), SqlType::SmallInt);
        assert_eq!(map(&DataType::TinyInt(Some(4))), SqlType::SmallInt);
        assert_eq!(map(&DataType::MediumInt(None)), SqlType::Integer);
    }

    #[test]
    fn integer_custom_aliases() {
        assert_eq!(map(&custom("INT")), SqlType::Integer);
        assert_eq!(map(&custom("BIGINT")), SqlType::BigInt);
        assert_eq!(map(&custom("TINYINT")), SqlType::SmallInt);
        assert_eq!(map(&custom("MEDIUMINT")), SqlType::Integer);
        assert_eq!(map(&custom("INT1")), SqlType::SmallInt);
        assert_eq!(map(&custom("INT3")), SqlType::Integer);
    }

    #[test]
    fn float_types() {
        assert_eq!(map(&DataType::Float(sqlparser::ast::ExactNumberInfo::None)), SqlType::Real);
        assert_eq!(map(&DataType::Real), SqlType::Real);
        assert_eq!(map(&DataType::Double(sqlparser::ast::ExactNumberInfo::None)), SqlType::Double);
        assert_eq!(map(&DataType::DoublePrecision), SqlType::Double);
    }

    #[test]
    fn decimal_type() {
        assert_eq!(map(&DataType::Decimal(ExactNumberInfo::PrecisionAndScale(10, 2))), SqlType::Decimal);
        assert_eq!(map(&DataType::Numeric(ExactNumberInfo::None)), SqlType::Decimal);
        assert_eq!(map(&custom("DECIMAL")), SqlType::Decimal);
        assert_eq!(map(&custom("FIXED")), SqlType::Decimal);
    }

    #[test]
    fn text_types() {
        assert_eq!(map(&DataType::Text), SqlType::Text);
        assert!(matches!(map(&DataType::Varchar(Some(CharacterLength::IntegerLength { length: 255, unit: None }))), SqlType::VarChar(_)));
        assert!(matches!(map(&DataType::Char(None)), SqlType::Char(_)));
        assert_eq!(map(&custom("TINYTEXT")), SqlType::Text);
        assert_eq!(map(&custom("MEDIUMTEXT")), SqlType::Text);
        assert_eq!(map(&custom("LONGTEXT")), SqlType::Text);
    }

    #[test]
    fn binary_types() {
        assert_eq!(map(&DataType::Blob(None)), SqlType::Bytes);
        assert_eq!(map(&custom("TINYBLOB")), SqlType::Bytes);
        assert_eq!(map(&custom("MEDIUMBLOB")), SqlType::Bytes);
        assert_eq!(map(&custom("LONGBLOB")), SqlType::Bytes);
    }

    #[test]
    fn boolean_type() {
        assert_eq!(map(&DataType::Boolean), SqlType::Boolean);
        assert_eq!(map(&DataType::Bool), SqlType::Boolean);
        assert_eq!(map(&DataType::TinyInt(Some(1))), SqlType::Boolean);
    }

    #[test]
    fn datetime_types() {
        assert_eq!(map(&DataType::Date), SqlType::Date);
        assert_eq!(map(&DataType::Time(None, TimezoneInfo::None)), SqlType::Time);
        assert_eq!(map(&DataType::Timestamp(None, TimezoneInfo::None)), SqlType::Timestamp);
        assert_eq!(map(&DataType::Datetime(None)), SqlType::Timestamp);
        assert_eq!(map(&custom("DATETIME")), SqlType::Timestamp);
        assert_eq!(map(&custom("YEAR")), SqlType::SmallInt);
    }

    #[test]
    fn json_type() {
        assert_eq!(map(&DataType::JSON), SqlType::Json);
        assert_eq!(map(&custom("JSON")), SqlType::Json);
    }

    #[test]
    fn unknown_type_becomes_custom() {
        assert!(matches!(map(&custom("GEOMETRY")), SqlType::Custom(_)));
        assert!(matches!(map(&custom("POINT")), SqlType::Custom(_)));
    }
}
