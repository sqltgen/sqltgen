use sqlparser::ast::{DataType, ObjectName};

use crate::ir::SqlType;

/// Maps a sqlparser [`DataType`] to [`SqlType`] using MySQL type rules.
///
/// Note: MySQL query files currently use `$N` positional placeholders (same as PostgreSQL).
/// Bare `?` and named parameter support are planned for a future release.
pub fn map(dt: &DataType) -> SqlType {
    match dt {
        DataType::Boolean | DataType::Bool => SqlType::Boolean,

        // TINYINT is 1-byte signed; map to SmallInt (use BOOLEAN/BOOL for booleans)
        DataType::TinyInt(_) | DataType::Int2(_) | DataType::SmallInt(_) => SqlType::SmallInt,

        DataType::MediumInt(_) | DataType::Int(_) | DataType::Int4(_) | DataType::Integer(_) | DataType::Unsigned | DataType::UnsignedInteger => {
            SqlType::Integer
        },

        DataType::Int8(_) | DataType::BigInt(_) | DataType::BigIntUnsigned(_) => SqlType::BigInt,

        // MySQL FLOAT is 32-bit single-precision
        DataType::Float(_) | DataType::Real | DataType::Float4 => SqlType::Real,

        DataType::Double(_) | DataType::DoublePrecision | DataType::Float8 | DataType::Float64 => SqlType::Double,

        DataType::Numeric(_) | DataType::Decimal(_) => SqlType::Decimal,

        DataType::Text | DataType::Clob(_) => SqlType::Text,

        DataType::Varchar(_) | DataType::CharacterVarying(_) | DataType::Nvarchar(_) => SqlType::VarChar(None),

        DataType::Char(_) | DataType::Character(_) => SqlType::Char(None),

        DataType::Blob(_) | DataType::Bytea | DataType::Binary(_) | DataType::Varbinary(_) => SqlType::Bytes,

        DataType::Date => SqlType::Date,

        DataType::Time(_, _) => SqlType::Time,

        // MySQL DATETIME and TIMESTAMP are both timezone-naive
        DataType::Datetime(_) => SqlType::Timestamp,
        DataType::Timestamp(_, _) => SqlType::Timestamp,

        DataType::JSON => SqlType::Json,

        // ENUM and SET are opaque strings at the application level
        DataType::Enum(_, _) => SqlType::Text,
        DataType::Set(_) => SqlType::Text,

        DataType::Custom(name, _) => map_custom(name),

        _ => SqlType::Custom(format!("{dt}").to_lowercase()),
    }
}

fn map_custom(name: &ObjectName) -> SqlType {
    use sqlparser::ast::ObjectNamePart;
    let upper =
        name.0.iter().filter_map(|p| if let ObjectNamePart::Identifier(i) = p { Some(i.value.to_uppercase()) } else { None }).collect::<Vec<_>>().join(".");
    match upper.as_str() {
        "TINYINT" | "INT1" => SqlType::SmallInt,
        "SMALLINT" | "INT2" => SqlType::SmallInt,
        "MEDIUMINT" | "INT3" | "MIDDLEINT" => SqlType::Integer,
        "INT" | "INT4" | "INTEGER" => SqlType::Integer,
        "BIGINT" | "INT8" => SqlType::BigInt,
        "FLOAT" | "FLOAT4" => SqlType::Real,
        "DOUBLE" | "FLOAT8" | "REAL" => SqlType::Double,
        "DECIMAL" | "NUMERIC" | "DEC" | "FIXED" => SqlType::Decimal,
        // MySQL extended text types (may arrive as Custom depending on parser version)
        "TEXT" | "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" | "CLOB" => SqlType::Text,
        "VARCHAR" | "NVARCHAR" | "NCHAR" => SqlType::Text,
        // MySQL extended blob types
        "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BINARY" | "VARBINARY" => SqlType::Bytes,
        "BOOL" | "BOOLEAN" => SqlType::Boolean,
        "DATE" => SqlType::Date,
        "TIME" => SqlType::Time,
        "DATETIME" | "TIMESTAMP" => SqlType::Timestamp,
        "YEAR" => SqlType::SmallInt,
        "JSON" => SqlType::Json,
        _ => SqlType::Custom(upper.to_lowercase()),
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
