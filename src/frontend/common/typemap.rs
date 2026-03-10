use sqlparser::ast::{DataType, ObjectName, ObjectNamePart};

use crate::ir::SqlType;

/// Maps a sqlparser [`DataType`] to [`SqlType`] for arms that are identical
/// across all supported dialects (PostgreSQL, SQLite, MySQL).
///
/// Returns `Some(SqlType)` for common mappings, `None` if the dialect should
/// handle this type itself.
pub(crate) fn map_common(dt: &DataType) -> Option<SqlType> {
    match dt {
        DataType::Boolean | DataType::Bool => Some(SqlType::Boolean),

        DataType::Int2(_) | DataType::SmallInt(_) => Some(SqlType::SmallInt),

        DataType::Int(_) | DataType::Int4(_) | DataType::Integer(_) | DataType::Unsigned | DataType::UnsignedInteger => Some(SqlType::Integer),

        DataType::Int8(_) | DataType::BigInt(_) | DataType::BigIntUnsigned(_) => Some(SqlType::BigInt),

        DataType::Double(_) | DataType::DoublePrecision | DataType::Float8 | DataType::Float64 => Some(SqlType::Double),

        DataType::Numeric(_) | DataType::Decimal(_) => Some(SqlType::Decimal),

        DataType::Text | DataType::Clob(_) => Some(SqlType::Text),

        DataType::Char(_) | DataType::Character(_) => Some(SqlType::Char(None)),

        DataType::Date => Some(SqlType::Date),

        _ => None,
    }
}

/// Extracts the uppercased, dot-joined name from an [`ObjectName`].
///
/// This is the standard way all dialects normalise a custom type name before
/// matching it against known aliases.
pub(crate) fn custom_name_upper(name: &ObjectName) -> String {
    name.0.iter().filter_map(|p| if let ObjectNamePart::Identifier(i) = p { Some(i.value.to_uppercase()) } else { None }).collect::<Vec<_>>().join(".")
}

/// Maps custom type names that are common across all dialects.
///
/// Returns `Some(SqlType)` for recognised names, `None` otherwise so the
/// dialect can try its own overrides before falling back to `Custom`.
pub(crate) fn map_custom_common(upper: &str) -> Option<SqlType> {
    match upper {
        "INT2" => Some(SqlType::SmallInt),
        "INT4" => Some(SqlType::Integer),
        "INT8" => Some(SqlType::BigInt),
        "FLOAT4" => Some(SqlType::Real),
        "FLOAT8" => Some(SqlType::Double),
        "BOOL" | "BOOLEAN" => Some(SqlType::Boolean),
        "NUMERIC" | "DECIMAL" => Some(SqlType::Decimal),
        "DATE" => Some(SqlType::Date),
        "TIME" => Some(SqlType::Time),
        "TIMESTAMP" => Some(SqlType::Timestamp),
        "JSON" => Some(SqlType::Json),
        _ => None,
    }
}

/// Produces the default fallback for an unrecognised [`DataType`]: a
/// lowercased `Custom` variant.
pub(crate) fn fallback_custom(dt: &DataType) -> SqlType {
    SqlType::Custom(format!("{dt}").to_lowercase())
}

/// Produces the default fallback for an unrecognised custom type name string:
/// a lowercased `Custom` variant.
pub(crate) fn fallback_custom_name(upper: &str) -> SqlType {
    SqlType::Custom(upper.to_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::{ExactNumberInfo, TimezoneInfo};

    #[test]
    fn test_map_common_boolean() {
        assert_eq!(map_common(&DataType::Boolean), Some(SqlType::Boolean));
        assert_eq!(map_common(&DataType::Bool), Some(SqlType::Boolean));
    }

    #[test]
    fn test_map_common_integers() {
        assert_eq!(map_common(&DataType::SmallInt(None)), Some(SqlType::SmallInt));
        assert_eq!(map_common(&DataType::Int2(None)), Some(SqlType::SmallInt));
        assert_eq!(map_common(&DataType::Integer(None)), Some(SqlType::Integer));
        assert_eq!(map_common(&DataType::Int(None)), Some(SqlType::Integer));
        assert_eq!(map_common(&DataType::Int4(None)), Some(SqlType::Integer));
        assert_eq!(map_common(&DataType::BigInt(None)), Some(SqlType::BigInt));
        assert_eq!(map_common(&DataType::Int8(None)), Some(SqlType::BigInt));
    }

    #[test]
    fn test_map_common_double() {
        assert_eq!(map_common(&DataType::DoublePrecision), Some(SqlType::Double));
        assert_eq!(map_common(&DataType::Float8), Some(SqlType::Double));
        assert_eq!(map_common(&DataType::Float64), Some(SqlType::Double));
    }

    #[test]
    fn test_map_common_decimal() {
        assert_eq!(map_common(&DataType::Numeric(ExactNumberInfo::None)), Some(SqlType::Decimal));
        assert_eq!(map_common(&DataType::Decimal(ExactNumberInfo::PrecisionAndScale(10, 2))), Some(SqlType::Decimal));
    }

    #[test]
    fn test_map_common_text() {
        assert_eq!(map_common(&DataType::Text), Some(SqlType::Text));
        assert_eq!(map_common(&DataType::Char(None)), Some(SqlType::Char(None)));
    }

    #[test]
    fn test_map_common_date() {
        assert_eq!(map_common(&DataType::Date), Some(SqlType::Date));
    }

    #[test]
    fn test_map_common_returns_none_for_dialect_specific() {
        // Uuid is Postgres-specific
        assert_eq!(map_common(&DataType::Uuid), None);
        // Blob is SQLite/MySQL-specific mapping
        assert_eq!(map_common(&DataType::Blob(None)), None);
        // Time with timezone is dialect-specific
        assert_eq!(map_common(&DataType::Time(None, TimezoneInfo::WithTimeZone)), None);
        // Float mapping differs between dialects
        assert_eq!(map_common(&DataType::Float(ExactNumberInfo::None)), None);
    }

    #[test]
    fn test_custom_name_upper() {
        let name = sqlparser::ast::ObjectName::from(vec![sqlparser::ast::Ident::new("bigserial")]);
        assert_eq!(custom_name_upper(&name), "BIGSERIAL");
    }

    #[test]
    fn test_custom_name_upper_dotted() {
        let name = sqlparser::ast::ObjectName::from(vec![sqlparser::ast::Ident::new("pg_catalog"), sqlparser::ast::Ident::new("int4")]);
        assert_eq!(custom_name_upper(&name), "PG_CATALOG.INT4");
    }

    #[test]
    fn test_map_custom_common_known() {
        assert_eq!(map_custom_common("INT2"), Some(SqlType::SmallInt));
        assert_eq!(map_custom_common("INT4"), Some(SqlType::Integer));
        assert_eq!(map_custom_common("INT8"), Some(SqlType::BigInt));
        assert_eq!(map_custom_common("FLOAT4"), Some(SqlType::Real));
        assert_eq!(map_custom_common("FLOAT8"), Some(SqlType::Double));
        assert_eq!(map_custom_common("BOOL"), Some(SqlType::Boolean));
        assert_eq!(map_custom_common("BOOLEAN"), Some(SqlType::Boolean));
        assert_eq!(map_custom_common("NUMERIC"), Some(SqlType::Decimal));
        assert_eq!(map_custom_common("DECIMAL"), Some(SqlType::Decimal));
        assert_eq!(map_custom_common("DATE"), Some(SqlType::Date));
        assert_eq!(map_custom_common("TIME"), Some(SqlType::Time));
        assert_eq!(map_custom_common("TIMESTAMP"), Some(SqlType::Timestamp));
        assert_eq!(map_custom_common("JSON"), Some(SqlType::Json));
    }

    #[test]
    fn test_map_custom_common_unknown() {
        assert_eq!(map_custom_common("GEOMETRY"), None);
        assert_eq!(map_custom_common("BIGSERIAL"), None);
    }

    #[test]
    fn test_fallback_custom() {
        assert_eq!(fallback_custom(&DataType::Uuid), SqlType::Custom("uuid".to_string()));
    }

    #[test]
    fn test_fallback_custom_name() {
        assert_eq!(fallback_custom_name("GEOMETRY"), SqlType::Custom("geometry".to_string()));
    }
}
