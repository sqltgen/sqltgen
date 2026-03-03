use crate::ir::SqlType;

/// Maps a raw PostgreSQL type string from DDL to the canonical `SqlType`.
///
/// Normalises to uppercase and strips size parameters before lookup.
/// Array brackets (`[]`) must be stripped by the caller; this function
/// handles only the base element type.
pub fn map(raw: &str) -> SqlType {
    match normalise(raw).as_str() {
        // Boolean
        "BOOLEAN" | "BOOL" => SqlType::Boolean,
        // Integers
        "SMALLINT" | "INT2" | "SMALLSERIAL" | "SERIAL2" => SqlType::SmallInt,
        "INTEGER" | "INT" | "INT4" | "SERIAL" | "SERIAL4" => SqlType::Integer,
        "BIGINT" | "INT8" | "BIGSERIAL" | "SERIAL8" => SqlType::BigInt,
        // Floating point
        "REAL" | "FLOAT4" => SqlType::Real,
        "FLOAT8" | "DOUBLE PRECISION" | "FLOAT" => SqlType::Double,
        // Exact numeric
        "NUMERIC" | "DECIMAL" | "MONEY" => SqlType::Decimal,
        // Text
        "TEXT" | "BPCHAR" | "BIT VARYING" => SqlType::Text,
        "VARCHAR" | "CHARACTER VARYING" => SqlType::VarChar(None),
        "CHAR" | "CHARACTER" => SqlType::Char(None),
        // Binary
        "BYTEA" => SqlType::Bytes,
        // Date / time
        "TIMESTAMP" | "TIMESTAMP WITHOUT TIME ZONE" => SqlType::Timestamp,
        "TIMESTAMPTZ" | "TIMESTAMP WITH TIME ZONE" => SqlType::TimestampTz,
        "DATE" => SqlType::Date,
        "TIME" | "TIME WITHOUT TIME ZONE" => SqlType::Time,
        "TIMETZ" | "TIME WITH TIME ZONE" => SqlType::TimestampTz,
        "INTERVAL" => SqlType::Interval,
        // UUID
        "UUID" => SqlType::Uuid,
        // JSON
        "JSON" => SqlType::Json,
        "JSONB" => SqlType::Jsonb,
        // OID
        "OID" => SqlType::BigInt,
        // BIT (fixed-width) — treat as boolean
        "BIT" => SqlType::Boolean,
        // Unknown / extension types
        other => SqlType::Custom(other.to_string()),
    }
}

fn normalise(raw: &str) -> String {
    // Strip size params: VARCHAR(255) → VARCHAR, NUMERIC(10,2) → NUMERIC
    let without_size = if let Some(paren) = raw.find('(') {
        raw[..paren].trim()
    } else {
        raw.trim()
    };
    // Strip array brackets handled by caller, but be safe
    let without_arrays = without_size.replace("[]", "");
    without_arrays.trim().to_uppercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_types() {
        assert_eq!(map("INTEGER"), SqlType::Integer);
        assert_eq!(map("INT"), SqlType::Integer);
        assert_eq!(map("INT4"), SqlType::Integer);
        assert_eq!(map("SERIAL"), SqlType::Integer);
    }

    #[test]
    fn bigint_types() {
        for t in &["BIGINT", "INT8", "BIGSERIAL", "SERIAL8"] {
            assert_eq!(map(t), SqlType::BigInt, "failed for {t}");
        }
    }

    #[test]
    fn smallint_types() {
        for t in &["SMALLINT", "INT2", "SMALLSERIAL", "SERIAL2"] {
            assert_eq!(map(t), SqlType::SmallInt, "failed for {t}");
        }
    }

    #[test]
    fn text_types() {
        assert_eq!(map("TEXT"), SqlType::Text);
        assert!(matches!(map("VARCHAR"), SqlType::VarChar(_)));
        assert!(matches!(map("CHARACTER VARYING"), SqlType::VarChar(_)));
        assert!(matches!(map("CHAR"), SqlType::Char(_)));
    }

    #[test]
    fn boolean_type() {
        assert_eq!(map("BOOLEAN"), SqlType::Boolean);
        assert_eq!(map("BOOL"), SqlType::Boolean);
    }

    #[test]
    fn timestamp_types() {
        assert_eq!(map("TIMESTAMP"), SqlType::Timestamp);
        assert_eq!(map("TIMESTAMP WITHOUT TIME ZONE"), SqlType::Timestamp);
        assert_eq!(map("TIMESTAMPTZ"), SqlType::TimestampTz);
        assert_eq!(map("TIMESTAMP WITH TIME ZONE"), SqlType::TimestampTz);
    }

    #[test]
    fn date_type() {
        assert_eq!(map("DATE"), SqlType::Date);
    }

    #[test]
    fn uuid_type() {
        assert_eq!(map("UUID"), SqlType::Uuid);
    }

    #[test]
    fn json_types() {
        assert_eq!(map("JSON"), SqlType::Json);
        assert_eq!(map("JSONB"), SqlType::Jsonb);
    }

    #[test]
    fn bytes_type() {
        assert_eq!(map("BYTEA"), SqlType::Bytes);
    }

    #[test]
    fn strips_size_params() {
        assert!(matches!(map("VARCHAR(255)"), SqlType::VarChar(_)));
        assert_eq!(map("NUMERIC(10,2)"), SqlType::Decimal);
    }

    #[test]
    fn case_insensitive() {
        assert_eq!(map("text"), SqlType::Text);
        assert_eq!(map("Boolean"), SqlType::Boolean);
        assert_eq!(map("bigint"), SqlType::BigInt);
    }

    #[test]
    fn unknown_type_becomes_custom() {
        assert!(matches!(map("GEOMETRY"), SqlType::Custom(_)));
        assert!(matches!(map("citext"), SqlType::Custom(_)));
    }
}
