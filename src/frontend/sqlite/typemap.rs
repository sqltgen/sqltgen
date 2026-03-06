use sqlparser::ast::{DataType, ObjectName};

use crate::ir::SqlType;

/// Maps a sqlparser [`DataType`] to [`SqlType`] using SQLite type affinity rules.
pub fn map(dt: &DataType) -> SqlType {
    match dt {
        DataType::Boolean | DataType::Bool => SqlType::Boolean,

        DataType::Int2(_) | DataType::SmallInt(_) => SqlType::SmallInt,

        DataType::Int(_) | DataType::Int4(_) | DataType::Integer(_) | DataType::UnsignedInt(_) | DataType::UnsignedInteger(_) => SqlType::Integer,

        DataType::Int8(_) | DataType::BigInt(_) | DataType::UnsignedBigInt(_) => SqlType::BigInt,

        DataType::Real | DataType::Float4 => SqlType::Real,

        DataType::Double | DataType::DoublePrecision | DataType::Float8 | DataType::Float64 => SqlType::Double,

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

fn map_custom(name: &ObjectName) -> SqlType {
    let upper = name.0.iter().map(|i| i.value.to_uppercase()).collect::<Vec<_>>().join(".");
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
        _ => SqlType::Custom(upper.to_lowercase()),
    }
}
