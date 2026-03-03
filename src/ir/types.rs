#[derive(Debug, Clone, PartialEq)]
pub enum SqlType {
    // Boolean
    Boolean,
    // Integers
    SmallInt,
    Integer,
    BigInt,
    // Floating point
    Real,
    Double,
    // Exact numeric
    Decimal,
    // Text
    Text,
    Char(Option<u32>),
    VarChar(Option<u32>),
    // Binary
    Bytes,
    // Date / time
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Interval,
    // UUID
    Uuid,
    // JSON
    Json,
    Jsonb,
    // Arrays
    Array(Box<SqlType>),
    // Unknown / extension types (e.g. citext, geometry)
    Custom(String),
}
