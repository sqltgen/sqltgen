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

impl SqlType {
    /// Whether values of this type need JSON string quoting (wrapping in `"…"`)
    /// when building a JSON array literal from application code.
    ///
    /// Numeric and boolean types produce valid JSON via `toString()`;
    /// everything else (text, dates, UUIDs, etc.) must be quoted.
    pub fn needs_json_quoting(&self) -> bool {
        !matches!(self, SqlType::Boolean | SqlType::SmallInt | SqlType::Integer | SqlType::BigInt | SqlType::Real | SqlType::Double | SqlType::Decimal)
    }
}
