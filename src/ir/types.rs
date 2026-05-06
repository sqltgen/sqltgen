#[derive(Debug, Clone, PartialEq)]
pub enum SqlType {
    // Boolean
    Boolean,
    // Signed integers
    SmallInt,
    Integer,
    BigInt,
    // Unsigned integers (MySQL `UNSIGNED` modifier).
    // Each variant carries a strictly-larger value range than its signed peer,
    // so backends without native unsigned types must widen accordingly to
    // preserve correctness — e.g. `TINYINT UNSIGNED` (0..=255) cannot fit in
    // Java's `byte` (-128..=127) and must widen to `Short`.
    TinyIntUnsigned,
    SmallIntUnsigned,
    IntegerUnsigned,
    BigIntUnsigned,
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
    // Enum types (PostgreSQL `CREATE TYPE ... AS ENUM`)
    Enum(String),
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
        !matches!(
            self,
            SqlType::Boolean
                | SqlType::SmallInt
                | SqlType::Integer
                | SqlType::BigInt
                | SqlType::TinyIntUnsigned
                | SqlType::SmallIntUnsigned
                | SqlType::IntegerUnsigned
                | SqlType::BigIntUnsigned
                | SqlType::Real
                | SqlType::Double
                | SqlType::Decimal
        )
    }
}
