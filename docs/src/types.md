# Type mapping

Each language guide below shows how SQL types map to host-language types, how
nullability is expressed, and which dependencies are required.

## Nullability

Nullability is determined from the schema and the query structure:

- A column with `NOT NULL` is non-null.
- A column without `NOT NULL` is nullable.
- `PRIMARY KEY` columns are implicitly non-null.
- Columns from the outer side of a `LEFT`/`RIGHT`/`FULL JOIN` are made nullable.
- Scalar subqueries in the SELECT list produce nullable results.

The sections below show the nullable form alongside the non-null form for each
type.

---

## Java

| SQL type | Non-null | Nullable |
|---|---|---|
| `BOOLEAN` | `boolean` | `Boolean` |
| `SMALLINT` | `short` | `Short` |
| `INTEGER` / `INT` | `int` | `Integer` |
| `BIGINT` / `BIGSERIAL` | `long` | `Long` |
| `REAL` / `FLOAT4` | `float` | `Float` |
| `DOUBLE PRECISION` / `FLOAT8` | `double` | `Double` |
| `NUMERIC` / `DECIMAL` | `BigDecimal` | `BigDecimal` |
| `TEXT` / `VARCHAR` / `CHAR` | `String` | `String` |
| `BYTEA` | `byte[]` | `byte[]` |
| `DATE` | `LocalDate` | `LocalDate` |
| `TIME` | `LocalTime` | `LocalTime` |
| `TIMESTAMP` | `LocalDateTime` | `LocalDateTime` |
| `TIMESTAMPTZ` | `OffsetDateTime` | `OffsetDateTime` |
| `INTERVAL` | `String` | `String` |
| `UUID` | `UUID` | `UUID` |
| `JSON` / `JSONB` | `String` | `String` |
| `type[]` | `List<T>` | `List<T>` |
| Unknown | `Object` | `Object` |

Reference types (`String`, `BigDecimal`, etc.) are nullable by passing `null`.
Primitive types (`boolean`, `int`, `long`, etc.) are boxed when the column is
nullable.

The generated code uses only `java.sql` and standard JDK classes. No extra
imports are needed for basic types.

---

## Kotlin

| SQL type | Non-null | Nullable |
|---|---|---|
| `BOOLEAN` | `Boolean` | `Boolean?` |
| `SMALLINT` | `Short` | `Short?` |
| `INTEGER` / `INT` | `Int` | `Int?` |
| `BIGINT` / `BIGSERIAL` | `Long` | `Long?` |
| `REAL` / `FLOAT4` | `Float` | `Float?` |
| `DOUBLE PRECISION` / `FLOAT8` | `Double` | `Double?` |
| `NUMERIC` / `DECIMAL` | `BigDecimal` | `BigDecimal?` |
| `TEXT` / `VARCHAR` / `CHAR` | `String` | `String?` |
| `BYTEA` | `ByteArray` | `ByteArray?` |
| `DATE` | `LocalDate` | `LocalDate?` |
| `TIME` | `LocalTime` | `LocalTime?` |
| `TIMESTAMP` | `LocalDateTime` | `LocalDateTime?` |
| `TIMESTAMPTZ` | `OffsetDateTime` | `OffsetDateTime?` |
| `INTERVAL` | `String` | `String?` |
| `UUID` | `UUID` | `UUID?` |
| `JSON` / `JSONB` | `String` | `String?` |
| `type[]` | `List<T>` | `List<T>?` |
| Unknown | `Any` | `Any?` |

---

## Rust

| SQL type | Non-null | Nullable |
|---|---|---|
| `BOOLEAN` | `bool` | `Option<bool>` |
| `SMALLINT` | `i16` | `Option<i16>` |
| `INTEGER` / `INT` | `i32` | `Option<i32>` |
| `BIGINT` / `BIGSERIAL` | `i64` | `Option<i64>` |
| `REAL` / `FLOAT4` | `f32` | `Option<f32>` |
| `DOUBLE PRECISION` / `FLOAT8` | `f64` | `Option<f64>` |
| `NUMERIC` / `DECIMAL` | `rust_decimal::Decimal` | `Option<rust_decimal::Decimal>` |
| `TEXT` / `VARCHAR` / `CHAR` | `String` | `Option<String>` |
| `BYTEA` / `BLOB` | `Vec<u8>` | `Option<Vec<u8>>` |
| `DATE` | `time::Date` | `Option<time::Date>` |
| `TIME` | `time::Time` | `Option<time::Time>` |
| `TIMESTAMP` | `time::PrimitiveDateTime` | `Option<time::PrimitiveDateTime>` |
| `TIMESTAMPTZ` | `time::OffsetDateTime` | `Option<time::OffsetDateTime>` |
| `INTERVAL` | `String` | `Option<String>` |
| `UUID` | `uuid::Uuid` | `Option<uuid::Uuid>` |
| `JSON` / `JSONB` | `serde_json::Value` | `Option<serde_json::Value>` |
| `type[]` | `Vec<T>` | `Option<Vec<T>>` |
| Unknown | `serde_json::Value` | `Option<serde_json::Value>` |

Enable sqlx features as needed:

```toml
sqlx = { version = "0.8", features = [
    "runtime-tokio",
    "postgres",      # or "sqlite" / "mysql"
    "time",          # Date, Time, Timestamp, TimestampTz
    "uuid",          # UUID
    "rust_decimal",  # NUMERIC / DECIMAL
] }
```

---

## Python

| SQL type | Non-null | Nullable |
|---|---|---|
| `BOOLEAN` | `bool` | `bool \| None` |
| `SMALLINT` / `INTEGER` / `BIGINT` | `int` | `int \| None` |
| `REAL` / `DOUBLE PRECISION` | `float` | `float \| None` |
| `NUMERIC` / `DECIMAL` | `decimal.Decimal` | `decimal.Decimal \| None` |
| `TEXT` / `VARCHAR` / `CHAR` | `str` | `str \| None` |
| `BYTEA` / `BLOB` | `bytes` | `bytes \| None` |
| `DATE` | `datetime.date` | `datetime.date \| None` |
| `TIME` | `datetime.time` | `datetime.time \| None` |
| `TIMESTAMP` / `TIMESTAMPTZ` | `datetime.datetime` | `datetime.datetime \| None` |
| `INTERVAL` | `datetime.timedelta` | `datetime.timedelta \| None` |
| `UUID` | `uuid.UUID` | `uuid.UUID \| None` |
| `JSON` (psycopg3) | `object` | `object \| None` |
| `JSON` (sqlite3, mysql-connector) | `str` | `str \| None` |
| `type[]` | `list[T]` | `list[T] \| None` |
| Unknown | `Any` | `Any \| None` |

All date/time, decimal, and uuid types come from the standard library. psycopg3
automatically deserializes JSON columns to Python objects; sqlite3 and
mysql-connector return the raw JSON string.

---

## TypeScript

| SQL type | Non-null | Nullable |
|---|---|---|
| `BOOLEAN` | `boolean` | `boolean \| null` |
| `SMALLINT` / `INTEGER` / `BIGINT` | `number` ⚠️ | `number \| null` |
| `REAL` / `DOUBLE PRECISION` / `NUMERIC` | `number` | `number \| null` |
| `TEXT` / `VARCHAR` / `CHAR` | `string` | `string \| null` |
| `BYTEA` / `BLOB` | `Buffer` | `Buffer \| null` |
| `DATE` / `TIME` / `TIMESTAMP` / `TIMESTAMPTZ` | `Date` | `Date \| null` |
| `INTERVAL` | `string` | `string \| null` |
| `UUID` | `string` | `string \| null` |
| `JSON` / `JSONB` | `unknown` | `unknown \| null` |
| `type[]` | `T[]` | `T[] \| null` |
| Unknown | `unknown` | `unknown \| null` |

> ⚠️ `BIGINT` maps to `number`, which loses precision above 2⁵³. Use `BigInt`
> in application code if your IDs or values may exceed this range.

All types are built-in to TypeScript/Node.js — no extra dependencies needed for
the type annotations themselves.

---

## JavaScript

Type mapping is identical to TypeScript. Types are expressed as JSDoc comments
(`@typedef`, `@param`, `@returns`) rather than inline TypeScript syntax, but the
underlying types are the same.

---

## Go

| SQL type | Non-null | Nullable |
|---|---|---|
| `BOOLEAN` | `bool` | `sql.NullBool` |
| `SMALLINT` / `INTEGER` | `int32` | `sql.NullInt32` |
| `BIGINT` / `BIGSERIAL` | `int64` | `sql.NullInt64` |
| `REAL` / `DOUBLE PRECISION` / `NUMERIC` | `float64` | `sql.NullFloat64` |
| `TEXT` / `VARCHAR` / `CHAR` | `string` | `sql.NullString` |
| `BYTEA` / `BLOB` | `[]byte` | `[]byte` |
| `DATE` / `TIME` / `TIMESTAMP` / `TIMESTAMPTZ` | `time.Time` | `sql.NullTime` |
| `INTERVAL` | `string` | `sql.NullString` |
| `UUID` | `string` | `sql.NullString` |
| `JSON` / `JSONB` | `json.RawMessage` | `json.RawMessage` |
| `type[]` | `[]T` | `[]T` |
| Unknown | `interface{}` | `interface{}` |

All nullable types use the standard `database/sql` null wrappers. No extra
dependencies are required beyond the driver itself.
