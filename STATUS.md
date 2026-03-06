# sqltgen — feature status

Legend: ✅ done · ⚠️ partial/known issue · 🚧 stub · ❌ not started

---

## Frontend — SQL parsing

| Feature | PostgreSQL | SQLite | MySQL |
|---|:---:|:---:|:---:|
| `CREATE TABLE` | ✅ | ✅ | ✅ |
| `IF NOT EXISTS` | ✅ | ✅ | ✅ |
| `NOT NULL` | ✅ | ✅ | ✅ |
| `PRIMARY KEY` (inline) | ✅ | ✅ | ✅ |
| `PRIMARY KEY` (table-level) | ✅ | ✅ | ✅ |
| `UNIQUE` (inline + table-level) | ✅ | ✅ | ✅ (parsed, ignored) |
| `FOREIGN KEY` | ✅ (parsed, ignored) | ✅ (parsed, ignored) | ✅ (parsed, ignored) |
| `DEFAULT` | ✅ (parsed, ignored) | ✅ (parsed, ignored) | ✅ (parsed, ignored) |
| `AUTO_INCREMENT` | — | — | ✅ (parsed, ignored) |
| `GENERATED … AS IDENTITY` | ✅ (parsed, ignored) | — | — |
| Multiple tables per file | ✅ | ✅ | ✅ |
| Schema from directory of migration files | ✅ | ✅ | ✅ |
| Type: boolean | ✅ | ✅ (INTEGER affinity) | ✅ |
| Type: smallint / int / bigint (+ serials / AUTO_INCREMENT) | ✅ | ✅ (INTEGER affinity) | ✅ (TINYINT/MEDIUMINT too) |
| Type: real / double | ✅ | ✅ (REAL affinity) | ✅ (FLOAT=32-bit Real) |
| Type: decimal / numeric | ✅ | ✅ (DECIMAL → `Decimal`) | ✅ |
| Type: text / varchar / char | ✅ | ✅ (TEXT affinity) | ✅ |
| Type: bytea / blob | ✅ | ✅ (BLOB affinity) | ✅ (TINYBLOB…LONGBLOB) |
| Type: date / time / timestamp / timestamptz | ✅ | ✅ (DATETIME → `Timestamp`) | ✅ (DATETIME+TIMESTAMP → `Timestamp`) |
| Type: interval | ✅ | — | — |
| Type: uuid | ✅ | ✅ (TEXT affinity) | — |
| Type: json / jsonb | ✅ | ✅ (TEXT affinity) | ✅ (JSON only) |
| Type: enum / set | — | — | ✅ (→ `Text`) |
| Type: arrays (`type[]`) | ✅ | — | — |
| Type: unknown → `Custom` | ✅ | ✅ | ✅ |
| Query: `-- name: X :cmd` annotation | ✅ | ✅ | ✅ |
| Query: `:one` / `:many` / `:exec` / `:execrows` | ✅ | ✅ | ✅ |
| Query: `:execresult` (return driver result object) | ❌ | ❌ | ❌ |
| Query: `:execlastid` (return last insert ID) | ❌ | ❌ | ❌ |
| Query: `:batchexec` / `:batchmany` / `:batchone` (batch ops) | ❌ | — | — |
| Query: `:copyfrom` (bulk insert) | ❌ | — | — |
| Query: `$N` parameter inference | ✅ | — | ✅ (via GenericDialect; bare `?` planned) |
| Query: `?N` parameter inference | — | ✅ | — |
| Query: named parameters (`@name` + `-- @name [type] [null\|not null]`) | ✅ | ✅ | ✅ |
| Query: nullable named parameters (`-- @name null`) | ✅ | ✅ | ✅ |
| Query: result struct embedding (inline macro) | ❌ | ❌ | ❌ |
| Query: dynamic IN clause expansion (inline macro) | ❌ | ❌ | ❌ |
| Query: result column inference | ✅ | ✅ | ✅ |
| `RETURNING` on INSERT | ✅ | — | — |
| `RETURNING` on UPDATE | ✅ | — | — |
| `RETURNING` on DELETE | ✅ | — | — |
| `DROP TABLE [IF EXISTS]` | ✅ | ✅ | ✅ |
| `DROP TABLE` (multiple names) | ✅ | — | ✅ |
| `ALTER TABLE ADD COLUMN [IF NOT EXISTS]` | ✅ | ✅ | ✅ |
| `ALTER TABLE DROP COLUMN [IF EXISTS]` | ✅ | — | ✅ |
| `ALTER TABLE ALTER COLUMN … SET/DROP NOT NULL` | ✅ | — | ✅ |
| `ALTER TABLE ALTER COLUMN … TYPE / SET DATA TYPE` | ✅ | — | ✅ |
| `ALTER TABLE RENAME COLUMN … TO …` | ✅ | ✅ | ✅ |
| `ALTER TABLE RENAME TO …` | ✅ | ✅ | ✅ |
| `ALTER TABLE ADD [CONSTRAINT …] PRIMARY KEY` | ✅ | — | ✅ |
| Other `ALTER TABLE` actions | ✅ (silently ignored) | ✅ (silently ignored) | ✅ (silently ignored) |
| JOIN queries (type inference) | ✅ qualified, unqualified, aliases, `SELECT *` | ✅ | ✅ |
| Subqueries in WHERE (`IN (SELECT …)`) | ✅ | ✅ | ✅ |
| Derived tables (`FROM (SELECT …) alias`) | ✅ | ✅ | ✅ |
| Scalar subqueries in SELECT list | ✅ | ✅ | ✅ |
| CTE (`WITH` … `SELECT`) | ✅ chained, joined with schema tables | ✅ | ✅ |
| Multiple query files | ✅ | ✅ | ✅ |
| Glob patterns for `schema` / `queries` paths | ⚠️ queries only | ⚠️ queries only | ⚠️ queries only |
| `UNION` / `INTERSECT` result columns | ❌ | ❌ | ❌ |
| `CAST(x AS type)` result type | ❌ | ❌ | ❌ |
| `HAVING` parameters | ❌ | ❌ | ❌ |
| Schema-qualified table refs (`schema.table`) | ❌ | ❌ | ❌ |
| `CREATE TYPE … AS ENUM` | ❌ | — | — |

---

## Backend — row model

| Feature | Java | Kotlin | Rust | Go | Python | TypeScript |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| Row model generated | ✅ record | ✅ data class | ✅ `#[derive(FromRow)]` struct | 🚧 | ✅ `@dataclass` | 🚧 |
| One file per table | ✅ | ✅ | ✅ | 🚧 | ✅ | 🚧 |
| Nullable fields | ✅ | ✅ | ✅ `Option<T>` | 🚧 | ✅ `T \| None` | 🚧 |
| Array fields | ✅ `List<T>` | ✅ `List<T>` | ✅ `Vec<T>` | 🚧 | ✅ `list[T]` | 🚧 |
| Package / namespace / module | ✅ | ✅ | ✅ `mod.rs` generated | 🚧 | ✅ `__init__.py` generated | 🚧 |
| Enum types (aliased string / sealed class) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| JSON serialization tags / annotations | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Struct embedding (nested row types) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

---

## Backend — query commands

| Feature | Java | Kotlin | Rust | Go | Python | TypeScript |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| `:one` | ✅ `Optional<T>` | ✅ `T?` | ✅ `Option<T>` | 🚧 | ✅ `T \| None` | 🚧 |
| `:many` | ✅ `List<T>` | ✅ `List<T>` | ✅ `Vec<T>` | 🚧 | ✅ `list[T]` | 🚧 |
| `:exec` | ✅ `void` | ✅ `Unit` | ✅ `()` | 🚧 | ✅ `None` | 🚧 |
| `:execrows` | ✅ `long` | ✅ `Long` | ✅ `u64` | 🚧 | ✅ `int` | 🚧 |
| `:execresult` (driver result object) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `:execlastid` (last insert ID) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `:batchexec` / `:batchmany` / `:batchone` | — | — | ❌ | ❌ | — | — |
| `:copyfrom` (bulk insert) | ❌ | — | ❌ | ❌ | ❌ | — |
| `$N` / `?N` → `?` placeholder rewrite | ✅ | ✅ | ✅ | 🚧 | ✅ `→ %s` | 🚧 |
| Named params struct (`{Query}Params`) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Table row-type inference | ✅ | ✅ | ✅ | 🚧 | ✅ | 🚧 |
| Join / CTE / RETURNING row type | ✅ `{Query}Row` record | ✅ `{Query}Row` data class | ✅ `{Query}Row` struct | 🚧 | ✅ `{Query}Row` dataclass | 🚧 |
| Nullable params use `setObject` | ✅ | ✅ | — | 🚧 | — | 🚧 |
| Typed result getters (Date, UUID…) | ✅ `getObject(n, T.class)` | ✅ `getObject(n, T::class.java)` | ✅ | 🚧 | — positional unpacking | 🚧 |
| Transaction support (`with_tx`) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Querier interface / protocol / ABC | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

---

## Backend — SQL type mapping

| `SqlType` | Java | Kotlin | Rust | Go | Python | TypeScript |
|---|---|---|---|---|---|---|
| `Boolean` | ✅ `boolean`/`Boolean` | ✅ `Boolean` | ✅ `bool` | 🚧 | ✅ `bool` | 🚧 |
| `SmallInt` | ✅ `short`/`Short` | ✅ `Short` | ✅ `i16` | 🚧 | ✅ `int` | 🚧 |
| `Integer` | ✅ `int`/`Integer` | ✅ `Int` | ✅ `i32` | 🚧 | ✅ `int` | 🚧 |
| `BigInt` | ✅ `long`/`Long` | ✅ `Long` | ✅ `i64` | 🚧 | ✅ `int` | 🚧 |
| `Real` | ✅ `float`/`Float` | ✅ `Float` | ✅ `f32` | 🚧 | ✅ `float` | 🚧 |
| `Double` | ✅ `double`/`Double` | ✅ `Double` | ✅ `f64` | 🚧 | ✅ `float` | 🚧 |
| `Decimal` | ✅ `BigDecimal` | ✅ `BigDecimal` | ✅ `f64` | 🚧 | ✅ `decimal.Decimal` | 🚧 |
| `Text`/`Char`/`VarChar` | ✅ `String` | ✅ `String` | ✅ `String` | 🚧 | ✅ `str` | 🚧 |
| `Bytes` | ✅ `byte[]` | ✅ `ByteArray` | ✅ `Vec<u8>` | 🚧 | ✅ `bytes` | 🚧 |
| `Date` | ✅ `LocalDate` | ✅ `LocalDate` | ✅ `time::Date` | 🚧 | ✅ `datetime.date` | 🚧 |
| `Time` | ✅ `LocalTime` | ✅ `LocalTime` | ✅ `time::Time` | 🚧 | ✅ `datetime.time` | 🚧 |
| `Timestamp` | ✅ `LocalDateTime` | ✅ `LocalDateTime` | ✅ `time::PrimitiveDateTime` | 🚧 | ✅ `datetime.datetime` | 🚧 |
| `TimestampTz` | ✅ `OffsetDateTime` | ✅ `OffsetDateTime` | ✅ `time::OffsetDateTime` | 🚧 | ✅ `datetime.datetime` | 🚧 |
| `Interval` | ✅ `String` | ✅ `String` | ✅ `String` | 🚧 | ✅ `datetime.timedelta` | 🚧 |
| `Uuid` | ✅ `UUID` | ✅ `UUID` | ✅ `uuid::Uuid` | 🚧 | ✅ `uuid.UUID` | 🚧 |
| `Json`/`Jsonb` | ✅ `String` | ✅ `String` | ✅ `serde_json::Value` | 🚧 | ✅ `Any` | 🚧 |
| `Array(T)` | ✅ `List<T>` | ✅ `List<T>` | ✅ `Vec<T>` | 🚧 | ✅ `list[T]` | 🚧 |
| `Custom` | ✅ `Object` | ✅ `Any` | ✅ `serde_json::Value` | 🚧 | ✅ `Any` | 🚧 |
| `Enum(name)` (aliased string / sealed class) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

---

## Config features

| Feature | Status | Notes |
|---|:---:|---|
| `engine` / `schema` / `queries` / `gen` | ✅ | Core config |
| Schema from directory of migration files | ✅ | Loaded in lex order |
| Multiple query files (list of paths) | ✅ | Supports list of files and globs |
| Glob patterns for `schema` / `queries` | ⚠️ | Queries only (schema still single file/dir) |
| Type overrides (map DB type / column → custom type) | ❌ | Per-language override map in config |
| Field / struct renaming | ❌ | `rename: { col: "FieldName" }` map in config |
| Emit JSON tags / annotations on generated types | ❌ | |
| Emit prepared query variants | ❌ | |
| Emit querier interface | ❌ | |
| Configurable strictness (warn vs. error) | ❌ | Per-project error level |
| `query_parameter_limit` (params struct threshold) | ❌ | Emit params struct when > N params |
| `emit_exact_table_names` (skip singularization) | ❌ | |
| `sqltgen init` subcommand | ❌ | Scaffold a starter config |

---

## Backend — runtime / library

| | Java | Kotlin | Rust | Go | Python | TypeScript |
|---|---|---|---|---|---|---|
| Current target | JDBC | JDBC | sqlx | — | psycopg3 (psycopg) / sqlite3 (stdlib) | — |
| Planned target | JDBC | JDBC | sqlx | database/sql | psycopg3 | postgres.js |

---

## Examples

| | Java | Kotlin | Rust | Go | Python | TypeScript |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| Example project | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ |
| PostgreSQL (real DB) | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ |
| SQLite (in-memory) | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ |
| MySQL (real DB) | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |
| Makefile (`make run`) | ✅ | ✅ | ✅ | ❌ | ✅ | ❌ |

---

## Test suite

| Area | Tests |
|---|---|
| Config parsing | 1 |
| PostgreSQL typemap | 12 |
| PostgreSQL DDL schema | 22 |
| PostgreSQL query parser (SELECT, INSERT, UPDATE, DELETE) | 28 |
| PostgreSQL RETURNING | 6 |
| SQLite DDL schema | 10 |
| CTE | 4 |
| Derived tables / subqueries | 8 |
| MySQL typemap | 10 |
| MySQL DDL schema | 13 |
| MySQL query parser | 7 |
| Named params (unit) | 10 |
| Named params (integration) | 5 |
| **Total** | **223 (all passing)** |

---

## Open-source launch

See `PLAN.md` → Roadmap section, and `memory/roadmap.md` for full distribution plan.

Pending: license choice, docs (mdBook), CI/CD (cargo-dist), distribution channels.
