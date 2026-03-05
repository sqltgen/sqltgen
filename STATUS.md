# sqltgen — feature status

Legend: ✅ done · ⚠️ partial/known issue · 🚧 stub · ❌ not started

---

## Frontend — SQL parsing

| Feature | PostgreSQL | SQLite |
|---|:---:|:---:|
| `CREATE TABLE` | ✅ | ✅ |
| `IF NOT EXISTS` | ✅ | ✅ |
| `NOT NULL` | ✅ | ✅ |
| `PRIMARY KEY` (inline) | ✅ | ✅ |
| `PRIMARY KEY` (table-level) | ✅ | ✅ |
| `UNIQUE` (inline + table-level) | ✅ | ✅ |
| `FOREIGN KEY` | ✅ (parsed, ignored) | ✅ (parsed, ignored) |
| `DEFAULT` | ✅ (parsed, ignored) | ✅ (parsed, ignored) |
| `GENERATED … AS IDENTITY` | ✅ (parsed, ignored) | — |
| Multiple tables per file | ✅ | ✅ |
| Schema from directory of migration files | ✅ | ✅ |
| Type: boolean | ✅ | ✅ (INTEGER affinity) |
| Type: smallint / int / bigint (+ serials) | ✅ | ✅ (INTEGER affinity) |
| Type: real / double | ✅ | ✅ (REAL affinity) |
| Type: decimal / numeric | ✅ | ✅ (DECIMAL → `Decimal`) |
| Type: text / varchar / char | ✅ | ✅ (TEXT affinity) |
| Type: bytea / blob | ✅ | ✅ (BLOB affinity) |
| Type: date / time / timestamp / timestamptz | ✅ | ✅ (DATETIME → `Timestamp`) |
| Type: interval | ✅ | — |
| Type: uuid | ✅ | ✅ (TEXT affinity) |
| Type: json / jsonb | ✅ | ✅ (TEXT affinity) |
| Type: arrays (`type[]`) | ✅ | — |
| Type: unknown → `Custom` | ✅ | ✅ |
| Query: `-- name: X :cmd` annotation | ✅ | ✅ |
| Query: `:one` / `:many` / `:exec` / `:execrows` | ✅ | ✅ |
| Query: `$N` parameter inference | ✅ | — |
| Query: `?N` parameter inference | — | ✅ |
| Query: result column inference | ✅ | ✅ |
| `RETURNING` on INSERT | ✅ | — |
| `RETURNING` on UPDATE | ✅ | — |
| `RETURNING` on DELETE | ✅ | — |
| `ALTER TABLE ADD COLUMN [IF NOT EXISTS]` | ✅ | ✅ |
| `ALTER TABLE DROP COLUMN [IF EXISTS]` | ✅ | — |
| `ALTER TABLE ALTER COLUMN … SET/DROP NOT NULL` | ✅ | — |
| `ALTER TABLE ALTER COLUMN … TYPE / SET DATA TYPE` | ✅ | — |
| `ALTER TABLE RENAME COLUMN … TO …` | ✅ | ✅ |
| `ALTER TABLE RENAME TO …` | ✅ | ✅ |
| `ALTER TABLE ADD [CONSTRAINT …] PRIMARY KEY` | ✅ | — |
| Other `ALTER TABLE` actions | ✅ (silently ignored) | ✅ (silently ignored) |
| JOIN queries (type inference) | ✅ qualified, unqualified, aliases, `SELECT *` | ✅ |
| Subqueries in WHERE (`IN (SELECT …)`) | ✅ | ✅ |
| Derived tables (`FROM (SELECT …) alias`) | ✅ | ✅ |
| Scalar subqueries in SELECT list | ✅ | ✅ |
| CTE (`WITH` … `SELECT`) | ✅ chained, joined with schema tables | ✅ |
| Multiple query files | ❌ | ❌ |
| `UNION` / `INTERSECT` result columns | ❌ | ❌ |
| `CAST(x AS type)` result type | ❌ | ❌ |
| `HAVING` parameters | ❌ | ❌ |

---

## Backend — row model

| Feature | Java | Kotlin | Rust | Go | Python | TypeScript |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| Row model generated | ✅ record | ✅ data class | ✅ `#[derive(FromRow)]` struct | 🚧 | 🚧 | 🚧 |
| One file per table | ✅ | ✅ | ✅ | 🚧 | 🚧 | 🚧 |
| Nullable fields | ✅ | ✅ | ✅ `Option<T>` | 🚧 | 🚧 | 🚧 |
| Array fields | ✅ `List<T>` | ✅ `List<T>` | ✅ `Vec<T>` | 🚧 | 🚧 | 🚧 |
| Package / namespace / module | ✅ | ✅ | ✅ `mod.rs` generated | 🚧 | 🚧 | 🚧 |

---

## Backend — query commands

| Feature | Java | Kotlin | Rust | Go | Python | TypeScript |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| `:one` | ✅ `Optional<T>` | ✅ `T?` | ✅ `Option<T>` | 🚧 | 🚧 | 🚧 |
| `:many` | ✅ `List<T>` | ✅ `List<T>` | ✅ `Vec<T>` | 🚧 | 🚧 | 🚧 |
| `:exec` | ✅ `void` | ✅ `Unit` | ✅ `()` | 🚧 | 🚧 | 🚧 |
| `:execrows` | ✅ `long` | ✅ `Long` | ✅ `u64` | 🚧 | 🚧 | 🚧 |
| `$N` / `?N` → `?` placeholder rewrite | ✅ | ✅ | ✅ | 🚧 | 🚧 | 🚧 |
| Table row-type inference | ✅ | ✅ | ✅ | 🚧 | 🚧 | 🚧 |
| Join / CTE / RETURNING row type | ✅ `{Query}Row` record | ✅ `{Query}Row` data class | ✅ `{Query}Row` struct | 🚧 | 🚧 | 🚧 |
| Nullable params use `setObject` | ✅ | ✅ | — | 🚧 | 🚧 | 🚧 |
| Typed result getters (Date, UUID…) | ✅ `getObject(n, T.class)` | ✅ `getObject(n, T::class.java)` | ✅ | 🚧 | 🚧 | 🚧 |

---

## Backend — SQL type mapping

| `SqlType` | Java | Kotlin | Rust | Go | Python | TypeScript |
|---|---|---|---|---|---|---|
| `Boolean` | ✅ `boolean`/`Boolean` | ✅ `Boolean` | ✅ `bool` | 🚧 | 🚧 | 🚧 |
| `SmallInt` | ✅ `short`/`Short` | ✅ `Short` | ✅ `i16` | 🚧 | 🚧 | 🚧 |
| `Integer` | ✅ `int`/`Integer` | ✅ `Int` | ✅ `i32` | 🚧 | 🚧 | 🚧 |
| `BigInt` | ✅ `long`/`Long` | ✅ `Long` | ✅ `i64` | 🚧 | 🚧 | 🚧 |
| `Real` | ✅ `float`/`Float` | ✅ `Float` | ✅ `f32` | 🚧 | 🚧 | 🚧 |
| `Double` | ✅ `double`/`Double` | ✅ `Double` | ✅ `f64` | 🚧 | 🚧 | 🚧 |
| `Decimal` | ✅ `BigDecimal` | ✅ `BigDecimal` | ✅ `f64` | 🚧 | 🚧 | 🚧 |
| `Text`/`Char`/`VarChar` | ✅ `String` | ✅ `String` | ✅ `String` | 🚧 | 🚧 | 🚧 |
| `Bytes` | ✅ `byte[]` | ✅ `ByteArray` | ✅ `Vec<u8>` | 🚧 | 🚧 | 🚧 |
| `Date` | ✅ `LocalDate` | ✅ `LocalDate` | ✅ `time::Date` | 🚧 | 🚧 | 🚧 |
| `Time` | ✅ `LocalTime` | ✅ `LocalTime` | ✅ `time::Time` | 🚧 | 🚧 | 🚧 |
| `Timestamp` | ✅ `LocalDateTime` | ✅ `LocalDateTime` | ✅ `time::PrimitiveDateTime` | 🚧 | 🚧 | 🚧 |
| `TimestampTz` | ✅ `OffsetDateTime` | ✅ `OffsetDateTime` | ✅ `time::OffsetDateTime` | 🚧 | 🚧 | 🚧 |
| `Interval` | ✅ `String` | ✅ `String` | ✅ `String` | 🚧 | 🚧 | 🚧 |
| `Uuid` | ✅ `UUID` | ✅ `UUID` | ✅ `uuid::Uuid` | 🚧 | 🚧 | 🚧 |
| `Json`/`Jsonb` | ✅ `String` | ✅ `String` | ✅ `serde_json::Value` | 🚧 | 🚧 | 🚧 |
| `Array(T)` | ✅ `List<T>` | ✅ `List<T>` | ✅ `Vec<T>` | 🚧 | 🚧 | 🚧 |
| `Custom` | ✅ `Object` | ✅ `Any` | ✅ `serde_json::Value` | 🚧 | 🚧 | 🚧 |

---

## Backend — runtime / library

| | Java | Kotlin | Rust | Go | Python | TypeScript |
|---|---|---|---|---|---|---|
| Current target | JDBC | JDBC | sqlx | — | — | — |
| Planned target | JDBC | JDBC | sqlx | database/sql | psycopg3 | postgres.js |

---

## Examples

| | Java | Kotlin | Rust | Go | Python | TypeScript |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| Example project | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| PostgreSQL (real DB) | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| SQLite (in-memory) | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |
| Makefile (`make run`) | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ |

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
| **Total** | **85 (all passing)** |

---

## Open-source launch

See `PLAN.md` → Roadmap section, and `memory/roadmap.md` for full distribution plan.

Pending: license choice, docs (mdBook), CI/CD (cargo-dist), distribution channels.
