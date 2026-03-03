# sqlt — feature status

Legend: ✅ done · ⚠️ bug/incomplete · 🚧 stub · ❌ not started

---

## Frontend — SQL parsing

| Feature | PostgreSQL | SQLite |
|---|:---:|:---:|
| `CREATE TABLE` | ✅ | ❌ |
| `IF NOT EXISTS` | ✅ | ❌ |
| `NOT NULL` | ✅ | ❌ |
| `PRIMARY KEY` (inline) | ✅ | ❌ |
| `PRIMARY KEY` (table-level) | ✅ | ❌ |
| `UNIQUE` (inline + table-level) | ✅ | ❌ |
| `FOREIGN KEY` | ✅ (parsed, ignored) | ❌ |
| `DEFAULT` | ✅ (parsed, ignored) | ❌ |
| `GENERATED … AS IDENTITY` | ✅ (parsed, ignored) | ❌ |
| Multiple tables per file | ✅ | ❌ |
| Type: boolean | ✅ | ❌ |
| Type: smallint / int / bigint (+ serials) | ✅ | ❌ |
| Type: real / double | ✅ | ❌ |
| Type: decimal / numeric | ✅ | ❌ |
| Type: text / varchar / char | ✅ | ❌ |
| Type: bytea | ✅ | ❌ |
| Type: date / time / timestamp / timestamptz | ✅ | ❌ |
| Type: interval | ✅ | ❌ |
| Type: uuid | ✅ | ❌ |
| Type: json / jsonb | ✅ | ❌ |
| Type: arrays (`type[]`) | ✅ | ❌ |
| Type: unknown → `Custom` | ✅ | ❌ |
| Query: `-- name: X :cmd` annotation | ✅ | ❌ |
| Query: `:one` / `:many` / `:exec` / `:execrows` | ✅ | ❌ |
| Query: `$N` parameter inference | ✅ | ❌ |
| Query: result column inference | ✅ | ❌ |
| `ALTER TABLE ADD COLUMN [IF NOT EXISTS]` | ✅ | ❌ |
| `ALTER TABLE DROP COLUMN [IF EXISTS]` | ✅ | ❌ |
| `ALTER TABLE ALTER COLUMN … SET/DROP NOT NULL` | ✅ | ❌ |
| `ALTER TABLE ALTER COLUMN … TYPE / SET DATA TYPE` | ✅ | ❌ |
| `ALTER TABLE RENAME COLUMN … TO …` | ✅ | ❌ |
| `ALTER TABLE RENAME TO …` | ✅ | ❌ |
| `ALTER TABLE ADD [CONSTRAINT …] PRIMARY KEY` | ✅ | ❌ |
| Other `ALTER TABLE` actions | ✅ (silently ignored) | ❌ |
| JOIN queries (type inference) | ✅ qualified (`t.col`), unqualified, aliases, `SELECT *` | ❌ |
| Multiple query files | ❌ | ❌ |

---

## Backend — row model

| Feature | Java | Kotlin | Rust | Go | Python | TypeScript |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| Row model generated | ✅ record | ✅ data class | 🚧 | 🚧 | 🚧 | 🚧 |
| One file per table | ✅ | ✅ | 🚧 | 🚧 | 🚧 | 🚧 |
| Nullable fields | ✅ | ✅ | 🚧 | 🚧 | 🚧 | 🚧 |
| Array fields | ✅ `List<T>` | ✅ `List<T>` | 🚧 | 🚧 | 🚧 | 🚧 |
| Package / namespace / module | ✅ | ✅ | 🚧 | 🚧 | 🚧 | 🚧 |

---

## Backend — query commands

| Feature | Java | Kotlin | Rust | Go | Python | TypeScript |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| `:one` | ✅ `Optional<T>` | ✅ `T?` | 🚧 | 🚧 | 🚧 | 🚧 |
| `:many` | ✅ `List<T>` | ✅ `List<T>` | 🚧 | 🚧 | 🚧 | 🚧 |
| `:exec` | ✅ `void` | ✅ `Unit` | 🚧 | 🚧 | 🚧 | 🚧 |
| `:execrows` | ✅ `long` | ✅ `Long` | 🚧 | 🚧 | 🚧 | 🚧 |
| `$N` → `?` placeholder rewrite | ✅ | ⚠️ missing | 🚧 | 🚧 | 🚧 | 🚧 |
| Table row-type inference | ✅ | ✅ | 🚧 | 🚧 | 🚧 | 🚧 |
| Join row type (`{Query}Row` record) | ✅ | ✅ | 🚧 | 🚧 | 🚧 | 🚧 |

---

## Backend — SQL type mapping

| `SqlType` | Java | Kotlin | Rust | Go | Python | TypeScript |
|---|---|---|---|---|---|---|
| `Boolean` | ✅ `boolean`/`Boolean` | ✅ `Boolean` | 🚧 | 🚧 | 🚧 | 🚧 |
| `SmallInt` | ✅ `short`/`Short` | ✅ `Short` | 🚧 | 🚧 | 🚧 | 🚧 |
| `Integer` | ✅ `int`/`Integer` | ✅ `Int` | 🚧 | 🚧 | 🚧 | 🚧 |
| `BigInt` | ✅ `long`/`Long` | ✅ `Long` | 🚧 | 🚧 | 🚧 | 🚧 |
| `Real` | ✅ `float`/`Float` | ✅ `Float` | 🚧 | 🚧 | 🚧 | 🚧 |
| `Double` | ✅ `double`/`Double` | ✅ `Double` | 🚧 | 🚧 | 🚧 | 🚧 |
| `Decimal` | ✅ `BigDecimal` | ✅ `BigDecimal` | 🚧 | 🚧 | 🚧 | 🚧 |
| `Text`/`Char`/`VarChar` | ✅ `String` | ✅ `String` | 🚧 | 🚧 | 🚧 | 🚧 |
| `Bytes` | ✅ `byte[]` | ✅ `ByteArray` | 🚧 | 🚧 | 🚧 | 🚧 |
| `Date` | ✅ `LocalDate` | ✅ `LocalDate` | 🚧 | 🚧 | 🚧 | 🚧 |
| `Time` | ✅ `LocalTime` | ✅ `LocalTime` | 🚧 | 🚧 | 🚧 | 🚧 |
| `Timestamp` | ✅ `LocalDateTime` | ✅ `LocalDateTime` | 🚧 | 🚧 | 🚧 | 🚧 |
| `TimestampTz` | ✅ `OffsetDateTime` | ✅ `OffsetDateTime` | 🚧 | 🚧 | 🚧 | 🚧 |
| `Interval` | ✅ `String` | ✅ `String` | 🚧 | 🚧 | 🚧 | 🚧 |
| `Uuid` | ✅ `UUID` | ✅ `UUID` | 🚧 | 🚧 | 🚧 | 🚧 |
| `Json`/`Jsonb` | ✅ `String` | ✅ `String` | 🚧 | 🚧 | 🚧 | 🚧 |
| `Array(T)` | ✅ `List<T>` | ✅ `List<T>` | 🚧 | 🚧 | 🚧 | 🚧 |
| `Custom` | ✅ `Object` | ✅ `Any` | 🚧 | 🚧 | 🚧 | 🚧 |

---

## Backend — runtime / library

| | Java | Kotlin | Rust | Go | Python | TypeScript |
|---|---|---|---|---|---|---|
| Current target | JDBC | JDBC | — | — | — | — |
| Planned target | JDBC | JDBC | sqlx | database/sql | psycopg3 | postgres.js |

---

## Examples

| | Java | Kotlin | Rust | Go | Python | TypeScript |
|---|:---:|:---:|:---:|:---:|:---:|:---:|
| Example project | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
