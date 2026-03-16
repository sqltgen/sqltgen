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
| Query: list/collection parameters (`IN (@ids)`, `-- @ids type[]`) | ✅ | ✅ | ✅ |
| Query: result struct embedding (inline macro) | ❌ | ❌ | ❌ |
| Query: dynamic IN clause expansion (`sqlc.slice` macro) | ❌ | ❌ | ❌ |
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
| `UNION` / `INTERSECT` / `EXCEPT` result columns | ✅ | ✅ | ✅ |
| `CAST(x AS type)` result type | ❌ | ❌ | ❌ |
| `HAVING` parameters | ✅ | ✅ | ✅ |
| Schema-qualified table refs (`schema.table`) | ❌ | ❌ | ❌ |
| `CREATE TYPE … AS ENUM` | ❌ | — | — |

---

## Backend — row model

| Feature | Java | Kotlin | Rust | Go | Python | TypeScript | JavaScript |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Row model generated | ✅ record | ✅ data class | ✅ `#[derive(FromRow)]` struct | 🚧 | ✅ `@dataclass` | ✅ `interface` | ✅ `@typedef` |
| One file per table | ✅ | ✅ | ✅ | 🚧 | ✅ | ✅ | ✅ |
| Nullable fields | ✅ | ✅ | ✅ `Option<T>` | 🚧 | ✅ `T \| None` | ✅ `T \| null` | ✅ `T \| null` |
| Array fields | ✅ `List<T>` | ✅ `List<T>` | ✅ `Vec<T>` | 🚧 | ✅ `list[T]` | ✅ `T[]` | ✅ `T[]` |
| Package / namespace / module | ✅ | ✅ | ✅ `mod.rs` generated | 🚧 | ✅ `__init__.py` generated | ✅ `index.ts` barrel | ✅ `index.js` barrel |
| Enum types (aliased string / sealed class) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| JSON serialization tags / annotations | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Struct embedding (nested row types) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

---

## Backend — query commands

| Feature | Java | Kotlin | Rust | Go | Python | TypeScript | JavaScript |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| `:one` | ✅ `Optional<T>` | ✅ `T?` | ✅ `Option<T>` | 🚧 | ✅ `T \| None` | ✅ `Promise<T \| null>` | ✅ `Promise<T \| null>` |
| `:many` | ✅ `List<T>` | ✅ `List<T>` | ✅ `Vec<T>` | 🚧 | ✅ `list[T]` | ✅ `Promise<T[]>` | ✅ `Promise<T[]>` |
| `:exec` | ✅ `void` | ✅ `Unit` | ✅ `()` | 🚧 | ✅ `None` | ✅ `Promise<void>` | ✅ `Promise<void>` |
| `:execrows` | ✅ `long` | ✅ `Long` | ✅ `u64` | 🚧 | ✅ `int` | ✅ `Promise<number>` | ✅ `Promise<number>` |
| List params (`IN (@ids)`) native + dynamic | ✅ | ✅ | ✅ | 🚧 | ✅ | ✅ | ✅ |
| `:execresult` (driver result object) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `:execlastid` (last insert ID) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `:batchexec` / `:batchmany` / `:batchone` | — | — | ❌ | ❌ | — | — | — |
| `:copyfrom` (bulk insert) | ❌ | — | ❌ | ❌ | ❌ | — | — |
| `$N` / `?N` → `?` placeholder rewrite | ✅ | ✅ | ✅ | 🚧 | ✅ `→ %s` | ✅ MySQL only | ✅ MySQL only |
| Named params struct (`{Query}Params`) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Table row-type inference | ✅ | ✅ | ✅ | 🚧 | ✅ | ✅ | ✅ |
| Join / CTE / RETURNING row type | ✅ `{Query}Row` record | ✅ `{Query}Row` data class | ✅ `{Query}Row` struct | 🚧 | ✅ `{Query}Row` dataclass | ✅ `{Query}Row` interface | ✅ `{Query}Row` typedef |
| Nullable params | ✅ `setObject` | ✅ `setObject` | — | 🚧 | — | ✅ pass `null` | ✅ pass `null` |
| Typed result getters (Date, UUID…) | ✅ `getObject(n, T.class)` | ✅ `getObject(n, T::class.java)` | ✅ | 🚧 | — positional unpacking | ✅ driver handles | ✅ driver handles |
| Transaction support (`with_tx`) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Querier interface / protocol / ABC | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

---

## Backend — SQL type mapping

| `SqlType` | Java | Kotlin | Rust | Go | Python | TypeScript | JavaScript |
|---|---|---|---|---|---|---|---|
| `Boolean` | ✅ `boolean`/`Boolean` | ✅ `Boolean` | ✅ `bool` | 🚧 | ✅ `bool` | ✅ `boolean` | ✅ `boolean` |
| `SmallInt` | ✅ `short`/`Short` | ✅ `Short` | ✅ `i16` | 🚧 | ✅ `int` | ✅ `number` | ✅ `number` |
| `Integer` | ✅ `int`/`Integer` | ✅ `Int` | ✅ `i32` | 🚧 | ✅ `int` | ✅ `number` | ✅ `number` |
| `BigInt` | ✅ `long`/`Long` | ✅ `Long` | ✅ `i64` | 🚧 | ✅ `int` | ✅ `number` ⚠️ lossy | ✅ `number` ⚠️ lossy |
| `Real` | ✅ `float`/`Float` | ✅ `Float` | ✅ `f32` | 🚧 | ✅ `float` | ✅ `number` | ✅ `number` |
| `Double` | ✅ `double`/`Double` | ✅ `Double` | ✅ `f64` | 🚧 | ✅ `float` | ✅ `number` | ✅ `number` |
| `Decimal` | ✅ `BigDecimal` | ✅ `BigDecimal` | ✅ `f64` | 🚧 | ✅ `decimal.Decimal` | ✅ `number` | ✅ `number` |
| `Text`/`Char`/`VarChar` | ✅ `String` | ✅ `String` | ✅ `String` | 🚧 | ✅ `str` | ✅ `string` | ✅ `string` |
| `Bytes` | ✅ `byte[]` | ✅ `ByteArray` | ✅ `Vec<u8>` | 🚧 | ✅ `bytes` | ✅ `Buffer` | ✅ `Buffer` |
| `Date` | ✅ `LocalDate` | ✅ `LocalDate` | ✅ `time::Date` | 🚧 | ✅ `datetime.date` | ✅ `Date` | ✅ `Date` |
| `Time` | ✅ `LocalTime` | ✅ `LocalTime` | ✅ `time::Time` | 🚧 | ✅ `datetime.time` | ✅ `Date` | ✅ `Date` |
| `Timestamp` | ✅ `LocalDateTime` | ✅ `LocalDateTime` | ✅ `time::PrimitiveDateTime` | 🚧 | ✅ `datetime.datetime` | ✅ `Date` | ✅ `Date` |
| `TimestampTz` | ✅ `OffsetDateTime` | ✅ `OffsetDateTime` | ✅ `time::OffsetDateTime` | 🚧 | ✅ `datetime.datetime` | ✅ `Date` | ✅ `Date` |
| `Interval` | ✅ `String` | ✅ `String` | ✅ `String` | 🚧 | ✅ `datetime.timedelta` | ✅ `string` | ✅ `string` |
| `Uuid` | ✅ `UUID` | ✅ `UUID` | ✅ `uuid::Uuid` | 🚧 | ✅ `uuid.UUID` | ✅ `string` | ✅ `string` |
| `Json`/`Jsonb` | ✅ `String` | ✅ `String` | ✅ `serde_json::Value` | 🚧 | ✅ `Any` | ✅ `unknown` | ✅ `unknown` |
| `Array(T)` | ✅ `List<T>` | ✅ `List<T>` | ✅ `Vec<T>` | 🚧 | ✅ `list[T]` | ✅ `T[]` | ✅ `T[]` |
| `Custom` | ✅ `Object` | ✅ `Any` | ✅ `serde_json::Value` | 🚧 | ✅ `Any` | ✅ `unknown` | ✅ `unknown` |
| `Enum(name)` (aliased string / sealed class) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

---

## Config features

| Feature | Status | Notes |
|---|:---:|---|
| `engine` / `schema` / `queries` / `gen` | ✅ | Core config |
| Schema from directory of migration files | ✅ | Loaded in lex order |
| Multiple query files (list of paths) | ✅ | Supports list of files and globs |
| Query grouping (map form: group name → paths) | ✅ | Each group → one output file per language |
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

> **TypeScript and JavaScript** share one backend implementation (`typescript.rs`).
> They are separate `gen` keys in the config (`"typescript"` / `"javascript"`), each
> routing to `TypeScriptCodegen` with a `JsOutput::TypeScript` / `JsOutput::JavaScript`
> flag. JS output uses JSDoc annotations instead of inline TypeScript types.

| | Java | Kotlin | Rust | Go | Python | TypeScript | JavaScript |
|---|---|---|---|---|---|---|---|
| Current target | JDBC | JDBC | sqlx | — | psycopg3 / sqlite3 / mysql-connector | pg / better-sqlite3 / mysql2 | pg / better-sqlite3 / mysql2 |
| Planned target | JDBC | JDBC | sqlx | database/sql | psycopg3 / sqlite3 / mysql-connector | pg / better-sqlite3 / mysql2 | pg / better-sqlite3 / mysql2 |

---

## Examples

| | Java | Kotlin | Rust | Go | Python | TypeScript | JavaScript |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Example project | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ |
| PostgreSQL (real DB) | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ |
| SQLite (in-memory) | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ |
| MySQL (real DB) | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ |
| Makefile (`make run`) | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ |

---

## Test suite

| Module | Tests |
|---|---|
| Config | 24 |
| Frontend — PostgreSQL (typemap + schema + query) | 75 |
| Frontend — SQLite (typemap + schema + query) | 39 |
| Frontend — MySQL (typemap + schema + query) | 34 |
| Frontend — common (query parser, CTEs, subqueries, named params, list params, source_table) | 204 |
| Backend — Java | 63 |
| Backend — Kotlin | 62 |
| Backend — Rust | 39 |
| Backend — Python | 44 |
| Backend — common (common + sql_rewrite + naming) | 48 |
| Backend — JDBC | 13 |
| Backend — TypeScript / JavaScript | 48 |
| Integration (snapshots + resilience) | 25 |
| **Total** | **718** |

---

## Open-source launch

See `PLAN.md` → Roadmap section, and `memory/roadmap.md` for full distribution plan.

Pending: license choice, docs (mdBook), CI/CD (cargo-dist), distribution channels.
