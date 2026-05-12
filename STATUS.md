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
| Type: enum / set | ✅ (`CREATE TYPE AS ENUM`) | — | ✅ (→ `Text`) |
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
| `CAST(x AS type)` result type | ✅ | ✅ | ✅ |
| `HAVING` parameters | ✅ | ✅ | ✅ |
| Schema-qualified table refs (`schema.table`) | ✅ | ✅ | ✅ |
| `CREATE VIEW` (column type inference) | ✅ | ✅ | ✅ |
| `DROP VIEW [IF EXISTS]` | ✅ | ✅ | ✅ |
| `CREATE TYPE … AS ENUM` | ✅ | — | — |
| `CREATE FUNCTION` (scalar UDF) | ✅ | — | ✅ |
| `CREATE FUNCTION … RETURNS TABLE` (TVF) | ✅ | — | ✅ |
| MySQL `UNSIGNED` integer modifiers | — | — | ✅ |

---

## Backend — row model

| Feature | Java | Kotlin | Rust | Go | Python | TypeScript | JavaScript |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Row model generated | ✅ record | ✅ data class | ✅ `#[derive(FromRow)]` struct | ✅ struct | ✅ `@dataclass` | ✅ `interface` | ✅ `@typedef` |
| One file per table | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Nullable fields | ✅ | ✅ | ✅ `Option<T>` | ✅ `*T` / `sql.NullX` | ✅ `T \| None` | ✅ `T \| null` | ✅ `T \| null` |
| Array fields | ✅ `List<T>` | ✅ `List<T>` | ✅ `Vec<T>` | ✅ `pq.Array` / `[]T` | ✅ `list[T]` | ✅ `T[]` | ✅ `T[]` |
| Package / namespace / module | ✅ `.models` + `.queries` subpackages | ✅ `.models` + `.queries` subpackages | ✅ `mod.rs` at root; `models/mod.rs` + `queries/mod.rs` | ✅ `models/models.go` + `queries/queries_{group}.go` | ✅ `models/__init__.py` + `queries/__init__.py` | ✅ `models/index.ts` + `queries/index.ts` barrel | ✅ `models/index.js` + `queries/index.js` barrel |
| Output subdirectory layout (models/ + queries/) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Helper file (`sqltgen.*` at output root, no `_` prefix) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Enum types (idiomatic per-language) | ✅ enum class | ✅ enum class | ✅ enum + `Display`/`FromStr`/`sqlx::Type` | ✅ string type + consts | ✅ `str + enum.Enum` | ✅ string union | ✅ `Object.freeze` |
| JSON serialization tags / annotations | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Struct embedding (nested row types) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |

---

## Backend — query commands

| Feature | Java | Kotlin | Rust | Go | Python | TypeScript | JavaScript |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| `:one` | ✅ `Optional<T>` | ✅ `T?` | ✅ `Option<T>` | ✅ `(*T, error)` | ✅ `T \| None` | ✅ `Promise<T \| null>` | ✅ `Promise<T \| null>` |
| `:many` | ✅ `List<T>` | ✅ `List<T>` | ✅ `Vec<T>` | ✅ `([]T, error)` | ✅ `list[T]` | ✅ `Promise<T[]>` | ✅ `Promise<T[]>` |
| `:exec` | ✅ `void` | ✅ `Unit` | ✅ `()` | ✅ `error` | ✅ `None` | ✅ `Promise<void>` | ✅ `Promise<void>` |
| `:execrows` | ✅ `long` | ✅ `Long` | ✅ `u64` | ✅ `(int64, error)` | ✅ `int` | ✅ `Promise<number>` | ✅ `Promise<number>` |
| List params (`IN (@ids)`) native + dynamic | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `:execresult` (driver result object) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `:execlastid` (last insert ID) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `:batchexec` / `:batchmany` / `:batchone` | — | — | ❌ | ❌ | — | — | — |
| `:copyfrom` (bulk insert) | ❌ | — | ❌ | ❌ | ❌ | — | — |
| `$N` / `?N` → `?` placeholder rewrite | ✅ | ✅ | ✅ | ✅ | ✅ `→ %s` | ✅ MySQL only | ✅ MySQL only |
| Named params struct (`{Query}Params`) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Table row-type inference | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Join / CTE / RETURNING row type | ✅ `{Query}Row` record | ✅ `{Query}Row` data class | ✅ `{Query}Row` struct | ✅ `{Query}Row` struct | ✅ `{Query}Row` dataclass | ✅ `{Query}Row` interface | ✅ `{Query}Row` typedef |
| Nullable params | ✅ `setObject` | ✅ `setObject` | — | ✅ `sql.NullX` | — | ✅ pass `null` | ✅ pass `null` |
| Typed result getters (Date, UUID…) | ✅ `getObject(n, T.class)` | ✅ `getObject(n, T::class.java)` | ✅ | ✅ `rows.Scan` | — positional unpacking | ✅ driver handles | ✅ driver handles |
| Transaction support (`with_tx`) | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Querier object / class wrapper | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

---

## Backend — SQL type mapping

| `SqlType` | Java | Kotlin | Rust | Go | Python | TypeScript | JavaScript |
|---|---|---|---|---|---|---|---|
| `Boolean` | ✅ `boolean`/`Boolean` | ✅ `Boolean` | ✅ `bool` | ✅ `bool` | ✅ `bool` | ✅ `boolean` | ✅ `boolean` |
| `SmallInt` | ✅ `short`/`Short` | ✅ `Short` | ✅ `i16` | ✅ `int16` | ✅ `int` | ✅ `number` | ✅ `number` |
| `Integer` | ✅ `int`/`Integer` | ✅ `Int` | ✅ `i32` | ✅ `int32` | ✅ `int` | ✅ `number` | ✅ `number` |
| `BigInt` | ✅ `long`/`Long` | ✅ `Long` | ✅ `i64` | ✅ `int64` | ✅ `int` | ✅ `number` ⚠️ lossy | ✅ `number` ⚠️ lossy |
| `Real` | ✅ `float`/`Float` | ✅ `Float` | ✅ `f32` | ✅ `float32` | ✅ `float` | ✅ `number` | ✅ `number` |
| `Double` | ✅ `double`/`Double` | ✅ `Double` | ✅ `f64` | ✅ `float64` | ✅ `float` | ✅ `number` | ✅ `number` |
| `Decimal` | ✅ `BigDecimal` | ✅ `BigDecimal` | ✅ `f64` | ✅ `string` | ✅ `decimal.Decimal` | ✅ `number` | ✅ `number` |
| `Text`/`Char`/`VarChar` | ✅ `String` | ✅ `String` | ✅ `String` | ✅ `string` | ✅ `str` | ✅ `string` | ✅ `string` |
| `Bytes` | ✅ `byte[]` | ✅ `ByteArray` | ✅ `Vec<u8>` | ✅ `[]byte` | ✅ `bytes` | ✅ `Buffer` | ✅ `Buffer` |
| `Date` | ✅ `LocalDate` | ✅ `LocalDate` | ✅ `time::Date` | ✅ `time.Time` | ✅ `datetime.date` | ✅ `Date` | ✅ `Date` |
| `Time` | ✅ `LocalTime` | ✅ `LocalTime` | ✅ `time::Time` | ✅ `time.Time` | ✅ `datetime.time` | ✅ `Date` | ✅ `Date` |
| `Timestamp` | ✅ `LocalDateTime` | ✅ `LocalDateTime` | ✅ `time::PrimitiveDateTime` | ✅ `time.Time` | ✅ `datetime.datetime` | ✅ `Date` | ✅ `Date` |
| `TimestampTz` | ✅ `OffsetDateTime` | ✅ `OffsetDateTime` | ✅ `time::OffsetDateTime` | ✅ `time.Time` | ✅ `datetime.datetime` | ✅ `Date` | ✅ `Date` |
| `Interval` | ✅ `String` | ✅ `String` | ✅ `String` | ✅ `string` | ✅ `datetime.timedelta` | ✅ `string` | ✅ `string` |
| `Uuid` | ✅ `UUID` | ✅ `UUID` | ✅ `uuid::Uuid` | ✅ `string` | ✅ `uuid.UUID` | ✅ `string` | ✅ `string` |
| `Json`/`Jsonb` | ✅ `String` | ✅ `String` | ✅ `serde_json::Value` | ✅ `json.RawMessage` | ✅ `Any` | ✅ `unknown` | ✅ `unknown` |
| `Array(T)` | ✅ `List<T>` | ✅ `List<T>` | ✅ `Vec<T>` | ✅ `pq.Array` / `[]T` | ✅ `list[T]` | ✅ `T[]` | ✅ `T[]` |
| `Custom` | ✅ `Object` | ✅ `Any` | ✅ `String` | ✅ `interface{}` | ✅ `Any` | ✅ `unknown` | ✅ `unknown` |
| `Enum(name)` | ✅ enum class | ✅ enum class | ✅ enum + `sqlx::Type` | ✅ string type | ✅ `str + enum.Enum` | ✅ string union | ✅ `Object.freeze` |
| `TinyIntUnsigned` / `SmallIntUnsigned` / `IntegerUnsigned` (MySQL) | ✅ widened to `Short`/`Int`/`Long` | ✅ widened to `Short`/`Int`/`Long` | ✅ `u8`/`u16`/`u32` | ✅ `uint8`/`uint16`/`uint32` | ✅ `int` | ✅ `number` | ✅ `number` |
| `BigIntUnsigned` (MySQL) | ✅ `BigInteger` (override → `long`) | ✅ `BigInteger` (override → `Long`) | ✅ `u64` | ✅ `uint64` | ✅ `int` | ✅ `bigint` | ✅ `bigint` |

---

## Config features

| Feature | Status | Notes |
|---|:---:|---|
| `engine` / `schema` / `queries` / `gen` | ✅ | Core config |
| Schema from directory of migration files | ✅ | Loaded in lex order; postgres-like strict collision detection across files |
| `schema_stop_marker` — strip down-migration sections | ✅ | Truncates each file at marker; supports dbmate, goose, golang-migrate |
| Multiple query files (list of paths) | ✅ | Supports list of files and globs |
| Query grouping (map form: group name → paths) | ✅ | Each group → one output file per language |
| Glob patterns for `schema` / `queries` | ⚠️ | Queries only (schema still single file/dir) |
| Type overrides (map DB type / column → custom type) | ✅ | Per-language override map in config |
| Field / struct renaming | ❌ | `rename: { col: "FieldName" }` map in config |
| Emit JSON tags / annotations on generated types | ❌ | |
| Emit prepared query variants | ❌ | |
| Emit querier interface | ❌ | |
| Configurable strictness (warn vs. error) | ❌ | Per-project error level |
| `params_struct_threshold` + `:params` annotation | ❌ | Emit `{Query}Params` struct + `QueriesParams` wrapper when above threshold |
| `emit_exact_table_names` (skip singularization) | ❌ | |
| `sqltgen init` subcommand | ❌ | Scaffold a starter config |

---

## Backend — runtime / library

> **TypeScript and JavaScript** share one backend implementation (`typescript/`).
> They are separate `gen` keys in the config (`"typescript"` / `"javascript"`), each
> routing to `TypeScriptCodegen` with a `JsOutput::TypeScript` / `JsOutput::JavaScript`
> flag. JS output uses JSDoc annotations instead of inline TypeScript types.

| | Java | Kotlin | Rust | Go | Python | TypeScript | JavaScript |
|---|---|---|---|---|---|---|---|
| Current target | JDBC | JDBC | sqlx | database/sql | psycopg3 / sqlite3 / mysql-connector | pg / better-sqlite3 / mysql2 | pg / better-sqlite3 / mysql2 |
| Two-layer adapter/core architecture | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Planned target | JDBC | JDBC | sqlx | database/sql | psycopg3 / sqlite3 / mysql-connector | pg / better-sqlite3 / mysql2 | pg / better-sqlite3 / mysql2 |

---

## Examples

| | Java | Kotlin | Rust | Go | Python | TypeScript | JavaScript |
|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| Example project | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| PostgreSQL (real DB) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| SQLite (in-memory) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| MySQL (real DB) | ✅ | ✅ | ✅ | ❌ | ✅ | ✅ | ✅ |
| Makefile (`make run`) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |

---

## Test suite

| Layer | Tests |
|---|---|
| Library unit tests (`cargo test --lib`) | 944 |
| Integration tests (`cargo test --tests`, includes lib) | 1000 |
| **Rust total** | **1000+** |
| E2E runtime fixtures (filesystem-driven, snapshot-gated) | 9 fixtures × 7 langs × {sqlite, postgresql, mysql} where applicable |

Run breakdown (per test crate):
- Codegen snapshot tests: `tests/codegen/`
- Runtime fixtures: `tests/e2e/fixtures/<fixture>/<engine>/<lang>/` — `bookstore`, `bookstore-returning`, `enums`, `unsigned_integers`, `views`, `schema_qualified`, `array_overrides`, `type_overrides`, `provenance`, `resilience`. Snapshot-as-gate logic in `snapshot-gate.mk`.

---

## Open-source launch

See `PLAN.md` → Roadmap section for full distribution plan.

**Done:**
- License (Apache 2.0)
- README, CHANGELOG, CONTRIBUTING
- mdBook documentation under `docs/src/` + `docs.yml` deploy action
- `ci.yml` — build/test/clippy/fmt + `quality-gate` snapshot ratchet on PRs
- `docker.yml` — Docker image build
- Code-quality ratchet: structural metrics (function/file size, complexity, args)
  snapshot-gated via `quality-report.json`; `cargo xtask quality {generate,check,
  ratchet}`; rules ensure no entity worsens and no per-category total grows
- All Tier 1 correctness bugs cleared (CASE WHEN, COALESCE/NULLIF, aggregate, INSERT...SELECT, UPDATE...FROM, schema-qualified tables, MySQL TINYINT(1) boolean, model/query file collision)
- Enum support (`CREATE TYPE AS ENUM` + arrays) across all 7 backends with e2e runtime tests
- MySQL UNSIGNED integers with type widening across all 7 backends with e2e runtime tests
- Upsert (ON CONFLICT / ON DUPLICATE KEY UPDATE)
- Window function type inference, recursive CTE param collection
- Snapshot-as-gate runtime test infrastructure
- `release.yml` + `cargo dist init` — cross-compiled binaries (5 targets) + shell/PowerShell installers attached to GitHub Releases; first release tagged `v0.1.0-rc.2` (task 005)

**Pending:**
- Tier 2 distribution channels (AUR, Scoop, .deb, .rpm) (task 007)
