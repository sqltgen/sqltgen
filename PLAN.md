# sqltgen — implementation plan

Multi-language SQL-to-code generator.
Inspired by [sqlc](https://sqlc.dev), using a 3-layer compiler architecture.

---

## Architecture

```
SQL files
   │
   ▼
┌──────────────────┐
│  FRONTEND layer  │  DialectParser trait — one impl per SQL dialect
│  src/frontend/   │  Reads DDL + annotated query files → IR
└────────┬─────────┘
         │  IR types (Schema, Table, Column, Query, …)
         ▼
┌──────────────────┐
│    IR layer      │  Language/dialect-agnostic model
│    src/ir/       │  SqlType enum, nullability, arrays, query commands
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│  BACKEND layer   │  Codegen trait — one impl per target language
│  src/backend/    │  Walks IR → emits source files
└──────────────────┘
```

---

## Status legend

- ✅ Done and tested
- ⚠️ Partial / known gaps
- 🚧 Stub exists, not implemented
- ❌ Not started

---

## IR layer (`src/ir/`)

| File | Status | Notes |
|---|---|---|
| `types.rs` — `SqlType` enum | ✅ | Boolean, integers (signed + MySQL unsigned), floats, decimal, text, bytes, date/time, uuid, json/jsonb, array, enum, custom |
| `schema.rs` — `Schema`, `Table`, `Column`, `TableKind`, `EnumType` | ✅ | `TableKind::Table` / `TableKind::View`; `Schema.enums` for `CREATE TYPE AS ENUM`; views and enums registered via two-pass schema parsing |
| `query.rs` — `Query`, `QueryCmd`, `Parameter`, `ResultColumn` | ✅ | |

## Config (`src/config.rs`)

| Item | Status | Notes |
|---|---|---|
| `SqltgenConfig` struct + serde | ✅ | |
| `Engine` enum | ✅ | `postgresql`, `sqlite`, `mysql` |
| `OutputConfig` (`out`, `package`) | ✅ | Keyed by language name in `gen` map |
| Multiple query files (list of paths / globs) | ✅ | List of files and glob patterns; schema still single file/dir |
| Query grouping (map form) | ✅ | `"queries": { "group": "path.sql" }` — one output file per group |

## Frontend layer (`src/frontend/`)

| Item | Status | Notes |
|---|---|---|
| `DialectParser` trait | ✅ | `parse_schema`, `parse_queries` |
| Named params (`@name`, `-- @name [type] [null\|not null]`) | ✅ | `src/frontend/common/named_params.rs`; rewrites to `$N` before parsing; `-- @name type[]` marks list params |
| **PostgreSQL** | ✅ | Full DDL + query parsing |
| `typemap.rs` | ✅ | Includes `JSON`, `JSONB` |
| `schema.rs` | ✅ | CREATE/ALTER/DROP TABLE + CREATE/DROP VIEW + `CREATE TYPE AS ENUM` + scalar UDF + RETURNS TABLE TVF; two-pass with enum/view/UDF resolution; schema-qualified table refs |
| `query.rs` | ✅ | SELECT/INSERT/UPDATE/DELETE + JOINs + subqueries + derived tables + CTEs (incl. recursive) + RETURNING + UNION/INTERSECT/EXCEPT + window functions + `INSERT ... SELECT` + `UPDATE ... FROM` + `ON CONFLICT` / `ON DUPLICATE KEY UPDATE` |
| **SQLite** | ✅ | Full DialectParser; schema + query; `?N` and `$N` params; schema-qualified |
| `typemap.rs` | ✅ | `JSON` recognized via `map_custom` → `SqlType::Json` |
| **MySQL** | ✅ | Full DialectParser; schema + query; `$N` params; `TINYINT(1)`→Boolean; `UNSIGNED` integer modifiers |
| `typemap.rs` | ✅ | Includes `JSON`; no `JSONB` (MySQL doesn't have it); UNSIGNED variants |

## Backend layer (`src/backend/`)

| Language | Status | Notes |
|---|---|---|
| `java/` | ✅ | Record classes + `Queries` class with JDBC methods + `Querier` DataSource wrapper; compile-time adapter contract drives language-constant selection |
| `kotlin/` | ✅ | Data classes + `Queries` object with JDBC methods + `Querier` DataSource wrapper; compile-time adapter contract drives language-constant selection |
| `rust/` | ✅ | `sqlx` async functions + `#[derive(FromRow)]` structs + `Querier` pool wrapper; compile-time adapter contract emits `_sqltgen.rs` helper + engine-agnostic core query modules |
| `python.rs` | ✅ | `@dataclass` models + `Querier`; engine differences resolved via compile-time adapter contract + generated helper module |
| `go.rs` | ✅ | `database/sql` structs + query functions + `Querier` wrapper; two-layer adapter/core architecture; `pq.Array` + dynamic expansion for list params |
| `typescript.rs` | ✅ | TypeScript (interfaces) + JavaScript (JSDoc) output; pg / better-sqlite3 / mysql2 drivers; emits `_sqltgen` runtime helper + `Querier` wrapper |

## CLI (`src/main.rs`)

| Item | Status | Notes |
|---|---|---|
| `sqltgen generate --config sqltgen.json` | ✅ | clap derive; reads config, runs frontend + backend, writes files |

---

## JSON support

sqltgen aims for excellent JSON support across all backends. Current state and gaps:

| Area | Status | Notes |
|---|---|---|
| `JSON` / `JSONB` in IR | ✅ | Two distinct `SqlType` variants |
| PostgreSQL `JSON` / `JSONB` parsing | ✅ | Both recognized |
| MySQL `JSON` parsing | ✅ | No `JSONB` (MySQL doesn't have it) |
| SQLite `JSON` parsing | ✅ | `JSON` keyword in `map_custom` → `SqlType::Json` |
| Rust: `serde_json::Value` | ✅ | Correct and idiomatic for both JSON and JSONB |
| Python psycopg3: JSON result type | ✅ | `object` (psycopg3 automatically deserializes to Python objects) |
| Python sqlite3 / mysql: JSON result type | ✅ | `str` (drivers return raw JSON text) |
| Java/Kotlin: JSON result type | ⚠️ | Mapped to `String` — correct at JDBC level but no Jackson/Gson integration |
| `json[]` / `jsonb[]` arrays (PostgreSQL) | ❌ | Untested / likely unhandled |
| Type overrides (e.g. `json → JsonNode`) | ✅ | Full type_overrides config with jackson/gson/serde_json/object presets |

---

## Remaining work

For the live, sequenced list see `tasks/PRIORITIES.md`. High-level summary below.

### Release blockers

_None — all blockers resolved._

### Should-ship before v0.1.0

1. **Tier 2 distribution** — AUR, Scoop, .deb, .rpm. (task 007)
2. **`REPLACE INTO` / `INSERT OR REPLACE`** — likely fails to parse today. (task 063)
3. **SQLite STRICT tables** — modern SQLite schemas may fail to parse. (task 064)
4. **MySQL TEXT variants** (TINYTEXT/MEDIUMTEXT/LONGTEXT) — may fall through to `Custom`. (task 061)

### Medium priority (post-launch)

1. **Transaction support** — `with_tx(tx)` on Querier. (task 037)
2. **Params struct** — `{Query}Params` + `QueriesParams` for queries with many params. (task 036)
3. **`:execresult` / `:execlastid`** — return driver result / last insert ID. (task 039)
4. **Driver-agnostic scan wrappers** — sqltgen owns the API; drivers are transport. (task 121, umbrella for 115/120)
5. **Multi-target config** — generate multiple language outputs from one config. (task 054)
6. **Querier interface / mock** — testability without a real database. (tasks 041, 083)
7. **Glob patterns for `schema`** — currently queries-only.

### Low priority / future

1. **Field renaming config** — `rename: { db_col: "FieldName" }` map. (task 079)
2. **JSON tags / serialization annotations** — Jackson/serde/dataclasses-json. (task 075)
3. **`sqltgen init` / `verify` / `lint` / `watch` / `explain` / `diff`** subcommands.
4. **PostgreSQL domain types, composite types, sequences, materialized views.**
5. **Additional backends** (C# / .NET, PHP, Swift, Ruby) — see task 105 for review framework.
6. **Additional dialects** (SQL Server / T-SQL, DuckDB, Oracle).

---

## Features from sqlc

Identified from the [sqlc documentation](https://docs.sqlc.dev).

### Query annotation commands

| Command | Meaning | Status |
|---|---|---|
| `:one` / `:many` / `:exec` / `:execrows` | Core commands | ✅ |
| `:execresult` | Returns driver result object (affected rows + last insert ID) | ❌ |
| `:execlastid` | Returns only the last inserted ID | ❌ |
| `:batchexec` / `:batchmany` / `:batchone` | Batch ops — pgx/v5 only | ❌ |
| `:copyfrom` | Bulk insert via `COPY FROM` | ❌ |

### sqlc macro functions

| Macro | Status | Notes |
|---|---|---|
| `sqlc.arg(name)` / `@name` — named params | ✅ | Implemented as `@name` with `-- @name [type] [null\|not null]` annotations |
| `sqlc.narg(name)` — nullable named param | ✅ | Use `-- @name null` annotation |
| `sqlc.embed(table)` — struct embedding | ❌ | Groups result columns into a nested type |
| `sqlc.slice(name)` — dynamic IN clause | ❌ | Runtime query rewriting required |

---

## Open-source launch

- ~~Phase 1: License, CHANGELOG, CONTRIBUTING, README~~ ✅
- ~~Phase 2: CI/CD via cargo-dist — `ci.yml` ✅, `docs.yml` ✅, `docker.yml` ✅, `release.yml` ✅ + `cargo dist init` ✅~~ ✅ (first release `v0.1.0-rc.2`)
- ~~Phase 3: mdBook documentation at docs.sqltgen.org; sqltgen.org redirects there~~ ✅
- Phase 4: Distribution — Homebrew (via cargo-dist) ❌, crates.io ❌, AUR ❌, Scoop ❌, .deb ❌, .rpm ❌
- Phase 5 (future): Full landing page + WASM playground at sqltgen.org
