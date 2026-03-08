# sqltgen — implementation plan

Multi-language SQL-to-code generator written in Rust.
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
| `types.rs` — `SqlType` enum | ✅ | Boolean, integers, floats, decimal, text, bytes, date/time, uuid, json/jsonb, array, custom |
| `schema.rs` — `Schema`, `Table`, `Column` | ✅ | |
| `query.rs` — `Query`, `QueryCmd`, `Parameter`, `ResultColumn` | ✅ | |

## Config (`src/config.rs`)

| Item | Status | Notes |
|---|---|---|
| `SqltgenConfig` struct + serde | ✅ | |
| `Engine` enum | ✅ | `postgresql`, `sqlite`, `mysql` |
| `OutputConfig` (`out`, `package`) | ✅ | Keyed by language name in `gen` map |
| Multiple query files (list of paths / globs) | ✅ | List of files and glob patterns; schema still single file/dir |

## Frontend layer (`src/frontend/`)

| Item | Status | Notes |
|---|---|---|
| `DialectParser` trait | ✅ | `parse_schema`, `parse_queries` |
| Named params (`@name`, `-- @name [type] [null\|not null]`) | ✅ | `src/frontend/common/named_params.rs`; rewrites to `$N` before parsing; `-- @name type[]` marks list params |
| **PostgreSQL** | ✅ | Full DDL + query parsing; 60+ tests |
| `typemap.rs` | ✅ | Includes `JSON`, `JSONB` |
| `schema.rs` | ✅ | CREATE/ALTER/DROP TABLE; 22 tests |
| `query.rs` | ✅ | SELECT/INSERT/UPDATE/DELETE + JOINs + subqueries + derived tables + CTEs + RETURNING |
| **SQLite** | ✅ | Full DialectParser; schema + query; `?N` and `$N` params |
| `typemap.rs` | ✅ | `JSON` recognized via `map_custom` → `SqlType::Json` |
| **MySQL** | ✅ | Full DialectParser; schema + query; `$N` params; 30+ tests |
| `typemap.rs` | ✅ | Includes `JSON`; no `JSONB` (MySQL doesn't have it) |

## Backend layer (`src/backend/`)

| Language | Status | Notes |
|---|---|---|
| `java.rs` | ✅ | Record classes + `Queries` class with JDBC methods |
| `kotlin.rs` | ✅ | Data classes + `Queries` object with JDBC methods |
| `rust.rs` | ✅ | `sqlx` async functions + `#[derive(FromRow)]` structs; `mod.rs` generated |
| `python.rs` | ✅ | `@dataclass` models + psycopg3 / sqlite3 / mysql-connector functions |
| `go.rs` | 🚧 | Stub — `unimplemented!()` |
| `typescript.rs` | 🚧 | Stub — `unimplemented!()` |

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
| Type overrides (e.g. `json → JsonNode`) | ❌ | Blocked on config type-override feature |

### Planned improvements
1. ~~**SQLite**: recognize `JSON` as a type keyword → `SqlType::Json`~~ ✅ Done
2. ~~**Python**: `Any` → `object` for psycopg3 (already deserialized); `str` for sqlite3/mysql~~ ✅ Done
3. **Java/Kotlin**: document the `String` limitation; unlock proper types via config type overrides
4. **Arrays**: test and fix `json[]` / `jsonb[]` in PostgreSQL

---

## Remaining work

### High priority

1. **Go backend** — generate structs + `database/sql` functions

### Medium priority

1. **`UNION` / `INTERSECT` result columns** — resolve from left branch of `SetExpr::SetOperation`
2. **`CAST(x AS type)` result type** — call `typemap::map()` on the cast's `DataType`
3. **`HAVING` params** — same `collect_params_from_expr` walk on `select.having`
4. **TypeScript backend** — generate interfaces + `postgres.js` functions
5. **Type overrides config** — map a DB type or `table.column` to a custom target-language type
6. **Better error messages** — surface parse errors with line numbers
7. **Glob patterns** for `schema` and `queries` config fields

### Low priority / future

1. **Querier interface** — emit an interface/protocol/ABC for the Queries type (testability)
2. **Transaction support** — `with_tx(tx)` constructor on the Queries wrapper
3. **Enum support** — `CREATE TYPE foo AS ENUM` → typed enum / sealed class / string alias
4. **Field renaming config** — `rename: { db_col: "FieldName" }` map in config
5. **JSON tags / serialization annotations** — emit Jackson/serde/dataclasses-json annotations
6. **`query_parameter_limit`** — emit a params struct when a query has more than N parameters
7. **Schema-qualified tables** — handle `schema.table` references in queries
8. **`sqltgen init`** subcommand — scaffold a starter `sqltgen.json`
9. **C / C++ / C# backends**

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

See `RELEASE_ROADMAP.md` (in the parent directory) for the full plan. Summary:

- Phase 1: License (deferred — see roadmap), CHANGELOG, CONTRIBUTING, README
- Phase 2: CI/CD via cargo-dist (ci.yml, release.yml, docs.yml)
- Phase 3: mdBook documentation at docs.sqltgen.org; sqltgen.org redirects there
- Phase 4: Distribution — crates.io, Homebrew, AUR, Scoop, .deb, .rpm
- Phase 5 (future): Full landing page + WASM playground at sqltgen.org
