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
- 🚧 Stub exists, not implemented
- ❌ Not started

---

## IR layer (`src/ir/`)

| File | Status | Notes |
|---|---|---|
| `types.rs` — `SqlType` enum | ✅ | Boolean, integers, floats, decimal, text, bytes, date/time, uuid, json, array, custom |
| `schema.rs` — `Schema`, `Table`, `Column` | ✅ | |
| `query.rs` — `Query`, `QueryCmd`, `Parameter`, `ResultColumn` | ✅ | |

## Config (`src/config.rs`)

| Item | Status | Notes |
|---|---|---|
| `SqltgenConfig` struct + serde | ✅ | Flat per-file (one config per DB, not a list) |
| `Engine` enum | ✅ | `postgresql`, `sqlite` |
| `OutputConfig` (`out`, `package`) | ✅ | Keyed by language name in `gen` map |
| Config file loading from path | ✅ | |

## Frontend layer (`src/frontend/`)

| Item | Status | Notes |
|---|---|---|
| `DialectParser` trait | ✅ | `parse_schema`, `parse_queries` |
| **PostgreSQL** | | |
| `typemap.rs` — `DataType` → `SqlType` | ✅ | Matches sqlparser AST variants directly; 12 unit tests |
| `schema.rs` — DDL parser → `Schema` | ✅ | sqlparser-rs AST; 22 unit tests |
| `query.rs` — annotated query file parser | ✅ | SELECT/INSERT/UPDATE/DELETE + JOINs + subqueries + derived tables + CTEs + RETURNING; 34+ unit tests |
| `query.rs` — CTE (`WITH`) support | ✅ | Chained CTEs, CTEs joined with schema tables |
| `query.rs` — RETURNING on INSERT/UPDATE/DELETE | ✅ | Resolves column types from table schema |
| **SQLite** | ✅ | Full DialectParser impl; schema + query parsing; ?N parameters |

## Backend layer (`src/backend/`)

| Language | Status | Notes |
|---|---|---|
| `java.rs` | ✅ | Record classes + `Queries` class with JDBC methods |
| `kotlin.rs` | ✅ | Data classes + `Queries` object with JDBC methods |
| `rust.rs` | ✅ | `sqlx` async functions + `FromRow` structs; `mod.rs` generated |
| `go.rs` | 🚧 | Stub — `unimplemented!()` |
| `python.rs` | 🚧 | Stub — `unimplemented!()` |
| `typescript.rs` | 🚧 | Stub — `unimplemented!()` |

## CLI (`src/main.rs`)

| Item | Status | Notes |
|---|---|---|
| `sqltgen generate --config sqltgen.json` | ✅ | clap derive; reads config, runs frontend + backend, writes files |

---

## Remaining work

### High priority

1. **Go backend** — generate structs + `database/sql` functions
2. **Multiple query files** — allow `queries` to be a list of paths (currently single file only)

### Medium priority

1. **`UNION` / `INTERSECT` result columns** — resolve from left branch of `SetExpr::SetOperation`
2. **`CAST(x AS type)` result type** — call `typemap::map()` on the cast's `DataType`
3. **`HAVING` params** — same `collect_params_from_expr` walk on `select.having`
4. **Python backend** — generate dataclasses + `psycopg3` functions
5. **TypeScript backend** — generate interfaces + `postgres.js` functions
6. **Better error messages** — surface parse errors with line numbers
7. **Glob patterns** for `schema` and `queries` config fields

### Low priority / future

1. **C / C++ / C# backends** — stubs to add later
2. **Schema-qualified tables** — handle `schema.table` references in queries
3. **`sqltgen init`** subcommand — scaffold a starter `sqltgen.json`

---

## Features from sqlc not yet in sqltgen

Identified by reading the [sqlc documentation](https://docs.sqlc.dev). Candidate features
to implement, roughly ordered by expected user value.

### Query annotation commands (missing variants)

| Command | Meaning | sqltgen status |
|---|---|---|
| `:execresult` | Returns the driver result object (affected rows + last insert ID) | ❌ |
| `:execlastid` | Returns only the last inserted ID | ❌ |
| `:batchexec` | Batch execute — pgx/v5 only | ❌ |
| `:batchmany` | Batch query returning multiple result sets — pgx/v5 only | ❌ |
| `:batchone` | Batch single-row query — pgx/v5 only | ❌ |
| `:copyfrom` | Bulk insert via `COPY FROM` — driver-specific | ❌ |

Notes:
- `:execresult` is the most broadly useful; `:execlastid` matters mainly for MySQL (no `RETURNING`).
- Batch variants are pgx-specific and only relevant once a Go backend exists.
- `:copyfrom` requires special driver support and is a niche feature.

### sqlc macro functions

sqlc embeds special function calls in SQL that are rewritten at parse time. sqltgen has
none of these yet.

#### `sqlc.arg(name)` / `@name` — named parameters
Instead of positional `$1`/`?1`, the user writes `sqlc.arg(user_id)` or `@user_id`.
The codegen emits a typed params struct with a meaningful field name rather than a
positional argument. Useful when parameter positions are unclear from context (e.g. two
`text` params with different semantics).

- Frontend: rewrite `sqlc.arg(x)` / `@x` → positional placeholder; record name in `Parameter`
- IR: `Parameter` already has `index`; add `Option<String> name`
- Backend: emit a `{QueryName}Params` struct/record/dataclass when any param is named
- `@name` shorthand works in PostgreSQL and SQLite; not in MySQL

#### `sqlc.narg(name)` — explicitly nullable named parameter
Same as `sqlc.arg()` but forces the parameter to be nullable regardless of schema inference.
Useful for optional PATCH-style updates: `SET col = COALESCE(sqlc.narg(col), col)`.

- IR: `Parameter.nullable` already exists; `sqlc.narg` sets it `true` unconditionally
- Backend: nullable params already use `setObject` in Java/Kotlin — no backend changes needed

#### `sqlc.embed(table)` — struct embedding in result types
Instead of a flat `{Query}Row` with all columns mixed, `sqlc.embed(t)` groups columns
from table `t` into a nested field of type `T` in the result struct.

Example: `SELECT sqlc.embed(students), sqlc.embed(scores) FROM ...` emits:
```
struct ScoreRow { student: Student, score: TestScore }
```
rather than a flat struct with all columns.

- Frontend: detect `sqlc.embed(t)` in SELECT list, replace with `t.*` for column
  resolution, record which columns belong to which embedded table
- IR: `ResultColumn` needs a way to carry the embedding group (e.g. `Option<String> embedded_table`)
- Backend: group result columns by `embedded_table` and emit nested types

#### `sqlc.slice(name)` — dynamic IN clause
Generates a query with a variable-length parameter list for `IN (...)` at runtime.
The placeholder expands to the correct number of `$N`/`?` at call time.

- Most useful for JDBC and sqlite3 drivers (no native array params).
  sqlx/pgx can use `= ANY($1)` with an array instead.
- Requires runtime query rewriting in the generated code, not just at codegen time.
- Lower priority; complex to implement correctly.

### Config enhancements

| Feature | Description |
|---|---|
| **Type overrides** | Map a DB type or specific `table.column` to a custom target-language type |
| **Field renaming** | Rename a generated struct field or model name in config |
| **JSON tags** | Emit JSON serialization annotations on generated structs/classes |
| **Prepared queries** | Emit an additional prepared-statement version of each query |
| **Querier interface** | Emit an interface/protocol/ABC for the Queries type (improves testability) |
| **Strictness config** | Control error-vs-warning behavior per project (already planned above) |
| **query_parameter_limit** | Emit a params struct when a query has more than N parameters |
| **emit_exact_table_names** | Skip singularization — use the raw table name as the model name |

Implementation notes:
- **Type overrides** are high value for users with custom DB types (PostGIS, ltree, enums).
  Requires a new config section and a type-override lookup step in each backend.
- **Field renaming** is straightforward: a `rename: { db_col: "FieldName" }` map in config,
  applied in the backend before emitting field names.
- **Querier interface** is most relevant for Go and TypeScript (dependency injection / mocking).

### Transaction support (`with_tx`)

sqlc generates a `WithTx(tx)` method on the `Queries` struct so callers can reuse all
generated query methods within a transaction without extra boilerplate.

- Relevant for all backends. Each backend's `Queries` wrapper stores a connection/pool
  reference; add a `with_tx(tx)` constructor that substitutes a transaction object.
- Rust/sqlx: accept `&mut Transaction<'_, Db>` instead of `&Pool`
- Java/Kotlin: accept `java.sql.Connection` (JDBC transactions are connection-scoped)
- Python: psycopg3 connections have a transaction context manager

### Enum support

sqlc maps PostgreSQL `CREATE TYPE foo AS ENUM (...)` to a proper aliased string type
(e.g. Go `type Foo string` with constants). sqltgen currently maps enums to `SqlType::Text`.

- Frontend: detect `CREATE TYPE ... AS ENUM` in DDL, store enum definitions in `Schema`
- IR: new `SqlType::Enum(String)` variant carrying the type name, or a separate `EnumDef`
  collection in `Schema`
- Backend: emit an enum / sealed class / string alias per enum type; use it wherever
  that column type appears

---

## Open-source launch

See `memory/roadmap.md` for the full distribution plan. Summary:

- Phase 1: License (Apache-2.0 + MIT dual), CHANGELOG, CONTRIBUTING, README
- Phase 2: CI/CD via cargo-dist (ci.yml, release.yml, docs.yml)
- Phase 3: mdBook documentation at sqltgen.org
- Phase 4: Distribution — crates.io, Homebrew, AUR, Scoop, .deb, .rpm
