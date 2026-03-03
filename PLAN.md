# sqlt — implementation plan

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
| `SqltConfig` struct + serde | ✅ | Flat per-file (one config per DB, not a list) |
| `Engine` enum | ✅ | `postgresql`, `sqlite` |
| `OutputConfig` (`out`, `package`) | ✅ | Keyed by language name in `gen` map |
| Config file loading from path | ✅ | |

## Frontend layer (`src/frontend/`)

| Item | Status | Notes |
|---|---|---|
| `DialectParser` trait | ✅ | `parse_schema`, `parse_queries` |
| **PostgreSQL** | | |
| `typemap.rs` — `DataType` → `SqlType` | ✅ | Matches sqlparser AST variants directly; 13 unit tests |
| `schema.rs` — DDL parser → `Schema` | ✅ | sqlparser-rs AST; 19 unit tests |
| `query.rs` — annotated query file parser | ✅ | SELECT/INSERT/UPDATE/DELETE + JOINs + subqueries + derived tables; 34 unit tests |
| `query.rs` — CTE (`WITH`) support | ❌ | Planned next — see Remaining work |
| **SQLite** | ❌ | Not started |

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
| `sqlt generate --config sqlt.json` | ✅ | clap derive; reads config, runs frontend + backend, writes files |

---

## Remaining work

### High priority

1. **CTE support** (`WITH` clauses) — reuse `cols_from_subquery` (same pattern as derived
   tables); build a synthetic table per CTE and add it to scope before resolving the outer
   `SELECT`. Four tests: basic, param in inner/outer, chained CTEs, CTE joined with schema table.

2. **Go backend** — generate structs + `database/sql` functions

### Medium priority

1. **`UNION` / `INTERSECT` result columns** — resolve from left branch of `SetExpr::SetOperation`
2. **`CAST(x AS type)` result type** — call `typemap::map()` on the cast's `DataType`
3. **`HAVING` params** — same `collect_params_from_expr` walk on `select.having`
4. **Python backend** — generate dataclasses + `psycopg2` / `asyncpg` functions
5. **TypeScript backend** — generate interfaces + `pg` / `postgres.js` functions
6. **Better error messages** — surface parse errors with line numbers
7. **Glob patterns** for `schema` and `queries` config fields (currently single file only)

### Low priority / future

1. **SQLite frontend** — `DialectParser` impl for SQLite DDL
2. **C / C++ / C# backends** — stubs to add later
3. **Multiple query files** — allow `queries` to be a list of paths
4. **Schema-qualified tables** — handle `schema.table` references in queries
5. **`sqlt init`** subcommand — scaffold a starter `sqlt.json`
