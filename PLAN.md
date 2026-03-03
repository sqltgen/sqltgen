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
| `ddl.pest` — PEG grammar for DDL | ✅ | Handles CREATE TABLE, constraints, arrays |
| `typemap.rs` — pg type string → `SqlType` | ✅ | Case-insensitive, strips size params; 14 unit tests |
| `schema.rs` — DDL parser → `Schema` | ✅ | 8 unit tests (table PK, array cols, IF NOT EXISTS, DEFAULT, GENERATED…) |
| `query.rs` — annotated query file parser | ✅ | SELECT/INSERT/UPDATE/DELETE + param inference; 13 unit tests |
| **SQLite** | ❌ | Not started |

## Backend layer (`src/backend/`)

| Language | Status | Notes |
|---|---|---|
| `java.rs` | ✅ | Record classes + `Queries` class with JDBC methods |
| `kotlin.rs` | ✅ | Data classes + `Queries` object with JDBC methods |
| `rust.rs` | 🚧 | Stub — `unimplemented!()` |
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

1. **Rust backend** — generate `sqlx` query functions (or plain `postgres` crate)
2. **Go backend** — generate structs + `database/sql` functions
3. **Smoke test** — add a sample `sqlt.json` + `schema.sql` + `queries.sql` under `examples/`
   and verify `sqlt generate` produces correct Java/Kotlin output end-to-end

### Medium priority

4. **Python backend** — generate dataclasses + `psycopg2` / `asyncpg` functions
5. **TypeScript backend** — generate interfaces + `pg` / `postgres.js` functions
6. **Better error messages** — surface parse errors with line numbers
7. **Glob patterns** for `schema` and `queries` config fields (currently single file only)

### Low priority / future

8. **SQLite frontend** — `DialectParser` impl for SQLite DDL
9. **C / C++ / C# backends** — stubs to add later
10. **Multiple query files** — allow `queries` to be a list of paths
11. **Schema-qualified tables** — handle `schema.table` references in queries
12. **`sqlt init`** subcommand — scaffold a starter `sqlt.json`
