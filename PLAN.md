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

## Open-source launch

See `memory/roadmap.md` for the full distribution plan. Summary:

- Phase 1: License (Apache-2.0 + MIT dual), CHANGELOG, CONTRIBUTING, README
- Phase 2: CI/CD via cargo-dist (ci.yml, release.yml, docs.yml)
- Phase 3: mdBook documentation at sqltgen.org
- Phase 4: Distribution — crates.io, Homebrew, AUR, Scoop, .deb, .rpm
