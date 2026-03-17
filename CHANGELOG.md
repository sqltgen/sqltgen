# Changelog

All notable changes to sqltgen will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
While pre-release, sqltgen uses date-based versions (`0.0.YYYYMMDD`).
Post-release it will switch to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- **Cross-language `Querier` wrappers** — all generated backends now emit a
  `Querier` object/class as the primary instance API for query execution:
  Java/Kotlin (DataSource-backed), Rust (`DbPool`-backed), Python
  (connection-factory-backed), and TypeScript/JavaScript
  (connect-factory-backed).
- **Java/Kotlin rename** — generated DataSource wrappers were renamed from
  `QueriesDs` to `Querier` (including grouped variants such as
  `UsersQuerier`/`PostsQuerier`).
- **Engine-agnostic query module aliases** — Rust query files now alias the
  selected sqlx pool type to `DbPool`, Python query files alias engine-specific
  connection types to `Connection`, and TypeScript/JavaScript query files alias
  driver connection types to `Db` so generated method signatures stay stable
  across engines.
- **Python: `_sqltgen.py` helper module** — each Python codegen run now emits a
  `_sqltgen.py` alongside the query files. It provides two engine-agnostic
  helpers (`execute` context manager, `exec_stmt` for `:exec` queries) that
  abstract away the cursor API differences between psycopg3/mysql-connector
  (`with conn.cursor() as cur`) and sqlite3 (`conn.execute()` directly).
  Generated query functions are now structurally identical across all three
  Python engines — only the connection type annotation and SQL placeholder
  style differ.
- **Query grouping** — the `queries` config field now accepts an object
  (map form) in addition to a string or array. Each key becomes a named
  group and each backend emits one output file per group. Java/Kotlin
  produce `{Group}Queries.java` / `{Group}Queries.kt` + a matching
  `{Group}Querier` class; Rust, Python, TypeScript, and JavaScript produce one file per
  group named after the key (`users.rs`, `users.py`, `users.ts`, …).
  Single-file configs and array configs are unchanged — the array form
  auto-derives the group name from each file's stem.
- **TypeScript backend** — generates typed interfaces + async query functions for
  pg (PostgreSQL), better-sqlite3 (SQLite), and mysql2 (MySQL) drivers
- **JavaScript backend** — same codegen engine as TypeScript but emits JSDoc type
  annotations instead of inline TypeScript types
- TypeScript and JavaScript examples for all three dialects (PostgreSQL, SQLite, MySQL)
- `UNION` / `UNION ALL` / `INTERSECT` / `EXCEPT` result column typing — resolves
  result columns from the leftmost SELECT branch (SQL standard)
- E2E snapshot test suite — 14 backend × dialect combinations with golden file comparison
- E2E runtime tests — Rust + SQLite (in-memory), Rust + PostgreSQL (Docker), and
  Java + PostgreSQL (Docker, 10+ test methods covering IS NULL, date range, DISTINCT,
  LEFT JOIN aggregates, ON CONFLICT upsert, EXISTS subquery, scalar subquery, COALESCE)
- New e2e fixture queries across all three dialects: `GetAuthorsWithNullBio`,
  `GetAuthorsWithBio`, `GetBooksPublishedBetween`, `GetDistinctGenres`,
  `GetBooksWithSalesCount`, `CountSaleItems`; `UpsertProduct` (PostgreSQL only,
  uses ON CONFLICT DO UPDATE RETURNING)
- Python backend regression tests that assert generated `queries.py` stays
  adapter-driven (uses `_sqltgen` helper API) and contains no engine-target
  conditional branching markers.

### Changed
- Added shared backend scaffolding (`generate_two_layer_backend`) for the
  adapter/core generation flow.
- **TypeScript/JavaScript backend internals:** migrated to the two-layer pattern.
  Engine-specific runtime details now generate into `_sqltgen.ts`/`_sqltgen.js`,
  while query/module emission runs through a contract-driven core layer.
- **Python backend internals:** refactored to a two-layer shape where engine/driver
  differences are resolved once at codegen time into a `PythonCoreContract` split
  into runtime and SQL sub-contracts. Query/model emitters now consume these
  pre-resolved contracts without branching on engine target.

### Fixed
- Type override named presets now emit one-time warnings when used on
  unsupported backends (for example, `jackson` in Python or `serde_json`
  in JVM backends) instead of being silently ignored.
- Rust backend SQL embedding now has explicit regression coverage for quoted
  identifiers and string literals to ensure generated SQL remains in raw string
  form without escaped double quotes.
- Literal and simple expression projection resolution now infers types for
  `NULL`, numeric, string, and boolean literals in `resolve_expr`, preventing
  unnamed literal select items from being dropped.
- `UNION`/set-operation result nullability now widens across branches by
  projection position, so `NULL` placeholders in non-left branches correctly
  mark result columns nullable.
- **`INSERT … SELECT` parameter inference** — params in the SELECT projection of an
  `INSERT INTO t (cols) SELECT $1, … FROM … WHERE …` were previously unresolved
  (falling back to `Text`/`Custom`). The frontend now maps each SELECT-list
  placeholder to the corresponding INSERT target column type, and delegates
  WHERE/JOIN/HAVING inference to the standard SELECT analysis pass.
- **Java/Kotlin**: native list strategy now correctly JSON-quotes text elements when
  building JSON arrays for SQLite `json_each` and MySQL `JSON_TABLE` — previously
  produced invalid JSON for string values containing `"` or `\`
- **Java/Kotlin/Python**: dynamic list strategy now binds scalar params at the correct
  JDBC/cursor slots when a scalar appears after the `IN` clause in the SQL — previously
  bound scalars before list elements regardless of their position
- Parameter type inference in ORDER BY expressions (e.g. `ORDER BY CASE WHEN id = $1 ...`)
- Parameter type inference in HAVING, JOIN ON, LIMIT/OFFSET, IN list, and BETWEEN
  (all expression contexts now covered)
- Parameter type inference inside `EXISTS` subqueries — params in the subquery
  `WHERE` clause were previously untyped (defaulting to Text)
- Parameter type inference for COALESCE fallback placeholders — `COALESCE(col, $1)`
  now infers `$1`'s type from the first non-placeholder argument
- Parameter type inference for `ON CONFLICT DO UPDATE SET` — params in the SET
  assignments were previously untyped; `excluded.col` references are now resolved
  correctly against the target table
- Parameter type inference for arithmetic operators (`+`, `-`, `*`, `/`, `%`),
  string concatenation (`||`), and bitwise operators (`&`, `|`, `^`, `<<`, `>>`)
- Parameter type inference for JSON operators: `->` / `->>` (right-hand param
  typed as `Text`), `#>` / `#>>` (right-hand param typed as `Text[]`), and
  `@>` / `<@` (both operands typed from the JSONB column)
- Duplicate parameter names in generated function signatures — `BETWEEN $1 AND $2`
  on the same column now produces `price, price_2` instead of `price, price`
- Non-deterministic import ordering in Rust backend — `HashSet` replaced with sorted `Vec`
- **False-positive table model inference** — backends could reuse a schema table's
  model type for queries whose column names happened to match that table, even when
  the rows came from a CTE, subquery, or a different table with the same column
  structure. Fixes three classes of bugs: type mismatch (CTE with same column names
  but different types), nullability mismatch (outer-join nullable columns), and
  ambiguity (two tables with identical column structure).
  - IR: added `Query.source_table: Option<String>` — set by the frontend when the
    SELECT projection is an unambiguous `table.*` or bare `*` over a single
    non-nullable schema table; `None` for all other projections.
  - Backend: `infer_table` now uses source identity (Tier 1) when `source_table` is
    set, and a stricter structural match requiring type + nullability equality and
    uniqueness (Tier 2) as fallback for test-constructed queries.

## [0.0.20260310] — unreleased

First public release.

### Added

#### Dialects (frontend)
- **PostgreSQL** — full DDL + query parsing; `$N` positional parameters
- **SQLite** — full DDL + query parsing; `?N` positional parameters
- **MySQL** — full DDL + query parsing; `$N` positional parameters (via GenericDialect)
- Schema loading from a directory of migration files (lexicographic order)
- Named parameters: `@param_name` in SQL body; `-- @param_name [type] [null|not null]`
  annotation lines for type and nullability overrides
- List/collection parameters: `-- @ids type[]` marks a param for `IN (@ids)` clauses;
  generates native array binding (MySQL: `JSON_TABLE`; SQLite: inline JSON)
- CTE (`WITH … SELECT/INSERT/UPDATE/DELETE`) — chained CTEs, JOIN with schema tables,
  parameter propagation through DML CTEs
- `RETURNING` clause on `INSERT`, `UPDATE`, `DELETE` (PostgreSQL)
- `JOIN` queries with full type inference (qualified columns, aliases, `SELECT *`)
- Subqueries in `WHERE` (`IN (SELECT …)`) and `FROM` (derived tables)
- Scalar subqueries in `SELECT` list
- `DROP TABLE [IF EXISTS]`, `ALTER TABLE` (ADD/DROP/RENAME/ALTER COLUMN, ADD CONSTRAINT,
  RENAME TABLE), with unknown operations silently ignored

#### Query commands
- `:one` — returns a single optional row
- `:many` — returns a list of rows
- `:exec` — executes and returns nothing
- `:execrows` — executes and returns the number of affected rows

#### Backends (codegen)
- **Java** — JDBC; `record` row models; `Queries` (connection) + `Querier`
  (DataSource) classes; nullable params via `setObject`
- **Kotlin** — JDBC; `data class` row models; `Queries` object + `Querier` class
- **Rust** — sqlx; async functions; `#[derive(sqlx::FromRow)]` structs; `mod.rs`
  generated for each output directory
- **Python** — psycopg3 (PostgreSQL), sqlite3 (SQLite), mysql-connector-python (MySQL);
  `@dataclass` row models; module `__init__.py` generated

#### CLI
- `sqltgen generate --config <path>` — reads config, runs frontend + backend, writes files

#### Examples
- Runnable bookstore examples for all four backends × three dialects (PostgreSQL,
  SQLite, MySQL), each with a `Makefile` (`make run`)
- `make run-all` at the repo root runs all examples using one shared container per
  engine (1× PG, 1× MySQL, no containers for SQLite)

[Unreleased]: https://github.com/sqltgen/sqltgen/compare/v0.0.20260310...HEAD
[0.0.20260310]: https://github.com/sqltgen/sqltgen/releases/tag/v0.0.20260310
