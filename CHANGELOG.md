# Changelog

All notable changes to sqltgen will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
While pre-release, sqltgen uses date-based versions (`0.0.YYYYMMDD`).
Post-release it will switch to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Breaking Changes
- **Output layout restructured** (all backends): model files are now emitted under
  `{out}/models/`, query files under `{out}/queries/`, and the shared helper is
  renamed from `_sqltgen.*` to `sqltgen.*`. Barrel/index/mod files at the output
  root re-export from both subdirectories. Java and Kotlin emit models in a
  `{package}.models` subpackage and queries in `{package}.queries` subpackage.
  Go consolidates all table structs into a single `models.go` and names query
  files `queries_{group}.go`. This eliminates a silent file overwrite that
  occurred when a table name matched a query group name.

### Changed
- **Pre-resolved type map refactor (all backends)** — each backend now builds a
  `<Lang>TypeMap` once before codegen, with all override, preset, and default
  resolution at construction time. Emitters are pure consumers and no longer call
  `resolve_type_override` at the call site. New `typemap.rs` modules added for
  Java, Rust, Python, Go, and TypeScript/JavaScript. This is a pure internal
  refactor with no user-visible behaviour change, except two bug fixes discovered
  during migration (see Fixed below).

### Fixed
- **Java: array column parameter type no longer double-wraps** — a `text[]`
  parameter was emitted as `List<java.util.List<String>>` instead of
  `java.util.List<String>` due to a bug in `java_param_type`. Fixed.
- **Python: import lines now emitted in alphabetical order** — the migration from
  a flag-based `TypeImports` struct to `BTreeSet<String>` changed import ordering
  from insertion order to alphabetical.
- **Kotlin and Java: array element type conversion now honours type overrides** — reads of
  SQL `ARRAY` columns that have a per-type override (e.g. `TIMESTAMP[]` with
  `"timestamp": "java.time.LocalDateTime"`) previously returned the raw JDBC element type
  (`java.sql.Timestamp`) instead of the configured override type, causing silent
  type-mismatches and potential `ClassCastException` at call sites.  Both backends now emit
  a per-element stream/map expression for temporal types (`TIMESTAMP`, `TIMESTAMPTZ`,
  `DATE`, `TIME`) and `UUID` arrays — applying the same conversion that scalar column reads
  use — instead of an unchecked direct cast.  Types with a `read_expr` override have the
  expression applied per element, with `{raw}` substituted by the appropriate JDBC element
  cast.  Types that need no conversion (text, numeric, boolean, …) continue to use the
  efficient `Arrays.asList` / `jdbcArrayToList` direct-cast path.

### Added
- **`schema_stop_marker` config field** — migration files that contain both an
  "up" and a "down" section (e.g. dbmate, goose, golang-migrate) can now be
  used directly as the `schema` source. Set `schema_stop_marker` to the comment
  that begins the down section (e.g. `"-- migrate:down"`) and sqltgen will
  discard everything from that line onward in each file, leaving only the DDL
  that builds up the schema.
- **Comprehensive type-overrides E2E runtime test suite** — 20 new runtime test
  projects covering all 6 backends × 3 SQL dialects (PostgreSQL, SQLite, MySQL)
  for the `type_overrides` fixture. Tests verify JSON codec round-trips
  (jackson/gson/serde_json/object presets), datetime parameter binding and
  reading (java.time, time crate, chrono), UUID handling, and `:execrows` counts.
  Includes a dedicated `chrono` test module in the Rust/PostgreSQL project that
  exercises explicit `TypeRef` overrides mapping `TIMESTAMP` → `chrono::NaiveDateTime`,
  `TIMESTAMPTZ` → `chrono::DateTime<Utc>`, `DATE` → `chrono::NaiveDate`, and
  `TIME` → `chrono::NaiveTime`. New Makefile targets `e2e-runtime-type-overrides`
  and per-project variants. New fixture schema/query files for SQLite and MySQL dialects.
- **Four new type-overrides queries** — `InsertEventRows :execrows`,
  `GetEventsByDateRange :many`, `CountEvents :one`, and `UpdateEventDate :exec`
  added to the PostgreSQL type_overrides fixture and all dialect variants.
- **mdBook documentation site** — comprehensive user documentation under
  `docs/src/` structured as an mdBook. Includes: `introduction.md` (what sqltgen
  is, why it exists, comparison to sqlc), `installation.md` (cargo, Homebrew,
  curl, .deb/.rpm), `quickstart.md` (end-to-end Java example), `config.md` (full
  `sqltgen.json` reference), `queries.md` (annotation syntax, named params,
  commands, RETURNING, CTEs, JOINs, list params), `types.md` (SQL type → language
  type tables for all backends), per-language guides for Java, Kotlin, Rust,
  Python, TypeScript, and JavaScript, and `contributing.md`. The `docs/book.toml`
  is configured with the correct title, repo link, and edit-URL template.
- **`docs.yml` CI action** — deploys the mdBook to S3/CloudFront on every push
  to `main` that touches `docs/`.


- **`CREATE VIEW` support** — all three SQL dialects (PostgreSQL, SQLite, MySQL)
  now parse `CREATE VIEW … AS SELECT …` in schema files. Views are registered in
  the `Schema` as `TableKind::View` entries; their column types are inferred
  automatically from the SELECT body using the same expression resolver used for
  query parsing. Views defined after the tables they reference, and views that
  reference earlier views, are resolved correctly via a two-pass approach: base
  tables are collected in pass 1, views resolved in pass 2 in declaration order.
  Views with unknown source tables fall back gracefully to an empty column list.
  The IR gains a `TableKind` enum (`Table` / `View`) on `Table`, accessible via
  `Table::is_view()`. All `Table` construction sites use the new `Table::new()`
  and `Table::view()` constructors so future IR changes propagate through one
  point. 13 new unit tests across the three dialect schema parsers.
- **View-focused test coverage** — added dedicated unit/snapshot/runtime coverage
  for view workflows.
  - Unit tests: added MySQL view-on-view chaining coverage and new `DROP VIEW`
    parser tests for PostgreSQL, SQLite, and MySQL; added backend unit tests for
    Rust/Go/Java/Kotlin/Python/TypeScript asserting view models are emitted.
  - Snapshot tests: added a new `tests/e2e/fixtures/views/` fixture (PostgreSQL,
    SQLite, MySQL) and 17 backend snapshot tests covering all emitted targets.
  - Runtime tests: added PostgreSQL runtime tests for all existing runtime
    languages (Rust, Go, Java, Kotlin, Python, TypeScript) that execute
    generated query methods against a `CREATE VIEW` schema object.
- **Go backend** — full `database/sql` code generation targeting PostgreSQL,
  SQLite, and MySQL. Generates Go structs for row models, query functions with
  idiomatic `(T, error)` returns, and a `Querier` struct wrapper around `*sql.DB`.
  Features: all four query commands (`:one`, `:many`, `:exec`, `:execrows`), list
  parameter support via `pq.Array` (native PostgreSQL) and dynamic `?` expansion,
  `$N`/`?N` → `?` placeholder rewriting, two-layer adapter/core architecture,
  nullable fields via `sql.NullX` types, table row-type inference,
  join/CTE/RETURNING row types, and `mod.go` package file generation.
  Includes example projects for PostgreSQL and SQLite, e2e snapshot tests, and
  39 unit tests.
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
- **Java backend internals:** migrated to the two-layer pattern. Language-specific
  constants (`SE`, `FALLBACK_TYPE`) are now resolved from a compile-time
  `JvmCoreContract` instead of being hard-coded, and the backend was split into
  `java/mod.rs`, `java/adapter.rs`, and `java/core.rs`.
- **Kotlin backend internals:** same restructuring as Java — split into
  `kotlin/mod.rs`, `kotlin/adapter.rs`, and `kotlin/core.rs` with a
  `JvmCoreContract` driving language-constant selection.
- **Rust backend internals:** migrated to the two-layer pattern. Engine-specific
  sqlx pool selection now emits into `_sqltgen.rs`, while table/query/module
  generation runs through a contract-driven core layer without engine-target
  branching.
- **TypeScript/JavaScript backend internals:** migrated to the two-layer pattern.
  Engine-specific runtime details now generate into `_sqltgen.ts`/`_sqltgen.js`,
  while query/module emission runs through a contract-driven core layer.
- **Python backend internals:** refactored to a two-layer shape where engine/driver
  differences are resolved once at codegen time into a `PythonCoreContract` split
  into runtime and SQL sub-contracts. Query/model emitters now consume these
  pre-resolved contracts without branching on engine target.

### Fixed
- `IS NULL` / `IS NOT NULL` nullable inference now recurses into CASE branches,
  subqueries, function arguments, JOIN ON clauses, IN lists, BETWEEN, and
  LIKE/ILIKE expressions. Previously, parameters tested with IS NULL inside
  these nested contexts were silently left non-nullable in generated code.
  The same fix was applied to `collect_params_from_subquery`, which was missing
  the nullable marking pass entirely.
- `DROP VIEW` statements are now applied during schema parsing (all three
  dialects). Previously `DROP VIEW` was silently ignored, leaving dropped views
  in the inferred schema.
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
