# Changelog

All notable changes to sqltgen will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
While pre-release, sqltgen uses date-based versions (`0.0.YYYYMMDD`).
Post-release it will switch to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- **TypeScript backend** — generates typed interfaces + async query functions for
  pg (PostgreSQL), better-sqlite3 (SQLite), and mysql2 (MySQL) drivers
- **JavaScript backend** — same codegen engine as TypeScript but emits JSDoc type
  annotations instead of inline TypeScript types
- TypeScript and JavaScript examples for all three dialects (PostgreSQL, SQLite, MySQL)
- `UNION` / `UNION ALL` / `INTERSECT` / `EXCEPT` result column typing — resolves
  result columns from the leftmost SELECT branch (SQL standard)
- E2E snapshot test suite — 14 backend × dialect combinations with golden file comparison
- E2E runtime tests — Rust + SQLite (in-memory) and Rust + PostgreSQL (Docker)

### Fixed
- **Java/Kotlin**: native list strategy now correctly JSON-quotes text elements when
  building JSON arrays for SQLite `json_each` and MySQL `JSON_TABLE` — previously
  produced invalid JSON for string values containing `"` or `\`
- **Java/Kotlin/Python**: dynamic list strategy now binds scalar params at the correct
  JDBC/cursor slots when a scalar appears after the `IN` clause in the SQL — previously
  bound scalars before list elements regardless of their position
- Parameter type inference in ORDER BY expressions (e.g. `ORDER BY CASE WHEN id = $1 ...`)
- Parameter type inference in HAVING, JOIN ON, LIMIT/OFFSET, IN list, and BETWEEN
  (all expression contexts now covered)
- Duplicate parameter names in generated function signatures — `BETWEEN $1 AND $2`
  on the same column now produces `price, price_2` instead of `price, price`
- Non-deterministic import ordering in Rust backend — `HashSet` replaced with sorted `Vec`

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
- **Java** — JDBC; `record` row models; `Queries` (connection) + `QueriesDs`
  (DataSource) classes; nullable params via `setObject`
- **Kotlin** — JDBC; `data class` row models; `Queries` object + `QueriesDs` class
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
