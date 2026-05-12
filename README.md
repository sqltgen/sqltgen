# sqltgen

A multi-language SQL-to-code generator. Annotate your SQL queries; sqltgen emits
idiomatic, type-safe database access code in Java, Kotlin, Rust, Go, Python,
TypeScript, and JavaScript.

Inspired by [sqlc](https://sqlc.dev).

> **Status:** pre-release / active development. APIs and generated code format may change.

---

## How it works

1. Write your schema (DDL) and annotate your queries with a name and command.
2. Point `sqltgen.json` at them.
3. Run `sqltgen generate`.
4. Get fully typed, ready-to-use database access code.

No reflection. No runtime query building. Just your SQL, compiled to code.
The generated code uses only your language's standard database driver — no
runtime library to depend on, no framework to lock into.

---

## Quick example

**`schema.sql`**

```sql
CREATE TABLE author (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT    NOT NULL,
    bio        TEXT,
    birth_year INTEGER
);
```

**`queries.sql`**

```sql
-- name: GetAuthor :one
SELECT id, name, bio, birth_year
FROM author
WHERE id = @id;

-- name: ListAuthors :many
SELECT id, name, bio, birth_year
FROM author
ORDER BY name;

-- name: CreateAuthor :one
INSERT INTO author (name, bio, birth_year)
VALUES (@name, @bio, @birth_year)
RETURNING *;
```

**`sqltgen.json`**

```json
{
  "version": "1",
  "engine": "postgresql",
  "schema": "schema.sql",
  "queries": "queries.sql",
  "gen": {
    "java":       { "out": "gen", "package": "com.example.db" },
    "kotlin":     { "out": "gen", "package": "com.example.db" },
    "rust":       { "out": "src/db", "package": "" },
    "go":         { "out": "db",    "package": "db" },
    "python":     { "out": "gen",   "package": "" },
    "typescript": { "out": "src/db", "package": "" },
    "javascript": { "out": "src/db", "package": "" }
  }
}
```

**`sqltgen generate --config sqltgen.json`** emits, for example in Java:

```java
// gen/com/example/db/Author.java
public record Author(long id, String name, String bio, Integer birthYear) {}

// gen/com/example/db/Queries.java
public static Optional<Author> getAuthor(Connection conn, long id) throws SQLException { … }
public static List<Author>     listAuthors(Connection conn) throws SQLException { … }
public static Optional<Author> createAuthor(Connection conn, String name, String bio,
                                            Integer birthYear) throws SQLException { … }

// gen/com/example/db/Querier.java
var q = new Querier(dataSource);
q.getAuthor(1L);
```

And equivalently in Kotlin, Rust (sqlx async functions), Go (database/sql),
Python (psycopg3), TypeScript (pg / better-sqlite3 / mysql2), and JavaScript
(with JSDoc types).

---

## Query annotation format

```sql
-- name: QueryName :command
SELECT …
```

| Command | Return type (Java example) |
|---|---|
| `:one` | `Optional<T>` |
| `:many` | `List<T>` |
| `:exec` | `void` |
| `:execrows` | `long` (rows affected) |

### Named parameters

Use `@param_name` in SQL. Optionally annotate type or nullability before the query:

```sql
-- name: UpdateAuthorBio :one
-- @bio null
UPDATE author SET bio = @bio WHERE id = @id
RETURNING *;
```

### List parameters

Use `-- @ids type[]` to mark a parameter as a collection for `IN` clauses:

```sql
-- name: GetBooksByIds :many
-- @ids bigint[]
SELECT * FROM book WHERE id IN (@ids);
```

---

## Supported dialects

| Dialect | Schema parsing | Query parsing |
|---|:---:|:---:|
| PostgreSQL | ✅ | ✅ |
| SQLite | ✅ | ✅ |
| MySQL | ✅ | ✅ |

## Supported backends

| Language | Driver | Status |
|---|---|:---:|
| Java | JDBC | ✅ |
| Kotlin | JDBC | ✅ |
| Rust | sqlx (async) | ✅ |
| Go | database/sql | ✅ |
| Python | psycopg3 / sqlite3 / mysql-connector | ✅ |
| TypeScript | pg / better-sqlite3 / mysql2 | ✅ |
| JavaScript | pg / better-sqlite3 / mysql2 | ✅ |

---

## Installation

### Pre-built binary (Linux / macOS)

The shell installer detects your platform, downloads the right archive from
the latest GitHub Release, and installs the binary to `~/.local/bin`:

```sh
curl -fsSL https://sqltgen.org/install.sh | sh
```

For a system-wide install, pass `SQLTGEN_INSTALL_DIR`:

```sh
curl -fsSL https://sqltgen.org/install.sh | sudo env SQLTGEN_INSTALL_DIR=/usr/local sh
```

To pin to a specific release tag instead of "latest", use the full GitHub URL:

```sh
curl --proto '=https' --tlsv1.2 -LsSf \
  https://github.com/sqltgen/sqltgen/releases/download/v0.1.0-rc.2/sqltgen-installer.sh | sh
```

### Pre-built binary (Windows)

```powershell
irm https://github.com/sqltgen/sqltgen/releases/latest/download/sqltgen-installer.ps1 | iex
```

### Build from source

```sh
git clone https://github.com/sqltgen/sqltgen.git
cd sqltgen
cargo build --release
# binary: ./target/release/sqltgen
```

> `cargo install sqltgen` is not yet supported — crates.io publish is deferred.
> Other distribution packages (Homebrew tap, AUR, Scoop, `.deb`, `.rpm`) are
> planned alongside the v0.1.0 stable release.

---

## Configuration reference

```json
{
  "version": "1",
  "engine": "postgresql",   // "postgresql" | "sqlite" | "mysql"
  "schema":  "migrations/", // path to a .sql file or directory of migration files
  "queries": "queries.sql", // path, list of paths, or glob pattern
  "gen": {
    "rust":       { "out": "src/db",  "package": "" },
    "go":         { "out": "db",      "package": "db" },
    "java":       { "out": "gen",     "package": "com.example.db" },
    "kotlin":     { "out": "gen",     "package": "com.example.db" },
    "python":     { "out": "gen",     "package": "" },
    "typescript": { "out": "src/db",  "package": "" },
    "javascript": { "out": "src/db",  "package": "" }
  }
}
```

The `schema` field accepts a directory; files are loaded in lexicographic order
(ideal for numbered migration files like `001_create_users.sql`).

DDL statements are applied with PostgreSQL-like strictness across the loaded
files: a bare `CREATE TABLE` (or `CREATE VIEW` / `CREATE TYPE` / `CREATE FUNCTION`)
that re-defines an existing object is reported as an error pointing at both
source locations. `IF NOT EXISTS` and `OR REPLACE` are honored. This catches
mixed inputs like a `schema.sql` dump sitting alongside the migrations that
produced it.

If your migration files contain both an "up" and a "down" section, set
`schema_stop_marker` to the comment that begins the down section. Everything
from that line onward is ignored when building the schema:

```json
{
  "schema": "migrations/",
  "schema_stop_marker": "-- migrate:down"
}
```

Common values: `"-- migrate:down"` (dbmate), `"-- +goose Down"` (goose),
`"-- +migrate Down"` (golang-migrate).

Most migration tools also keep their own bookkeeping table in the schema dump
(dbmate's `schema_migrations`, Flyway's `flyway_schema_history`, etc.). To
keep those out of the generated models, list them in `ignore_tables`:

```json
{
  "schema": "migrations/schema.sql",
  "ignore_tables": ["schema_migrations"]
}
```

The match is by bare table name (no schema qualifier). Filtering happens after
the schema is parsed, so queries that reference an ignored table still get
correctly typed result columns — only the model class emission is suppressed.

The `queries` field accepts a string (single file), an array of paths/globs, or
a **grouped map** that names each group explicitly:

```json
"queries": {
  "users": "queries/users.sql",
  "posts": ["queries/posts/**/*.sql", "queries/extra.sql"]
}
```

Each group produces its own output file. In Java/Kotlin the group name is
PascalCased and suffixed with `Queries` (`UsersQueries.java`) plus a matching
`UsersQuerier.java` / `.kt` wrapper. In Rust, Go, Python, TypeScript, and
JavaScript the group name is used directly (`users.rs`, `users.go`, `users.py`,
`users.ts`, `users.js`) and each queries module also emits a `UsersQuerier`
(or default `Querier`) wrapper. Rust, Go, Python, and TypeScript/JavaScript also
emit a generated runtime/helper module (`_sqltgen.rs`, `_sqltgen.go`,
`_sqltgen.py`, `_sqltgen.ts`, `_sqltgen.js`) that centralizes engine/driver-specific
wiring.
The single-file form always uses `Queries` + `Querier` (JVM) and `queries` +
`Querier` (Rust/Go/Python/TS/JS).

---

## Examples

Runnable examples for all seven backends × three dialects live in `examples/`.
Each is a self-contained project with a `Makefile`:

```sh
# Run a single example (starts its own Docker container)
make -C examples/java/postgresql run
make -C examples/rust/sqlite run

# Run all examples with one shared container per engine
make run-all
```

See [`examples/README.md`](examples/README.md) for prerequisites.

---

## License

Copyright 2026 The sqltgen Authors.

Licensed under the [Apache License, Version 2.0](LICENSE).

Generated output (code produced by sqltgen from your schemas and queries) is
owned by you and is not subject to this license. See [`legal/NOTICE`](legal/NOTICE)
for details.

---

## Documentation

- **[Documentation](https://docs.sqltgen.org)** — the full user documentation site.
- **[Contributor guide](docs/contributor-guide.md)** — architecture, IR data model, how to
  add a new backend or dialect, testing, and code conventions.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
