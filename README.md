# sqltgen

A multi-language SQL-to-code generator. Annotate your SQL queries; sqltgen emits
idiomatic, type-safe database access code in Java, Kotlin, Rust, Python,
TypeScript, and JavaScript.

Inspired by [sqlc](https://sqlc.dev). Written in Rust.

> **Status:** pre-release / active development. APIs and generated code format may change.

---

## How it works

1. Write your schema (DDL) and annotate your queries with a name and command.
2. Point `sqltgen.json` at them.
3. Run `sqltgen generate`.
4. Get fully typed, ready-to-use database access code.

No reflection. No runtime query building. Just your SQL, compiled to code.

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

And equivalently in Kotlin, Rust (sqlx async functions), Python (psycopg3),
TypeScript (pg / better-sqlite3 / mysql2), and JavaScript (with JSDoc types).

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
| Python | psycopg3 / sqlite3 / mysql-connector | ✅ |
| TypeScript | pg / better-sqlite3 / mysql2 | ✅ |
| JavaScript | pg / better-sqlite3 / mysql2 | ✅ |
| Go | database/sql | 🚧 planned |

---

## Installation

### Build from source

```sh
git clone https://github.com/sqltgen/sqltgen.git
cd sqltgen
cargo build --release
# binary: ./target/release/sqltgen
```

### cargo install

```sh
cargo install sqltgen
```

> Distribution packages (Homebrew, AUR, etc.) are planned for the v0.1.0 release.

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
`UsersQuerier.java` / `.kt` wrapper. In Rust, Python, TypeScript, and JavaScript
the group name is used directly (`users.rs`, `users.py`, `users.ts`, `users.js`) and
each queries module also emits a `UsersQuerier` (or default `Querier`) wrapper.
Rust, Python, and TypeScript/JavaScript also emit a generated runtime/helper module
(`_sqltgen.rs`, `_sqltgen.py`, `_sqltgen.ts`, `_sqltgen.js`) that centralizes
engine/driver-specific wiring.
The single-file form always uses `Queries` + `Querier` (JVM) and `queries` +
`Querier` (Rust/Python/TS/JS).

---

## Examples

Runnable examples for all six backends × three dialects live in `examples/`.
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

> **To be defined.** See [#3](https://github.com/sqltgen/sqltgen/issues/3).

---

## Documentation

- **[User guide](docs/user-guide.md)** — installation, configuration, query annotation
  syntax, per-language wiring, type mapping reference, and more.
- **[Developer guide](docs/developer-guide.md)** — architecture, IR data model, how to
  add a new backend or dialect, testing, and code conventions.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
