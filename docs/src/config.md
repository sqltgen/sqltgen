# Configuration

sqltgen is configured by a JSON file, `sqltgen.json` by default.

```sh
sqltgen generate                          # reads sqltgen.json in the current directory
sqltgen generate --config path/to/sqltgen.json
sqltgen generate -c path/to/sqltgen.json
```

## Full reference

```json
{
  "version": "1",
  "engine": "postgresql",
  "schema": "migrations/",
  "queries": "queries.sql",
  "gen": {
    "java":       { "out": "src/main/java",   "package": "com.example.db" },
    "kotlin":     { "out": "src/main/kotlin", "package": "com.example.db" },
    "rust":       { "out": "src/db",          "package": "" },
    "python":     { "out": "gen",             "package": "" },
    "typescript": { "out": "src/db",          "package": "" },
    "javascript": { "out": "src/db",          "package": "" }
  }
}
```

## Top-level fields

| Field | Required | Description |
|---|:---:|---|
| `version` | yes | Must be `"1"`. |
| `engine` | yes | SQL dialect. One of `"postgresql"`, `"sqlite"`, `"mysql"`. |
| `schema` | yes | Path to a `.sql` file **or** a directory. See [Schema path](#schema-path). |
| `schema_stop_marker` | no | Strip down-migration sections. See [Migration files with up/down sections](#migration-files-with-updown-sections). |
| `queries` | yes | Query source(s). See [Queries field](#queries-field). |
| `gen` | yes | Map of language key → output config. At least one entry required. |

## Schema path

The `schema` field accepts:

- **A single file** — `"schema.sql"`.
- **A directory** — all `.sql` files in the directory are loaded in
  lexicographic order. This is ideal for numbered migration files:

  ```
  migrations/
    001_create_users.sql
    002_create_posts.sql
    003_add_tags.sql
  ```

  ```json
  "schema": "migrations/"
  ```

  sqltgen applies `CREATE TABLE`, `ALTER TABLE`, and `DROP TABLE` in file order,
  so the final schema reflects the fully-migrated state.

Statements sqltgen does not recognise (`CREATE INDEX`, `CREATE FUNCTION`, etc.)
are silently skipped.

## Migration files with up/down sections

Some migration tools (dbmate, goose, golang-migrate) store both the "up" and
"down" SQL in a single file, separated by a comment marker:

```sql
-- migrate:up
CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL);

-- migrate:down
DROP TABLE users;
```

Set `schema_stop_marker` to the down-section marker and sqltgen will discard
everything from that line onward in each file:

```json
{
  "schema": "migrations/",
  "schema_stop_marker": "-- migrate:down"
}
```

Common values by tool:

| Tool | `schema_stop_marker` |
|---|---|
| dbmate | `"-- migrate:down"` |
| goose | `"-- +goose Down"` |
| golang-migrate / sql-migrate | `"-- +migrate Down"` |

Omit the field (or set it to `null`) to read files in full — this is the
default and preserves existing behaviour.

## Queries field

The `queries` field accepts three forms.

### Single file

All queries land in one output file per language (`Queries.java`, `queries.ts`, etc.):

```json
"queries": "queries.sql"
```

### Array of paths or globs

Each file becomes its own group, named after the file stem. `users.sql` → group
`users` → `UsersQueries.java` / `users.ts`.

```json
"queries": ["queries/users.sql", "queries/posts.sql"]
```

Glob patterns are supported and are sorted lexicographically:

```json
"queries": ["queries/**/*.sql"]
```

An error is raised if a glob matches no files.

### Grouped map

Full control over group names and which files belong to each group. Values can
be a single path/glob or an array:

```json
"queries": {
  "users": "queries/users.sql",
  "posts": ["queries/posts/**/*.sql", "queries/extra.sql"]
}
```

### Output file names per group

| Group name | Java / Kotlin | Rust / Python / TypeScript / JavaScript |
|---|---|---|
| `users` | `UsersQueries.java` / `.kt` | `users.rs` / `users.py` / `users.ts` / `users.js` |
| `posts` | `PostsQueries.java` / `.kt` | `posts.rs` / `posts.py` / `posts.ts` / `posts.js` |
| _(single file)_ | `Queries.java` / `.kt` | `queries.rs` / `queries.py` / `queries.ts` / `queries.js` |

## Per-language output config (`gen.*`)

| Field | Required | Description |
|---|:---:|---|
| `out` | yes | Output root directory. Generated files are written under this path. |
| `package` | yes | Package or module name. Empty string `""` for languages without packages (Rust, Python, TypeScript, JavaScript). For Java/Kotlin: `"com.example.db"`. |
| `list_params` | no | Strategy for list/IN parameters: `"native"` (default) or `"dynamic"`. See [List parameter strategies](#list-parameter-strategies). |

## Language keys

Valid keys in the `gen` map:

| Key | Language |
|---|---|
| `java` | Java (JDBC) |
| `kotlin` | Kotlin (JDBC) |
| `rust` | Rust (sqlx) |
| `python` | Python (psycopg3 / sqlite3 / mysql-connector) |
| `typescript` | TypeScript |
| `javascript` | JavaScript (JSDoc types) |
| `go` | Go (`database/sql`) |

## List parameter strategies

When a query uses `-- @ids type[]` to pass a list to `WHERE id IN (@ids)`,
sqltgen rewrites the SQL in one of two ways.

### `native` (default)

A single bind is used with an engine-native JSON/array expression. The list
size does not need to be known at code-generation time.

| Engine | Rewritten SQL |
|---|---|
| PostgreSQL | `WHERE id = ANY($1)` — native PostgreSQL array |
| SQLite | `WHERE id IN (SELECT value FROM json_each(?))` — JSON array string |
| MySQL | `WHERE id IN (SELECT value FROM JSON_TABLE(?, '$[*]' COLUMNS (value BIGINT PATH '$')))` |

### `dynamic`

The `IN (?,?,…)` clause is built at runtime with one placeholder per element.
The SQL string is reconstructed on every call.

```json
"java": { "out": "gen", "package": "com.example.db", "list_params": "dynamic" }
```

Use `dynamic` when:
- Your driver does not support the native array/JSON approach.
- You prefer simple, portable SQL.
- Lists are always small and performance is not a concern.

## Complete example

```json
{
  "version": "1",
  "engine": "postgresql",
  "schema": "migrations/",
  "queries": {
    "users":  "sql/users.sql",
    "posts":  ["sql/posts.sql", "sql/post_tags.sql"],
    "search": "sql/search/**/*.sql"
  },
  "gen": {
    "java": {
      "out": "src/main/java",
      "package": "com.example.db",
      "list_params": "dynamic"
    },
    "typescript": {
      "out": "src/db",
      "package": ""
    }
  }
}
```
