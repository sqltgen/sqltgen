# Writing your schema

sqltgen reads standard DDL. Point the `schema` field in `sqltgen.json` at a
single `.sql` file or a directory of migration files — sqltgen loads them all
and applies every statement in order.

## Tables

```sql
CREATE TABLE author (
    id         BIGSERIAL    PRIMARY KEY,
    name       TEXT         NOT NULL,
    bio        TEXT,                      -- nullable: no NOT NULL → nullable
    birth_year INTEGER
);

CREATE TABLE book (
    id           BIGSERIAL      PRIMARY KEY,
    author_id    BIGINT         NOT NULL,
    title        TEXT           NOT NULL,
    genre        TEXT           NOT NULL,
    price        NUMERIC(10, 2) NOT NULL,
    published_at DATE           -- nullable
);
```

**Nullability rules:**
- A column with `NOT NULL` is non-null.
- A column without `NOT NULL` is nullable.
- `PRIMARY KEY` columns are implicitly non-null.

## Views

`CREATE VIEW` is supported. sqltgen infers the column types of a view from
its `SELECT` body using the same type resolver used for query parsing:

```sql
CREATE VIEW author_book_count AS
SELECT a.id, a.name, COUNT(b.id) AS book_count
FROM author a
LEFT JOIN book b ON b.author_id = a.id
GROUP BY a.id, a.name;
```

Views are registered in the schema as first-class entities. You can query
them in annotated query files exactly like tables:

```sql
-- name: GetAuthorBookCount :one
SELECT id, name, book_count
FROM author_book_count
WHERE id = @id;
```

Views that reference other views are resolved correctly as long as they are
declared after the views they depend on.

`DROP VIEW [IF EXISTS]` is also supported.

## ALTER TABLE

sqltgen applies `ALTER TABLE` statements in declaration order, so a directory
of numbered migration files works exactly as expected:

| Operation | PostgreSQL | SQLite | MySQL |
|---|:---:|:---:|:---:|
| `ADD COLUMN [IF NOT EXISTS]` | ✅ | ✅ | ✅ |
| `DROP COLUMN [IF EXISTS]` | ✅ | — | ✅ |
| `RENAME COLUMN … TO …` | ✅ | ✅ | ✅ |
| `RENAME TO …` | ✅ | ✅ | ✅ |
| `ALTER COLUMN … SET/DROP NOT NULL` | ✅ | — | ✅ |
| `ALTER COLUMN … TYPE` | ✅ | — | ✅ |
| `ADD [CONSTRAINT …] PRIMARY KEY` | ✅ | — | ✅ |

## DROP TABLE

`DROP TABLE [IF EXISTS]` removes the table from the schema model. Subsequent
queries referencing the dropped table will produce a warning.

## What is silently ignored

Statements that do not affect the type model — `CREATE INDEX`, `CREATE FUNCTION`,
`CREATE TRIGGER`, `CREATE SEQUENCE`, `COMMENT ON`, etc. — are skipped without
error. This lets you point sqltgen at a real migration directory without curating
the SQL.

## Migration directory example

```
migrations/
  001_create_authors.sql
  002_create_books.sql
  003_add_genre_to_books.sql
  004_create_sales.sql
```

```json
"schema": "migrations/"
```

sqltgen loads the files in lexicographic order (which matches numeric prefixes)
and applies all DDL statements in sequence.

If each migration file contains both an "up" and a "down" section, use
`schema_stop_marker` to tell sqltgen where the down section begins:

```json
"schema": "migrations/",
"schema_stop_marker": "-- migrate:down"
```

Everything from the marker line onward is ignored, so only the up DDL reaches
the schema parser. See [Migration files with up/down sections](config.md#migration-files-with-updown-sections)
for the full list of supported tools.
