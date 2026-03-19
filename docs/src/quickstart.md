# Quickstart

This guide walks you from zero to running code in about five minutes. We will
build a small bookstore schema, write a few queries, generate Java code, and run
it against a PostgreSQL database.

> If you prefer a different language, swap `"java"` for `"rust"`, `"kotlin"`,
> `"python"`, `"typescript"`, or `"javascript"` in the config below. The schema
> and query files stay the same.

## 1. Install sqltgen

```sh
cargo install sqltgen
sqltgen --version
```

## 2. Write your schema

Create `schema.sql`:

```sql
CREATE TABLE author (
    id         BIGSERIAL    PRIMARY KEY,
    name       TEXT         NOT NULL,
    bio        TEXT,
    birth_year INTEGER
);

CREATE TABLE book (
    id           BIGSERIAL      PRIMARY KEY,
    author_id    BIGINT         NOT NULL,
    title        TEXT           NOT NULL,
    genre        TEXT           NOT NULL,
    price        NUMERIC(10, 2) NOT NULL,
    published_at DATE
);
```

## 3. Annotate your queries

Create `queries.sql`:

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

-- name: DeleteAuthor :exec
DELETE FROM author WHERE id = @id;

-- name: GetBooksByAuthor :many
SELECT id, author_id, title, genre, price, published_at
FROM book
WHERE author_id = @author_id
ORDER BY title;
```

Each `-- name: QueryName :command` annotation names the query and tells sqltgen
what kind of result to expect:

- `:one` — return a single optional row
- `:many` — return all rows as a list
- `:exec` — execute and return nothing
- `:execrows` — execute and return the affected row count

## 4. Create the config

Create `sqltgen.json`:

```json
{
  "version": "1",
  "engine": "postgresql",
  "schema": "schema.sql",
  "queries": "queries.sql",
  "gen": {
    "java": { "out": "gen", "package": "com.example.db" }
  }
}
```

## 5. Generate

```sh
sqltgen generate
```

sqltgen writes the following files under `gen/com/example/db/`:

```
gen/com/example/db/
  Author.java        — record type for the author table
  Book.java          — record type for the book table
  Queries.java       — static query functions
  Querier.java       — DataSource-backed wrapper
```

## 6. Use the generated code

Add the PostgreSQL JDBC driver to your project:

```xml
<!-- pom.xml -->
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>42.7.3</version>
</dependency>
```

Then call the generated functions:

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Optional;
import com.example.db.Author;
import com.example.db.Queries;

Connection conn = DriverManager.getConnection(
    "jdbc:postgresql://localhost:5432/mydb", "user", "pass");

// Insert a new author
Optional<Author> author = Queries.createAuthor(conn,
    "Ursula K. Le Guin", "American author of science fiction", 1929);

// Fetch by ID
Optional<Author> found = Queries.getAuthor(conn, author.get().id());

// List all authors
List<Author> all = Queries.listAuthors(conn);

// Delete
Queries.deleteAuthor(conn, author.get().id());
```

## 7. What was generated

Open `gen/com/example/db/Queries.java`. You will find:

- A `private static final String SQL_…` constant for each query.
- A static method for each query, with typed parameters matching the `@param`
  annotations, and a return type derived from the `:command` and the inferred
  result columns.

```java
public static Optional<Author> getAuthor(Connection conn, long id)
        throws SQLException {
    try (var ps = conn.prepareStatement(SQL_GET_AUTHOR)) {
        ps.setLong(1, id);
        try (var rs = ps.executeQuery()) {
            if (!rs.next()) return Optional.empty();
            return Optional.of(new Author(
                rs.getLong(1),
                rs.getString(2),
                rs.getString(3),
                rs.getObject(4, Integer.class)
            ));
        }
    }
}
```

No reflection. No runtime overhead. The generated code does exactly what you
would write by hand — just without the repetition.

## Next steps

- [Configuration](config.md) — full reference for `sqltgen.json`
- [Writing queries](queries.md) — named params, list params, RETURNING, CTEs
- [Type mapping](types.md) — how SQL types map to each language
- [Language guides](languages/java.md) — driver setup, Querier pattern, and tips
  per language
