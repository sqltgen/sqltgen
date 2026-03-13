# sqltgen — User Guide

sqltgen is a multi-language SQL-to-code generator. You write SQL schema (DDL)
and annotated query files; sqltgen emits fully typed, idiomatic database access
code in Java, Kotlin, Rust, Python, TypeScript, and JavaScript. No ORM, no
reflection, no runtime query building — just your SQL compiled to code.

---

## Table of contents

1. [Installation](#installation)
2. [How it works](#how-it-works)
3. [Configuration](#configuration)
   - [queries field](#queries-field)
   - [Per-language output config](#per-language-output-config)
4. [Writing your schema](#writing-your-schema)
5. [Writing annotated queries](#writing-annotated-queries)
   - [Query commands](#query-commands)
   - [Named parameters](#named-parameters)
   - [Nullable parameters](#nullable-parameters)
   - [Positional parameters](#positional-parameters)
   - [List parameters (IN clauses)](#list-parameters-in-clauses)
6. [SQL features and type inference](#sql-features-and-type-inference)
   - [Simple SELECT](#simple-select)
   - [INSERT / UPDATE / DELETE with RETURNING](#insert--update--delete-with-returning)
   - [JOINs](#joins)
   - [CTEs (WITH)](#ctes-with)
   - [Subqueries](#subqueries)
   - [Aggregates and expressions](#aggregates-and-expressions)
   - [DISTINCT and LIMIT/OFFSET](#distinct-and-limitoffset)
7. [Language backends](#language-backends)
   - [Java](#java)
   - [Kotlin](#kotlin)
   - [Rust](#rust)
   - [Python](#python)
   - [TypeScript](#typescript)
   - [JavaScript](#javascript)
8. [Type mapping reference](#type-mapping-reference)
9. [List parameter strategies](#list-parameter-strategies)
10. [Running examples](#running-examples)

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

> Distribution packages (Homebrew, AUR, etc.) are planned for v0.1.0.

---

## How it works

1. Write your database schema as standard DDL (`CREATE TABLE`, `ALTER TABLE`, etc.).
2. Write your queries in `.sql` files with a `-- name: QueryName :command` annotation before each one.
3. Point a `sqltgen.json` config at your schema and query files, and specify which languages to generate.
4. Run `sqltgen generate`.

```
sqltgen generate                       # reads sqltgen.json in the current directory
sqltgen generate --config path/to/sqltgen.json
sqltgen generate -c path/to/sqltgen.json
```

sqltgen reads the schema to build a type model, then parses each query against
that model to infer parameter types and result column types. It then emits source
files — one model type per table, one file of query functions — for every
configured language.

---

## Configuration

The config file is JSON, named `sqltgen.json` by default.

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

### Fields

| Field | Required | Description |
|---|:---:|---|
| `version` | yes | Must be `"1"`. |
| `engine` | yes | SQL dialect: `"postgresql"`, `"sqlite"`, or `"mysql"`. |
| `schema` | yes | Path to a `.sql` file **or** a directory. If a directory, all `.sql` files are loaded in lexicographic order (ideal for numbered migration files like `001_create_users.sql`). |
| `queries` | yes | Path to a `.sql` file, a list of paths, or glob patterns. See below. |
| `gen` | yes | Map of language key → output config. At least one entry required. |

### `queries` field

The `queries` field accepts three forms.

**Single file** — all queries land in one output file per language (`Queries.java`,
`queries.ts`, etc.):
```json
"queries": "queries.sql"
```

**Array of paths/globs** — each file becomes its own group, named after the file
stem. `users.sql` → group `users` → `UsersQueries.java` / `users.ts`.
Files with the same stem are merged into one group:
```json
"queries": ["queries/users.sql", "queries/posts.sql"]
```

Glob patterns are supported in both forms and are sorted lexicographically.
An error is raised if a pattern matches no files:
```json
"queries": ["queries/**/*.sql"]
```

**Grouped map** — explicit group names with full control over which files belong
to each group. Values can be a single path/glob or an array:
```json
"queries": {
  "users": "queries/users.sql",
  "posts": ["queries/posts/**/*.sql", "queries/extra.sql"]
}
```

Each named group produces its own output file:

| Group name | Java / Kotlin | Rust / Python / TypeScript / JavaScript |
|---|---|---|
| `users` | `UsersQueries.java` / `.kt` | `users.rs` / `users.py` / `users.ts` / `users.js` |
| `posts` | `PostsQueries.java` / `.kt` | `posts.rs` / `posts.py` / `posts.ts` / `posts.js` |

The single-file form always produces the default name (`Queries.java`,
`queries.ts`, etc.) regardless of the file name.

### Per-language output config

| Field | Required | Description |
|---|:---:|---|
| `out` | yes | Output root directory. Files are written under this path. |
| `package` | yes | Package/module name. Empty string `""` for languages that don't use packages (Rust, Python, TypeScript, JavaScript). For Java/Kotlin, e.g. `"com.example.db"`. |
| `list_params` | no | Strategy for list/IN parameters: `"native"` (default) or `"dynamic"`. See [List parameter strategies](#list-parameter-strategies). |

---

## Writing your schema

sqltgen reads standard DDL. Write `CREATE TABLE` statements; sqltgen tracks
column names, types, and nullability. Use `ALTER TABLE` statements freely —
sqltgen applies them in order, so a directory of numbered migration files works
perfectly.

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
- A column with `NOT NULL` is non-nullable.
- A column without `NOT NULL` is nullable.
- `PRIMARY KEY` columns are implicitly non-nullable.

**What sqltgen tracks:**
- `CREATE TABLE` — adds the table and all its columns.
- `ALTER TABLE ADD COLUMN` — adds a new column to an existing table.
- `ALTER TABLE DROP COLUMN` — removes a column.
- `ALTER TABLE RENAME COLUMN` — renames a column.
- `ALTER TABLE RENAME TO` — renames a table.
- `ALTER TABLE ALTER COLUMN … SET/DROP NOT NULL` — changes nullability.
- `ALTER TABLE ALTER COLUMN … TYPE` — changes the column type.
- `DROP TABLE` — removes the table.

**What is silently ignored:**
Statements that don't affect the type model — `CREATE INDEX`, `CREATE FUNCTION`,
`CREATE TRIGGER`, sequences, comments, etc. — are skipped without error. This
lets you point sqltgen at a real migration directory without curating the SQL.

---

## Writing annotated queries

Each query block starts with a `-- name:` annotation:

```sql
-- name: QueryName :command
SELECT …
```

The `QueryName` is used to derive function/method names in all generated
backends (in the target language's naming convention). The `:command` determines
the return type.

### Query commands

| Command | What it returns |
|---|---|
| `:one` | A single optional row (`Optional<T>`, `T?`, `Option<T>`, `T \| None`, `T \| null`) |
| `:many` | All rows (`List<T>`, `Vec<T>`, `list[T]`, `T[]`, `Promise<T[]>`) |
| `:exec` | Nothing (`void`, `Unit`, `()`, `None`, `Promise<void>`) |
| `:execrows` | Number of affected rows (`long`, `Long`, `u64`, `int`, `Promise<number>`) |

### Named parameters

Use `@param_name` in the SQL body. sqltgen infers the parameter's type from the
context in which it is used (compared against a column, bound in a SET clause,
etc.).

```sql
-- name: GetBook :one
SELECT id, author_id, title, genre, price, published_at
FROM book
WHERE id = @id;
```

Named parameters become named function arguments in every generated backend.
`@id` above becomes `id: long` in Java, `id: i64` in Rust, `id: int` in Python,
and so on.

### Nullable parameters

A parameter is non-nullable by default (inferred from the column it's compared
against). To mark it explicitly nullable, add a `-- @name null` annotation before
the query:

```sql
-- name: UpdateAuthorBio :one
-- @bio null
UPDATE author SET bio = @bio WHERE id = @id
RETURNING *;
```

To explicitly force non-null:
```sql
-- @bio not null
```

You can also override the type entirely:
```sql
-- @published_at date not null
```

Annotations apply only to the query immediately following them.

### Positional parameters

Instead of `@name`, you can use engine-native positional placeholders directly:

- PostgreSQL: `$1`, `$2`, `$3`, …
- SQLite: `?1`, `?2`, `?3`, …

```sql
-- name: ListBooksWithLimit :many
SELECT id, title, genre, price
FROM book
ORDER BY title
LIMIT $1 OFFSET $2;
```

Positional parameters get auto-generated names (`p1`, `p2`, etc.) in function
signatures.

### List parameters (IN clauses)

To pass a variable-length list for an `IN` clause, declare the parameter as an
array type:

```sql
-- name: GetBooksByIds :many
-- @ids bigint[] not null
SELECT id, author_id, title, genre, price, published_at
FROM book
WHERE id IN (@ids)
ORDER BY title;
```

The generated function accepts a collection type (`List<Long>` in Java, `&[i64]`
in Rust, `list[int]` in Python, `number[]` in TypeScript). The SQL is rewritten
to the engine's native approach — see [List parameter strategies](#list-parameter-strategies).

---

## SQL features and type inference

### Simple SELECT

Column types are resolved from the schema. Aliases are preserved as the field name.

```sql
-- name: GetAuthor :one
SELECT id, name, bio, birth_year
FROM author
WHERE id = @id;
```

When the result is a simple `SELECT *` or `SELECT t.*` from a single table, the
generated function reuses the table's existing model type (e.g. `Author`) instead
of emitting a new struct.

```sql
-- name: GetAllBookFields :many
SELECT b.*
FROM book b
ORDER BY b.id;
-- returns: List<Book> (reuses the Book model)
```

### INSERT / UPDATE / DELETE with RETURNING

`RETURNING` columns are resolved just like SELECT projections.

```sql
-- name: CreateAuthor :one
INSERT INTO author (name, bio, birth_year)
VALUES (@name, @bio, @birth_year)
RETURNING *;
-- returns: Optional<Author> (RETURNING * over a single table reuses Author)

-- name: DeleteAuthor :one
DELETE FROM author WHERE id = @id
RETURNING id, name;
-- returns: Optional<DeleteAuthorRow> (explicit column list → new inline type)
```

### JOINs

Result columns from JOINs are resolved per-table. Columns from the nullable
side of an outer join (LEFT/RIGHT/FULL JOIN) are automatically made nullable.
When an explicit column list spans multiple tables, an inline row type is emitted
(e.g. `ListBooksWithAuthorRow`).

```sql
-- name: ListBooksWithAuthor :many
SELECT b.id, b.title, b.genre, b.price, b.published_at,
       a.name AS author_name, a.bio AS author_bio
FROM book b
JOIN author a ON a.id = b.author_id
ORDER BY b.title;
-- emits: ListBooksWithAuthorRow { id, title, genre, price, published_at,
--                                 author_name, author_bio }

-- name: GetBooksNeverOrdered :many
SELECT b.id, b.author_id, b.title, b.genre, b.price, b.published_at
FROM book b
LEFT JOIN sale_item si ON si.book_id = b.id
WHERE si.id IS NULL
ORDER BY b.title;
-- returns: Book (LEFT JOIN — but result columns are only from b, so
--          book columns stay non-nullable, reuses Book type)
```

### CTEs (WITH)

CTE result columns are available as if they were tables. Data-modifying CTEs
(`WITH archived AS (DELETE … RETURNING …) SELECT …`) are fully supported.

```sql
-- name: GetTopSellingBooks :many
WITH book_sales AS (
    SELECT book_id, SUM(quantity) AS units_sold
    FROM sale_item
    GROUP BY book_id
)
SELECT b.id, b.title, b.genre, b.price, bs.units_sold
FROM book b
JOIN book_sales bs ON bs.book_id = b.id
ORDER BY bs.units_sold DESC;
-- emits: GetTopSellingBooksRow { id, title, genre, price, units_sold: Option<i64> }

-- name: ArchiveAndReturnBooks :many
WITH archived AS (
    DELETE FROM book WHERE published_at < $1 RETURNING id, title, genre, price
)
SELECT id, title, genre, price FROM archived ORDER BY title;
```

### Subqueries

Subqueries in `WHERE`, `EXISTS`, scalar `SELECT` in the projection list, and
derived tables in `FROM` are all supported.

```sql
-- name: GetBooksNotByAuthor :many
SELECT id, title, genre
FROM book
WHERE author_id NOT IN (SELECT id FROM author WHERE name = $1)
ORDER BY title;

-- name: GetBookWithAuthorName :many
SELECT b.id, b.title,
       (SELECT a.name FROM author a WHERE a.id = b.author_id) AS author_name
FROM book b
ORDER BY b.title;
-- author_name: String | null (scalar subquery → nullable)
```

### Aggregates and expressions

Aggregate functions (`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`) are supported.
`COUNT(*)` and `COUNT(col)` always produce a non-null integer. `SUM`, `MIN`,
`MAX`, and `AVG` over a nullable column or a LEFT JOIN group produce a nullable
result.

```sql
-- name: CountBooksByGenre :many
SELECT genre, COUNT(*) AS book_count
FROM book
GROUP BY genre
ORDER BY genre;

-- name: GetSaleItemQuantityAggregates :one
SELECT MIN(quantity) AS min_qty,
       MAX(quantity) AS max_qty,
       SUM(quantity) AS sum_qty,
       AVG(quantity) AS avg_qty
FROM sale_item;
```

`CASE WHEN … END`, `COALESCE`, `HAVING`, `BETWEEN`, `LIKE`, and `EXISTS` parameters
are all inferred from their context.

```sql
-- name: GetGenresWithManyBooks :many
SELECT genre, COUNT(*) AS book_count
FROM book
GROUP BY genre
HAVING COUNT(*) > $1   -- $1 inferred as integer
ORDER BY genre;

-- name: GetBookPriceLabel :many
SELECT id, title, price,
       CASE WHEN price > $1 THEN 'expensive' ELSE 'affordable' END AS price_label
FROM book
ORDER BY title;
-- price_label: String (non-null — CASE with string literals)
```

### DISTINCT and LIMIT/OFFSET

```sql
-- name: GetDistinctGenres :many
SELECT DISTINCT genre FROM book ORDER BY genre;

-- name: ListBooksWithLimit :many
SELECT id, title, genre, price FROM book ORDER BY title
LIMIT $1 OFFSET $2;
```

---

## Language backends

### Java

**Driver:** JDBC (standard `java.sql`)

**Config:**
```json
"java": { "out": "src/main/java", "package": "com.example.db" }
```

**What is generated:**

One `{TableName}.java` Java record per table:
```java
// src/main/java/com/example/db/Author.java
package com.example.db;

public record Author(
    long id,
    String name,
    String bio,         // nullable → String (null if absent)
    Integer birthYear   // nullable → boxed Integer
) {}
```

One `Queries.java` with static methods + inline row records for non-table result
types (or `UsersQueries.java` / `PostsQueries.java` when using [query grouping](#queries-field)):
```java
package com.example.db;
// ...
public final class Queries {

    public static Optional<Author> getAuthor(Connection conn, long id)
            throws SQLException { … }

    public static List<Author> listAuthors(Connection conn)
            throws SQLException { … }

    public static Optional<Author> createAuthor(Connection conn,
            String name, String bio, Integer birthYear)
            throws SQLException { … }

    public static long deleteBookById(Connection conn, long p1)
            throws SQLException { … }  // :execrows
}
```

One `QueriesDs.java` (or `UsersQueriesDs.java` etc.) — a DataSource-backed
wrapper that opens and closes its own connection per call:
```java
QueriesDs qds = new QueriesDs(dataSource);
Optional<Author> a = qds.getAuthor(42L);
```

**Wiring up in a Maven project:**

```xml
<!-- pom.xml — add your JDBC driver, e.g. for PostgreSQL: -->
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <version>42.7.3</version>
</dependency>
```

```java
import java.sql.Connection;
import java.sql.DriverManager;
import com.example.db.Queries;
import com.example.db.Author;

Connection conn = DriverManager.getConnection(
    "jdbc:postgresql://localhost:5432/mydb", "user", "pass");

Optional<Author> author = Queries.getAuthor(conn, 1L);
List<Author> all = Queries.listAuthors(conn);
```

**Column name convention:** SQL `snake_case` → Java `camelCase` (`birth_year` → `birthYear`).

---

### Kotlin

**Driver:** JDBC (standard `java.sql`)

**Config:**
```json
"kotlin": { "out": "src/main/kotlin", "package": "com.example.db" }
```

**What is generated:**

One `{TableName}.kt` Kotlin data class per table:
```kotlin
// src/main/kotlin/com/example/db/Author.kt
package com.example.db

data class Author(
    val id: Long,
    val name: String,
    val bio: String?,      // nullable → T?
    val birthYear: Int?
)
```

One `Queries.kt` Kotlin object with functions:
```kotlin
object Queries {
    fun getAuthor(conn: Connection, id: Long): Author? { … }
    fun listAuthors(conn: Connection): List<Author> { … }
    fun createAuthor(conn: Connection, name: String, bio: String?,
                     birthYear: Int?): Author? { … }
    fun deleteBookById(conn: Connection, p1: Long): Long { … } // :execrows
}
```

One `QueriesDs.kt` — DataSource-backed wrapper, same pattern as Java.

**Wiring up:**
```kotlin
import java.sql.DriverManager
import com.example.db.Queries

val conn = DriverManager.getConnection(
    "jdbc:postgresql://localhost:5432/mydb", "user", "pass")

val author: Author? = Queries.getAuthor(conn, 1L)
val all: List<Author> = Queries.listAuthors(conn)
```

---

### Rust

**Driver:** [sqlx](https://github.com/launchbadge/sqlx) (async)

**Config:**
```json
"rust": { "out": "src/db", "package": "" }
```

**What is generated:**

One `{table_name}.rs` struct per table with `sqlx::FromRow`:
```rust
// src/db/author.rs
#[derive(Debug, sqlx::FromRow)]
pub struct Author {
    pub id: i64,
    pub name: String,
    pub bio: Option<String>,
    pub birth_year: Option<i32>,
}
```

One `queries.rs` with async functions:
```rust
// src/db/queries.rs
use sqlx::PgPool;
use super::author::Author;

pub async fn get_author(pool: &PgPool, id: i64)
        -> Result<Option<Author>, sqlx::Error> {
    sqlx::query_as::<_, Author>(
        "SELECT id, name, bio, birth_year FROM author WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn list_authors(pool: &PgPool)
        -> Result<Vec<Author>, sqlx::Error> { … }

pub async fn delete_book_by_id(pool: &PgPool, p1: i64)
        -> Result<u64, sqlx::Error> { … }  // :execrows
```

One `mod.rs` that re-exports all modules (table modules + one module per query group).

**Wiring up:**

Add to `Cargo.toml`:
```toml
[dependencies]
sqlx = { version = "0.8", features = ["runtime-tokio", "postgres", "time", "uuid", "rust_decimal"] }
tokio = { version = "1", features = ["full"] }
```

Point sqltgen at `src/db` and then:
```rust
mod db;
use db::queries;

let pool = sqlx::PgPool::connect("postgres://user:pass@localhost/mydb").await?;
let author = queries::get_author(&pool, 1).await?;
let all = queries::list_authors(&pool).await?;
```

**Notes:**
- PostgreSQL pool type: `sqlx::PgPool`; SQLite: `sqlx::SqlitePool`; MySQL: `sqlx::MySqlPool`.
- Inline row types (JOINs, partial RETURNING, etc.) are emitted as additional structs in `queries.rs`.
- List params use `= ANY($1)` (PostgreSQL), `json_each` (SQLite), or `JSON_TABLE` (MySQL) by default.

---

### Python

**Driver:** psycopg (psycopg3) for PostgreSQL · sqlite3 (stdlib) for SQLite · mysql-connector-python for MySQL

**Config:**
```json
"python": { "out": "gen", "package": "" }
```

**What is generated:**

One `{table_name}.py` dataclass per table:
```python
# gen/author.py
from __future__ import annotations
import dataclasses

@dataclasses.dataclass
class Author:
    id: int
    name: str
    bio: str | None
    birth_year: int | None
```

One `queries.py` with typed functions:
```python
# gen/queries.py
import psycopg
from .author import Author

def get_author(conn: psycopg.Connection, id: int) -> Author | None:
    with conn.cursor() as cur:
        cur.execute(SQL_GET_AUTHOR, (id,))
        row = cur.fetchone()
        if row is None:
            return None
        return Author(*row)

def list_authors(conn: psycopg.Connection) -> list[Author]:
    with conn.cursor() as cur:
        cur.execute(SQL_LIST_AUTHORS)
        return [Author(*row) for row in cur.fetchall()]

def delete_book_by_id(conn: psycopg.Connection, p1: int) -> int:
    with conn.cursor() as cur:
        cur.execute(SQL_DELETE_BOOK_BY_ID, (p1,))
        return cur.rowcount  # :execrows
```

One `__init__.py` barrel import.

**Wiring up (PostgreSQL):**
```sh
pip install psycopg
```
```python
import psycopg
from gen.queries import get_author, list_authors

with psycopg.connect("postgresql://user:pass@localhost/mydb") as conn:
    author = get_author(conn, 1)
    all_authors = list_authors(conn)
    conn.commit()
```

**Wiring up (SQLite):**
```python
import sqlite3
from gen.queries import get_author

conn = sqlite3.connect("mydb.db")
author = get_author(conn, 1)
```

**Wiring up (MySQL):**
```sh
pip install mysql-connector-python
```
```python
import mysql.connector
from gen.queries import get_author

conn = mysql.connector.connect(host="localhost", database="mydb",
                                user="user", password="pass")
author = get_author(conn, 1)
```

**Notes:**
- Results are unpacked positionally (`Author(*row)`), so column order in the query
  must match the dataclass field order.
- JSON columns: psycopg3 automatically deserializes JSON to Python objects (`object`);
  sqlite3 and mysql-connector return the raw JSON string (`str`).

---

### TypeScript

**Driver:** [pg](https://node-postgres.com) (node-postgres) for PostgreSQL · [better-sqlite3](https://github.com/WiseLibs/better-sqlite3) for SQLite · [mysql2](https://github.com/sidorares/node-mysql2) for MySQL

**Config:**
```json
"typescript": { "out": "src/db", "package": "" }
```

**What is generated:**

One `{table_name}.ts` interface per table:
```typescript
// src/db/author.ts
export interface Author {
  id: number;
  name: string;
  bio: string | null;
  birth_year: number | null;
}
```

One `queries.ts` with async functions:
```typescript
// src/db/queries.ts
import type { ClientBase } from 'pg';
import type { Author } from './author';

export async function getAuthor(
    db: ClientBase, id: number): Promise<Author | null> {
  const result = await db.query<Author>(SQL_GET_AUTHOR, [id]);
  return result.rows[0] ?? null;
}

export async function listAuthors(db: ClientBase): Promise<Author[]> {
  const result = await db.query<Author>(SQL_LIST_AUTHORS);
  return result.rows;
}

export async function deleteBookById(
    db: ClientBase, p1: number): Promise<number> {
  const result = await db.query(SQL_DELETE_BOOK_BY_ID, [p1]);
  return result.rowCount ?? 0;  // :execrows
}
```

One `index.ts` barrel export.

**Wiring up (PostgreSQL):**
```sh
npm install pg
npm install --save-dev @types/pg
```
```typescript
import { Client } from 'pg';
import { getAuthor, listAuthors } from './src/db/queries';

const client = new Client({ connectionString: 'postgres://user:pass@localhost/mydb' });
await client.connect();

const author = await getAuthor(client, 1);
const all = await listAuthors(client);
```

**Wiring up (SQLite):**
```sh
npm install better-sqlite3
npm install --save-dev @types/better-sqlite3
```
```typescript
import Database from 'better-sqlite3';
import { getAuthor } from './src/db/queries';

const db = new Database('mydb.db');
const author = getAuthor(db, 1);  // SQLite functions are synchronous
```

**Wiring up (MySQL):**
```sh
npm install mysql2
```
```typescript
import mysql from 'mysql2/promise';
import { getAuthor } from './src/db/queries';

const conn = await mysql.createConnection({ host: 'localhost', database: 'mydb',
                                            user: 'user', password: 'pass' });
const author = await getAuthor(conn, 1);
```

**Note:** SQLite better-sqlite3 is synchronous. Generated SQLite functions do not
return `Promise` — they return values directly.

---

### JavaScript

**Driver:** same as TypeScript (pg / better-sqlite3 / mysql2)

**Config:**
```json
"javascript": { "out": "src/db", "package": "" }
```

JavaScript output is identical to TypeScript except:
- Files end in `.js` instead of `.ts`.
- Types are expressed as JSDoc `@typedef` and `@param`/`@returns` annotations
  instead of inline TypeScript syntax.

```javascript
// src/db/author.js
/**
 * @typedef {Object} Author
 * @property {number} id
 * @property {string} name
 * @property {string | null} bio
 * @property {number | null} birth_year
 */

// src/db/queries.js
/**
 * @param {import('pg').ClientBase} db
 * @param {number} id
 * @returns {Promise<Author | null>}
 */
export async function getAuthor(db, id) { … }
```

Wiring up is identical to TypeScript — use the same drivers.

---

## Type mapping reference

| SQL type | Java | Kotlin | Rust | Python | TypeScript / JS |
|---|---|---|---|---|---|
| `BOOLEAN` | `boolean` / `Boolean` | `Boolean` | `bool` | `bool` | `boolean` |
| `SMALLINT` | `short` / `Short` | `Short` | `i16` | `int` | `number` |
| `INTEGER` / `INT` | `int` / `Integer` | `Int` | `i32` | `int` | `number` |
| `BIGINT` / `BIGSERIAL` | `long` / `Long` | `Long` | `i64` | `int` | `number` ⚠️ |
| `REAL` / `FLOAT` | `float` / `Float` | `Float` | `f32` | `float` | `number` |
| `DOUBLE PRECISION` | `double` / `Double` | `Double` | `f64` | `float` | `number` |
| `NUMERIC` / `DECIMAL` | `BigDecimal` | `BigDecimal` | `rust_decimal::Decimal` | `decimal.Decimal` | `number` |
| `TEXT` / `VARCHAR` / `CHAR` | `String` | `String` | `String` | `str` | `string` |
| `BYTEA` / `BLOB` | `byte[]` | `ByteArray` | `Vec<u8>` | `bytes` | `Buffer` |
| `DATE` | `LocalDate` | `LocalDate` | `time::Date` | `datetime.date` | `Date` |
| `TIME` | `LocalTime` | `LocalTime` | `time::Time` | `datetime.time` | `Date` |
| `TIMESTAMP` | `LocalDateTime` | `LocalDateTime` | `time::PrimitiveDateTime` | `datetime.datetime` | `Date` |
| `TIMESTAMPTZ` | `OffsetDateTime` | `OffsetDateTime` | `time::OffsetDateTime` | `datetime.datetime` | `Date` |
| `INTERVAL` | `String` | `String` | `String` | `datetime.timedelta` | `string` |
| `UUID` | `UUID` | `UUID` | `uuid::Uuid` | `uuid.UUID` | `string` |
| `JSON` | `String` | `String` | `serde_json::Value` | `object` (psycopg3) / `str` (others) | `unknown` |
| `JSONB` | `String` | `String` | `serde_json::Value` | `object` (psycopg3) | `unknown` |
| `TEXT[]` / `type[]` | `List<T>` | `List<T>` | `Vec<T>` | `list[T]` | `T[]` |
| Unknown type | `Object` | `Any` | `serde_json::Value` | `Any` | `unknown` |

> ⚠️ `BIGINT` in TypeScript/JavaScript maps to `number`, which loses precision
> for values above 2^53. Use `BigInt` in application code if your IDs or values
> exceed this range.

**Nullable columns:** non-null types are used as-is for `NOT NULL` columns;
nullable wrappers (`Integer`, `Optional<T>`, `T?`, `Option<T>`, `T | None`,
`T | null`) are used for nullable columns.

---

## List parameter strategies

When you use `-- @ids type[]` to pass a list to `WHERE id IN (@ids)`, sqltgen
can rewrite the SQL in two ways:

### `native` (default)

A single bind is used with an engine-native JSON/array unpacking expression.
The number of elements doesn't need to be known at compile time.

| Engine | SQL rewrite |
|---|---|
| PostgreSQL | `WHERE id = ANY($1)` — binds the list as a native PostgreSQL array |
| SQLite | `WHERE id IN (SELECT value FROM json_each(?))` — JSON array string |
| MySQL | `WHERE id IN (SELECT value FROM JSON_TABLE(?, '$[*]' COLUMNS (value BIGINT PATH '$')))` |

### `dynamic`

The `IN (?,?,…)` clause is built at runtime with one placeholder per element.
The SQL string is reconstructed on every call.

```json
"java": { "out": "gen", "package": "com.example.db", "list_params": "dynamic" }
```

Use `dynamic` when:
- Your database or driver doesn't support the native array/JSON approach.
- You prefer simpler, portable SQL.
- The list is always small and performance isn't a concern.

---

## Running examples

Runnable example projects for all six backends × three dialects live in
`examples/`. Each is a self-contained project with its own `Makefile`.

```sh
# Single example
make -C examples/rust/sqlite run
make -C examples/java/postgresql run
make -C examples/python/mysql run

# All examples at once (one shared PostgreSQL container, one MySQL container)
make run-all
```

See `examples/README.md` for prerequisites (Docker, Java 21, Node 22, Python 3.11+).
