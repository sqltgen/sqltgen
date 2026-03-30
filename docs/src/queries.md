# Writing queries

sqltgen reads query files containing standard SQL with a lightweight annotation
format. Each query starts with a `-- name:` comment that gives it a name and a
command.

## Annotation syntax

```sql
-- name: QueryName :command
SELECT …
```

- **`QueryName`** — PascalCase name used to derive function/method names in all
  generated backends (converted to `camelCase` for Java/Kotlin/TypeScript/JavaScript
  and `snake_case` for Rust/Python).
- **`:command`** — see [Query commands](#query-commands) below.

The annotation must appear immediately before the SQL statement. Any whitespace
between the annotation and the statement is ignored.

Multiple queries live in the same file, separated by their annotations:

```sql
-- name: GetAuthor :one
SELECT id, name FROM author WHERE id = @id;

-- name: ListAuthors :many
SELECT id, name FROM author ORDER BY name;
```

## Query commands

| Command | Return type | Notes |
|---|---|---|
| `:one` | One optional row | `Optional<T>` / `T?` / `Option<T>` / `T \| None` / `T \| null` |
| `:many` | All rows as a list | `List<T>` / `Vec<T>` / `list[T]` / `T[]` |
| `:exec` | Nothing | `void` / `Unit` / `()` / `None` / `Promise<void>` |
| `:execrows` | Affected row count | `long` / `Long` / `u64` / `int` / `Promise<number>` |

## Named parameters

Use `@param_name` in the SQL body. sqltgen infers the parameter type from the
context in which it is used — a `WHERE` clause comparison, a `SET` assignment,
a `VALUES` row, etc.

```sql
-- name: GetBook :one
SELECT id, author_id, title, genre, price, published_at
FROM book
WHERE id = @id;
```

`@id` above is compared against the `book.id` column (`BIGINT NOT NULL`), so
sqltgen infers `id: i64` in Rust, `id: long` in Java, `id: int` in Python, etc.

Named parameters become named function arguments in every generated backend.

### Type and nullability overrides

Above the `-- name:` annotation, you can add per-parameter annotation lines to
override the inferred type or nullability:

```sql
-- name: UpdateAuthorBio :exec
-- @bio null
UPDATE author SET bio = @bio WHERE id = @id;
```

| Annotation | Effect |
|---|---|
| `-- @name null` | Mark the parameter nullable |
| `-- @name not null` | Mark the parameter non-null |
| `-- @name type` | Override the type (e.g. `-- @published_at date`) |
| `-- @name type null` | Override type and mark nullable |
| `-- @name type not null` | Override type and mark non-null |

Valid types follow the SQL type names used in the [type mapping reference](types.md):
`bigint`, `text`, `boolean`, `date`, `timestamp`, `uuid`, etc.

Annotations apply only to the query immediately following them.

## Positional parameters

Instead of `@name`, you can use the engine's native positional placeholders:

- PostgreSQL: `$1`, `$2`, `$3`, …
- SQLite: `?1`, `?2`, `?3`, …
- MySQL: `$1`, `$2`, `$3`, …

```sql
-- name: ListBooksWithLimit :many
SELECT id, title, genre, price
FROM book
ORDER BY title
LIMIT $1 OFFSET $2;
```

Positional parameters get auto-generated names (`p1`, `p2`, …) in generated
function signatures.

Named and positional parameters cannot be mixed in the same query.

## List parameters (IN clauses)

To pass a variable-length list to an `IN` clause, declare the parameter with
an array type annotation:

```sql
-- name: GetBooksByIds :many
-- @ids bigint[] not null
SELECT id, author_id, title, genre, price, published_at
FROM book
WHERE id IN (@ids)
ORDER BY title;
```

The generated function accepts a collection (`List<Long>` in Java, `&[i64]` in
Rust, `list[int]` in Python, `number[]` in TypeScript). The SQL is rewritten to
the engine's native expression — see
[List parameter strategies](config.md#list-parameter-strategies).

## Nested result annotations (`-- nest:`)

Use `-- nest:` to map flat result columns into nested arrays in generated
TypeScript/JavaScript query row types.

```sql
-- name: GetUserWithCompanies :many
-- nest: company(company_id, company_name)
SELECT u.id, u.name,
       c.id AS company_id, c.name AS company_name
FROM users u
LEFT JOIN companies c ON c.user_id = u.id;
```

Rules:

- Supported only for `:one` and `:many`.
- `field` in `-- nest: field(...)` must be a valid JS identifier.
- Column entries can be `source_col` or `source_col as alias`.
- `-- nest:` cannot currently be combined with list parameters.

For detailed parser/codegen behavior and rationale, see
[Nested query results](nested-results.md).

## RETURNING clauses

`RETURNING` columns are resolved just like SELECT projections. When `RETURNING *`
targets a single table, sqltgen reuses the existing table model type:

```sql
-- name: CreateAuthor :one
INSERT INTO author (name, bio, birth_year)
VALUES (@name, @bio, @birth_year)
RETURNING *;
-- returns: Optional<Author> (reuses Author type)

-- name: DeleteAuthor :one
DELETE FROM author WHERE id = @id
RETURNING id, name;
-- returns: Optional<DeleteAuthorRow> (explicit column list → inline type)
```

## JOINs

Result columns from JOINs are resolved per-table. Columns from the nullable
side of an outer join (LEFT/RIGHT/FULL JOIN) are automatically made nullable.
When the result spans multiple tables, an inline row type is emitted:

```sql
-- name: ListBooksWithAuthor :many
SELECT b.id, b.title, b.genre, b.price,
       a.name AS author_name, a.bio AS author_bio
FROM book b
JOIN author a ON a.id = b.author_id
ORDER BY b.title;
-- emits: ListBooksWithAuthorRow { id, title, genre, price, author_name, author_bio }
```

## CTEs (WITH)

CTE result columns are available to the outer query as if they were tables.
Data-modifying CTEs (`WITH … DELETE … RETURNING …`) are fully supported:

```sql
-- name: GetTopSellingBooks :many
WITH book_sales AS (
    SELECT book_id, SUM(quantity) AS units_sold
    FROM sale_item
    GROUP BY book_id
)
SELECT b.id, b.title, b.genre, bs.units_sold
FROM book b
JOIN book_sales bs ON bs.book_id = b.id
ORDER BY bs.units_sold DESC;

-- name: ArchiveOldBooks :many
WITH archived AS (
    DELETE FROM book WHERE published_at < $1 RETURNING id, title
)
SELECT id, title FROM archived ORDER BY title;
```

## Subqueries

Subqueries in `WHERE`, `EXISTS`, scalar positions in the SELECT list, and
derived tables in `FROM` are all supported:

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
-- author_name is nullable (scalar subquery → may return NULL)
```

## Aggregates and expressions

`COUNT`, `SUM`, `MIN`, `MAX`, `AVG` are all supported:

- `COUNT(*)` and `COUNT(col)` always produce a non-null integer.
- `SUM`, `MIN`, `MAX`, `AVG` produce a nullable result if the argument or its
  source column is nullable, or when the query has a LEFT JOIN.

```sql
-- name: CountBooksByGenre :many
SELECT genre, COUNT(*) AS book_count
FROM book
GROUP BY genre
ORDER BY genre;

-- name: GetSaleStats :one
SELECT MIN(quantity) AS min_qty,
       MAX(quantity) AS max_qty,
       SUM(quantity) AS sum_qty
FROM sale_item;
```

`CASE WHEN … END`, `COALESCE`, `BETWEEN`, `LIKE`, `HAVING`, and `EXISTS`
parameters are all inferred from their surrounding context.

## UNION / INTERSECT / EXCEPT

Set operations are supported. Result columns are derived from the first branch,
and column types are coerced across branches:

```sql
-- name: GetAllPeopleNames :many
SELECT name FROM author
UNION
SELECT display_name AS name FROM user_account
ORDER BY name;
```

## DISTINCT and LIMIT / OFFSET

```sql
-- name: GetDistinctGenres :many
SELECT DISTINCT genre FROM book ORDER BY genre;

-- name: ListBooksWithLimit :many
SELECT id, title, genre, price FROM book ORDER BY title
LIMIT $1 OFFSET $2;
```

## Table-wildcard reuse

When a query selects all columns from a single table (`SELECT *` or `SELECT t.*`)
and the join structure means no outer-join nullability is introduced, sqltgen
reuses the table's existing model type instead of emitting a per-query row struct:

```sql
-- name: GetAllBookFields :many
SELECT b.*
FROM book b
ORDER BY b.id;
-- returns: List<Book>  (not List<GetAllBookFieldsRow>)
```
