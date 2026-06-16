# Rust

sqltgen generates async Rust code using [sqlx](https://github.com/launchbadge/sqlx).
Model structs derive `sqlx::FromRow`. Query functions are `async fn` returning
`Result<…, sqlx::Error>`, generic over `sqlx::Executor` so they accept a pool or a
transaction.

## Configuration

```json
"rust": {
  "out": "src/db",
  "package": ""
}
```

| Field | Description |
|---|---|
| `out` | Output directory (relative to the project root). |
| `package` | Unused for Rust — set to `""`. |
| `list_params` | `"native"` (default) or `"dynamic"`. |

## What is generated

```
src/db/
  mod.rs              — re-exports all modules
  author.rs           — Author struct with sqlx::FromRow
  book.rs             — Book struct
  _sqltgen.rs         — shared helper trait (SqlxAdapter)
  queries.rs          — async query functions  (single-file form)
  users.rs            — per-group query files  (grouped form)
```

### Model structs

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

- Non-null columns → bare type (`i64`, `String`, `bool`, …).
- Nullable columns → `Option<T>`.
- `snake_case` SQL names → `snake_case` Rust field names (unchanged).

### Query functions

Each function is generic over `sqlx::Executor`, so the same function runs against a
connection pool, a pooled connection, or a transaction. The `Database` bound is fixed
to the configured engine (`sqlx::Postgres` / `sqlx::Sqlite` / `sqlx::MySql`).

```rust
// src/db/queries.rs
use super::author::Author;

pub async fn get_author<'e, E>(executor: E, id: i64)
        -> Result<Option<Author>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query_as::<_, Author>(SQL_GET_AUTHOR)
        .bind(id)
        .fetch_optional(executor)
        .await
}

pub async fn list_authors<'e, E>(executor: E)
        -> Result<Vec<Author>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query_as::<_, Author>(SQL_LIST_AUTHORS)
        .fetch_all(executor)
        .await
}

pub async fn delete_author<'e, E>(executor: E, id: i64)
        -> Result<(), sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query(SQL_DELETE_AUTHOR)
        .bind(id)
        .execute(executor)
        .await
        .map(|_| ())
}
```

### `_sqltgen.rs` helper

The generated `_sqltgen.rs` file provides a shared `SqlxAdapter` trait that
abstracts over the three pool types. This is an implementation detail — you do
not need to call it directly.

### mod.rs

```rust
pub mod author;
pub mod book;
pub mod queries;
mod _sqltgen;
```

## Wiring up

### Cargo.toml

```toml
[dependencies]
sqlx = { version = "0.8", features = [
    "runtime-tokio",
    "postgres",          # or "sqlite" / "mysql"
    "time",              # for Date, Time, Timestamp, TimestampTz
    "uuid",              # for UUID
    "rust_decimal",      # for NUMERIC / DECIMAL
] }
tokio = { version = "1", features = ["full"] }
```

### Using the pool directly

```rust
mod db;
use db::queries;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    let pool = sqlx::PgPool::connect("postgres://user:pass@localhost/mydb").await?;

    let author = queries::get_author(&pool, 1).await?;
    let all    = queries::list_authors(&pool).await?;

    Ok(())
}
```

`&pool` satisfies the `Executor` bound, so the call sites are unchanged from a
concrete-pool API.

### Inside a transaction

Because the functions are generic over `Executor`, pass `&mut *tx` to run several
generated operations in one transaction and commit them atomically:

```rust
let mut tx = pool.begin().await?;
queries::create_sale(&mut *tx, customer_id).await?;
queries::add_sale_item(&mut *tx, book_id, qty, price).await?;
tx.commit().await?;   // both statements commit together, or neither
```

### Database type per dialect

The `Database` bound on each function's `Executor` is fixed to the configured engine:

| Dialect | Pool type | `Database` bound |
|---|---|---|
| PostgreSQL | `sqlx::PgPool` | `sqlx::Postgres` |
| SQLite | `sqlx::SqlitePool` | `sqlx::Sqlite` |
| MySQL | `sqlx::MySqlPool` | `sqlx::MySql` |

## Inline row types

JOIN queries or partial RETURNING queries produce additional structs in the query
file:

```rust
#[derive(Debug, sqlx::FromRow)]
pub struct ListBooksWithAuthorRow {
    pub id: i64,
    pub title: String,
    pub genre: String,
    pub price: rust_decimal::Decimal,
    pub author_name: String,
    pub author_bio: Option<String>,
}

pub async fn list_books_with_author<'e, E>(executor: E)
        -> Result<Vec<ListBooksWithAuthorRow>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{ … }
```

## List parameters

For PostgreSQL, list parameters use `= ANY($1)` with a slice bind:

```rust
// generated
pub async fn get_books_by_ids<'e, E>(executor: E, ids: &[i64])
        -> Result<Vec<Book>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Postgres>,
{
    sqlx::query_as::<_, Book>(SQL_GET_BOOKS_BY_IDS)
        .bind(ids)
        .fetch_all(executor)
        .await
}
```

For SQLite and MySQL, the `native` strategy uses `json_each` / `JSON_TABLE`
with a JSON-serialized string:

```rust
pub async fn get_books_by_ids<'e, E>(executor: E, ids: &[i64])
        -> Result<Vec<Book>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let ids_json = serde_json::to_string(ids).unwrap();
    sqlx::query_as::<_, Book>(SQL_GET_BOOKS_BY_IDS)
        .bind(ids_json)
        .fetch_all(executor)
        .await
}
```

## Naming conventions

| SQL | Rust |
|---|---|
| `GetAuthor` | `get_author` |
| `ListBooksWithAuthor` | `list_books_with_author` |
| `birth_year` column | `birth_year` field |
| `Author` table | `Author` struct |
