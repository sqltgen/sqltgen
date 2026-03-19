# Rust

sqltgen generates async Rust code using [sqlx](https://github.com/launchbadge/sqlx).
Model structs derive `sqlx::FromRow`. Query functions are `async fn` returning
`Result<…, sqlx::Error>`.

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

```rust
// src/db/queries.rs
use sqlx::PgPool;
use super::author::Author;

pub async fn get_author(pool: &PgPool, id: i64)
        -> Result<Option<Author>, sqlx::Error> {
    sqlx::query_as::<_, Author>(SQL_GET_AUTHOR)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn list_authors(pool: &PgPool)
        -> Result<Vec<Author>, sqlx::Error> {
    sqlx::query_as::<_, Author>(SQL_LIST_AUTHORS)
        .fetch_all(pool)
        .await
}

pub async fn delete_author(pool: &PgPool, id: i64)
        -> Result<(), sqlx::Error> {
    sqlx::query(SQL_DELETE_AUTHOR)
        .bind(id)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn count_authors(pool: &PgPool)
        -> Result<u64, sqlx::Error> {
    sqlx::query(SQL_COUNT_AUTHORS)
        .execute(pool)
        .await
        .map(|r| r.rows_affected())
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

### Pool types per dialect

| Dialect | Pool type |
|---|---|
| PostgreSQL | `sqlx::PgPool` |
| SQLite | `sqlx::SqlitePool` |
| MySQL | `sqlx::MySqlPool` |

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

pub async fn list_books_with_author(pool: &PgPool)
        -> Result<Vec<ListBooksWithAuthorRow>, sqlx::Error> { … }
```

## List parameters

For PostgreSQL, list parameters use `= ANY($1)` with a slice bind:

```rust
// generated
pub async fn get_books_by_ids(pool: &PgPool, ids: &[i64])
        -> Result<Vec<Book>, sqlx::Error> {
    sqlx::query_as::<_, Book>(SQL_GET_BOOKS_BY_IDS)
        .bind(ids)
        .fetch_all(pool)
        .await
}
```

For SQLite and MySQL, the `native` strategy uses `json_each` / `JSON_TABLE`
with a JSON-serialized string:

```rust
pub async fn get_books_by_ids(pool: &SqlitePool, ids: &[i64])
        -> Result<Vec<Book>, sqlx::Error> {
    let ids_json = serde_json::to_string(ids).unwrap();
    sqlx::query_as::<_, Book>(SQL_GET_BOOKS_BY_IDS)
        .bind(ids_json)
        .fetch_all(pool)
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
