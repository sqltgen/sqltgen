# Go

sqltgen generates Go code using the standard `database/sql` package. Functions
take a `context.Context` and a `*sql.DB` and return idiomatic `(T, error)` pairs.

## Configuration

```json
"go": {
  "out": "db",
  "package": "db"
}
```

| Field | Description |
|---|---|
| `out` | Output directory. |
| `package` | Go package name declared at the top of each generated file. |
| `list_params` | `"native"` (default) or `"dynamic"`. |

## What is generated

```
db/
  mod.go          — package declaration
  author.go       — Author struct
  book.go         — Book struct
  sqltgen.go      — shared helpers (execRows, buildInClause, scanArray)
  queries.go      — query functions
```

### Model structs

```go
// db/author.go
package db

import "database/sql"

// Author represents a row from the author table.
type Author struct {
    Id        int64
    Name      string
    Bio       sql.NullString
    BirthYear sql.NullInt32
}
```

- Non-null columns use bare Go types (`int64`, `string`, `bool`, …).
- Nullable columns use `sql.Null*` types (`sql.NullString`, `sql.NullInt32`,
  `sql.NullInt64`, `sql.NullFloat64`, `sql.NullBool`, `sql.NullTime`).
- Field names are `PascalCase` (e.g. `birth_year` → `BirthYear`).

### Query functions

```go
// db/queries.go
package db

import (
    "context"
    "database/sql"
)

// GetAuthor executes the GetAuthor query.
func GetAuthor(ctx context.Context, db *sql.DB, id int64) (*Author, error) {
    row := db.QueryRowContext(ctx, SQL_GET_AUTHOR, id)
    var r Author
    err := row.Scan(&r.Id, &r.Name, &r.Bio, &r.BirthYear)
    if err == sql.ErrNoRows {
        return nil, nil
    }
    if err != nil {
        return nil, err
    }
    return &r, nil
}

// ListAuthors executes the ListAuthors query.
func ListAuthors(ctx context.Context, db *sql.DB) ([]Author, error) {
    rows, err := db.QueryContext(ctx, SQL_LIST_AUTHORS)
    if err != nil {
        return nil, err
    }
    defer rows.Close()
    var results []Author
    for rows.Next() {
        var r Author
        if err := rows.Scan(&r.Id, &r.Name, &r.Bio, &r.BirthYear); err != nil {
            return nil, err
        }
        results = append(results, r)
    }
    return results, rows.Err()
}

// DeleteAuthor executes the DeleteAuthor query.
func DeleteAuthor(ctx context.Context, db *sql.DB, id int64) error {
    _, err := db.ExecContext(ctx, SQL_DELETE_AUTHOR, id)
    return err
}

// CountAuthors executes the CountAuthors query.
func CountAuthors(ctx context.Context, db *sql.DB) (int64, error) {
    // :execrows — returns affected row count
}
```

Return types by command:

| `:one` | `:many` | `:exec` | `:execrows` |
|---|---|---|---|
| `(*T, error)` — `nil, nil` if no row | `([]T, error)` | `error` | `(int64, error)` |

## Wiring up

### go.mod

```sh
go get github.com/lib/pq          # PostgreSQL
go get modernc.org/sqlite          # SQLite (pure Go, no CGo)
go get github.com/go-sql-driver/mysql  # MySQL
```

### PostgreSQL

```go
import (
    "context"
    "database/sql"
    _ "github.com/lib/pq"
    "yourmodule/db"
)

conn, err := sql.Open("postgres", "postgres://user:pass@localhost/mydb?sslmode=disable")

author, err := db.GetAuthor(context.Background(), conn, 1)
all, err    := db.ListAuthors(context.Background(), conn)
```

### SQLite

```go
import (
    "context"
    "database/sql"
    _ "modernc.org/sqlite"
    "yourmodule/db"
)

conn, err := sql.Open("sqlite", "mydb.db")

author, err := db.GetAuthor(context.Background(), conn, 1)
```

### MySQL

```go
import (
    "context"
    "database/sql"
    _ "github.com/go-sql-driver/mysql"
    "yourmodule/db"
)

conn, err := sql.Open("mysql", "user:pass@tcp(localhost:3306)/mydb")

author, err := db.GetAuthor(context.Background(), conn, 1)
```

## Inline row types

```go
type ListBooksWithAuthorRow struct {
    Id         int64
    Title      string
    Genre      string
    Price      float64
    AuthorName string
    AuthorBio  sql.NullString
}

func ListBooksWithAuthor(ctx context.Context, db *sql.DB) ([]ListBooksWithAuthorRow, error) { … }
```

## List parameters

For SQLite and MySQL the `native` strategy serializes the list to a JSON string
and uses `json_each` / `JSON_TABLE`. For PostgreSQL it uses `pq.Array`:

```go
// SQLite / MySQL
func GetBooksByIds(ctx context.Context, db *sql.DB, ids []int64) ([]Book, error) {
    idsJSON, _ := json.Marshal(ids)
    rows, err := db.QueryContext(ctx, SQL_GET_BOOKS_BY_IDS, string(idsJSON))
    …
}

// PostgreSQL
import "github.com/lib/pq"

func GetBooksByIds(ctx context.Context, db *sql.DB, ids []int64) ([]Book, error) {
    rows, err := db.QueryContext(ctx, SQL_GET_BOOKS_BY_IDS, pq.Array(ids))
    …
}
```

## Naming conventions

| SQL | Go |
|---|---|
| `GetAuthor` | `GetAuthor` (PascalCase — exported) |
| `ListBooksWithAuthor` | `ListBooksWithAuthor` |
| `birth_year` column | `BirthYear` field |
| `Author` table | `Author` struct |
