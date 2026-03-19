# Python

sqltgen generates Python code using standard database drivers: psycopg (psycopg3)
for PostgreSQL, `sqlite3` (stdlib) for SQLite, and `mysql-connector-python` for MySQL.
Model types are `@dataclass` classes. Functions are plain synchronous functions.

## Configuration

```json
"python": {
  "out": "gen",
  "package": ""
}
```

| Field | Description |
|---|---|
| `out` | Output directory. |
| `package` | Unused for Python — set to `""`. |
| `list_params` | `"native"` (default) or `"dynamic"`. |

## What is generated

```
gen/
  __init__.py         — barrel import
  author.py           — Author dataclass
  book.py             — Book dataclass
  queries.py          — query functions (single-file form)
  users.py            — per-group query file (grouped form)
```

### Model dataclasses

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

- Non-null columns → bare type (`int`, `str`, `bool`, `float`, …).
- Nullable columns → `T | None`.
- `snake_case` SQL names → `snake_case` Python field names (unchanged).

### Query functions

```python
# gen/queries.py
import psycopg
from .author import Author

SQL_GET_AUTHOR    = "SELECT id, name, bio, birth_year FROM author WHERE id = %s"
SQL_LIST_AUTHORS  = "SELECT id, name, bio, birth_year FROM author ORDER BY name"
SQL_CREATE_AUTHOR = "INSERT INTO author (name, bio, birth_year) VALUES (%s, %s, %s) RETURNING *"
SQL_DELETE_AUTHOR = "DELETE FROM author WHERE id = %s"

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

def create_author(conn: psycopg.Connection,
                  name: str, bio: str | None,
                  birth_year: int | None) -> Author | None:
    with conn.cursor() as cur:
        cur.execute(SQL_CREATE_AUTHOR, (name, bio, birth_year))
        row = cur.fetchone()
        if row is None:
            return None
        return Author(*row)

def delete_author(conn: psycopg.Connection, id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(SQL_DELETE_AUTHOR, (id,))

def count_authors(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(SQL_COUNT_AUTHORS)
        return cur.rowcount  # :execrows
```

Results are unpacked positionally (`Author(*row)`), so the column order in the
query **must match** the dataclass field order.

### Querier wrapper

```python
# gen/queries.py  (excerpt)
import contextlib
from typing import Callable

class Querier:
    def __init__(self, connect: Callable[[], psycopg.Connection]) -> None:
        self._connect = connect

    def get_author(self, id: int) -> Author | None:
        with contextlib.closing(self._connect()) as conn:
            return get_author(conn, id)

    def list_authors(self) -> list[Author]:
        with contextlib.closing(self._connect()) as conn:
            return list_authors(conn)
```

## Wiring up

### PostgreSQL

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

### SQLite (stdlib — no install needed)

```python
import sqlite3
from gen.queries import get_author, list_authors

conn = sqlite3.connect("mydb.db")
author = get_author(conn, 1)
all_authors = list_authors(conn)
```

### MySQL

```sh
pip install mysql-connector-python
```

```python
import mysql.connector
from gen.queries import get_author, list_authors

conn = mysql.connector.connect(
    host="localhost", database="mydb",
    user="user", password="pass")

author = get_author(conn, 1)
all_authors = list_authors(conn)
```

### Using the Querier wrapper

```python
from gen.queries import Querier
import psycopg

q = Querier(lambda: psycopg.connect("postgresql://user:pass@localhost/mydb"))
author = q.get_author(1)
```

## JSON columns

| Driver | JSON column Python type |
|---|---|
| psycopg3 | `object` — psycopg3 automatically deserializes JSON |
| sqlite3 | `str` — returns the raw JSON string |
| mysql-connector | `str` — returns the raw JSON string |

Application code is responsible for parsing the string in the sqlite3 and
mysql-connector cases.

## Naming conventions

| SQL | Python |
|---|---|
| `GetAuthor` | `get_author` |
| `ListBooksWithAuthor` | `list_books_with_author` |
| `birth_year` column | `birth_year` field |
| `Author` table | `Author` class |

## Python version requirements

The generated code uses PEP 604 union syntax (`str | None`) and `from __future__
import annotations`. Python 3.10+ is required for runtime evaluation of these
annotations. Python 3.11+ is recommended.
