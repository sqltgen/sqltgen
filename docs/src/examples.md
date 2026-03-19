# Running examples

Runnable example projects for all backends and dialects live in `examples/`.
Each is a self-contained project with its own `Makefile` containing `generate`,
`build`, and `run` targets.

## Prerequisites

- **Docker** — for PostgreSQL and MySQL examples
- **Java 21 + Maven** — for Java and Kotlin examples
- **Go 1.23+** — for Go examples
- **Node 22+** — for TypeScript and JavaScript examples
- **Python 3.11+** — for Python examples
- **Rust stable** — for Rust examples (same toolchain used to build sqltgen)

## Running a single example

```sh
make -C examples/rust/postgresql run
make -C examples/java/sqlite run
make -C examples/python/mysql run
make -C examples/typescript/postgresql run
make -C examples/go/sqlite run
```

Each example starts its own database container if needed, runs
`sqltgen generate`, compiles the project, executes it against the database,
and tears down the container.

## Available examples

| Language | PostgreSQL | SQLite | MySQL |
|---|:---:|:---:|:---:|
| Java | ✅ | ✅ | ✅ |
| Kotlin | ✅ | ✅ | ✅ |
| Rust | ✅ | ✅ | ✅ |
| Python | ✅ | ✅ | ✅ |
| TypeScript | ✅ | ✅ | ✅ |
| JavaScript | ✅ | ✅ | ✅ |
| Go | ✅ | ✅ | — |

## Running all examples at once

```sh
make run-all
```

This starts one shared PostgreSQL container and one shared MySQL container, runs
all examples against them in sequence, and tears the containers down at the end.
Faster than running each example independently.

## Example layout

Every example follows the same structure:

```
examples/{lang}/{dialect}/
  sqltgen.json     — config pointing at schema.sql + queries.sql
  schema.sql       — bookstore DDL
  queries.sql      — annotated queries
  Makefile         — generate / build / run targets
  gen/             — generated code (checked in)
  src/             — application code (or main.py, Main.java, etc.)
```

The bookstore schema (`author`, `book`, `sale`, `sale_item` tables) is shared
across all examples. It exercises a representative range of features: JOINs,
CTEs, aggregates, RETURNING, list parameters, nullable columns, and multiple
query types.

See `examples/README.md` for more detail.
