# Introduction

sqltgen is a multi-language SQL-to-code generator. You write standard SQL — a
schema (DDL) and annotated query files — and sqltgen emits fully typed, idiomatic
database access code in Java, Kotlin, Rust, Go, Python, TypeScript, and JavaScript.

No ORM. No reflection. No runtime query building. Just your SQL, compiled to code.

```sh
sqltgen generate
```

## What it does

1. **Parse your schema** — sqltgen reads `CREATE TABLE` and `ALTER TABLE`
   statements and builds an in-memory type model of every table and column.

2. **Analyse your queries** — each annotated query is parsed and its parameter
   types and result column types are inferred from the schema. No guessing; the
   inference is derived from the actual SQL.

3. **Emit typed code** — for every configured target language, sqltgen writes
   source files containing model types (one per table) and typed query functions.

The generated code is ready to use with standard database drivers — JDBC for
Java/Kotlin, sqlx for Rust, psycopg3/sqlite3/mysql-connector for Python, pg /
better-sqlite3 / mysql2 for TypeScript/JavaScript.

## Why sqltgen?

**You already wrote the SQL.** ORMs ask you to translate your mental model into a
different API; sqltgen lets you keep writing SQL and adds type safety around it.

**It's not another dependency — it's your code.** The generated files are plain
source code that belongs to your project. They use only your language's standard
database driver (JDBC, sqlx, psycopg3, pg, etc.) — there is no `sqltgen-runtime`
package, no framework sitting in your import path. If you stop using sqltgen
tomorrow, your code still compiles and runs. You can read it, debug it, even
hand-edit it. Stack traces go through your code and your driver, not through a
library you don't control. Upgrading sqltgen is just re-running the generator and
reviewing the diff — there is no runtime migration.

**The generated code is readable.** Output looks like code a competent developer
wrote by hand: idiomatic function names, proper nullability, correct types. There
are no codegen artifacts to work around.

**No live database required — and fast because of it.** sqltgen analyses your DDL
statically. There is nothing to connect, migrate, or seed before you can generate.
This also makes it genuinely fast: analysis and codegen are pure in-memory
computation, so generation completes in milliseconds regardless of schema size.
In CI, that means a single binary invocation with no infrastructure setup — no
database service container, no network, no health-check polling.

**All the SQL features you use.** JOINs, CTEs, subqueries, aggregates, UNION,
RETURNING, list parameters — all supported. Unknown constructs are handled
gracefully rather than failing the whole build.

## Comparison to sqlc

sqltgen is inspired by [sqlc](https://sqlc.dev) and shares its core philosophy:
write SQL, get typed functions. Both tools perform static analysis from DDL files
and neither requires a running database. The main design difference is in how
language targets are delivered:

| | sqltgen | sqlc |
|---|---|---|
| Implementation language | Rust | Go |
| Supported targets | Java, Kotlin, Rust, Go, Python, TypeScript, JavaScript | Go, Python, TypeScript (+ community plugins) |
| Supported dialects | PostgreSQL, SQLite, MySQL | PostgreSQL, MySQL, SQLite |
| Language targets | Built-in, native per driver | Built-in + WASM plugin system |
| Config format | `sqltgen.json` | `sqlc.yaml` |

sqltgen builds each language target directly into the binary with first-class
support for the idiomatic driver in that ecosystem. sqlc's plugin system allows
the community to add targets independently. Neither approach is strictly better —
they reflect different trade-offs between extensibility and native integration.

## Supported targets

| Language | Driver |
|---|---|
| Java | JDBC (`java.sql`) |
| Kotlin | JDBC (`java.sql`) |
| Rust | [sqlx](https://github.com/launchbadge/sqlx) (async) |
| Python | psycopg3 / sqlite3 / mysql-connector |
| TypeScript | pg / better-sqlite3 / mysql2 |
| JavaScript | same as TypeScript (JSDoc types) |
| Go | `database/sql` |

## Supported dialects

- **PostgreSQL** — full support
- **SQLite** — full support
- **MySQL** — full support
