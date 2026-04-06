# sqltgen — Developer Guide

This document covers the codebase architecture, key data structures, conventions,
and step-by-step guides for adding new backends and dialects. Read this before
making non-trivial changes.

---

## Table of contents

1. [Prerequisites and setup](#prerequisites-and-setup)
2. [Architecture overview](#architecture-overview)
3. [IR layer — the shared data model](#ir-layer--the-shared-data-model)
4. [Frontend layer — SQL dialect parsers](#frontend-layer--sql-dialect-parsers)
5. [Backend layer — code generators](#backend-layer--code-generators)
6. [Backend utilities](#backend-utilities)
7. [Config layer](#config-layer)
8. [Testing](#testing)
9. [Code style and conventions](#code-style-and-conventions)
10. [How to add a new backend](#how-to-add-a-new-backend)
11. [How to add a new dialect](#how-to-add-a-new-dialect)
12. [How to add a new example project](#how-to-add-a-new-example-project)
13. [Definition of done](#definition-of-done)

---

## Prerequisites and setup

- **Rust** stable toolchain — [rustup.rs](https://rustup.rs)
- **Docker** — for running integration tests against PostgreSQL and MySQL
- **Java 21 + Maven** — for Java/Kotlin runtime tests and examples
- **Node 22** — for TypeScript/JavaScript runtime tests and examples
- **Python 3.11+** — for Python runtime tests and examples

```sh
git clone https://github.com/sqltgen/sqltgen.git
cd sqltgen
cargo build           # debug build — binary at target/debug/sqltgen
cargo build --release # release build
cargo test            # all 571 unit tests (fully offline)
```

---

## Architecture overview

sqltgen is a classic 3-layer compiler pipeline:

```
SQL files (DDL + annotated queries)
         │
         ▼  FRONTEND  (src/frontend/)
  DialectParser trait
  ├── parse_schema(ddl) ──→ Schema (IR)
  └── parse_queries(sql, schema) ──→ Vec<Query> (IR)
         │
         ▼  IR  (src/ir/)
  Schema { tables: Vec<Table { columns: Vec<Column { SqlType, nullable, … } }> }
  Query  { name, cmd, sql, params: Vec<Parameter>, result_columns: Vec<ResultColumn> }
         │
         ▼  BACKEND  (src/backend/)
  Codegen trait
  └── generate(schema, queries, config) ──→ Vec<GeneratedFile { path, content }>
         │
         ▼  src/main.rs
  Write files to disk
```

**The IR is a strict boundary.** Backends must consume only IR types and never
import from `sqlparser` or from `frontend`. The frontend must produce complete,
correct IR and never call backend code.

**File layout:**

```
src/
  main.rs              CLI entry point (clap); reads config, runs pipeline, writes files
  lib.rs               Crate root; re-exports frontend, backend, ir, config modules
  config.rs            JSON config parsing (SqltgenConfig, Engine, Language, OutputConfig)
  ir/
    mod.rs             Re-exports Schema, Table, Column, Query, Parameter, ResultColumn, SqlType
    types.rs           SqlType enum
    schema.rs          Schema, Table, Column structs
    query.rs           Query, QueryCmd, Parameter, ResultColumn structs
  frontend/
    mod.rs             DialectParser trait
    common/            Shared parsing logic used by all dialects
      mod.rs           ALTER TABLE helpers, AlterCaps, build_column, build_create_table
      schema.rs        parse_schema_impl — shared DDL parser loop
      typemap.rs       Shared SQL-type → SqlType mappings (map_common)
      named_params.rs  @name parameter preprocessing and rewriting
      query/
        mod.rs         parse_queries_with_config — main query-parsing entry point
        select.rs      SELECT query building
        dml.rs         INSERT/UPDATE/DELETE building
        params.rs      Parameter collection from expressions
        resolve.rs     Column/expression type resolution
    postgres/          PostgreSQL dialect
    sqlite/            SQLite dialect
    mysql/             MySQL dialect
  backend/
    mod.rs             Codegen trait, GeneratedFile struct
    common.rs          Shared helpers: infer_table, has_inline_rows, infer_row_type_name
    naming.rs          to_pascal_case, to_camel_case, to_snake_case
    sql_rewrite.rs     Placeholder rewriting, list param SQL rewriting
    jdbc.rs            Shared Java+Kotlin JDBC logic
    java.rs            Java backend
    kotlin.rs          Kotlin backend
    rust.rs            Rust/sqlx backend
    python.rs          Python backend
    typescript.rs      TypeScript + JavaScript backend (JsOutput flag)
    go.rs              Go stub (unimplemented!)
    test_helpers.rs    Test fixtures (cfg(test) only)
tests/
  e2e/
    main.rs            E2E snapshot tests
    fixtures/          Input SQL (bookstore schema/queries per dialect)
    golden/            Expected output files (committed, regenerated with UPDATE_GOLDEN=1)
    runtime/           Self-contained sub-projects that actually run against real databases
examples/
  {java,kotlin,rust,python,typescript,javascript}/{postgresql,sqlite,mysql}/
```

---

## IR layer — the shared data model

The IR is the contract between the frontend and backend. Both sides depend on
it; neither depends on the other. All IR types live in `src/ir/`.

### `SqlType` (`src/ir/types.rs`)

The canonical type vocabulary. Every SQL column type and expression type gets
mapped to one of these variants:

```rust
pub enum SqlType {
    Boolean,
    SmallInt, Integer, BigInt,
    Real, Double, Decimal,
    Text, Char(Option<u32>), VarChar(Option<u32>),
    Bytes,
    Date, Time, Timestamp, TimestampTz, Interval,
    Uuid,
    Json, Jsonb,
    Array(Box<SqlType>),
    Custom(String),   // unknown/extension types — e.g. citext, geometry
}
```

`Custom(String)` is the safe fallback for any type the frontend doesn't
recognize. Backends should emit a reasonable language-specific fallback
(`Object`, `Any`, `unknown`, etc.) rather than crashing.

### `Schema` and `Table` (`src/ir/schema.rs`)

```rust
pub struct Schema { pub tables: Vec<Table> }
pub struct Table  {
    pub name: String,
    pub schema: Option<String>,  // e.g. Some("public") for public.users
    pub columns: Vec<Column>,
    pub kind: TableKind,         // Table or View
}
pub struct Column {
    pub name: String,
    pub sql_type: SqlType,
    pub nullable: bool,
    pub is_primary_key: bool,
}
```

Tables are stored in declaration order. Column order matches the DDL (important:
backends emit positional field accessors for Python, so order must be stable).

When looking up a table by name, use `Schema::find_table(query_schema, table_name,
default_schema)` rather than searching `schema.tables` directly. This method
handles the four matching cases for qualified/unqualified references with a
configurable default schema.

### `Query` and friends (`src/ir/query.rs`)

```rust
pub struct Query {
    pub name: String,              // PascalCase, from the annotation
    pub cmd: QueryCmd,             // One | Many | Exec | ExecRows
    pub sql: String,               // Original SQL with $N placeholders intact
    pub params: Vec<Parameter>,
    pub result_columns: Vec<ResultColumn>,
    pub source_table: Option<String>,  // set when SELECT * from a single table
}

pub struct Parameter {
    pub index: usize,     // 1-based, matches $1/$2/… in sql
    pub name: String,
    pub sql_type: SqlType,
    pub nullable: bool,
    pub is_list: bool,    // true → accept a collection; SQL is rewritten per strategy
}

pub struct ResultColumn {
    pub name: String,
    pub sql_type: SqlType,
    pub nullable: bool,
}
```

**`source_table`:** set by the frontend when the query's projection is an
unambiguous `SELECT *` or `SELECT t.*` from a single, non-nullable schema table.
Backends use this to reuse the existing table model type instead of emitting a
per-query row struct. When `None`, backends emit an inline `{QueryName}Row` type.

---

## Frontend layer — SQL dialect parsers

### `DialectParser` trait (`src/frontend/mod.rs`)

```rust
pub trait DialectParser {
    fn parse_schema(&self, ddl: &str, default_schema: Option<&str>) -> anyhow::Result<Schema>;
    fn parse_queries(&self, sql: &str, schema: &Schema, default_schema: Option<&str>) -> anyhow::Result<Vec<Query>>;
}
```

Each dialect (PostgreSQL, SQLite, MySQL) provides one struct that implements
this trait. The struct is stateless and constructed in `main.rs`.

The `default_schema` parameter controls how unqualified table references match
schema-qualified tables (and vice versa). Pass `None` to use the engine default
(`"public"` for PostgreSQL, `"main"` for SQLite, `None` for MySQL).

### Dialect structure

Each dialect lives in `src/frontend/{postgres,sqlite,mysql}/` and has three files:

| File | Purpose |
|---|---|
| `mod.rs` | Defines the parser struct and implements `DialectParser` by delegating. |
| `schema.rs` | Calls `parse_schema_impl` with the dialect, typemap function, and `AlterCaps`. |
| `typemap.rs` | Maps `sqlparser::ast::DataType` → `SqlType` for this dialect. |

### Schema parsing (`src/frontend/common/schema.rs`)

`parse_schema_impl` is a shared tokenizer-level loop. It:

1. Tokenizes the DDL text using sqlparser's tokenizer for the given dialect.
2. Scans tokens to find statement boundaries (`;` at the top level).
3. For each statement, tries to parse it with sqlparser; if parsing fails, it
   silently skips the statement (this is important for resilience against
   unsupported SQL like `CREATE FUNCTION … LEAKPROOF`).
4. Dispatches recognized statements: `CREATE TABLE` → `build_create_table`,
   `ALTER TABLE` → `apply_alter_table`, `DROP TABLE` → `apply_drop_tables`.

**`AlterCaps`** is a bitflag that controls which `ALTER TABLE` operations the
dialect supports. PostgreSQL sets `AlterCaps::ALL`; SQLite sets a subset
(it doesn't support `DROP COLUMN` or `ALTER COLUMN`).

### Query parsing (`src/frontend/common/query/mod.rs`)

`parse_queries_with_config` is the shared query-parsing entry point:

1. **Block splitting:** splits the SQL text into annotated blocks using the
   `-- name: QueryName :cmd` pattern. Everything between two annotations (or
   between the last annotation and EOF) is one query block.

2. **Named parameter preprocessing** (`src/frontend/common/named_params.rs`):
   - Scans for `-- @name [type[]] [null|not null]` annotation lines above the SQL.
   - Scans the SQL body for `@name` occurrences.
   - Rewrites `@name` to `$N` positional placeholders (1-indexed).
   - Stores the name-to-index mapping and any type/nullability overrides.

3. **sqlparser parsing:** the rewritten SQL is parsed as a single statement with
   `sqlparser`. The result is an AST.

4. **IR building:**
   - For `SELECT`: `select.rs` walks the projection and resolves each item
     against the schema and any CTEs via `resolve.rs`.
   - For `INSERT/UPDATE/DELETE`: `dml.rs` handles `RETURNING` and DML-specific
     semantics.
   - `params.rs` collects parameters from `WHERE`, `JOIN ON`, `HAVING`, `SET`,
     `VALUES`, `LIMIT`, `OFFSET`, and expression contexts.

5. **source_table inference:** after building result columns, the parser checks
   whether the query is an unambiguous `SELECT *` / `SELECT t.*` from a single
   non-nullable table and sets `query.source_table` accordingly.

### Type inference rules (summary)

- **Column references** (`WHERE col = @x`): type and nullability come from the
  schema column.
- **Aggregates:** `COUNT(*)` → `BigInt, not null`. `SUM`/`MIN`/`MAX`/`AVG` →
  same type as the argument, nullable if the argument or its source is nullable.
- **Scalar subqueries** in SELECT list → nullable (the subquery may return NULL).
- **LEFT/RIGHT/FULL JOIN** outer side columns → nullable.
- **COALESCE** → non-null if any argument is non-null.
- **CASE WHEN … END** → nullable if any branch could be NULL.
- **Unknown type** → `SqlType::Custom("unknown")` with a warning to stderr.

### Typemap convention

Each dialect's `typemap.rs` exports a `pub(crate) fn map(dt: &DataType) -> SqlType`.

The recommended pattern:

```rust
pub(crate) fn map(dt: &DataType) -> SqlType {
    match dt {
        // Dialect-specific arms first …
        DataType::Bytea => SqlType::Bytes,
        DataType::Custom(name, _) => map_custom(name),
        // Fall through to shared mappings, then produce Custom as fallback
        other => map_common(other).unwrap_or_else(|| fallback_custom(dt)),
    }
}
```

`map_common` (in `src/frontend/common/typemap.rs`) handles types that are
identical across all three dialects (`INTEGER`, `BOOLEAN`, `TEXT`, `DATE`, etc.).
Dialect-specific types come before the `other` fallback arm.

---

## Backend layer — code generators

### `Codegen` trait (`src/backend/mod.rs`)

```rust
pub trait Codegen {
    fn generate(
        &self,
        schema: &Schema,
        queries: &[Query],
        config: &OutputConfig,
    ) -> anyhow::Result<Vec<GeneratedFile>>;
}

pub struct GeneratedFile {
    pub path: PathBuf,
    pub content: String,
}
```

The `path` is relative to the config's `out` directory. `main.rs` prepends the
base dir and the `out` path, then creates directories and writes the files.

### How a backend is wired in

`src/main.rs` matches on the `Language` enum:

```rust
Language::Rust => Box::new(backend::rust::RustCodegen { target: cfg.engine.into() }),
```

Add a new arm here when adding a backend.

### Common backend utilities (`src/backend/`)

These are the most important shared helpers — use them rather than reimplementing.

**`naming.rs`**
- `to_pascal_case(s)` — `get_user_by_id` → `GetUserById`
- `to_camel_case(s)` — `get_user_by_id` → `getUserById`
- `to_snake_case(s)` — `GetUserById` → `get_user_by_id`

Query names arrive as `PascalCase` from the annotation; column names are `snake_case`
from SQL. Use the appropriate converter for the target language's convention.

**`common.rs`**

- `infer_table(query, schema) -> Option<&str>` — returns the schema table name
  if this query's result columns exactly match it (identity via `source_table`,
  then structural fallback). Used to decide whether to reuse an existing model type.

- `has_inline_rows(query, schema) -> bool` — `true` if the query has result
  columns that don't match any schema table. Use this to decide whether to emit a
  per-query row type.

- `infer_row_type_name(query, schema) -> Option<String>` — returns the PascalCase
  type name for the query's result: the table name if it matches, `{QueryName}Row`
  for inline types, or `None` for exec/execrows (no result).

- `needs_null_safe_getter(sql_type) -> bool` — `true` for JDBC primitive types
  (`boolean`, `short`, `int`, `long`, `float`, `double`) that return `0`/`false`
  for SQL NULL instead of a null reference. Used by Java/Kotlin backends.

**`sql_rewrite.rs`**

- `rewrite_to_anon_params(sql) -> String` — replaces `$N` placeholders with `?`
  (for JDBC, better-sqlite3, mysql2).

- `rewrite_to_percent_s(sql) -> String` — replaces `$N` placeholders with `%s`
  (for psycopg3 / mysql-connector-python).

- `rewrite_list_sql_native(sql, param_idx, engine)` — rewrites `= ANY($N)` /
  `IN (SELECT value FROM json_each(?))` / `JSON_TABLE` for list params (native strategy).

- `positional_bind_names(query) -> Vec<String>` — returns the bind names in `$N`
  order for positional parameters.

**`jdbc.rs`**

Shared Java + Kotlin JDBC code generation helpers:
- `emit_jdbc_binds(ps_name, params, target)` — emits `ps.setLong(1, x); …` for
  each parameter.
- `emit_dynamic_binds(…)` — same for the dynamic list param strategy.
- `prepare_sql_const(name, sql)` — emits the `private static final String SQL_X = …;`
  or `private const val SQL_X = …;` constant.

---

## Config layer

`src/config.rs` contains:

- `SqltgenConfig` — the top-level deserialized config struct.
- `Engine` — `Postgresql | Sqlite | Mysql`; used to pick the frontend parser and
  to inform backends of the target engine.
- `Language` — the keys allowed in the `gen` map.
- `OutputConfig` — `out`, `package`, `list_params` per target language.
- `ListParamStrategy` — `Native` (default) | `Dynamic`.
- `QueryPaths` — `Single(String)` | `Many(Vec<String>)`; untagged serde, so either
  a bare string or a JSON array is accepted.
- `SqltgenConfig::expand_queries(base_dir)` — resolves glob patterns and returns
  sorted file paths.

---

## Testing

### Unit tests

Run with `cargo test`. All 571 tests are offline — no database or Docker needed.

Tests live in the same file as the code they test (`#[cfg(test)]` modules). Every
public function should have at least a happy-path test; edge cases and error paths
are also tested extensively.

**Test utilities** (`src/backend/test_helpers.rs`):
- `cfg()` — a default `OutputConfig` for unit tests.
- `get_file(files, name)` — finds a generated file by filename; panics if absent.
- `user_table()` — a minimal `user` table fixture (id, name, bio).

**Typical backend unit test structure:**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::test_helpers::{cfg, get_file};
    use crate::ir::{Column, Query, QueryCmd, ResultColumn, Schema, SqlType, Table};

    #[test]
    fn generates_simple_select_one() {
        let schema = Schema {
            tables: vec![Table {
                name: "user".to_string(),
                columns: vec![
                    Column { name: "id".to_string(), sql_type: SqlType::BigInt,
                             nullable: false, is_primary_key: true },
                    Column { name: "name".to_string(), sql_type: SqlType::Text,
                             nullable: false, is_primary_key: false },
                ],
            }],
        };
        let queries = vec![
            Query::new("GetUser", QueryCmd::One,
                "SELECT id, name FROM user WHERE id = $1",
                vec![Parameter::scalar(1, "p1", SqlType::BigInt, false)],
                vec![
                    ResultColumn { name: "id".to_string(), sql_type: SqlType::BigInt, nullable: false },
                    ResultColumn { name: "name".to_string(), sql_type: SqlType::Text, nullable: false },
                ])
            .with_source_table(Some("user".to_string())),
        ];
        let files = MyCodegen.generate(&schema, &queries, &cfg()).unwrap();
        let queries_file = get_file(&files, "queries.xyz");
        assert!(queries_file.contains("get_user"));
        assert!(queries_file.contains("id: i64"));
    }
}
```

### E2E snapshot tests

Run with `cargo test --test e2e`. These tests:

1. Load the fixture schema and queries from `tests/e2e/fixtures/bookstore/{dialect}/`.
2. Run the full frontend → IR → backend pipeline.
3. Compare output against the golden files in `tests/e2e/golden/{lang}/bookstore/{dialect}/`.

**To regenerate golden files** after an intentional output change:

```sh
UPDATE_GOLDEN=1 cargo test --test e2e
```

The `MANIFEST` file in each golden directory lists which files are expected.
If you add a new generated file, the manifest is updated automatically.

### Runtime tests

Runtime tests live in `tests/e2e/runtime/{lang}/{dialect}/`. Each is a
self-contained sub-project (Maven, Cargo, npm, etc.) that:

1. Reads a `sqltgen.json` and runs `sqltgen generate`.
2. Compiles and runs the generated code against a real database.
3. Asserts the results are correct.

```sh
# No Docker needed:
make e2e-runtime-rust-sqlite
make e2e-runtime-python-sqlite
make e2e-runtime-typescript-sqlite
make e2e-runtime-java-sqlite

# Requires Docker:
make e2e-db-up                       # start PostgreSQL + MySQL containers
make e2e-runtime-rust-postgresql
make e2e-runtime-kotlin-mysql
# …etc.
```

### CI targets

```sh
make ci-fmt       # cargo fmt --check
make ci-clippy    # cargo clippy -- -D warnings
make ci-test      # cargo test
make ci-check-suite  # python tests/e2e/check_suite.py --ci
```

`check_suite.py` verifies that every fixture dialect/lang combination has a
matching golden directory.

---

## Code style and conventions

### Formatting and linting

Always run before committing:

```sh
cargo fmt
cargo clippy -- -D warnings
```

CI rejects PRs with formatter diffs or clippy warnings. The line length limit is
160 characters (`rustfmt.toml`).

### Function and file size

- Functions over ~75 lines are a red flag — consider splitting.
- Functions over 100 lines are not accepted.
- Each file should have a single, clear responsibility.

### Error handling

- **Recoverable errors** (unknown type, unsupported expression): emit a warning
  with `eprintln!` and continue. Map unknown types to `SqlType::Custom(name)`.
  The pipeline must never crash on valid-but-unsupported SQL.

- **Fatal errors** (missing file, malformed config, I/O failure): return
  `anyhow::Error` and let `main` print and exit cleanly.

### Documentation

Every `pub` item must have a `///` doc comment. Internal functions that have
non-obvious behavior should also have comments. Comments should explain _why_,
not just _what_.

### Naming conventions in generated code

SQL names are `snake_case`; map to the target language's convention:

| Target | Function/method names | Field names | Type names |
|---|---|---|---|
| Java | `camelCase` | `camelCase` | `PascalCase` |
| Kotlin | `camelCase` | `camelCase` | `PascalCase` |
| Rust | `snake_case` | `snake_case` | `PascalCase` |
| Python | `snake_case` | `snake_case` | `PascalCase` |
| TypeScript | `camelCase` | `snake_case` (matches DB column) | `PascalCase` |
| JavaScript | `camelCase` | `snake_case` (matches DB column) | N/A (typedef) |

Use `to_camel_case` / `to_pascal_case` / `to_snake_case` from `src/backend/naming.rs`.

---

## How to add a new backend

This is the most common type of contribution. The Go backend is a good example
of what needs to be done — `src/backend/go.rs` is currently a stub with
`unimplemented!()`.

### Step 1 — Create the backend file

Create `src/backend/{lang}.rs`. Implement the `Codegen` trait:

```rust
use crate::backend::{Codegen, GeneratedFile};
use crate::config::OutputConfig;
use crate::ir::{Query, Schema};

pub struct GoCodegen;

impl Codegen for GoCodegen {
    fn generate(
        &self,
        schema: &Schema,
        queries: &[Query],
        config: &OutputConfig,
    ) -> anyhow::Result<Vec<GeneratedFile>> {
        let mut files = Vec::new();

        // 1. Emit one file per table (model/struct)
        for table in &schema.tables {
            files.push(emit_table_file(table, config)?);
        }

        // 2. Emit queries file
        files.push(emit_queries_file(schema, queries, config)?);

        Ok(files)
    }
}
```

### Step 2 — Emit model types

Walk `schema.tables`. For each table, emit the target language's struct/class/
interface/typedef. Use `to_pascal_case(&table.name)` for the type name.

For each column:
- Map `column.sql_type` to the target language type using a new `map_type(sql_type, nullable) -> String` function.
- Respect `column.nullable` — emit the nullable variant of the type.

### Step 3 — Emit query functions

Walk `queries`. For each query:

1. Determine the result type:
   - Use `infer_row_type_name(query, schema)` to get the type name.
   - Use `has_inline_rows(query, schema)` to decide whether to emit an inline row type.

2. Determine the function signature:
   - Name: `to_snake_case(&query.name)` (Rust/Python/Go) or `to_camel_case(&query.name)` (Java/Kotlin/TS/JS).
   - Parameters: iterate `query.params` and map each `param.sql_type` (with `param.nullable`).
   - Return type: depends on `query.cmd` (One/Many/Exec/ExecRows) and the row type.

3. Rewrite SQL placeholders for the target driver:
   - JDBC / better-sqlite3 / mysql2: `rewrite_to_anon_params(&query.sql)` → `?`
   - psycopg3 / mysql-connector: `rewrite_to_percent_s(&query.sql)` → `%s`
   - sqlx postgres / node-postgres: keep `$N` as-is
   - sqlx sqlite / mysql: `rewrite_to_anon_params` → `?`

4. Emit the SQL as a constant and the function body.

### Step 4 — Wire the backend into the CLI

In `src/main.rs`, add an arm to the `Language` match:

```rust
Language::Go => Box::new(backend::go::GoCodegen { target: cfg.engine.into() }),
```

In `src/lib.rs`, make sure `backend::go` is accessible:
```rust
pub use backend::go;  // (it's already pub in backend/mod.rs)
```

### Step 5 — Add the language to `Language` enum and config

`src/config.rs` already has `Go` in the `Language` enum and `OutputConfig`
is already shared. No config changes needed.

### Step 6 — Write unit tests

Add `#[cfg(test)]` tests in `src/backend/{lang}.rs` for:
- A simple `:one` query (uses table type)
- A `:many` query
- An `:exec` query
- An `:execrows` query
- A query with an inline row type (JOIN or partial RETURNING)
- A list parameter query
- A nullable parameter
- All SQL types your backend handles (at least the common ones)

Use `test_helpers::{cfg, get_file, user_table}` for fixtures.

### Step 7 — Add golden fixture output

Add the new language to the E2E snapshot test in `tests/e2e/main.rs`:

```rust
#[test]
fn snapshot_go_bookstore() {
    run_snapshot_tests(&GoCodegen { target: GoTarget::Postgresql }, …);
}
```

Then regenerate golden files:
```sh
UPDATE_GOLDEN=1 cargo test --test e2e
```

Inspect the golden files and verify the output looks correct.

### Step 8 — Update docs and status

- `STATUS.md` — fill in the new language column with ✅/⚠️/❌ for each feature.
- `PLAN.md` — update the backend status table.
- `docs/src/languages/{lang}.md` — add a language guide page to the mdBook.

---

## How to add a new dialect

### Step 1 — Create the dialect directory

```
src/frontend/{dialect}/
  mod.rs
  schema.rs
  typemap.rs
  query.rs   (if the dialect needs query-parse overrides; most don't)
```

### Step 2 — Implement the typemap

Create `src/frontend/{dialect}/typemap.rs`:

```rust
use sqlparser::ast::DataType;
use crate::frontend::common::typemap::{map_common, fallback_custom};
use crate::ir::SqlType;

pub(crate) fn map(dt: &DataType) -> SqlType {
    match dt {
        // Dialect-specific type arms
        DataType::Custom(name, _) => map_custom(name),
        other => map_common(other).unwrap_or_else(|| fallback_custom(other)),
    }
}

fn map_custom(name: &sqlparser::ast::ObjectName) -> SqlType {
    use crate::frontend::common::typemap::custom_name_upper;
    match custom_name_upper(name).as_str() {
        // Dialect-specific custom type names
        _ => crate::frontend::common::typemap::fallback_custom_name(&custom_name_upper(name)),
    }
}
```

Write tests in the same file covering all type aliases your dialect uses.
Look at `src/frontend/postgres/typemap.rs` and `src/frontend/mysql/typemap.rs`
for examples.

### Step 3 — Implement schema parsing

Create `src/frontend/{dialect}/schema.rs`:

```rust
use sqlparser::dialect::YourDialect;
use crate::frontend::common::{schema::parse_schema_impl, AlterCaps, DdlDialect};
use crate::frontend::common::query::ResolverConfig;
use crate::ir::Schema;

pub(crate) fn parse_schema(ddl: &str, default_schema: Option<&str>) -> anyhow::Result<Schema> {
    let ds = default_schema.unwrap_or("your_default"); // e.g. "public" for PG
    parse_schema_impl(
        ddl,
        &YourDialect {},
        DdlDialect { map_type: super::typemap::map, alter_caps: AlterCaps::ALL },
        &ResolverConfig {
            typemap: super::typemap::map,
            default_schema: Some(ds.to_string()),
            ..ResolverConfig::default()
        },
    )
}
```

Determine the right `AlterCaps` for your dialect. Check `src/frontend/common/mod.rs`
for the available flags. Use `AlterCaps::ALL` for dialects that support all ALTER
operations; restrict to what your dialect actually supports.

### Step 4 — Implement `mod.rs`

```rust
pub mod query;
pub mod schema;
pub mod typemap;

use crate::frontend::DialectParser;
use crate::ir::{Query, Schema};

pub struct YourParser;

impl DialectParser for YourParser {
    fn parse_schema(&self, ddl: &str, default_schema: Option<&str>) -> anyhow::Result<Schema> {
        schema::parse_schema(ddl, default_schema)
    }

    fn parse_queries(&self, sql: &str, schema: &Schema, default_schema: Option<&str>) -> anyhow::Result<Vec<Query>> {
        query::parse_queries(sql, schema, default_schema)
    }
}
```

### Step 5 — Wire the dialect into the CLI

In `src/main.rs`:
```rust
Engine::YourEngine => Box::new(frontend::your_dialect::YourParser),
```

In `src/config.rs`, add the new value to the `Engine` enum:
```rust
#[serde(rename_all = "lowercase")]
pub enum Engine { Postgresql, Sqlite, Mysql, YourEngine }
```

### Step 6 — Write tests

Add `#[cfg(test)]` tests in `schema.rs` and `typemap.rs`:
- Each type the dialect supports maps to the correct `SqlType`.
- Basic `CREATE TABLE` parsing.
- `ALTER TABLE` operations the dialect supports (and verify unsupported ones are ignored).
- Drop table.
- Resilience: unsupported statements are silently skipped.

Add E2E fixture files:
```
tests/e2e/fixtures/bookstore/{your_dialect}/schema.sql
tests/e2e/fixtures/bookstore/{your_dialect}/queries.sql
```

Then regenerate golden files and add snapshot test cases.

---

## How to add a new example project

Each example lives in `examples/{lang}/{dialect}/` and is a self-contained
project. The pattern is consistent across all existing examples:

```
examples/{lang}/{dialect}/
  sqltgen.json          config pointing at schema.sql + queries.sql
  schema.sql            DDL for the bookstore schema
  queries.sql           Annotated queries
  Makefile              targets: generate, build, run
  gen/                  generated code (checked in)
  src/                  or main.py, Main.java, etc. — the application code
```

The `Makefile` must have at least:
- `generate` — runs `sqltgen generate`
- `build` — compiles the project
- `run` — runs the application (starts Docker if needed, verifies output)

Look at `examples/python/sqlite/Makefile` for the simplest reference.

---

## Definition of done

Before a PR adding a new backend or dialect can be merged:

- [ ] `cargo build` — zero warnings
- [ ] `cargo clippy -- -D warnings` — clean
- [ ] `cargo fmt` — no diff
- [ ] Every public function has a `///` doc comment
- [ ] Every function has at least a happy-path unit test
- [ ] E2E snapshot golden files generated and committed
- [ ] `STATUS.md` updated — flip feature cells from ❌/🚧 to ✅/⚠️
- [ ] `PLAN.md` updated — mark completed items
- [ ] `docs/src/languages/{lang}.md` added or updated in the mdBook
