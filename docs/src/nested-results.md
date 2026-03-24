# Nested query results (`-- nest:`)

This page documents the nested-result feature introduced in the query parser,
IR, and TypeScript/JavaScript backends.

## Goal

SQL joins naturally return a flat row shape. For many APIs, the desired output
is hierarchical:

- parent fields (`user.id`, `user.name`)
- child arrays (`company[]`, `address[]`)

The `-- nest:` annotation lets a query declare how some flat columns should be
aggregated into nested array fields in generated code.

## Annotation syntax

```sql
-- name: GetUserWithCompanies :many
-- nest: company(company_id, company_name, company_sector)
SELECT u.id, u.name,
       c.id AS company_id, c.name AS company_name, c.sector AS company_sector
FROM users u
JOIN user_companies uc ON uc.user_id = u.id
JOIN companies c ON c.id = uc.company_id;
```

Supported column forms inside `-- nest:`:

- `source_col` (auto target name derived by prefix stripping)
- `source_col as alias` (explicit target name)

Example:

```sql
-- nest: companies(c_id as id, c_name as name)
```

## How parsing works

Implementation lives in `src/frontend/common/query/mod.rs`.

High-level flow:

1. Extract and remove all `-- nest:` lines from SQL text.
2. Parse each line into `(field_name, columns[])`.
3. Validate:
   - only `:one` and `:many` accept nesting
   - `field_name` must be a valid JavaScript identifier
   - `nest + list params` is rejected for now
4. Resolve result columns as usual.
5. Build `NestedGroup` entries by matching annotated source columns to resolved
   result columns.

Important behavior:

- Unknown columns in an annotation are ignored.
- A group is dropped if none of its columns match the resolved result.
- Nest annotations are stripped from the stored SQL in IR.

## IR model and why it was chosen

Implementation lives in `src/ir/query.rs` and `src/ir/mod.rs`.

Added structures:

- `NestedColumn { source_name, target_name, sql_type, nullable }`
- `NestedGroup { field_name, columns }`
- `Query.nested_groups: Vec<NestedGroup>`
- Helpers:
  - `has_nested_groups()`
  - `parent_columns()`

Why this model:

- **Backend-agnostic**: the frontend resolves nesting once; all backends consume
  the same IR contract.
- **Type-safe**: each nested field carries SQL type/nullability already resolved.
- **Low coupling**: keeps SQL parsing concerns out of codegen logic.
- **Predictable aggregation**: `parent_columns()` explicitly defines grouping keys.

## TypeScript/JavaScript codegen model

Implementation lives in `src/backend/typescript/core.rs` and
`src/backend/common.rs`.

For a nested query, the generator emits:

1. **Private flat row type**
   - `interface _QueryFlatRow` (TS only)
   - mirrors raw SQL result shape before aggregation
2. **Child type per group**
   - TS: `export interface Query_Group`
   - JS: `@typedef {Object} Query_Group`
3. **Parent type**
   - TS: `export interface QueryRow`
   - JS: `@typedef {Object} QueryRow`
   - contains only parent scalar fields + nested arrays

Naming helpers:

- `nested_type_name(query, field)` -> `Query_Field`
- `flat_row_type_name(query)` -> `_QueryFlatRow`

## Runtime aggregation model and why it was chosen

The generated runtime path for nested `:many` / `:one` uses:

- `Map` to group rows by parent key
- `Set` per nested group for child de-duplication
- `JSON.stringify([...])` keys for both parent and child identity

Why this model:

- **Portable** across PostgreSQL, SQLite, and MySQL outputs.
- **Simple generated code** (easy to debug and test).
- **Deterministic behavior** for repeated join rows.
- **No DB-specific JSON aggregation dependency** in SQL.

## Current constraints

The following combinations are currently rejected:

- `-- nest:` with `:exec` or `:execrows`
- `-- nest:` with list parameters (`@ids bigint[] ...`)

Reason:

- nesting currently relies on result-row aggregation, while list-param rewriting
  has target-specific SQL rewriting paths that are not yet integrated for nested
  queries in TS/JS generation.

## Test coverage added

Frontend tests: `src/frontend/common/query/tests/nest.rs`

- annotation parsing
- auto prefix stripping
- explicit aliases
- nullability with outer joins
- multiple groups
- invalid field name rejection
- non-supported command rejection
- list-param rejection
- stored SQL cleanup

Backend tests: `src/backend/typescript/tests/nested.rs`

- emitted flat/child/parent types
- parent-field projection correctness
- aggregation code generation (`Map`/`Set`/keys)
- `:one` and `:many` behavior
- PostgreSQL/SQLite/MySQL generation
- TypeScript and JavaScript output differences
- guardrail for `nest + list params`
- parser->IR->codegen roundtrip smoke test

## What is still missing

Recommended next steps:

1. Support `nest + list params` end-to-end in TS/JS codegen.
2. Add docs examples for multi-group nesting in `queries.md`.
3. Add one integration/e2e test that executes generated code against a real DB.
4. Evaluate configurable child identity keys (instead of all child columns).
5. Optionally expose a future backend-agnostic nested API to non-JS languages.

## Quick generated-shape example

Input:

```sql
-- name: GetUserWithCompanies :many
-- nest: company(company_id, company_name)
SELECT u.id, u.name, c.id AS company_id, c.name AS company_name
FROM users u
LEFT JOIN companies c ON c.user_id = u.id;
```

Conceptual output shape:

```ts
type GetUserWithCompaniesRow = {
  id: number;
  name: string;
  company: { id: number | null; name: string | null }[];
}
```
