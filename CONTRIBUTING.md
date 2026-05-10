# Contributing to sqltgen

Thank you for your interest in contributing. This document covers how to build,
test, and submit changes.

For a detailed walkthrough of the architecture, IR data model, and step-by-step
guides for adding new backends and dialects, see the
[Contributor Guide](docs/contributor-guide.md).

---

## Prerequisites

- **Rust** (stable toolchain) — [rustup.rs](https://rustup.rs)
- **Docker** (for running example integration tests)
- Java 21, Maven (for Java/Kotlin examples)
- Rust (for Rust example — same toolchain)
- Python 3.11+ (for Python example)

---

## Building

```sh
cargo build          # debug build
cargo build --release
```

The `sqltgen` binary lands at `target/debug/sqltgen` or `target/release/sqltgen`.

---

## Running the test suite

```sh
cargo test
```

All 289 tests should pass on a clean checkout. The suite is fully offline — no
database or Docker required.

### What the tests cover

- Config parsing
- Frontend: DDL parsing (CREATE/ALTER/DROP TABLE) for all three dialects
- Frontend: query parsing (SELECT/INSERT/UPDATE/DELETE, CTEs, JOINs, subqueries,
  named params, list params)
- Backend: generated Java, Kotlin, Rust, and Python code for representative queries

---

## Running the examples

Examples require Docker (PostgreSQL and MySQL) or just Rust/Java/Kotlin/Python.

```sh
# Single example (standalone — starts its own container, runs, tears down)
make -C examples/java/postgresql run
make -C examples/rust/sqlite run

# All examples at once (one shared PG container, one shared MySQL container)
make run-all
```

Each per-example directory has its own `Makefile`. See the file for `generate`,
`build`, and `run` targets.

---

## Code style

### Formatting and linting

Always run before committing:

```sh
cargo fmt
cargo clippy -- -D warnings
```

CI will reject PRs with formatter diffs or clippy warnings.

### Function and file size

- Functions over ~75 lines are a red flag — consider splitting.
- Functions over 100 lines are not accepted; split them.
- Each file should have a single clear responsibility.

### Quality ratchet

A snapshot of structural code-quality metrics is committed at
`quality-report.json` and enforced by CI via the `quality-gate` job.

- **What's measured.** Function length, function cognitive/cyclomatic
  complexity, function arg count, file length, functions per file, files
  per module. Thresholds and per-entity excesses live in the JSON file.
- **What's enforced.** Two rules per metric category: (1) no entity
  present in both old and new reports may regress individually, and
  (2) the per-category total of excesses may not increase. Improvements
  in one category cannot pay for regressions in another.
- **When you change code.** Run `make quality-generate` and commit the
  refreshed `quality-report.json` alongside your code. CI will fail if the
  committed file is out of sync (`make quality-check`) or if the ratchet
  rules are violated against `main` (`make quality-ratchet`).
- **Threshold edits.** Changing a threshold is a policy move; it must
  land in its own PR after the codebase complies.

Local quick-reference:

```sh
make quality-generate   # refresh quality-report.json
make quality-check      # fail if the file is out of sync
make quality-ratchet    # compare against origin/main
make quality            # quality-check + quality-ratchet
```

### Error handling

- **Recoverable errors** (unknown type, unsupported syntax): emit a warning with
  `eprintln!` and continue. Map unknown types to `SqlType::Custom`.
- **Fatal errors** (missing file, malformed config): return `anyhow::Error` and let
  `main` print and exit.

### Documentation

Every `pub` item must have a `///` doc comment explaining what it does and, where
not obvious, when to use it.

---

## Project structure

```
src/
  main.rs          — CLI entry point (clap)
  config.rs        — SqltgenConfig, Engine, OutputConfig
  frontend/        — SQL dialect parsers (DialectParser trait)
    common/        — Shared query parser, named params, list params
    postgres/      — PostgreSQL DDL + query parsing
    sqlite/        — SQLite DDL + query parsing
    mysql/         — MySQL DDL + query parsing
  ir/              — Intermediate representation (SqlType, Schema, Query, …)
  backend/         — Code generators (Codegen trait)
    java.rs        — Java backend
    kotlin.rs      — Kotlin backend
    rust.rs        — Rust/sqlx backend
    python.rs      — Python backend
    go.rs          — Go stub (unimplemented)
    typescript.rs  — TypeScript stub (unimplemented)
examples/
  common/          — Shared SQL (migrations, queries, docker-compose)
  java/            — Java example (postgresql / sqlite / mysql)
  kotlin/          — Kotlin example
  rust/            — Rust example
  python/          — Python example
```

The **IR is a strict boundary**: backends consume only IR types, never raw
`sqlparser` AST nodes.

---

## Adding a new backend or dialect

After any implementation change, update:
- `STATUS.md` — flip feature cells from ❌/⚠️ to ✅
- `PLAN.md` — mark completed items and remove from "Remaining work"

### Definition of done (new backend or dialect)

- [ ] `cargo build` — zero warnings
- [ ] `cargo clippy -- -D warnings` — clean
- [ ] Every public function has a `///` doc comment
- [ ] Every function has at least a happy-path test
- [ ] `STATUS.md` updated
- [ ] `PLAN.md` updated

---

## Opening a pull request

1. Fork the repository and create a feature branch.
2. Make your changes; run `cargo fmt` and `cargo clippy`.
3. Add or update tests to cover the change.
4. Open a PR with a clear description of what changes and why.

Please keep PRs focused — one logical change per PR makes review much easier.

---

## Questions?

Open an issue or start a discussion on GitHub.
