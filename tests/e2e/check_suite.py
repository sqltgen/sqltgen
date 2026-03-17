#!/usr/bin/env python3
"""
tests/e2e/check_suite.py — e2e suite consistency checker

Verifies two things:

  1. FIXTURE CONSISTENCY — all three SQL dialect fixture files contain the
     same set of named queries. Gaps are flagged; genuinely dialect-specific
     queries (e.g. ones that use RETURNING or data-modifying CTEs) can be
     listed in KNOWN_DIALECT_GAPS to suppress the warning.

  2. TEST COVERAGE — every query declared in a dialect's fixture file has
     at least one test function in every runtime suite that targets that
     dialect. Coverage is determined by a normalized prefix match: the test
     name (after stripping the language-specific "test" prefix) must start
     with the normalized query name. One query can have many test functions
     (e.g. happy path + not-found), all of which count as coverage.

Usage:
  python tests/e2e/check_suite.py          # human-readable report
  python tests/e2e/check_suite.py --ci     # exit 1 if any unexpected gap
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────

E2E = Path(__file__).parent
RUNTIME = E2E / "runtime"
FIXTURES = E2E / "fixtures"

# ── Fixture files (dialect → path) ────────────────────────────────────────────

FIXTURE_FILES: dict[str, Path] = {
    "postgresql": FIXTURES / "bookstore" / "postgresql" / "queries.sql",
    "sqlite": FIXTURES / "bookstore" / "sqlite" / "queries.sql",
    "mysql": FIXTURES / "bookstore" / "mysql" / "queries.sql",
}

# ── Runtime test suites ────────────────────────────────────────────────────────
#
# Each entry: (dialect, language, test_file, name_pattern)
#
# name_pattern is a regex with one capture group that extracts the raw test
# name from the file. The normalization step (see `normalized_test_names`)
# handles stripping language prefixes and lowercasing.

SUITES: list[tuple[str, str, Path, str]] = [
    (
        "postgresql",
        "rust",
        RUNTIME / "bookstore/rust/postgresql/tests/runtime.rs",
        r"async fn (test_\w+)",
    ),
    (
        "sqlite",
        "rust",
        RUNTIME / "bookstore/rust/sqlite/tests/runtime.rs",
        r"async fn (test_\w+)",
    ),
    (
        "mysql",
        "rust",
        RUNTIME / "bookstore/rust/mysql/tests/runtime.rs",
        r"async fn (test_\w+)",
    ),
    (
        "postgresql",
        "java",
        RUNTIME
        / "bookstore/java/postgresql/src/test/java/com/example/db/RuntimeTest.java",
        r"void (test\w+)",
    ),
    (
        "postgresql",
        "kotlin",
        RUNTIME
        / "bookstore/kotlin/postgresql/src/test/kotlin/com/example/db/RuntimeTest.kt",
        r"fun (test\w+)",
    ),
    (
        "postgresql",
        "python",
        RUNTIME / "bookstore/python/postgresql/test_runtime.py",
        r"def (test_\w+)",
    ),
    (
        "sqlite",
        "python",
        RUNTIME / "bookstore/python/sqlite/test_runtime.py",
        r"def (test_\w+)",
    ),
    (
        "postgresql",
        "typescript",
        RUNTIME / "bookstore/typescript/postgresql/runtime.test.ts",
        r"it\(['\"]([^'\"]+)['\"]",
    ),
    (
        "sqlite",
        "typescript",
        RUNTIME / "bookstore/typescript/sqlite/runtime.test.ts",
        r"it\(['\"]([^'\"]+)['\"]",
    ),
    (
        "mysql",
        "typescript",
        RUNTIME / "bookstore/typescript/mysql/runtime.test.ts",
        r"it\(['\"]([^'\"]+)['\"]",
    ),
    (
        "sqlite",
        "java",
        RUNTIME / "bookstore/java/sqlite/src/test/java/com/example/db/RuntimeTest.java",
        r"void (test\w+)",
    ),
    (
        "mysql",
        "java",
        RUNTIME / "bookstore/java/mysql/src/test/java/com/example/db/RuntimeTest.java",
        r"void (test\w+)",
    ),
    (
        "sqlite",
        "kotlin",
        RUNTIME
        / "bookstore/kotlin/sqlite/src/test/kotlin/com/example/db/RuntimeTest.kt",
        r"fun (test\w+)",
    ),
    (
        "mysql",
        "kotlin",
        RUNTIME
        / "bookstore/kotlin/mysql/src/test/kotlin/com/example/db/RuntimeTest.kt",
        r"fun (test\w+)",
    ),
    (
        "mysql",
        "python",
        RUNTIME / "bookstore/python/mysql/test_runtime.py",
        r"def (test_\w+)",
    ),
    (
        "postgresql",
        "go",
        RUNTIME / "bookstore/go/postgresql/runtime_test.go",
        r"func (Test\w+)",
    ),
    (
        "sqlite",
        "go",
        RUNTIME / "bookstore/go/sqlite/runtime_test.go",
        r"func (Test\w+)",
    ),
    ("mysql", "go", RUNTIME / "bookstore/go/mysql/runtime_test.go", r"func (Test\w+)"),
]

# ── Known dialect gaps ────────────────────────────────────────────────────────
#
# Queries that are intentionally absent from some dialects because the dialect
# does not support the required SQL feature. These are NOT reported as gaps.
# Format: query_name → set of dialects where it is intentionally absent.

KNOWN_DIALECT_GAPS: dict[str, set[str]] = {
    # Data-modifying CTEs (WITH … DELETE … RETURNING) — PostgreSQL only.
    "ArchiveAndReturnBooks": {"sqlite", "mysql"},
    # INSERT … RETURNING — not supported by MySQL.
    "InsertProduct": {"mysql"},
    # ON CONFLICT … DO UPDATE SET — not supported by MySQL.
    "UpsertProduct": {"mysql"},
}

# ── Helpers ───────────────────────────────────────────────────────────────────


def parse_query_names(path: Path) -> list[str]:
    """Return all query names declared in a fixture file."""
    return re.findall(r"^-- name:\s+(\w+)", path.read_text(), re.MULTILINE)


def raw_test_names(path: Path, pattern: str) -> list[str]:
    """Extract raw test identifiers from a test file using the given regex."""
    if not path.exists():
        return []
    return re.findall(pattern, path.read_text())


def norm(s: str) -> str:
    """Normalize to lowercase alpha-only for fuzzy matching."""
    return re.sub(r"[^a-z]", "", s.lower())


def strip_test_prefix(name: str) -> str:
    """
    Remove the language-specific test prefix so the remainder can be matched
    against a normalized query name.

    - Rust / Python: ``test_get_author`` → ``get_author``
    - Java / Kotlin:  ``testGetAuthor``  → ``GetAuthor``
    - Go:             ``TestGetAuthor``  → ``GetAuthor``
    - TypeScript:     free-form string   → returned as-is (already starts
                      with the camelCase function name by convention)
    """
    if name.startswith("test_"):
        return name[5:]
    if re.match(r"[Tt]est[A-Z]", name):
        return name[4:]
    return name


def normalized_test_names(path: Path, pattern: str) -> list[str]:
    """Return normalized, prefix-stripped test names ready for matching."""
    return [norm(strip_test_prefix(t)) for t in raw_test_names(path, pattern)]


def is_covered(query_norm: str, test_norms: list[str]) -> bool:
    """
    Return True if any normalized test name starts with the normalized query
    name. One query can have many tests (happy path, not-found, edge cases);
    all count as coverage.

    Using a prefix match rather than substring avoids false positives where
    a longer query name (e.g. ``GetBooksWithSalesCount``) accidentally
    satisfies a shorter one (e.g. a hypothetical ``GetBooks``).
    """
    return any(t.startswith(query_norm) for t in test_norms)


# ── Report ────────────────────────────────────────────────────────────────────

PASS = "✓"
FAIL = "✗"
WARN = "!"


def print_fixture_section(
    fixture_queries: dict[str, list[str]],
    dialects: list[str],
) -> int:
    """Print fixture consistency table. Returns number of unexpected gaps."""
    all_queries = sorted({q for qs in fixture_queries.values() for q in qs})
    col_w = max(len(q) for q in all_queries)
    header_dialects = "  ".join(f"{d:>14}" for d in dialects)

    print("═" * 70)
    print("FIXTURE CONSISTENCY")
    print("═" * 70)
    print(f"  {'Query':<{col_w}}  {header_dialects}")
    print("  " + "─" * (col_w + 2 + 16 * len(dialects)))

    unexpected_gaps = 0
    for q in all_queries:
        cells = []
        unexpected_missing: list[str] = []
        for d in dialects:
            if q in fixture_queries[d]:
                cells.append(f"{PASS:>14}")
            else:
                known = KNOWN_DIALECT_GAPS.get(q, set())
                if d in known:
                    cells.append(f"{'(skip)':>14}")
                else:
                    cells.append(f"{FAIL:>14}")
                    unexpected_missing.append(d)
        flag = f"  {WARN} " if unexpected_missing else "    "
        if unexpected_missing:
            unexpected_gaps += 1
        print(f"{flag}{q:<{col_w}}  {'  '.join(cells)}")

    print()
    if unexpected_gaps:
        print(
            f"  {WARN} {unexpected_gaps} quer{'y' if unexpected_gaps == 1 else 'ies'} "
            f"missing from ≥1 dialect (not in KNOWN_DIALECT_GAPS)"
        )
    else:
        print(f"  {PASS} All queries present in all dialects (or marked as known gaps)")
    return unexpected_gaps


def print_coverage_section(
    fixture_queries: dict[str, list[str]],
    dialect_suites: dict[str, list[tuple[str, Path, str]]],
) -> int:
    """Print per-dialect coverage tables. Returns total number of gaps."""
    print()
    print("═" * 70)
    print("RUNTIME TEST COVERAGE")
    print("═" * 70)

    total_gaps = 0
    for dialect, queries in fixture_queries.items():
        suites = dialect_suites.get(dialect, [])
        print(f"\n  [{dialect}]")
        if not suites:
            print("    (no runtime suites configured)")
            continue

        # Pre-compute normalized test name lists for each suite.
        suite_test_norms: dict[str, list[str]] = {
            lang: normalized_test_names(path, pattern) for lang, path, pattern in suites
        }
        langs = [lang for lang, _, _ in suites]
        col_w = max(len(q) for q in queries)
        header_langs = "  ".join(f"{l:>12}" for l in langs)
        print(f"    {'Query':<{col_w}}  {header_langs}")
        print("    " + "─" * (col_w + 2 + 14 * len(langs)))

        for q in queries:
            qn = norm(q)
            cells = []
            missing: list[str] = []
            for lang in langs:
                if is_covered(qn, suite_test_norms[lang]):
                    cells.append(f"{PASS:>12}")
                else:
                    cells.append(f"{FAIL:>12}")
                    missing.append(lang)
            flag = f"  {WARN} " if missing else "    "
            if missing:
                total_gaps += 1
            print(f"  {flag}{q:<{col_w}}  {'  '.join(cells)}")

    return total_gaps


# ── Entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ci",
        action="store_true",
        help="Exit with status 1 if any unexpected gap is found.",
    )
    args = parser.parse_args()

    fixture_queries: dict[str, list[str]] = {
        d: parse_query_names(p) for d, p in FIXTURE_FILES.items()
    }
    dialects = list(FIXTURE_FILES.keys())

    dialect_suites: dict[str, list[tuple[str, Path, str]]] = {}
    for dialect, lang, path, pattern in SUITES:
        dialect_suites.setdefault(dialect, []).append((lang, path, pattern))

    fixture_gaps = print_fixture_section(fixture_queries, dialects)
    coverage_gaps = print_coverage_section(fixture_queries, dialect_suites)

    print()
    print("─" * 70)
    total = fixture_gaps + coverage_gaps
    if total:
        print(
            f"{WARN} {total} gap{'s' if total != 1 else ''} found "
            f"({fixture_gaps} fixture, {coverage_gaps} coverage)"
        )
    else:
        print(f"{PASS} All checks passed")
    print()

    if args.ci and total:
        sys.exit(1)


if __name__ == "__main__":
    main()
