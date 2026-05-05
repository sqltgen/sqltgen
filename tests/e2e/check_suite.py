#!/usr/bin/env python3
"""
tests/e2e/check_suite.py — fixture consistency checker

Verifies that all three SQL dialect bookstore fixtures contain the same set
of named queries. Genuine dialect-specific gaps (queries that use features
not supported in some engines) can be listed in KNOWN_DIALECT_GAPS to
suppress the warning.

Usage:
  python tests/e2e/check_suite.py          # human-readable report
  python tests/e2e/check_suite.py --ci     # exit 1 if any unexpected gap
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

E2E = Path(__file__).parent
FIXTURES = E2E / "fixtures"

# Dialect → fixture queries.sql path
FIXTURE_FILES: dict[str, Path] = {
    "postgresql": FIXTURES / "bookstore" / "postgresql" / "queries.sql",
    "sqlite":     FIXTURES / "bookstore" / "sqlite"     / "queries.sql",
    "mysql":      FIXTURES / "bookstore" / "mysql"      / "queries.sql",
}

# Queries that are intentionally absent from some dialects because the dialect
# does not support the required SQL feature.
KNOWN_DIALECT_GAPS: dict[str, set[str]] = {
    # Data-modifying CTEs (WITH … DELETE … RETURNING) — PostgreSQL only.
    "ArchiveAndReturnBooks": {"sqlite", "mysql"},
    # INSERT … RETURNING — not supported by MySQL.
    "InsertProduct": {"mysql"},
    # ON CONFLICT … DO UPDATE SET — not supported by MySQL.
    "UpsertProduct": {"mysql"},
    # Runtime-only view fixture currently exists only in PostgreSQL bookstore schema.
    "ListBookSummariesView": {"sqlite", "mysql"},
}

PASS = "✓"
FAIL = "✗"
WARN = "!"


def parse_query_names(path: Path) -> list[str]:
    """Return all query names declared in a fixture file."""
    return re.findall(r"^-- name:\s+(\w+)", path.read_text(), re.MULTILINE)


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
    all_queries = sorted({q for qs in fixture_queries.values() for q in qs})
    col_w = max(len(q) for q in all_queries)

    print("═" * 70)
    print("FIXTURE CONSISTENCY (bookstore)")
    print("═" * 70)
    header_dialects = "  ".join(f"{d:>14}" for d in dialects)
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

    if args.ci and unexpected_gaps:
        sys.exit(1)


if __name__ == "__main__":
    main()
