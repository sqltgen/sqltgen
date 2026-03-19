"""End-to-end runtime tests for type overrides: Python/SQLite.

Uses an in-memory SQLite database — no external services required.
"""

import json
import pathlib
import sqlite3

import pytest

from gen import queries

_FIXTURES = pathlib.Path(__file__).parent / "../../../../fixtures/type_overrides/sqlite"


# ─── Setup helpers ────────────────────────────────────────────────────────────


def make_db() -> sqlite3.Connection:
    """Return a fresh in-memory SQLite connection with the fixture schema applied."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript((_FIXTURES / "schema.sql").read_text())
    conn.commit()
    return conn


# ─── :one tests ───────────────────────────────────────────────────────────────


def test_insert_and_get_event():
    conn = make_db()
    queries.insert_event(
        conn,
        "login",
        json.dumps({"type": "click", "x": 10}),
        json.dumps({"source": "web"}),
        "doc-001",
        "2024-06-01 12:00:00",
        "2024-06-01 14:00:00",
        "2024-06-01",
        "09:00:00",
    )
    ev = queries.get_event(conn, 1)
    assert ev is not None
    assert ev.name == "login"
    assert ev.doc_id == "doc-001"
    assert ev.event_date == "2024-06-01"
    assert ev.event_time == "09:00:00"


def test_get_event_not_found():
    conn = make_db()
    assert queries.get_event(conn, 999) is None


# ─── :many tests ──────────────────────────────────────────────────────────────


def test_list_events():
    conn = make_db()
    queries.insert_event(
        conn, "alpha", "{}", None, "doc-1", "2024-06-01 12:00:00", None, None, None
    )
    queries.insert_event(
        conn, "beta", "{}", None, "doc-2", "2024-06-01 12:00:00", None, None, None
    )
    queries.insert_event(
        conn, "gamma", "{}", None, "doc-3", "2024-06-01 12:00:00", None, None, None
    )

    events = queries.list_events(conn)
    assert len(events) == 3
    assert events[0].name == "alpha"
    assert events[1].name == "beta"
    assert events[2].name == "gamma"


def test_get_events_by_date_range():
    conn = make_db()
    queries.insert_event(
        conn, "early", "{}", None, "doc-1", "2024-01-01 10:00:00", None, None, None
    )
    queries.insert_event(
        conn, "mid", "{}", None, "doc-2", "2024-06-01 12:00:00", None, None, None
    )
    queries.insert_event(
        conn, "late", "{}", None, "doc-3", "2024-12-01 15:00:00", None, None, None
    )

    events = queries.get_events_by_date_range(
        conn, "2024-01-01 00:00:00", "2024-07-01 00:00:00"
    )
    assert len(events) == 2
    assert events[0].name == "early"
    assert events[1].name == "mid"


# ─── :exec tests ──────────────────────────────────────────────────────────────


def test_update_payload():
    conn = make_db()
    queries.insert_event(
        conn, "test", '{"v":1}', None, "doc-1", "2024-06-01 12:00:00", None, None, None
    )
    queries.update_payload(conn, '{"v":2}', None, 1)

    ev = queries.get_event(conn, 1)
    assert ev is not None
    assert ev.payload == '{"v":2}'
    assert ev.meta is None


def test_update_event_date():
    conn = make_db()
    queries.insert_event(
        conn,
        "dated",
        "{}",
        None,
        "doc-1",
        "2024-06-01 12:00:00",
        None,
        "2024-01-01",
        None,
    )
    queries.update_event_date(conn, "2024-12-31", 1)

    ev = queries.get_event(conn, 1)
    assert ev is not None
    assert ev.event_date == "2024-12-31"


# ─── :execrows tests ──────────────────────────────────────────────────────────


def test_insert_event_rows():
    conn = make_db()
    n = queries.insert_event_rows(
        conn, "rowtest", "{}", None, "doc-1", "2024-06-01 12:00:00", None, None, None
    )
    assert n == 1


# ─── projection tests ─────────────────────────────────────────────────────────


def test_find_by_date():
    conn = make_db()
    queries.insert_event(
        conn,
        "dated",
        "{}",
        None,
        "doc-1",
        "2024-06-01 12:00:00",
        None,
        "2024-06-15",
        None,
    )

    row = queries.find_by_date(conn, "2024-06-15")
    assert row is not None
    assert row.name == "dated"


def test_find_by_doc_id():
    conn = make_db()
    queries.insert_event(
        conn,
        "doctest",
        "{}",
        None,
        "unique-doc-id",
        "2024-06-01 12:00:00",
        None,
        None,
        None,
    )

    row = queries.find_by_doc_id(conn, "unique-doc-id")
    assert row is not None
    assert row.name == "doctest"


# ─── count tests ──────────────────────────────────────────────────────────────


def test_count_events():
    conn = make_db()
    for i in range(1, 4):
        queries.insert_event(
            conn,
            f"ev{i}",
            "{}",
            None,
            f"doc-{i}",
            f"2024-06-0{i} 00:00:00",
            None,
            None,
            None,
        )

    row = queries.count_events(conn, "2024-01-01 00:00:00")
    assert row is not None
    assert row.total == 3
