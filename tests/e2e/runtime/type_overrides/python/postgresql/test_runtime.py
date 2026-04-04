"""End-to-end runtime tests for type overrides: Python/PostgreSQL.

Each test creates an isolated PostgreSQL schema so tests can run in parallel.
Requires the docker-compose postgres service on port 15432.
"""

import datetime
import json
import os
import pathlib
import uuid

import psycopg
import pytest

from gen.queries import queries

_FIXTURES = pathlib.Path(__file__).parent / "../../../../fixtures/type_overrides"
_DB_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://sqltgen:sqltgen@localhost:15432/sqltgen_e2e",
)


# ─── Setup helpers ────────────────────────────────────────────────────────────


@pytest.fixture()
def conn():
    """Yield a psycopg connection with an isolated schema; drop it on teardown."""
    schema = "test_" + uuid.uuid4().hex
    schema_sql = (_FIXTURES / "schema.sql").read_text()

    with psycopg.connect(_DB_URL, autocommit=True) as c:
        c.execute(f'CREATE SCHEMA "{schema}"')
        c.execute(f'SET search_path TO "{schema}"')
        c.execute(schema_sql)
        yield c
        c.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


# ─── :one tests ───────────────────────────────────────────────────────────────


def test_insert_and_get_event(conn):
    doc_id = str(uuid.uuid4())
    payload = {"type": "click", "x": 10}
    meta = {"source": "web"}
    created_at = datetime.datetime(2024, 6, 1, 12, 0, 0)
    event_date = datetime.date(2024, 6, 1)
    event_time = datetime.time(9, 0, 0)

    scheduled_at = datetime.datetime(2024, 6, 1, 14, 0, 0, tzinfo=datetime.timezone.utc)

    queries.insert_event(
        conn, "login", payload, meta, doc_id, created_at, scheduled_at, event_date, event_time
    )

    ev = queries.get_event(conn, 1)
    assert ev is not None
    assert ev.name == "login"
    assert ev.payload == payload
    assert ev.meta == meta
    assert str(ev.doc_id) == doc_id
    assert ev.scheduled_at == scheduled_at
    assert ev.event_date == event_date
    assert ev.event_time == event_time


def test_get_event_not_found(conn):
    assert queries.get_event(conn, 999) is None


# ─── :many tests ──────────────────────────────────────────────────────────────


def test_list_events(conn):
    ts = datetime.datetime(2024, 6, 1, 12, 0, 0)
    queries.insert_event(
        conn, "alpha", {}, None, str(uuid.uuid4()), ts, None, None, None
    )
    queries.insert_event(
        conn, "beta", {}, None, str(uuid.uuid4()), ts, None, None, None
    )
    queries.insert_event(
        conn, "gamma", {}, None, str(uuid.uuid4()), ts, None, None, None
    )

    events = queries.list_events(conn)
    assert len(events) == 3
    assert events[0].name == "alpha"
    assert events[1].name == "beta"
    assert events[2].name == "gamma"


def test_get_events_by_date_range(conn):
    queries.insert_event(
        conn,
        "early",
        {},
        None,
        str(uuid.uuid4()),
        datetime.datetime(2024, 1, 1, 10, 0, 0),
        None,
        None,
        None,
    )
    queries.insert_event(
        conn,
        "mid",
        {},
        None,
        str(uuid.uuid4()),
        datetime.datetime(2024, 6, 1, 12, 0, 0),
        None,
        None,
        None,
    )
    queries.insert_event(
        conn,
        "late",
        {},
        None,
        str(uuid.uuid4()),
        datetime.datetime(2024, 12, 1, 15, 0, 0),
        None,
        None,
        None,
    )

    events = queries.get_events_by_date_range(
        conn,
        datetime.datetime(2024, 1, 1, 0, 0, 0),
        datetime.datetime(2024, 7, 1, 0, 0, 0),
    )
    assert len(events) == 2
    assert events[0].name == "early"
    assert events[1].name == "mid"


# ─── :exec tests ──────────────────────────────────────────────────────────────


def test_update_payload(conn):
    ts = datetime.datetime(2024, 6, 1, 12, 0, 0)
    queries.insert_event(
        conn, "test", {"v": 1}, {"source": "web"}, str(uuid.uuid4()), ts, None, None, None
    )

    updated = {"v": 2, "changed": True}
    queries.update_payload(conn, updated, None, 1)

    ev = queries.get_event(conn, 1)
    assert ev is not None
    assert ev.payload == updated
    assert ev.meta is None


def test_update_event_date(conn):
    ts = datetime.datetime(2024, 6, 1, 12, 0, 0)
    queries.insert_event(
        conn,
        "dated",
        {},
        None,
        str(uuid.uuid4()),
        ts,
        None,
        datetime.date(2024, 1, 1),
        None,
    )

    queries.update_event_date(conn, datetime.date(2024, 12, 31), 1)

    ev = queries.get_event(conn, 1)
    assert ev is not None
    assert ev.event_date == datetime.date(2024, 12, 31)


# ─── :execrows tests ──────────────────────────────────────────────────────────


def test_insert_event_rows(conn):
    n = queries.insert_event_rows(
        conn,
        "rowtest",
        {},
        None,
        str(uuid.uuid4()),
        datetime.datetime(2024, 6, 1, 12, 0, 0),
        None,
        None,
        None,
    )
    assert n == 1


# ─── projection tests ─────────────────────────────────────────────────────────


def test_find_by_date(conn):
    target = datetime.date(2024, 6, 15)
    queries.insert_event(
        conn,
        "dated",
        {},
        None,
        str(uuid.uuid4()),
        datetime.datetime(2024, 6, 1, 12, 0, 0),
        None,
        target,
        None,
    )

    row = queries.find_by_date(conn, target)
    assert row is not None
    assert row.name == "dated"


def test_find_by_uuid(conn):
    doc_id = str(uuid.uuid4())
    queries.insert_event(
        conn,
        "uuid-test",
        {},
        None,
        doc_id,
        datetime.datetime(2024, 6, 1, 12, 0, 0),
        None,
        None,
        None,
    )

    row = queries.find_by_uuid(conn, uuid.UUID(doc_id))
    assert row is not None
    assert row.name == "uuid-test"


# ─── count tests ──────────────────────────────────────────────────────────────


def test_count_events(conn):
    for i in range(1, 4):
        ts = datetime.datetime(2024, 6, i, 0, 0, 0)
        queries.insert_event(
            conn, f"ev{i}", {}, None, str(uuid.uuid4()), ts, None, None, None
        )

    row = queries.count_events(conn, datetime.datetime(2024, 1, 1, 0, 0, 0))
    assert row is not None
    assert row.total == 3
