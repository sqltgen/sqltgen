"""End-to-end runtime tests for type overrides: Python/MySQL.

Each test creates an isolated MySQL database named test_<uuid> and drops it
on teardown. Requires the docker-compose MySQL service on port 13306.
"""

import json
import os
import pathlib
import uuid

import mysql.connector
import pytest

from gen import queries

_FIXTURES = pathlib.Path(__file__).parent / "../../../../fixtures/type_overrides/mysql"
_MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
_MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "13306"))


# ─── Database lifecycle helpers ───────────────────────────────────────────────


def _admin_conn() -> mysql.connector.MySQLConnection:
    return mysql.connector.connect(
        host=_MYSQL_HOST,
        port=_MYSQL_PORT,
        user="root",
        password="sqltgen",
    )


@pytest.fixture()
def conn():
    """Yield a mysql.connector connection backed by a fresh per-test database."""
    db_name = "test_" + uuid.uuid4().hex

    admin = _admin_conn()
    cur = admin.cursor()
    cur.execute(f"CREATE DATABASE `{db_name}`")
    cur.execute(f"GRANT ALL ON `{db_name}`.* TO 'sqltgen'@'%'")
    admin.commit()
    cur.close()
    admin.close()

    c = mysql.connector.connect(
        host=_MYSQL_HOST,
        port=_MYSQL_PORT,
        user="sqltgen",
        password="sqltgen",
        database=db_name,
    )
    schema_sql = (_FIXTURES / "schema.sql").read_text()
    with c.cursor() as cur:
        for stmt in schema_sql.split(";"):
            stmt = stmt.strip()
            if stmt:
                cur.execute(stmt)
    c.commit()

    yield c

    c.close()
    admin = _admin_conn()
    cur = admin.cursor()
    cur.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
    admin.commit()
    cur.close()
    admin.close()


# ─── :one tests ───────────────────────────────────────────────────────────────


def test_insert_and_get_event(conn):
    payload = {"type": "click", "x": 10}
    meta = {"source": "web"}

    queries.insert_event(
        conn,
        "login",
        json.dumps(payload),
        json.dumps(meta),
        "doc-001",
        "2024-06-01 12:00:00",
        "2024-06-01 14:00:00",
        "2024-06-01",
        "09:00:00",
    )
    conn.commit()

    ev = queries.get_event(conn, 1)
    assert ev is not None
    assert ev.name == "login"
    assert ev.doc_id == "doc-001"
    assert str(ev.event_date) == "2024-06-01"
    assert str(ev.event_time) == "09:00:00"


def test_get_event_not_found(conn):
    assert queries.get_event(conn, 999) is None


# ─── :many tests ──────────────────────────────────────────────────────────────


def test_list_events(conn):
    ts = "2024-06-01 12:00:00"
    for name, doc in [("alpha", "doc-1"), ("beta", "doc-2"), ("gamma", "doc-3")]:
        queries.insert_event(conn, name, "{}", None, doc, ts, None, None, None)
    conn.commit()

    events = queries.list_events(conn)
    assert len(events) == 3
    assert events[0].name == "alpha"
    assert events[1].name == "beta"
    assert events[2].name == "gamma"


def test_get_events_by_date_range(conn):
    queries.insert_event(
        conn, "early", "{}", None, "doc-1", "2024-01-01 10:00:00", None, None, None
    )
    queries.insert_event(
        conn, "mid", "{}", None, "doc-2", "2024-06-01 12:00:00", None, None, None
    )
    queries.insert_event(
        conn, "late", "{}", None, "doc-3", "2024-12-01 15:00:00", None, None, None
    )
    conn.commit()

    events = queries.get_events_by_date_range(
        conn, "2024-01-01 00:00:00", "2024-07-01 00:00:00"
    )
    assert len(events) == 2
    assert events[0].name == "early"
    assert events[1].name == "mid"


# ─── :exec tests ──────────────────────────────────────────────────────────────


def test_update_payload(conn):
    queries.insert_event(
        conn, "test", '{"v":1}', None, "doc-1", "2024-06-01 12:00:00", None, None, None
    )
    conn.commit()

    queries.update_payload(conn, '{"v":2}', None, 1)
    conn.commit()

    ev = queries.get_event(conn, 1)
    assert ev is not None
    assert ev.meta is None


def test_update_event_date(conn):
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
    conn.commit()

    queries.update_event_date(conn, "2024-12-31", 1)
    conn.commit()

    ev = queries.get_event(conn, 1)
    assert ev is not None
    assert str(ev.event_date) == "2024-12-31"


# ─── :execrows tests ──────────────────────────────────────────────────────────


def test_insert_event_rows(conn):
    n = queries.insert_event_rows(
        conn, "rowtest", "{}", None, "doc-1", "2024-06-01 12:00:00", None, None, None
    )
    conn.commit()
    assert n == 1


# ─── projection tests ─────────────────────────────────────────────────────────


def test_find_by_date(conn):
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
    conn.commit()

    row = queries.find_by_date(conn, "2024-06-15")
    assert row is not None
    assert row.name == "dated"


def test_find_by_doc_id(conn):
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
    conn.commit()

    row = queries.find_by_doc_id(conn, "unique-doc-id")
    assert row is not None
    assert row.name == "doctest"


# ─── count tests ──────────────────────────────────────────────────────────────


def test_count_events(conn):
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
    conn.commit()

    row = queries.count_events(conn, "2024-01-01 00:00:00")
    assert row is not None
    assert row.total == 3
