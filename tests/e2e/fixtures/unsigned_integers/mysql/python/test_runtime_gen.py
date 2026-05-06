"""Runtime e2e test for MySQL UNSIGNED integer columns."""

import os
import pathlib
import uuid

import mysql.connector
import pytest

from gen.queries import queries

_FIXTURES = pathlib.Path(__file__).parent / ".."
_DB_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
_DB_PORT = int(os.environ.get("MYSQL_PORT", "13306"))
_DB_USER = os.environ.get("MYSQL_USER", "sqltgen")
_DB_PASS = os.environ.get("MYSQL_PASS", "sqltgen")
_DB_ROOT_USER = os.environ.get("MYSQL_ROOT_USER", "root")
_DB_ROOT_PASS = os.environ.get("MYSQL_ROOT_PASS", "sqltgen")
_DB_NAME = os.environ.get("MYSQL_DB", "sqltgen_e2e")


@pytest.fixture()
def conn():
    db_name = "test_" + uuid.uuid4().hex
    schema_sql = (_FIXTURES / "schema.sql").read_text()

    admin = mysql.connector.connect(
        host=_DB_HOST, port=_DB_PORT, user=_DB_ROOT_USER, password=_DB_ROOT_PASS, database=_DB_NAME
    )
    cur = admin.cursor()
    cur.execute(f"CREATE DATABASE `{db_name}`")
    cur.execute(f"GRANT ALL ON `{db_name}`.* TO '{_DB_USER}'@'%'")
    cur.close()
    admin.close()

    c = mysql.connector.connect(
        host=_DB_HOST, port=_DB_PORT, user=_DB_USER, password=_DB_PASS, database=db_name
    )
    cur = c.cursor()
    for stmt in schema_sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            cur.execute(stmt)
    c.commit()
    cur.close()
    yield c
    c.close()

    admin = mysql.connector.connect(
        host=_DB_HOST, port=_DB_PORT, user=_DB_ROOT_USER, password=_DB_ROOT_PASS, database=_DB_NAME
    )
    cur = admin.cursor()
    cur.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
    cur.close()
    admin.close()


def test_unsigned_integers_round_trip_through_full_range(conn):
    # Row 1: zero. Row 2: small. Row 3: each column at its maximum unsigned value.
    queries.insert_unsigned_row(conn, 0, 0, 0, 0, 0)
    queries.insert_unsigned_row(conn, 1, 1, 1, 1, 1)
    # BIGINT UNSIGNED max = 2^64 - 1; Python int is arbitrary precision.
    u64_max = (1 << 64) - 1
    queries.insert_unsigned_row(conn, 255, 65_535, 16_777_215, 4_294_967_295, u64_max)

    rows = queries.get_unsigned_rows(conn)
    assert len(rows) == 3

    assert (rows[0].u8_val, rows[0].u16_val, rows[0].u24_val, rows[0].u32_val, rows[0].u64_val) == (0, 0, 0, 0, 0)
    assert (rows[1].u8_val, rows[1].u16_val, rows[1].u24_val, rows[1].u32_val, rows[1].u64_val) == (1, 1, 1, 1, 1)

    assert rows[2].u8_val == 255
    assert rows[2].u16_val == 65_535
    assert rows[2].u24_val == 16_777_215
    assert rows[2].u32_val == 4_294_967_295
    # The critical correctness gate: 2^64-1 must round-trip without truncation.
    assert rows[2].u64_val == u64_max

    # The id column itself is BIGINT UNSIGNED.
    assert rows[0].id == 1
    assert rows[2].id == 3
