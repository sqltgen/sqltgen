"""End-to-end runtime tests for the generated Python/MySQL queries.

Each test creates an isolated MySQL database named test_<uuid> and drops it
on teardown. Requires the docker-compose MySQL service on port 13306.
"""
import datetime
import decimal
import os
import pathlib
import uuid

import mysql.connector
import pytest

from gen import queries

_FIXTURES = pathlib.Path(__file__).parent / "../../../fixtures/bookstore/mysql"
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


def seed(conn: mysql.connector.MySQLConnection) -> None:
    """Insert a consistent set of test fixtures.

    Known IDs: author 1=Asimov, 2=Herbert, 3=Le Guin;
    book 1=Foundation, 2=I Robot, 3=Dune, 4=Earthsea; customer 1=Alice; sale 1.
    """
    queries.create_author(conn, "Asimov", "Sci-fi master", 1920)
    queries.create_author(conn, "Herbert", None, 1920)
    queries.create_author(conn, "Le Guin", "Earthsea", 1929)

    queries.create_book(conn, 1, "Foundation", "sci-fi", decimal.Decimal("9.99"),
                        datetime.date(1951, 1, 1))
    queries.create_book(conn, 1, "I Robot", "sci-fi", decimal.Decimal("7.99"),
                        datetime.date(1950, 1, 1))
    queries.create_book(conn, 2, "Dune", "sci-fi", decimal.Decimal("12.99"),
                        datetime.date(1965, 1, 1))
    queries.create_book(conn, 3, "Earthsea", "fantasy", decimal.Decimal("8.99"),
                        datetime.date(1968, 1, 1))

    queries.create_customer(conn, "Alice", "alice@example.com")
    queries.create_sale(conn, 1)
    queries.add_sale_item(conn, 1, 1, 2, decimal.Decimal("9.99"))   # Foundation qty 2
    queries.add_sale_item(conn, 1, 3, 1, decimal.Decimal("12.99"))  # Dune qty 1
    conn.commit()


# ─── :one tests ───────────────────────────────────────────────────────────────

def test_get_author(conn):
    seed(conn)
    author = queries.get_author(conn, 1)
    assert author is not None
    assert author.name == "Asimov"
    assert author.bio == "Sci-fi master"
    assert author.birth_year == 1920


def test_get_author_not_found(conn):
    assert queries.get_author(conn, 999) is None


def test_get_book(conn):
    seed(conn)
    book = queries.get_book(conn, 1)
    assert book is not None
    assert book.title == "Foundation"
    assert book.genre == "sci-fi"


# ─── :many tests ──────────────────────────────────────────────────────────────

def test_list_authors(conn):
    seed(conn)
    authors = queries.list_authors(conn)
    assert len(authors) == 3
    assert authors[0].name == "Asimov"
    assert authors[1].name == "Herbert"
    assert authors[2].name == "Le Guin"


def test_list_books_by_genre(conn):
    seed(conn)
    sci_fi = queries.list_books_by_genre(conn, "sci-fi")
    assert len(sci_fi) == 3
    fantasy = queries.list_books_by_genre(conn, "fantasy")
    assert len(fantasy) == 1
    assert fantasy[0].title == "Earthsea"


def test_list_books_by_genre_or_all(conn):
    seed(conn)
    all_books = queries.list_books_by_genre_or_all(conn, "all")
    assert len(all_books) == 4
    sci_fi = queries.list_books_by_genre_or_all(conn, "sci-fi")
    assert len(sci_fi) == 3


# ─── UpdateAuthorBio / DeleteAuthor tests ────────────────────────────────────

def test_update_author_bio(conn):
    seed(conn)
    queries.update_author_bio(conn, "Updated bio", 1)
    conn.commit()
    author = queries.get_author(conn, 1)
    assert author is not None
    assert author.bio == "Updated bio"


def test_delete_author(conn):
    queries.create_author(conn, "Temp", None, None)
    conn.commit()
    queries.delete_author(conn, 1)
    conn.commit()
    assert queries.get_author(conn, 1) is None


# ─── CreateBook / AddSaleItem tests ──────────────────────────────────────────

def test_create_book(conn):
    seed(conn)
    queries.create_book(conn, 1, "New Book", "mystery", decimal.Decimal("14.50"), None)
    conn.commit()
    book = queries.get_book(conn, 5)
    assert book is not None
    assert book.title == "New Book"
    assert book.genre == "mystery"
    assert book.published_at is None


def test_add_sale_item(conn):
    seed(conn)
    queries.add_sale_item(conn, 1, 4, 1, decimal.Decimal("8.99"))
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM sale_item WHERE sale_id = 1")
        count = cur.fetchone()[0]
    assert count == 3


# ─── :exec tests ──────────────────────────────────────────────────────────────

def test_create_author_exec(conn):
    queries.create_author(conn, "Test", None, None)
    conn.commit()
    author = queries.get_author(conn, 1)
    assert author is not None
    assert author.name == "Test"
    assert author.bio is None
    assert author.birth_year is None


def test_create_customer(conn):
    queries.create_customer(conn, "Bob", "bob@example.com")
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM customer WHERE name = 'Bob'")
        count = cur.fetchone()[0]
    assert count == 1


def test_create_sale(conn):
    seed(conn)
    queries.create_sale(conn, 1)
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM sale WHERE customer_id = 1")
        count = cur.fetchone()[0]
    assert count == 2


# ─── :execrows tests ──────────────────────────────────────────────────────────

def test_delete_book_by_id(conn):
    seed(conn)
    # I Robot (id=2) has no sale_items
    affected = queries.delete_book_by_id(conn, 2)
    conn.commit()
    assert affected == 1
    affected = queries.delete_book_by_id(conn, 999)
    conn.commit()
    assert affected == 0


# ─── CASE / COALESCE tests ────────────────────────────────────────────────────

def test_get_book_price_label(conn):
    seed(conn)
    rows = queries.get_book_price_label(conn, decimal.Decimal("10.00"))
    assert len(rows) == 4
    dune = next(r for r in rows if r.title == "Dune")
    assert dune.price_label == "expensive"
    earthsea = next(r for r in rows if r.title == "Earthsea")
    assert earthsea.price_label == "affordable"


def test_get_book_price_or_default(conn):
    seed(conn)
    rows = queries.get_book_price_or_default(conn, decimal.Decimal("0.00"))
    assert len(rows) == 4
    assert all(r.effective_price > 0 for r in rows)


# ─── Product type coverage ────────────────────────────────────────────────────

def test_get_product(conn):
    pid = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO product (id, sku, name, active, stock_count) VALUES (%s, %s, %s, TRUE, %s)",
            (pid, "SKU-001", "Widget", 5),
        )
    conn.commit()
    row = queries.get_product(conn, pid)
    assert row is not None
    assert row.id == pid
    assert row.name == "Widget"
    assert row.stock_count == 5


def test_list_active_products(conn):
    pid1, pid2 = str(uuid.uuid4()), str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO product (id, sku, name, active, stock_count) VALUES (%s, %s, %s, TRUE, %s)",
            (pid1, "ACT-1", "Active", 10),
        )
        cur.execute(
            "INSERT INTO product (id, sku, name, active, stock_count) VALUES (%s, %s, %s, FALSE, %s)",
            (pid2, "INACT-1", "Inactive", 0),
        )
    conn.commit()
    active = queries.list_active_products(conn, True)
    assert len(active) == 1
    assert active[0].name == "Active"
    inactive = queries.list_active_products(conn, False)
    assert len(inactive) == 1
    assert inactive[0].name == "Inactive"


# ─── JOIN tests ───────────────────────────────────────────────────────────────

def test_list_books_with_author(conn):
    seed(conn)
    rows = queries.list_books_with_author(conn)
    assert len(rows) == 4

    dune = next(r for r in rows if r.title == "Dune")
    assert dune.author_name == "Herbert"
    assert dune.author_bio is None

    foundation = next(r for r in rows if r.title == "Foundation")
    assert foundation.author_name == "Asimov"
    assert foundation.author_bio == "Sci-fi master"


def test_get_books_never_ordered(conn):
    seed(conn)
    # Seed has only Alice buying Foundation + Dune; I Robot and Earthsea were never ordered
    books = queries.get_books_never_ordered(conn)
    assert len(books) == 2
    titles = {b.title for b in books}
    assert titles == {"I Robot", "Earthsea"}


# ─── CTE tests ────────────────────────────────────────────────────────────────

def test_get_top_selling_books(conn):
    seed(conn)
    rows = queries.get_top_selling_books(conn)
    assert len(rows) > 0
    # Foundation qty 2 > Dune qty 1
    assert rows[0].title == "Foundation"


def test_get_best_customers(conn):
    seed(conn)
    rows = queries.get_best_customers(conn)
    assert len(rows) == 1
    assert rows[0].name == "Alice"


def test_get_author_stats(conn):
    seed(conn)
    rows = queries.get_author_stats(conn)
    assert len(rows) == 3
    asimov = next(r for r in rows if r.name == "Asimov")
    assert asimov.num_books == 2


# ─── Aggregate tests ──────────────────────────────────────────────────────────

def test_count_books_by_genre(conn):
    seed(conn)
    rows = queries.count_books_by_genre(conn)
    assert len(rows) == 2
    fantasy = next(r for r in rows if r.genre == "fantasy")
    assert fantasy.book_count == 1
    sci_fi = next(r for r in rows if r.genre == "sci-fi")
    assert sci_fi.book_count == 3


# ─── LIMIT/OFFSET tests ───────────────────────────────────────────────────────

def test_list_books_with_limit(conn):
    seed(conn)
    page1 = queries.list_books_with_limit(conn, 2, 0)
    assert len(page1) == 2
    page2 = queries.list_books_with_limit(conn, 2, 2)
    assert len(page2) == 2
    assert set(r.title for r in page1).isdisjoint(r.title for r in page2)


# ─── LIKE tests ───────────────────────────────────────────────────────────────

def test_search_books_by_title(conn):
    seed(conn)
    results = queries.search_books_by_title(conn, "%ound%")
    assert len(results) == 1
    assert results[0].title == "Foundation"
    results = queries.search_books_by_title(conn, "NOPE%")
    assert results == []


# ─── BETWEEN tests ────────────────────────────────────────────────────────────

def test_get_books_by_price_range(conn):
    seed(conn)
    # Foundation (9.99) and Earthsea (8.99) are in [8.00, 10.00]
    results = queries.get_books_by_price_range(
        conn, decimal.Decimal("8.00"), decimal.Decimal("10.00")
    )
    assert len(results) == 2


# ─── IN list tests ────────────────────────────────────────────────────────────

def test_get_books_in_genres(conn):
    seed(conn)
    results = queries.get_books_in_genres(conn, "sci-fi", "fantasy", "horror")
    assert len(results) == 4


# ─── HAVING tests ─────────────────────────────────────────────────────────────

def test_get_genres_with_many_books(conn):
    seed(conn)
    results = queries.get_genres_with_many_books(conn, 1)
    assert len(results) == 1
    assert results[0].genre == "sci-fi"
    assert results[0].book_count == 3


# ─── Subquery tests ───────────────────────────────────────────────────────────

def test_get_books_not_by_author(conn):
    seed(conn)
    results = queries.get_books_not_by_author(conn, "Asimov")
    assert len(results) == 2
    titles = {r.title for r in results}
    assert "Foundation" not in titles
    assert "I Robot" not in titles


def test_get_books_with_recent_sales(conn):
    seed(conn)
    # Sales are current; use a far-past cutoff
    results = queries.get_books_with_recent_sales(
        conn, datetime.datetime(2000, 1, 1)
    )
    # Foundation and Dune have sale_items
    assert len(results) == 2


# ─── Scalar subquery test ─────────────────────────────────────────────────────

def test_get_book_with_author_name(conn):
    seed(conn)
    rows = queries.get_book_with_author_name(conn)
    assert len(rows) == 4
    dune = next(r for r in rows if r.title == "Dune")
    assert dune.author_name == "Herbert"


# ─── JOIN with param tests ────────────────────────────────────────────────────

def test_get_books_by_author_param(conn):
    seed(conn)
    # birth_year > 1925 → only Le Guin (1929) → Earthsea
    results = queries.get_books_by_author_param(conn, 1925)
    assert len(results) == 1
    assert results[0].title == "Earthsea"


# ─── Qualified wildcard tests ─────────────────────────────────────────────────

def test_get_all_book_fields(conn):
    seed(conn)
    books = queries.get_all_book_fields(conn)
    assert len(books) == 4
    assert books[0].title == "Foundation"


# ─── List param tests ─────────────────────────────────────────────────────────

def test_get_books_by_ids(conn):
    seed(conn)
    books = queries.get_books_by_ids(conn, [1, 3])
    assert len(books) == 2
    titles = {b.title for b in books}
    assert titles == {"Foundation", "Dune"}


# ─── IS NULL / IS NOT NULL tests ─────────────────────────────────────────────

def test_get_authors_with_null_bio(conn):
    seed(conn)
    rows = queries.get_authors_with_null_bio(conn)
    assert len(rows) == 1
    assert rows[0].name == "Herbert"


def test_get_authors_with_bio(conn):
    seed(conn)
    rows = queries.get_authors_with_bio(conn)
    assert len(rows) == 2
    names = {r.name for r in rows}
    assert names == {"Asimov", "Le Guin"}


# ─── Date range tests ─────────────────────────────────────────────────────────

def test_get_books_published_between(conn):
    seed(conn)
    # 1951-01-01 to 1966-01-01 → Foundation (1951) and Dune (1965)
    rows = queries.get_books_published_between(
        conn,
        datetime.date(1951, 1, 1),
        datetime.date(1966, 1, 1),
    )
    assert len(rows) == 2
    titles = {r.title for r in rows}
    assert titles == {"Foundation", "Dune"}


# ─── DISTINCT tests ───────────────────────────────────────────────────────────

def test_get_distinct_genres(conn):
    seed(conn)
    rows = queries.get_distinct_genres(conn)
    assert len(rows) == 2
    genres = {r.genre for r in rows}
    assert genres == {"sci-fi", "fantasy"}


# ─── LEFT JOIN aggregate tests ────────────────────────────────────────────────

def test_get_books_with_sales_count(conn):
    seed(conn)
    rows = queries.get_books_with_sales_count(conn)
    assert len(rows) == 4

    foundation = next(r for r in rows if r.title == "Foundation")
    assert foundation.total_quantity == 2

    dune = next(r for r in rows if r.title == "Dune")
    assert dune.total_quantity == 1

    i_robot = next(r for r in rows if r.title == "I Robot")
    assert i_robot.total_quantity == 0


# ─── Scalar aggregate tests ───────────────────────────────────────────────────

def test_count_sale_items(conn):
    seed(conn)
    # Sale 1 (Alice): Foundation + Dune = 2 items
    row = queries.count_sale_items(conn, 1)
    assert row is not None
    assert row.item_count == 2


# ─── MIN/MAX/SUM/AVG aggregate tests ─────────────────────────────────────────

def test_get_sale_item_quantity_aggregates(conn):
    seed(conn)
    # Seed: Foundation qty 2 + Dune qty 1 → min=1, max=2, sum=3, avg=1.5
    row = queries.get_sale_item_quantity_aggregates(conn)
    assert row is not None
    assert row.min_qty == 1
    assert row.max_qty == 2
    assert row.sum_qty == decimal.Decimal("3")
    assert abs(float(row.avg_qty) - 1.5) < 0.01


def test_get_book_price_aggregates(conn):
    seed(conn)
    # Book prices: 9.99, 7.99, 12.99, 8.99 → min=7.99, max=12.99, sum=39.96, avg≈9.99
    row = queries.get_book_price_aggregates(conn)
    assert row is not None
    assert abs(float(row.min_price) - 7.99) < 0.01
    assert abs(float(row.max_price) - 12.99) < 0.01
    assert abs(float(row.sum_price) - 39.96) < 0.01
    assert abs(float(row.avg_price) - 9.99) < 0.01
