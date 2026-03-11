"""End-to-end runtime tests for the generated Python/PostgreSQL queries.

Each test creates an isolated PostgreSQL schema so tests can run in parallel.
Requires the docker-compose postgres service on port 15432.
"""
import decimal
import os
import pathlib
import uuid

import psycopg
import pytest

from gen import queries

_FIXTURES = pathlib.Path(__file__).parent / "../../../fixtures/postgresql"
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


def seed(conn: psycopg.Connection) -> None:
    """Insert a consistent set of test fixtures."""
    a1 = queries.create_author(conn, "Asimov", "Sci-fi master", 1920)
    a2 = queries.create_author(conn, "Herbert", None, 1920)
    a3 = queries.create_author(conn, "Le Guin", "Earthsea", 1929)
    assert a1 and a2 and a3

    b1 = queries.create_book(conn, a1.id, "Foundation", "sci-fi", decimal.Decimal("9.99"),
                             __import__("datetime").date(1951, 1, 1))
    b2 = queries.create_book(conn, a1.id, "I Robot", "sci-fi", decimal.Decimal("7.99"),
                             __import__("datetime").date(1950, 1, 1))
    b3 = queries.create_book(conn, a2.id, "Dune", "sci-fi", decimal.Decimal("12.99"),
                             __import__("datetime").date(1965, 1, 1))
    b4 = queries.create_book(conn, a3.id, "Earthsea", "fantasy", decimal.Decimal("8.99"),
                             __import__("datetime").date(1968, 1, 1))
    assert b1 and b2 and b3 and b4

    alice = queries.create_customer(conn, "Alice", "alice@example.com")
    bob = queries.create_customer(conn, "Bob", "bob@example.com")
    assert alice and bob

    # Alice buys Foundation (qty 2) + Dune (qty 1)
    sale1 = queries.create_sale(conn, alice.id)
    assert sale1
    queries.add_sale_item(conn, sale1.id, b1.id, 2, decimal.Decimal("9.99"))
    queries.add_sale_item(conn, sale1.id, b3.id, 1, decimal.Decimal("12.99"))

    # Bob buys Earthsea (qty 1)
    sale2 = queries.create_sale(conn, bob.id)
    assert sale2
    queries.add_sale_item(conn, sale2.id, b4.id, 1, decimal.Decimal("8.99"))


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


# ─── :exec tests ──────────────────────────────────────────────────────────────

def test_create_author_returns_row(conn):
    author = queries.create_author(conn, "Test", None, None)
    assert author is not None
    assert author.name == "Test"
    assert author.bio is None
    assert author.birth_year is None


# ─── :execrows tests ──────────────────────────────────────────────────────────

def test_delete_book_by_id(conn):
    seed(conn)
    affected = queries.delete_book_by_id(conn, 2)
    assert affected == 1
    affected = queries.delete_book_by_id(conn, 999)
    assert affected == 0


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
    books = queries.get_books_never_ordered(conn)
    # I Robot (b2) was not ordered
    assert len(books) == 1
    assert books[0].title == "I Robot"


# ─── CTE tests ────────────────────────────────────────────────────────────────

def test_get_top_selling_books(conn):
    seed(conn)
    rows = queries.get_top_selling_books(conn)
    assert len(rows) > 0
    assert rows[0].title == "Foundation"  # qty 2 > others


def test_get_best_customers(conn):
    seed(conn)
    rows = queries.get_best_customers(conn)
    assert rows[0].name == "Alice"  # spent more than Bob


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
    results = queries.get_books_by_price_range(
        conn, decimal.Decimal("8.00"), decimal.Decimal("10.00")
    )
    # Foundation (9.99) and Earthsea (8.99) are in range
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
    import datetime
    results = queries.get_books_with_recent_sales(
        conn, datetime.datetime(2000, 1, 1)
    )
    # Foundation (b1), Dune (b3), Earthsea (b4) all have sale_items
    assert len(results) == 3


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
    import datetime
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
    row = queries.count_sale_items(conn, 1)
    assert row is not None
    assert row.item_count == 2


# ─── Upsert tests (PostgreSQL-specific) ──────────────────────────────────────

def test_upsert_product(conn):
    product_id = uuid.UUID("00000000-0000-0000-0000-000000000001")

    # Insert
    row = queries.upsert_product(conn, product_id, "SKU-001", "Widget", True, ["tag1"], 10)
    assert row is not None
    assert row.name == "Widget"
    assert row.stock_count == 10

    # Update — same id, different name and stock_count
    row = queries.upsert_product(conn, product_id, "SKU-001", "Widget Pro", True, ["tag1", "tag2"], 25)
    assert row is not None
    assert row.name == "Widget Pro"
    assert row.stock_count == 25


# ─── CTE DELETE tests (PostgreSQL-specific) ───────────────────────────────────

def test_archive_and_return_books(conn):
    seed(conn)
    import datetime
    # Archive books published before 1951-01-01.
    # Only I Robot (1950-01-01) qualifies; it has no sale_items so no FK violation.
    archived = queries.archive_and_return_books(conn, datetime.date(1951, 1, 1))
    assert len(archived) == 1
    assert archived[0].title == "I Robot"

    # Verify it's gone from the main table
    books = queries.list_books_by_genre(conn, "sci-fi")
    assert all(b.title != "I Robot" for b in books)
