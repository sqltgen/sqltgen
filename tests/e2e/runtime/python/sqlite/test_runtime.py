"""End-to-end runtime tests for the generated Python/SQLite queries.

Uses an in-memory SQLite database so no external services are required.
"""
import decimal
import pathlib
import sqlite3

import pytest

from gen import queries

_FIXTURES = pathlib.Path(__file__).parent / "../../../fixtures/sqlite"


# ─── Setup helpers ────────────────────────────────────────────────────────────

def make_db() -> sqlite3.Connection:
    """Return a fresh in-memory SQLite connection with the fixture schema applied."""
    sqlite3.register_adapter(decimal.Decimal, float)
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript((_FIXTURES / "schema.sql").read_text())
    conn.commit()
    return conn


def seed(conn: sqlite3.Connection) -> None:
    """Insert a consistent set of test fixtures."""
    queries.create_author(conn, "Asimov", "Sci-fi master", 1920)
    queries.create_author(conn, "Herbert", None, 1920)
    queries.create_author(conn, "Le Guin", "Earthsea", 1929)

    queries.create_book(conn, 1, "Foundation", "sci-fi", decimal.Decimal("9.99"), "1951-01-01")
    queries.create_book(conn, 1, "I Robot", "sci-fi", decimal.Decimal("7.99"), "1950-01-01")
    queries.create_book(conn, 2, "Dune", "sci-fi", decimal.Decimal("12.99"), "1965-01-01")
    queries.create_book(conn, 3, "Earthsea", "fantasy", decimal.Decimal("8.99"), "1968-01-01")

    queries.create_customer(conn, "Alice", "alice@example.com")
    queries.create_sale(conn, 1)
    queries.add_sale_item(conn, 1, 1, 2, decimal.Decimal("9.99"))   # Foundation qty 2
    queries.add_sale_item(conn, 1, 3, 1, decimal.Decimal("12.99"))  # Dune qty 1

    conn.commit()


# ─── :one tests ───────────────────────────────────────────────────────────────

def test_get_author():
    conn = make_db()
    seed(conn)

    author = queries.get_author(conn, 1)
    assert author is not None
    assert author.name == "Asimov"
    assert author.bio == "Sci-fi master"
    assert author.birth_year == 1920


def test_get_author_not_found():
    conn = make_db()
    assert queries.get_author(conn, 999) is None


def test_get_book():
    conn = make_db()
    seed(conn)

    book = queries.get_book(conn, 1)
    assert book is not None
    assert book.title == "Foundation"
    assert book.genre == "sci-fi"
    assert book.author_id == 1


# ─── :many tests ──────────────────────────────────────────────────────────────

def test_list_authors():
    conn = make_db()
    seed(conn)

    authors = queries.list_authors(conn)
    assert len(authors) == 3
    # ORDER BY name
    assert authors[0].name == "Asimov"
    assert authors[1].name == "Herbert"
    assert authors[2].name == "Le Guin"


def test_list_books_by_genre():
    conn = make_db()
    seed(conn)

    sci_fi = queries.list_books_by_genre(conn, "sci-fi")
    assert len(sci_fi) == 3

    fantasy = queries.list_books_by_genre(conn, "fantasy")
    assert len(fantasy) == 1
    assert fantasy[0].title == "Earthsea"


def test_list_books_by_genre_or_all():
    conn = make_db()
    seed(conn)

    all_books = queries.list_books_by_genre_or_all(conn, "all")
    assert len(all_books) == 4

    sci_fi = queries.list_books_by_genre_or_all(conn, "sci-fi")
    assert len(sci_fi) == 3


# ─── CreateBook tests ─────────────────────────────────────────────────────────

def test_create_book():
    conn = make_db()
    seed(conn)
    queries.create_book(conn, 1, "New Book", "mystery", decimal.Decimal("14.50"), None)
    conn.commit()
    book = queries.get_book(conn, 5)
    assert book is not None
    assert book.title == "New Book"
    assert book.genre == "mystery"


# ─── CreateCustomer tests ─────────────────────────────────────────────────────

def test_create_customer():
    conn = make_db()
    queries.create_customer(conn, "Bob", "bob@example.com")
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM customer WHERE name = 'Bob'").fetchone()[0]
    assert count == 1


# ─── CreateSale tests ─────────────────────────────────────────────────────────

def test_create_sale():
    conn = make_db()
    queries.create_author(conn, "Alice", None, None)
    queries.create_customer(conn, "Alice", "alice@example.com")
    conn.commit()
    queries.create_sale(conn, 1)
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM sale WHERE customer_id = 1").fetchone()[0]
    assert count == 1


# ─── AddSaleItem tests ────────────────────────────────────────────────────────

def test_add_sale_item():
    conn = make_db()
    seed(conn)
    # Add Earthsea (book 4) to sale 1
    queries.add_sale_item(conn, 1, 4, 1, decimal.Decimal("8.99"))
    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM sale_item WHERE sale_id = 1").fetchone()[0]
    assert count == 3


# ─── CASE / COALESCE tests ────────────────────────────────────────────────────

def test_get_book_price_label():
    conn = make_db()
    seed(conn)
    rows = queries.get_book_price_label(conn, decimal.Decimal("10.00"))
    assert len(rows) == 4
    dune = next(r for r in rows if r.title == "Dune")
    assert dune.price_label == "expensive"
    earthsea = next(r for r in rows if r.title == "Earthsea")
    assert earthsea.price_label == "affordable"


def test_get_book_price_or_default():
    conn = make_db()
    seed(conn)
    rows = queries.get_book_price_or_default(conn, decimal.Decimal("0.00"))
    assert len(rows) == 4
    assert all(r.effective_price > 0 for r in rows)


# ─── Product type coverage ────────────────────────────────────────────────────

def test_get_product():
    conn = make_db()
    import uuid
    pid = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO product (id, sku, name, active, stock_count) VALUES (?, ?, ?, ?, ?)",
        (pid, "SKU-001", "Widget", 1, 5),
    )
    conn.commit()
    row = queries.get_product(conn, pid)
    assert row is not None
    assert row.id == pid
    assert row.name == "Widget"
    assert row.stock_count == 5


def test_list_active_products():
    conn = make_db()
    import uuid
    pid1, pid2 = str(uuid.uuid4()), str(uuid.uuid4())
    conn.execute(
        "INSERT INTO product (id, sku, name, active, stock_count) VALUES (?, ?, ?, ?, ?)",
        (pid1, "ACT-1", "Active", 1, 10),
    )
    conn.execute(
        "INSERT INTO product (id, sku, name, active, stock_count) VALUES (?, ?, ?, ?, ?)",
        (pid2, "INACT-1", "Inactive", 0, 0),
    )
    conn.commit()
    active = queries.list_active_products(conn, 1)
    assert len(active) == 1
    assert active[0].name == "Active"
    inactive = queries.list_active_products(conn, 0)
    assert len(inactive) == 1
    assert inactive[0].name == "Inactive"


# ─── UpdateAuthorBio / DeleteAuthor tests (new fixture queries) ───────────────

def test_update_author_bio():
    conn = make_db()
    seed(conn)
    queries.update_author_bio(conn, "Updated bio", 1)
    conn.commit()
    author = queries.get_author(conn, 1)
    assert author is not None
    assert author.bio == "Updated bio"


def test_delete_author():
    conn = make_db()
    queries.create_author(conn, "Temp", None, None)
    conn.commit()
    queries.delete_author(conn, 1)
    conn.commit()
    assert queries.get_author(conn, 1) is None


# ─── InsertProduct / UpsertProduct tests (new fixture queries) ────────────────

def test_insert_product():
    conn = make_db()
    import uuid
    pid = str(uuid.uuid4())
    queries.insert_product(conn, pid, "SKU-NEW", "Gadget", 1, None, None, None, None, 7)
    conn.commit()
    row = queries.get_product(conn, pid)
    assert row is not None
    assert row.name == "Gadget"
    assert row.stock_count == 7


def test_upsert_product():
    conn = make_db()
    import uuid
    pid = str(uuid.uuid4())
    queries.upsert_product(conn, pid, "SKU-UP", "Thing", 1, None, 10)
    conn.commit()
    row = queries.get_product(conn, pid)
    assert row is not None
    assert row.name == "Thing"
    assert row.stock_count == 10

    queries.upsert_product(conn, pid, "SKU-UP", "Thing Pro", 1, None, 20)
    conn.commit()
    updated = queries.get_product(conn, pid)
    assert updated is not None
    assert updated.name == "Thing Pro"
    assert updated.stock_count == 20


# ─── :exec tests ──────────────────────────────────────────────────────────────

def test_create_author_exec():
    conn = make_db()
    queries.create_author(conn, "Test", None, None)
    conn.commit()

    author = queries.get_author(conn, 1)
    assert author is not None
    assert author.name == "Test"
    assert author.bio is None
    assert author.birth_year is None


# ─── :execrows tests ──────────────────────────────────────────────────────────

def test_delete_book_by_id():
    conn = make_db()
    seed(conn)

    # Book 2 (I Robot) has no sale_items so can be deleted
    affected = queries.delete_book_by_id(conn, 2)
    conn.commit()
    assert affected == 1

    affected = queries.delete_book_by_id(conn, 999)
    conn.commit()
    assert affected == 0


# ─── JOIN tests ───────────────────────────────────────────────────────────────

def test_list_books_with_author():
    conn = make_db()
    seed(conn)

    rows = queries.list_books_with_author(conn)
    assert len(rows) == 4

    dune = next(r for r in rows if r.title == "Dune")
    assert dune.author_name == "Herbert"
    assert dune.author_bio is None

    foundation = next(r for r in rows if r.title == "Foundation")
    assert foundation.author_name == "Asimov"
    assert foundation.author_bio == "Sci-fi master"


def test_get_books_never_ordered():
    conn = make_db()
    seed(conn)

    books = queries.get_books_never_ordered(conn)
    # I Robot (2) and Earthsea (4) have no sale_items
    assert len(books) == 2
    titles = {b.title for b in books}
    assert titles == {"I Robot", "Earthsea"}


# ─── CTE tests ────────────────────────────────────────────────────────────────

def test_get_top_selling_books():
    conn = make_db()
    seed(conn)

    rows = queries.get_top_selling_books(conn)
    assert len(rows) > 0
    # Foundation qty 2 > Dune qty 1
    assert rows[0].title == "Foundation"


def test_get_best_customers():
    conn = make_db()
    seed(conn)

    rows = queries.get_best_customers(conn)
    assert len(rows) == 1
    assert rows[0].name == "Alice"


def test_get_author_stats():
    conn = make_db()
    seed(conn)

    rows = queries.get_author_stats(conn)
    assert len(rows) == 3
    # ORDER BY name → Asimov first
    asimov = rows[0]
    assert asimov.name == "Asimov"
    assert asimov.num_books == 2


# ─── Aggregate tests ──────────────────────────────────────────────────────────

def test_count_books_by_genre():
    conn = make_db()
    seed(conn)

    rows = queries.count_books_by_genre(conn)
    assert len(rows) == 2

    fantasy = next(r for r in rows if r.genre == "fantasy")
    assert fantasy.book_count == 1

    sci_fi = next(r for r in rows if r.genre == "sci-fi")
    assert sci_fi.book_count == 3


# ─── LIMIT/OFFSET tests ───────────────────────────────────────────────────────

def test_list_books_with_limit():
    conn = make_db()
    seed(conn)

    page1 = queries.list_books_with_limit(conn, 2, 0)
    assert len(page1) == 2

    page2 = queries.list_books_with_limit(conn, 2, 2)
    assert len(page2) == 2

    titles_p1 = {r.title for r in page1}
    titles_p2 = {r.title for r in page2}
    assert titles_p1.isdisjoint(titles_p2)


# ─── LIKE tests ───────────────────────────────────────────────────────────────

def test_search_books_by_title():
    conn = make_db()
    seed(conn)

    results = queries.search_books_by_title(conn, "%ound%")
    assert len(results) == 1
    assert results[0].title == "Foundation"

    results = queries.search_books_by_title(conn, "NOPE%")
    assert results == []


# ─── BETWEEN tests ────────────────────────────────────────────────────────────

def test_get_books_by_price_range():
    conn = make_db()
    seed(conn)

    # Foundation (9.99) and Earthsea (8.99) in [8.00, 10.00]
    results = queries.get_books_by_price_range(conn, decimal.Decimal("8.00"), decimal.Decimal("10.00"))
    assert len(results) == 2


# ─── IN list tests ────────────────────────────────────────────────────────────

def test_get_books_in_genres():
    conn = make_db()
    seed(conn)

    results = queries.get_books_in_genres(conn, "sci-fi", "fantasy", "horror")
    assert len(results) == 4


# ─── HAVING tests ─────────────────────────────────────────────────────────────

def test_get_genres_with_many_books():
    conn = make_db()
    seed(conn)

    # HAVING COUNT(*) > 1 → only sci-fi (3 books)
    results = queries.get_genres_with_many_books(conn, 1)
    assert len(results) == 1
    assert results[0].genre == "sci-fi"
    assert results[0].book_count == 3


# ─── Subquery tests ───────────────────────────────────────────────────────────

def test_get_books_not_by_author():
    conn = make_db()
    seed(conn)

    results = queries.get_books_not_by_author(conn, "Asimov")
    # Dune and Earthsea — not Foundation or I Robot
    assert len(results) == 2
    titles = {r.title for r in results}
    assert "Foundation" not in titles
    assert "I Robot" not in titles


def test_get_books_with_recent_sales():
    conn = make_db()
    seed(conn)

    # Sale was created with DEFAULT CURRENT_TIMESTAMP; use a past date
    results = queries.get_books_with_recent_sales(conn, "2000-01-01")
    # Foundation (book 1) and Dune (book 3) have sale_items
    assert len(results) == 2


# ─── Scalar subquery test ─────────────────────────────────────────────────────

def test_get_book_with_author_name():
    conn = make_db()
    seed(conn)

    rows = queries.get_book_with_author_name(conn)
    assert len(rows) == 4
    dune = next(r for r in rows if r.title == "Dune")
    assert dune.author_name == "Herbert"


# ─── JOIN with param tests ────────────────────────────────────────────────────

def test_get_books_by_author_param():
    conn = make_db()
    seed(conn)

    # birth_year > 1925 → only Le Guin (1929)
    results = queries.get_books_by_author_param(conn, 1925)
    assert len(results) == 1
    assert results[0].title == "Earthsea"


# ─── Qualified wildcard tests ─────────────────────────────────────────────────

def test_get_all_book_fields():
    conn = make_db()
    seed(conn)

    books = queries.get_all_book_fields(conn)
    assert len(books) == 4
    assert books[0].id == 1
    assert books[0].title == "Foundation"


# ─── List param tests ─────────────────────────────────────────────────────────

def test_get_books_by_ids():
    conn = make_db()
    seed(conn)

    books = queries.get_books_by_ids(conn, [1, 3])
    assert len(books) == 2
    titles = {b.title for b in books}
    assert titles == {"Foundation", "Dune"}


# ─── IS NULL / IS NOT NULL tests ─────────────────────────────────────────────

def test_get_authors_with_null_bio():
    conn = make_db()
    seed(conn)

    rows = queries.get_authors_with_null_bio(conn)
    assert len(rows) == 1
    assert rows[0].name == "Herbert"


def test_get_authors_with_bio():
    conn = make_db()
    seed(conn)

    rows = queries.get_authors_with_bio(conn)
    assert len(rows) == 2
    names = {r.name for r in rows}
    assert names == {"Asimov", "Le Guin"}


# ─── Date range tests ─────────────────────────────────────────────────────────

def test_get_books_published_between():
    conn = make_db()
    seed(conn)

    # 1951-01-01 to 1966-01-01 → Foundation (1951) and Dune (1965)
    rows = queries.get_books_published_between(conn, "1951-01-01", "1966-01-01")
    assert len(rows) == 2
    titles = {r.title for r in rows}
    assert titles == {"Foundation", "Dune"}


# ─── DISTINCT tests ───────────────────────────────────────────────────────────

def test_get_distinct_genres():
    conn = make_db()
    seed(conn)

    rows = queries.get_distinct_genres(conn)
    assert len(rows) == 2
    genres = {r.genre for r in rows}
    assert genres == {"sci-fi", "fantasy"}


# ─── LEFT JOIN aggregate tests ────────────────────────────────────────────────

def test_get_books_with_sales_count():
    conn = make_db()
    seed(conn)

    rows = queries.get_books_with_sales_count(conn)
    assert len(rows) == 4

    foundation = next(r for r in rows if r.title == "Foundation")
    assert foundation.total_quantity == 2

    dune = next(r for r in rows if r.title == "Dune")
    assert dune.total_quantity == 1

    earthsea = next(r for r in rows if r.title == "Earthsea")
    assert earthsea.total_quantity == 0


# ─── Scalar aggregate tests ───────────────────────────────────────────────────

def test_count_sale_items():
    conn = make_db()
    seed(conn)

    row = queries.count_sale_items(conn, 1)
    assert row is not None
    assert row.item_count == 2

    row = queries.count_sale_items(conn, 999)
    # COUNT(*) always returns a row, even for non-existent sale_id
    assert row is not None
    assert row.item_count == 0


# ─── MIN/MAX/SUM/AVG aggregate tests ─────────────────────────────────────────

def test_get_sale_item_quantity_aggregates():
    conn = make_db()
    seed(conn)
    # Seed: Foundation qty 2 + Dune qty 1 → min=1, max=2, sum=3, avg=1.5
    row = queries.get_sale_item_quantity_aggregates(conn)
    assert row is not None
    assert row.min_qty == 1
    assert row.max_qty == 2
    assert row.sum_qty == 3
    assert abs(float(row.avg_qty) - 1.5) < 0.01


def test_get_book_price_aggregates():
    conn = make_db()
    seed(conn)
    # Book prices: 9.99, 7.99, 12.99, 8.99 → min=7.99, max=12.99, sum=39.96, avg=9.99
    row = queries.get_book_price_aggregates(conn)
    assert row is not None
    assert abs(float(row.min_price) - 7.99) < 0.01
    assert abs(float(row.max_price) - 12.99) < 0.01
    assert abs(float(row.sum_price) - 39.96) < 0.01
    assert abs(float(row.avg_price) - 9.99) < 0.01
