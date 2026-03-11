import decimal
import os
import secrets
from pathlib import Path

import psycopg

from gen import queries

_HOST = "localhost"
_PORT = 5433
_USER = "sqltgen"
_PASS = "sqltgen"


def seed(conn: psycopg.Connection) -> None:
    le_guin = queries.create_author(conn, "Ursula K. Le Guin", "Science fiction and fantasy author", 1929)
    herbert = queries.create_author(conn, "Frank Herbert",     "Author of the Dune series",           1920)
    asimov  = queries.create_author(conn, "Isaac Asimov",      None,                                  1920)
    print(f"[pg] inserted 3 authors (ids: {le_guin.id}, {herbert.id}, {asimov.id})")

    lhod  = queries.create_book(conn, le_guin.id, "The Left Hand of Darkness", "sci-fi", decimal.Decimal("12.99"), None)
    disp  = queries.create_book(conn, le_guin.id, "The Dispossessed",           "sci-fi", decimal.Decimal("11.50"), None)
    dune  = queries.create_book(conn, herbert.id, "Dune",                       "sci-fi", decimal.Decimal("14.99"), None)
    found = queries.create_book(conn, asimov.id,  "Foundation",                 "sci-fi", decimal.Decimal("10.99"), None)
    _caves = queries.create_book(conn, asimov.id, "The Caves of Steel",         "sci-fi", decimal.Decimal("9.99"),  None)
    print("[pg] inserted 5 books")

    alice = queries.create_customer(conn, "Alice", "alice@example.com")
    bob   = queries.create_customer(conn, "Bob",   "bob@example.com")
    print("[pg] inserted 2 customers")

    sale1 = queries.create_sale(conn, alice.id)
    queries.add_sale_item(conn, sale1.id, dune.id,  2, decimal.Decimal("14.99"))
    queries.add_sale_item(conn, sale1.id, found.id, 1, decimal.Decimal("10.99"))
    sale2 = queries.create_sale(conn, bob.id)
    queries.add_sale_item(conn, sale2.id, dune.id, 1, decimal.Decimal("14.99"))
    queries.add_sale_item(conn, sale2.id, lhod.id, 1, decimal.Decimal("12.99"))
    print("[pg] inserted 2 sales with items")


def query(conn: psycopg.Connection) -> None:
    authors = queries.list_authors(conn)
    print(f"[pg] listAuthors: {len(authors)} row(s)")

    # Book IDs are BIGSERIAL starting at 1 on a fresh DB; 1=Left Hand, 3=Dune.
    by_ids = queries.get_books_by_ids(conn, [1, 3])
    print(f"[pg] getBooksByIds([1,3]): {len(by_ids)} row(s)")
    for b in by_ids:
        print(f'  "{b.title}"')

    scifi = queries.list_books_by_genre(conn, "sci-fi")
    print(f"[pg] listBooksByGenre(sci-fi): {len(scifi)} row(s)")

    all_books = queries.list_books_by_genre_or_all(conn, "all")
    print(f"[pg] listBooksByGenreOrAll(all): {len(all_books)} row(s) (repeated-param demo)")
    scifi2 = queries.list_books_by_genre_or_all(conn, "sci-fi")
    print(f"[pg] listBooksByGenreOrAll(sci-fi): {len(scifi2)} row(s)")

    print("[pg] listBooksWithAuthor:")
    for r in queries.list_books_with_author(conn):
        print(f'  "{r.title}" by {r.author_name}')

    never_ordered = queries.get_books_never_ordered(conn)
    print(f"[pg] getBooksNeverOrdered: {len(never_ordered)} book(s)")
    for b in never_ordered:
        print(f'  "{b.title}"')

    print("[pg] getTopSellingBooks:")
    for r in queries.get_top_selling_books(conn):
        print(f'  "{r.title}" sold {r.units_sold}')

    print("[pg] getBestCustomers:")
    for r in queries.get_best_customers(conn):
        print(f"  {r.name} spent {r.total_spent}")

    # Demonstrate UPDATE RETURNING and DELETE RETURNING with a transient author
    temp = queries.create_author(conn, "Temp Author", None, None)
    updated = queries.update_author_bio(conn, "Updated via UPDATE RETURNING", temp.id)
    if updated:
        print(f'[pg] updateAuthorBio: updated "{updated.name}" — bio: {updated.bio}')
    deleted = queries.delete_author(conn, temp.id)
    if deleted:
        print(f'[pg] deleteAuthor: deleted "{deleted.name}" (id={deleted.id})')


def run(db_url: str) -> None:
    with psycopg.connect(db_url, autocommit=True) as conn:
        seed(conn)
        query(conn)


def main() -> None:
    migrations_dir = os.environ.get("MIGRATIONS_DIR")
    if migrations_dir is None:
        db_url = os.environ.get(
            "DATABASE_URL",
            f"postgresql://{_USER}:{_PASS}@{_HOST}:{_PORT}/sqltgen",
        )
        run(db_url)
        return

    db_name   = f"sqltgen_{secrets.token_hex(4)}"
    admin_url = f"postgresql://{_USER}:{_PASS}@{_HOST}:{_PORT}/postgres"
    db_url    = f"postgresql://{_USER}:{_PASS}@{_HOST}:{_PORT}/{db_name}"

    with psycopg.connect(admin_url, autocommit=True) as conn:
        conn.execute(f'CREATE DATABASE "{db_name}"')
    try:
        migration_files = sorted(Path(migrations_dir).glob("*.sql"))
        with psycopg.connect(db_url, autocommit=True) as conn:
            for f in migration_files:
                conn.execute(f.read_text())
        run(db_url)
    finally:
        try:
            with psycopg.connect(admin_url, autocommit=True) as conn:
                conn.execute(f'DROP DATABASE IF EXISTS "{db_name}"')
        except Exception as e:
            print(f"[pg] warning: could not drop database {db_name}: {e}", flush=True)


if __name__ == "__main__":
    main()
