import decimal
import os
import secrets
from pathlib import Path

import mysql.connector
import pymysql

from gen.queries import queries

_HOST      = os.environ.get("MYSQL_HOST",     "127.0.0.1")
_PORT      = int(os.environ.get("MYSQL_PORT", "3307"))
_USER      = os.environ.get("MYSQL_USER",     "sqltgen")
_PASS      = os.environ.get("MYSQL_PASSWORD", "sqltgen")
_ROOT_USER = "root"
_ROOT_PASS = "sqltgen_root"


def seed(conn: mysql.connector.MySQLConnection) -> None:
    queries.create_author(conn, "Ursula K. Le Guin", "Science fiction and fantasy author", 1929)
    queries.create_author(conn, "Frank Herbert",     "Author of the Dune series",           1920)
    queries.create_author(conn, "Isaac Asimov",      None,                                  1920)
    print("[mysql] inserted 3 authors")

    # MySQL has no RETURNING — use known auto-increment IDs (1-based, fresh schema)
    queries.create_book(conn, 1, "The Left Hand of Darkness", "sci-fi", decimal.Decimal("12.99"), None)
    queries.create_book(conn, 1, "The Dispossessed",           "sci-fi", decimal.Decimal("11.50"), None)
    queries.create_book(conn, 2, "Dune",                       "sci-fi", decimal.Decimal("14.99"), None)
    queries.create_book(conn, 3, "Foundation",                 "sci-fi", decimal.Decimal("10.99"), None)
    queries.create_book(conn, 3, "The Caves of Steel",         "sci-fi", decimal.Decimal("9.99"),  None)
    print("[mysql] inserted 5 books")

    queries.create_customer(conn, "Eve",   "eve@example.com")
    queries.create_customer(conn, "Frank", "frank@example.com")
    print("[mysql] inserted 2 customers")

    queries.create_sale(conn, 1)
    queries.add_sale_item(conn, 1, 3, 2, decimal.Decimal("14.99"))
    queries.add_sale_item(conn, 1, 4, 1, decimal.Decimal("10.99"))
    queries.create_sale(conn, 2)
    queries.add_sale_item(conn, 2, 3, 1, decimal.Decimal("14.99"))
    queries.add_sale_item(conn, 2, 1, 1, decimal.Decimal("12.99"))
    print("[mysql] inserted 2 sales with items")
    conn.commit()


def query(conn: mysql.connector.MySQLConnection) -> None:
    authors = queries.list_authors(conn)
    print(f"[mysql] listAuthors: {len(authors)} row(s)")

    # Books inserted in seed have IDs 1–5; 1=Left Hand, 3=Dune.
    by_ids = queries.get_books_by_ids(conn, [1, 3])
    print(f"[mysql] getBooksByIds([1,3]): {len(by_ids)} row(s)")
    for b in by_ids:
        print(f'  "{b.title}"')

    scifi = queries.list_books_by_genre(conn, "sci-fi")
    print(f"[mysql] listBooksByGenre(sci-fi): {len(scifi)} row(s)")

    all_books = queries.list_books_by_genre_or_all(conn, "all")
    print(f"[mysql] listBooksByGenreOrAll(all): {len(all_books)} row(s) (repeated-param demo)")
    scifi2 = queries.list_books_by_genre_or_all(conn, "sci-fi")
    print(f"[mysql] listBooksByGenreOrAll(sci-fi): {len(scifi2)} row(s)")

    print("[mysql] listBooksWithAuthor:")
    for r in queries.list_books_with_author(conn):
        print(f'  "{r.title}" by {r.author_name}')

    never_ordered = queries.get_books_never_ordered(conn)
    print(f"[mysql] getBooksNeverOrdered: {len(never_ordered)} book(s)")
    for b in never_ordered:
        print(f'  "{b.title}"')

    print("[mysql] getTopSellingBooks:")
    for r in queries.get_top_selling_books(conn):
        print(f'  "{r.title}" sold {r.units_sold}')

    print("[mysql] getBestCustomers:")
    for r in queries.get_best_customers(conn):
        print(f"  {r.name} spent {r.total_spent}")

    # Demonstrate UPDATE and DELETE with a transient author (id=4, inserted last)
    queries.update_author_bio(conn, "Updated bio", 4)
    print("[mysql] updateAuthorBio: updated author id=4")
    queries.delete_author(conn, 4)
    print("[mysql] deleteAuthor: deleted author id=4")
    conn.commit()


def run(db_name: str) -> None:
    conn = mysql.connector.connect(
        host=_HOST, port=_PORT,
        user=_USER, password=_PASS,
        database=db_name,
        autocommit=False,
    )
    seed(conn)
    query(conn)
    conn.close()


def main() -> None:
    migrations_dir = os.environ.get("MIGRATIONS_DIR")
    if migrations_dir is None:
        run(os.environ.get("MYSQL_DATABASE", "sqltgen"))
        return

    db_name = f"sqltgen_{secrets.token_hex(4)}"

    # Use root to CREATE DATABASE and GRANT access to sqltgen user.
    admin = mysql.connector.connect(
        host=_HOST, port=_PORT,
        user=_ROOT_USER, password=_ROOT_PASS,
        autocommit=True,
    )
    try:
        cur = admin.cursor()
        cur.execute(f"CREATE DATABASE `{db_name}`")
        cur.execute(f"GRANT ALL ON `{db_name}`.* TO '{_USER}'@'%'")
        cur.close()

        mig_conn = pymysql.connect(
            host=_HOST, port=_PORT,
            user=_USER, password=_PASS,
            database=db_name,
            autocommit=True,
            client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS,
        )
        try:
            cur = mig_conn.cursor()
            for f in sorted(Path(migrations_dir).glob("*.sql")):
                cur.execute(f.read_text())
            cur.close()
        finally:
            mig_conn.close()

        run(db_name)
    finally:
        try:
            cur = admin.cursor()
            cur.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
            cur.close()
        except Exception as e:
            print(f"[mysql] warning: could not drop database {db_name}: {e}", flush=True)
        admin.close()


if __name__ == "__main__":
    main()
