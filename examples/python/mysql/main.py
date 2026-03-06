import decimal
import os

import mysql.connector

from gen import queries

DB_HOST = os.environ.get("MYSQL_HOST",     "127.0.0.1")
DB_PORT = int(os.environ.get("MYSQL_PORT", "3307"))
DB_USER = os.environ.get("MYSQL_USER",     "sqltgen")
DB_PASS = os.environ.get("MYSQL_PASSWORD", "sqltgen")
DB_NAME = os.environ.get("MYSQL_DATABASE", "sqltgen")


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


def main() -> None:
    conn = mysql.connector.connect(
        host=DB_HOST, port=DB_PORT,
        user=DB_USER, password=DB_PASS,
        database=DB_NAME,
        autocommit=False,
    )
    seed(conn)
    query(conn)
    conn.close()


if __name__ == "__main__":
    main()
