import decimal
import pathlib
import sqlite3

from gen import queries


def apply_migrations(conn: sqlite3.Connection) -> None:
    migrations_dir = pathlib.Path("../../common/sqlite/migrations")
    for path in sorted(migrations_dir.glob("*.sql")):
        sql = path.read_text()
        for stmt in sql.split(";"):
            s = stmt.strip()
            if s:
                conn.execute(s)
    conn.commit()


def seed(conn: sqlite3.Connection) -> None:
    queries.create_author(conn, "Ursula K. Le Guin", "Science fiction and fantasy author", 1929)
    queries.create_author(conn, "Frank Herbert",     "Author of the Dune series",           1920)
    queries.create_author(conn, "Isaac Asimov",      None,                                  1920)
    print("[sqlite] inserted 3 authors")

    queries.create_book(conn, 1, "The Left Hand of Darkness", "sci-fi", decimal.Decimal("12.99"), None)
    queries.create_book(conn, 1, "The Dispossessed",           "sci-fi", decimal.Decimal("11.50"), None)
    queries.create_book(conn, 2, "Dune",                       "sci-fi", decimal.Decimal("14.99"), None)
    queries.create_book(conn, 3, "Foundation",                 "sci-fi", decimal.Decimal("10.99"), None)
    queries.create_book(conn, 3, "The Caves of Steel",         "sci-fi", decimal.Decimal("9.99"),  None)
    print("[sqlite] inserted 5 books")

    queries.create_customer(conn, "Carol", "carol@example.com")
    queries.create_customer(conn, "Dave",  "dave@example.com")
    print("[sqlite] inserted 2 customers")

    queries.create_sale(conn, 1)
    queries.add_sale_item(conn, 1, 3, 2, decimal.Decimal("14.99"))
    queries.add_sale_item(conn, 1, 4, 1, decimal.Decimal("10.99"))
    queries.create_sale(conn, 2)
    queries.add_sale_item(conn, 2, 3, 1, decimal.Decimal("14.99"))
    queries.add_sale_item(conn, 2, 1, 1, decimal.Decimal("12.99"))
    print("[sqlite] inserted 2 sales with items")
    conn.commit()


def query(conn: sqlite3.Connection) -> None:
    authors = queries.list_authors(conn)
    print(f"[sqlite] listAuthors: {len(authors)} row(s)")

    scifi = queries.list_books_by_genre(conn, "sci-fi")
    print(f"[sqlite] listBooksByGenre(sci-fi): {len(scifi)} row(s)")

    print("[sqlite] listBooksWithAuthor:")
    for r in queries.list_books_with_author(conn):
        print(f'  "{r.title}" by {r.author_name}')

    never_ordered = queries.get_books_never_ordered(conn)
    print(f"[sqlite] getBooksNeverOrdered: {len(never_ordered)} book(s)")
    for b in never_ordered:
        print(f'  "{b.title}"')

    print("[sqlite] getTopSellingBooks:")
    for r in queries.get_top_selling_books(conn):
        print(f'  "{r.title}" sold {r.units_sold}')

    print("[sqlite] getBestCustomers:")
    for r in queries.get_best_customers(conn):
        print(f"  {r.name} spent {r.total_spent}")


def main() -> None:
    sqlite3.register_adapter(decimal.Decimal, float)
    conn = sqlite3.connect(":memory:")
    apply_migrations(conn)
    seed(conn)
    query(conn)
    conn.close()


if __name__ == "__main__":
    main()