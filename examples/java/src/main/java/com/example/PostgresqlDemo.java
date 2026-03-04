package com.example;

import com.example.db.pg.Queries;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class PostgresqlDemo {

    public static void run() throws Exception {
        // H2 in PostgreSQL-compatible mode stands in for PostgreSQL in this demo.
        String url = "jdbc:h2:mem:pg_demo;DB_CLOSE_DELAY=-1";

        try (Connection conn = DriverManager.getConnection(url, "sa", "")) {
            createSchema(conn);
            seed(conn);
            query(conn);
        }
    }

    private static void createSchema(Connection conn) throws Exception {
        try (Statement st = conn.createStatement()) {
            st.execute("""
                CREATE TABLE author (
                    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
                    name       VARCHAR(255) NOT NULL,
                    bio        VARCHAR(1024),
                    birth_year INTEGER
                )""");
            st.execute("""
                CREATE TABLE book (
                    id           BIGINT         AUTO_INCREMENT PRIMARY KEY,
                    author_id    BIGINT         NOT NULL,
                    title        VARCHAR(255)   NOT NULL,
                    genre        VARCHAR(100)   NOT NULL,
                    price        NUMERIC(10, 2) NOT NULL,
                    published_at DATE,
                    FOREIGN KEY (author_id) REFERENCES author(id)
                )""");
            st.execute("""
                CREATE TABLE customer (
                    id    BIGINT       AUTO_INCREMENT PRIMARY KEY,
                    name  VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NOT NULL UNIQUE
                )""");
            st.execute("""
                CREATE TABLE sale (
                    id          BIGINT    AUTO_INCREMENT PRIMARY KEY,
                    customer_id BIGINT    NOT NULL,
                    ordered_at  TIMESTAMP NOT NULL DEFAULT NOW(),
                    FOREIGN KEY (customer_id) REFERENCES customer(id)
                )""");
            st.execute("""
                CREATE TABLE sale_item (
                    id         BIGINT         AUTO_INCREMENT PRIMARY KEY,
                    sale_id    BIGINT         NOT NULL,
                    book_id    BIGINT         NOT NULL,
                    quantity   INTEGER        NOT NULL,
                    unit_price NUMERIC(10, 2) NOT NULL,
                    FOREIGN KEY (sale_id)  REFERENCES sale(id),
                    FOREIGN KEY (book_id)  REFERENCES book(id)
                )""");
        }
    }

    private static void seed(Connection conn) throws Exception {
        Queries.createAuthor(conn, "Ursula K. Le Guin", "Science fiction and fantasy author", 1929);
        Queries.createAuthor(conn, "Frank Herbert",     "Author of the Dune series",           1920);
        Queries.createAuthor(conn, "Isaac Asimov",      null,                                  1920);
        System.out.println("[pg] inserted 3 authors");

        Queries.createBook(conn, 1L, "The Left Hand of Darkness", "sci-fi", new BigDecimal("12.99"), null);
        Queries.createBook(conn, 1L, "The Dispossessed",           "sci-fi", new BigDecimal("11.50"), null);
        Queries.createBook(conn, 2L, "Dune",                       "sci-fi", new BigDecimal("14.99"), null);
        Queries.createBook(conn, 3L, "Foundation",                 "sci-fi", new BigDecimal("10.99"), null);
        Queries.createBook(conn, 3L, "The Caves of Steel",         "sci-fi", new BigDecimal("9.99"),  null);
        System.out.println("[pg] inserted 5 books");

        Queries.createCustomer(conn, "Alice", "alice@example.com");
        Queries.createCustomer(conn, "Bob",   "bob@example.com");
        System.out.println("[pg] inserted 2 customers");

        Queries.createSale(conn, 1L);
        Queries.addSaleItem(conn, 1L, 3L, 2, new BigDecimal("14.99"));
        Queries.addSaleItem(conn, 1L, 4L, 1, new BigDecimal("10.99"));
        Queries.createSale(conn, 2L);
        Queries.addSaleItem(conn, 2L, 3L, 1, new BigDecimal("14.99"));
        Queries.addSaleItem(conn, 2L, 1L, 1, new BigDecimal("12.99"));
        System.out.println("[pg] inserted 2 sales with items");
    }

    private static void query(Connection conn) throws Exception {
        var authors = Queries.listAuthors(conn);
        System.out.println("[pg] listAuthors: " + authors.size() + " row(s)");

        var scifi = Queries.listBooksByGenre(conn, "sci-fi");
        System.out.println("[pg] listBooksByGenre(sci-fi): " + scifi.size() + " row(s)");

        System.out.println("[pg] listBooksWithAuthor:");
        Queries.listBooksWithAuthor(conn)
            .forEach(r -> System.out.println("  \"" + r.title() + "\" by " + r.authorName()));

        var neverOrdered = Queries.getBooksNeverOrdered(conn);
        System.out.println("[pg] getBooksNeverOrdered: " + neverOrdered.size() + " book(s)");
        neverOrdered.forEach(b -> System.out.println("  \"" + b.title() + "\""));

        System.out.println("[pg] getTopSellingBooks:");
        Queries.getTopSellingBooks(conn)
            .forEach(r -> System.out.println("  \"" + r.title() + "\" sold " + r.unitsSold()));

        System.out.println("[pg] getBestCustomers:");
        Queries.getBestCustomers(conn)
            .forEach(r -> System.out.println("  " + r.name() + " spent " + r.totalSpent()));
    }
}
