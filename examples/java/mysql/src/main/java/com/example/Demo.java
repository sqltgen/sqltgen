package com.example;

import com.example.db.Queries;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;

public class Demo {

    // MySQL container runs on 3307 to avoid conflicts with any local MySQL instance.
    // allowPublicKeyRetrieval and useSSL=false are required by MySQL Connector/J 8+ for
    // plain-password auth when connecting without a client certificate.
    private static final String MYSQL_URL  = "jdbc:mysql://localhost:3307/sqltgen?allowPublicKeyRetrieval=true&useSSL=false";
    private static final String MYSQL_USER = "sqltgen";
    private static final String MYSQL_PASS = "sqltgen";

    public static void run() throws Exception {
        try (Connection conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASS)) {
            seed(conn);
            query(conn);
        }
    }

    private static void seed(Connection conn) throws Exception {
        // MySQL has no RETURNING, so INSERT returns void and IDs are sequential
        // starting from 1 on a fresh database (which docker compose always provides).
        Queries.createAuthor(conn, "Ursula K. Le Guin", "Science fiction and fantasy author", 1929);
        Queries.createAuthor(conn, "Frank Herbert",     "Author of the Dune series",           1920);
        Queries.createAuthor(conn, "Isaac Asimov",      null,                                  1920);
        System.out.println("[mysql] inserted 3 authors");

        Queries.createBook(conn, 1L, "The Left Hand of Darkness", "sci-fi", new BigDecimal("12.99"), null);
        Queries.createBook(conn, 1L, "The Dispossessed",           "sci-fi", new BigDecimal("11.50"), null);
        Queries.createBook(conn, 2L, "Dune",                       "sci-fi", new BigDecimal("14.99"), null);
        Queries.createBook(conn, 3L, "Foundation",                 "sci-fi", new BigDecimal("10.99"), null);
        Queries.createBook(conn, 3L, "The Caves of Steel",         "sci-fi", new BigDecimal("9.99"),  null);
        System.out.println("[mysql] inserted 5 books");

        Queries.createCustomer(conn, "Ed",   "ed@example.com");
        Queries.createCustomer(conn, "Faye", "faye@example.com");
        System.out.println("[mysql] inserted 2 customers");

        Queries.createSale(conn, 1L);
        Queries.addSaleItem(conn, 1L, 3L, 2, new BigDecimal("14.99"));  // Ed buys 2x Dune
        Queries.addSaleItem(conn, 1L, 4L, 1, new BigDecimal("10.99"));  // Ed buys 1x Foundation
        Queries.createSale(conn, 2L);
        Queries.addSaleItem(conn, 2L, 3L, 1, new BigDecimal("14.99"));  // Faye buys 1x Dune
        Queries.addSaleItem(conn, 2L, 1L, 1, new BigDecimal("12.99"));  // Faye buys 1x Left Hand
        System.out.println("[mysql] inserted 2 sales with items");

        // Insert a temp author (no books) so we can demo update/delete without FK violations.
        // Docker always provides a fresh DB, so sequential IDs are predictable.
        Queries.createAuthor(conn, "Temp Author", null, null);
    }

    private static void query(Connection conn) throws Exception {
        var authors = Queries.listAuthors(conn);
        System.out.println("[mysql] listAuthors: " + authors.size() + " row(s)");

        var scifi = Queries.listBooksByGenre(conn, "sci-fi");
        System.out.println("[mysql] listBooksByGenre(sci-fi): " + scifi.size() + " row(s)");

        System.out.println("[mysql] listBooksWithAuthor:");
        Queries.listBooksWithAuthor(conn)
            .forEach(r -> System.out.println("  \"" + r.title() + "\" by " + r.authorName()));

        var neverOrdered = Queries.getBooksNeverOrdered(conn);
        System.out.println("[mysql] getBooksNeverOrdered: " + neverOrdered.size() + " book(s)");
        neverOrdered.forEach(b -> System.out.println("  \"" + b.title() + "\""));

        System.out.println("[mysql] getTopSellingBooks:");
        Queries.getTopSellingBooks(conn)
            .forEach(r -> System.out.println("  \"" + r.title() + "\" sold " + r.unitsSold()));

        System.out.println("[mysql] getBestCustomers:");
        Queries.getBestCustomers(conn)
            .forEach(r -> System.out.println("  " + r.name() + " spent " + r.totalSpent()));

        // Demonstrate UPDATE and DELETE (no RETURNING in MySQL).
        // Uses author 4 (the temp author with no books) to avoid FK constraint violations.
        Queries.updateAuthorBio(conn, "Updated bio", 4L);
        System.out.println("[mysql] updateAuthorBio: updated temp author");
        Queries.deleteAuthor(conn, 4L);
        System.out.println("[mysql] deleteAuthor: deleted temp author");
    }
}
