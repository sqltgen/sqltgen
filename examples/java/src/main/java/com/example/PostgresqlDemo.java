package com.example;

import com.example.db.pg.Queries;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;

public class PostgresqlDemo {

    private static final String PG_URL  = "jdbc:postgresql://localhost:5433/sqltgen";
    private static final String PG_USER = "sqltgen";
    private static final String PG_PASS = "sqltgen";

    public static void run() throws Exception {
        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USER, PG_PASS)) {
            seed(conn);
            query(conn);
        }
    }

    private static void seed(Connection conn) throws Exception {
        var leGuin  = Queries.createAuthor(conn, "Ursula K. Le Guin", "Science fiction and fantasy author", 1929).orElseThrow();
        var herbert = Queries.createAuthor(conn, "Frank Herbert",     "Author of the Dune series",           1920).orElseThrow();
        var asimov  = Queries.createAuthor(conn, "Isaac Asimov",      null,                                  1920).orElseThrow();
        System.out.println("[pg] inserted 3 authors (ids: " + leGuin.id() + ", " + herbert.id() + ", " + asimov.id() + ")");

        var lhod  = Queries.createBook(conn, leGuin.id(),  "The Left Hand of Darkness", "sci-fi", new BigDecimal("12.99"), null).orElseThrow();
        var disp  = Queries.createBook(conn, leGuin.id(),  "The Dispossessed",           "sci-fi", new BigDecimal("11.50"), null).orElseThrow();
        var dune  = Queries.createBook(conn, herbert.id(), "Dune",                       "sci-fi", new BigDecimal("14.99"), null).orElseThrow();
        var found = Queries.createBook(conn, asimov.id(),  "Foundation",                 "sci-fi", new BigDecimal("10.99"), null).orElseThrow();
        var caves = Queries.createBook(conn, asimov.id(),  "The Caves of Steel",         "sci-fi", new BigDecimal("9.99"),  null).orElseThrow();
        System.out.println("[pg] inserted 5 books");

        var alice = Queries.createCustomer(conn, "Alice", "alice@example.com").orElseThrow();
        var bob   = Queries.createCustomer(conn, "Bob",   "bob@example.com").orElseThrow();
        System.out.println("[pg] inserted 2 customers");

        var sale1 = Queries.createSale(conn, alice.id()).orElseThrow();
        Queries.addSaleItem(conn, sale1.id(), dune.id(),  2, new BigDecimal("14.99"));
        Queries.addSaleItem(conn, sale1.id(), found.id(), 1, new BigDecimal("10.99"));
        var sale2 = Queries.createSale(conn, bob.id()).orElseThrow();
        Queries.addSaleItem(conn, sale2.id(), dune.id(), 1, new BigDecimal("14.99"));
        Queries.addSaleItem(conn, sale2.id(), lhod.id(), 1, new BigDecimal("12.99"));
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

        // Demonstrate UPDATE RETURNING and DELETE RETURNING with a transient author
        var temp = Queries.createAuthor(conn, "Temp Author", null, null).orElseThrow();
        Queries.updateAuthorBio(conn, "Updated via UPDATE RETURNING", temp.id())
            .ifPresent(a -> System.out.println("[pg] updateAuthorBio: updated \"" + a.name() + "\" — bio: " + a.bio()));
        Queries.deleteAuthor(conn, temp.id())
            .ifPresent(r -> System.out.println("[pg] deleteAuthor: deleted \"" + r.name() + "\" (id=" + r.id() + ")"));
    }
}
