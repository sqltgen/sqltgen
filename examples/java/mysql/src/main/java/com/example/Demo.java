package com.example;

import com.example.db.QueriesDs;

import java.math.BigDecimal;
import com.mysql.cj.jdbc.MysqlDataSource;

public class Demo {

    // MySQL container runs on 3307 to avoid conflicts with any local MySQL instance.
    // allowPublicKeyRetrieval and useSSL=false are required by MySQL Connector/J 8+ for
    // plain-password auth when connecting without a client certificate.
    private static final String MYSQL_URL  = "jdbc:mysql://localhost:3307/sqltgen?allowPublicKeyRetrieval=true&useSSL=false";
    private static final String MYSQL_USER = "sqltgen";
    private static final String MYSQL_PASS = "sqltgen";

    public static void run() throws Exception {
        MysqlDataSource ds = new MysqlDataSource();
        ds.setURL(MYSQL_URL);
        ds.setUser(MYSQL_USER);
        ds.setPassword(MYSQL_PASS);
        var q = new QueriesDs(ds);
        seed(q);
        query(q);
    }

    private static void seed(QueriesDs q) throws Exception {
        // MySQL has no RETURNING, so INSERT returns void and IDs are sequential
        // starting from 1 on a fresh database (which docker compose always provides).
        q.createAuthor("Ursula K. Le Guin", "Science fiction and fantasy author", 1929);
        q.createAuthor("Frank Herbert",     "Author of the Dune series",           1920);
        q.createAuthor("Isaac Asimov",      null,                                  1920);
        System.out.println("[mysql] inserted 3 authors");

        q.createBook(1L, "The Left Hand of Darkness", "sci-fi", new BigDecimal("12.99"), null);
        q.createBook(1L, "The Dispossessed",           "sci-fi", new BigDecimal("11.50"), null);
        q.createBook(2L, "Dune",                       "sci-fi", new BigDecimal("14.99"), null);
        q.createBook(3L, "Foundation",                 "sci-fi", new BigDecimal("10.99"), null);
        q.createBook(3L, "The Caves of Steel",         "sci-fi", new BigDecimal("9.99"),  null);
        System.out.println("[mysql] inserted 5 books");

        q.createCustomer("Ed",   "ed@example.com");
        q.createCustomer("Faye", "faye@example.com");
        System.out.println("[mysql] inserted 2 customers");

        q.createSale(1L);
        q.addSaleItem(1L, 3L, 2, new BigDecimal("14.99"));  // Ed buys 2x Dune
        q.addSaleItem(1L, 4L, 1, new BigDecimal("10.99"));  // Ed buys 1x Foundation
        q.createSale(2L);
        q.addSaleItem(2L, 3L, 1, new BigDecimal("14.99"));  // Faye buys 1x Dune
        q.addSaleItem(2L, 1L, 1, new BigDecimal("12.99"));  // Faye buys 1x Left Hand
        System.out.println("[mysql] inserted 2 sales with items");

        // Insert a temp author (no books) so we can demo update/delete without FK violations.
        // Docker always provides a fresh DB, so sequential IDs are predictable.
        q.createAuthor("Temp Author", null, null);
    }

    private static void query(QueriesDs q) throws Exception {
        var authors = q.listAuthors();
        System.out.println("[mysql] listAuthors: " + authors.size() + " row(s)");

        var scifi = q.listBooksByGenre("sci-fi");
        System.out.println("[mysql] listBooksByGenre(sci-fi): " + scifi.size() + " row(s)");

        System.out.println("[mysql] listBooksWithAuthor:");
        q.listBooksWithAuthor()
            .forEach(r -> System.out.println("  \"" + r.title() + "\" by " + r.authorName()));

        var neverOrdered = q.getBooksNeverOrdered();
        System.out.println("[mysql] getBooksNeverOrdered: " + neverOrdered.size() + " book(s)");
        neverOrdered.forEach(b -> System.out.println("  \"" + b.title() + "\""));

        System.out.println("[mysql] getTopSellingBooks:");
        q.getTopSellingBooks()
            .forEach(r -> System.out.println("  \"" + r.title() + "\" sold " + r.unitsSold()));

        System.out.println("[mysql] getBestCustomers:");
        q.getBestCustomers()
            .forEach(r -> System.out.println("  " + r.name() + " spent " + r.totalSpent()));

        // Demonstrate UPDATE and DELETE (no RETURNING in MySQL).
        // Uses author 4 (the temp author with no books) to avoid FK constraint violations.
        q.updateAuthorBio("Updated bio", 4L);
        System.out.println("[mysql] updateAuthorBio: updated temp author");
        q.deleteAuthor(4L);
        System.out.println("[mysql] deleteAuthor: deleted temp author");
    }
}
