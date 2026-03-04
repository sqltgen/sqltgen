package com.example;

import com.example.db.sqlite.Queries;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;

public class SqliteDemo {

    public static void run() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:")) {
            applyMigrations(conn);
            seed(conn);
            query(conn);
        }
    }

    private static void applyMigrations(Connection conn) throws Exception {
        Path migrationsDir = Path.of("../common/sqlite/migrations");
        List<Path> files = Files.list(migrationsDir)
            .filter(p -> p.toString().endsWith(".sql"))
            .sorted()
            .toList();

        try (Statement st = conn.createStatement()) {
            for (Path file : files) {
                String sql = Files.readString(file);
                for (String stmt : sql.split(";")) {
                    String s = stmt.strip();
                    if (!s.isEmpty()) st.execute(s);
                }
            }
        }
    }

    private static void seed(Connection conn) throws Exception {
        Queries.createAuthor(conn, "Ursula K. Le Guin", "Science fiction and fantasy author", 1929);
        Queries.createAuthor(conn, "Frank Herbert",     "Author of the Dune series",           1920);
        Queries.createAuthor(conn, "Isaac Asimov",      null,                                  1920);
        System.out.println("[sqlite] inserted 3 authors");

        Queries.createBook(conn, 1, "The Left Hand of Darkness", "sci-fi", new BigDecimal("12.99"), null);
        Queries.createBook(conn, 1, "The Dispossessed",           "sci-fi", new BigDecimal("11.50"), null);
        Queries.createBook(conn, 2, "Dune",                       "sci-fi", new BigDecimal("14.99"), null);
        Queries.createBook(conn, 3, "Foundation",                 "sci-fi", new BigDecimal("10.99"), null);
        Queries.createBook(conn, 3, "The Caves of Steel",         "sci-fi", new BigDecimal("9.99"),  null);
        System.out.println("[sqlite] inserted 5 books");

        Queries.createCustomer(conn, "Carol", "carol@example.com");
        Queries.createCustomer(conn, "Dave",  "dave@example.com");
        System.out.println("[sqlite] inserted 2 customers");

        Queries.createSale(conn, 1);
        Queries.addSaleItem(conn, 1, 3, 2, new BigDecimal("14.99"));
        Queries.addSaleItem(conn, 1, 4, 1, new BigDecimal("10.99"));
        Queries.createSale(conn, 2);
        Queries.addSaleItem(conn, 2, 3, 1, new BigDecimal("14.99"));
        Queries.addSaleItem(conn, 2, 1, 1, new BigDecimal("12.99"));
        System.out.println("[sqlite] inserted 2 sales with items");
    }

    private static void query(Connection conn) throws Exception {
        var authors = Queries.listAuthors(conn);
        System.out.println("[sqlite] listAuthors: " + authors.size() + " row(s)");

        var scifi = Queries.listBooksByGenre(conn, "sci-fi");
        System.out.println("[sqlite] listBooksByGenre(sci-fi): " + scifi.size() + " row(s)");

        System.out.println("[sqlite] listBooksWithAuthor:");
        Queries.listBooksWithAuthor(conn)
            .forEach(r -> System.out.println("  \"" + r.title() + "\" by " + r.authorName()));

        var neverOrdered = Queries.getBooksNeverOrdered(conn);
        System.out.println("[sqlite] getBooksNeverOrdered: " + neverOrdered.size() + " book(s)");
        neverOrdered.forEach(b -> System.out.println("  \"" + b.title() + "\""));

        System.out.println("[sqlite] getTopSellingBooks:");
        Queries.getTopSellingBooks(conn)
            .forEach(r -> System.out.println("  \"" + r.title() + "\" sold " + r.unitsSold()));

        System.out.println("[sqlite] getBestCustomers:");
        Queries.getBestCustomers(conn)
            .forEach(r -> System.out.println("  " + r.name() + " spent " + r.totalSpent()));
    }
}
