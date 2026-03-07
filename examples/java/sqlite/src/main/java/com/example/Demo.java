package com.example;

import com.example.db.QueriesDs;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import org.sqlite.SQLiteDataSource;

public class Demo {

    // file::memory:?cache=shared allows multiple connections to share the same
    // in-memory database. The keeper connection in run() holds it open for the
    // full demo lifetime so the data survives across QueriesDs method calls.
    private static final String SQLITE_URL = "jdbc:sqlite:file::memory:?cache=shared";

    public static void run() throws Exception {
        SQLiteDataSource ds = new SQLiteDataSource();
        ds.setUrl(SQLITE_URL);
        // Keep one connection open so the in-memory DB is not dropped between calls.
        try (Connection keeper = ds.getConnection()) {
            applyMigrations(keeper);
            var q = new QueriesDs(ds);
            seed(q);
            query(q);
        }
    }

    private static void applyMigrations(Connection conn) throws Exception {
        Path migrationsDir = Path.of("../../common/sqlite/migrations");
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

    private static void seed(QueriesDs q) throws Exception {
        q.createAuthor("Ursula K. Le Guin", "Science fiction and fantasy author", 1929);
        q.createAuthor("Frank Herbert",     "Author of the Dune series",           1920);
        q.createAuthor("Isaac Asimov",      null,                                  1920);
        System.out.println("[sqlite] inserted 3 authors");

        q.createBook(1, "The Left Hand of Darkness", "sci-fi", new BigDecimal("12.99"), null);
        q.createBook(1, "The Dispossessed",           "sci-fi", new BigDecimal("11.50"), null);
        q.createBook(2, "Dune",                       "sci-fi", new BigDecimal("14.99"), null);
        q.createBook(3, "Foundation",                 "sci-fi", new BigDecimal("10.99"), null);
        q.createBook(3, "The Caves of Steel",         "sci-fi", new BigDecimal("9.99"),  null);
        System.out.println("[sqlite] inserted 5 books");

        q.createCustomer("Carol", "carol@example.com");
        q.createCustomer("Dave",  "dave@example.com");
        System.out.println("[sqlite] inserted 2 customers");

        q.createSale(1);
        q.addSaleItem(1, 3, 2, new BigDecimal("14.99"));
        q.addSaleItem(1, 4, 1, new BigDecimal("10.99"));
        q.createSale(2);
        q.addSaleItem(2, 3, 1, new BigDecimal("14.99"));
        q.addSaleItem(2, 1, 1, new BigDecimal("12.99"));
        System.out.println("[sqlite] inserted 2 sales with items");
    }

    private static void query(QueriesDs q) throws Exception {
        var authors = q.listAuthors();
        System.out.println("[sqlite] listAuthors: " + authors.size() + " row(s)");

        // Books inserted in seed have IDs 1–5; 1=Left Hand, 3=Dune.
        var byIds = q.getBooksByIds(List.of(1L, 3L));
        System.out.println("[sqlite] getBooksByIds([1,3]): " + byIds.size() + " row(s)");
        byIds.forEach(b -> System.out.println("  \"" + b.title() + "\""));

        var scifi = q.listBooksByGenre("sci-fi");
        System.out.println("[sqlite] listBooksByGenre(sci-fi): " + scifi.size() + " row(s)");

        var allBooks = q.listBooksByGenreOrAll("all");
        System.out.println("[sqlite] listBooksByGenreOrAll(all): " + allBooks.size() + " row(s) (repeated-param demo)");
        var scifi2 = q.listBooksByGenreOrAll("sci-fi");
        System.out.println("[sqlite] listBooksByGenreOrAll(sci-fi): " + scifi2.size() + " row(s)");

        System.out.println("[sqlite] listBooksWithAuthor:");
        q.listBooksWithAuthor()
            .forEach(r -> System.out.println("  \"" + r.title() + "\" by " + r.authorName()));

        var neverOrdered = q.getBooksNeverOrdered();
        System.out.println("[sqlite] getBooksNeverOrdered: " + neverOrdered.size() + " book(s)");
        neverOrdered.forEach(b -> System.out.println("  \"" + b.title() + "\""));

        System.out.println("[sqlite] getTopSellingBooks:");
        q.getTopSellingBooks()
            .forEach(r -> System.out.println("  \"" + r.title() + "\" sold " + r.unitsSold()));

        System.out.println("[sqlite] getBestCustomers:");
        q.getBestCustomers()
            .forEach(r -> System.out.println("  " + r.name() + " spent " + r.totalSpent()));
    }
}
