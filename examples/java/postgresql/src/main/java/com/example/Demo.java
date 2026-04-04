package com.example;

import com.example.db.queries.Querier;

import java.math.BigDecimal;
import java.util.List;
import org.postgresql.ds.PGSimpleDataSource;

public class Demo {

    private static final String PG_USER = "sqltgen";
    private static final String PG_PASS = "sqltgen";

    public static void run(String url) throws Exception {
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setURL(url);
        ds.setUser(PG_USER);
        ds.setPassword(PG_PASS);
        var q = new Querier(ds);
        seed(q);
        query(q);
    }

    private static void seed(Querier q) throws Exception {
        var leGuin  = q.createAuthor("Ursula K. Le Guin", "Science fiction and fantasy author", 1929).orElseThrow();
        var herbert = q.createAuthor("Frank Herbert",     "Author of the Dune series",           1920).orElseThrow();
        var asimov  = q.createAuthor("Isaac Asimov",      null,                                  1920).orElseThrow();
        System.out.println("[pg] inserted 3 authors (ids: " + leGuin.id() + ", " + herbert.id() + ", " + asimov.id() + ")");

        var lhod  = q.createBook(leGuin.id(),  "The Left Hand of Darkness", "sci-fi", new BigDecimal("12.99"), null).orElseThrow();
        var disp  = q.createBook(leGuin.id(),  "The Dispossessed",           "sci-fi", new BigDecimal("11.50"), null).orElseThrow();
        var dune  = q.createBook(herbert.id(), "Dune",                       "sci-fi", new BigDecimal("14.99"), null).orElseThrow();
        var found = q.createBook(asimov.id(),  "Foundation",                 "sci-fi", new BigDecimal("10.99"), null).orElseThrow();
        var caves = q.createBook(asimov.id(),  "The Caves of Steel",         "sci-fi", new BigDecimal("9.99"),  null).orElseThrow();
        System.out.println("[pg] inserted 5 books");

        var alice = q.createCustomer("Alice", "alice@example.com").orElseThrow();
        var bob   = q.createCustomer("Bob",   "bob@example.com").orElseThrow();
        System.out.println("[pg] inserted 2 customers");

        var sale1 = q.createSale(alice.id()).orElseThrow();
        q.addSaleItem(sale1.id(), dune.id(),  2, new BigDecimal("14.99"));
        q.addSaleItem(sale1.id(), found.id(), 1, new BigDecimal("10.99"));
        var sale2 = q.createSale(bob.id()).orElseThrow();
        q.addSaleItem(sale2.id(), dune.id(), 1, new BigDecimal("14.99"));
        q.addSaleItem(sale2.id(), lhod.id(), 1, new BigDecimal("12.99"));
        System.out.println("[pg] inserted 2 sales with items");
    }

    private static void query(Querier q) throws Exception {
        var authors = q.listAuthors();
        System.out.println("[pg] listAuthors: " + authors.size() + " row(s)");

        // Book IDs are BIGSERIAL starting at 1 on a fresh DB; 1=Left Hand, 3=Dune.
        var byIds = q.getBooksByIds(List.of(1L, 3L));
        System.out.println("[pg] getBooksByIds([1,3]): " + byIds.size() + " row(s)");
        byIds.forEach(b -> System.out.println("  \"" + b.title() + "\""));

        var scifi = q.listBooksByGenre("sci-fi");
        System.out.println("[pg] listBooksByGenre(sci-fi): " + scifi.size() + " row(s)");

        var allBooks = q.listBooksByGenreOrAll("all");
        System.out.println("[pg] listBooksByGenreOrAll(all): " + allBooks.size() + " row(s) (repeated-param demo)");
        var scifi2 = q.listBooksByGenreOrAll("sci-fi");
        System.out.println("[pg] listBooksByGenreOrAll(sci-fi): " + scifi2.size() + " row(s)");

        System.out.println("[pg] listBooksWithAuthor:");
        q.listBooksWithAuthor()
            .forEach(r -> System.out.println("  \"" + r.title() + "\" by " + r.authorName()));

        var neverOrdered = q.getBooksNeverOrdered();
        System.out.println("[pg] getBooksNeverOrdered: " + neverOrdered.size() + " book(s)");
        neverOrdered.forEach(b -> System.out.println("  \"" + b.title() + "\""));

        System.out.println("[pg] getTopSellingBooks:");
        q.getTopSellingBooks()
            .forEach(r -> System.out.println("  \"" + r.title() + "\" sold " + r.unitsSold()));

        System.out.println("[pg] getBestCustomers:");
        q.getBestCustomers()
            .forEach(r -> System.out.println("  " + r.name() + " spent " + r.totalSpent()));

        // Demonstrate UPDATE RETURNING and DELETE RETURNING with a transient author
        var temp = q.createAuthor("Temp Author", null, null).orElseThrow();
        q.updateAuthorBio("Updated via UPDATE RETURNING", temp.id())
            .ifPresent(a -> System.out.println("[pg] updateAuthorBio: updated \"" + a.name() + "\" — bio: " + a.bio()));
        q.deleteAuthor(temp.id())
            .ifPresent(r -> System.out.println("[pg] deleteAuthor: deleted \"" + r.name() + "\" (id=" + r.id() + ")"));
    }
}
