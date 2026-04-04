package com.example.db;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import com.example.db.models.Author;
import com.example.db.models.Book;
import com.example.db.queries.Queries;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end runtime tests for the generated Java/SQLite queries.
 *
 * Uses an in-memory SQLite database — no external services required.
 * Each test method gets a fresh database via setUp().
 */
class RuntimeTest {

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException, IOException {
        conn = DriverManager.getConnection("jdbc:sqlite::memory:");
        conn.setAutoCommit(true);
        try (Statement s = conn.createStatement()) {
            s.execute("PRAGMA foreign_keys = ON");
        }
        String schemaSql = Files.readString(Path.of("../../../../fixtures/bookstore/sqlite/schema.sql"));
        try (Statement s = conn.createStatement()) {
            for (String stmt : schemaSql.split(";")) {
                String t = stmt.strip();
                if (!t.isEmpty()) s.execute(t);
            }
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null) conn.close();
    }

    /** Insert a consistent set of test fixtures. Known IDs: author 1=Asimov, 2=Herbert, 3=Le Guin;
     *  book 1=Foundation, 2=I Robot, 3=Dune, 4=Earthsea; customer 1=Alice; sale 1. */
    private void seed() throws SQLException {
        Queries.createAuthor(conn, "Asimov", "Sci-fi master", 1920);
        Queries.createAuthor(conn, "Herbert", null, 1920);
        Queries.createAuthor(conn, "Le Guin", "Earthsea", 1929);

        Queries.createBook(conn, 1, "Foundation", "sci-fi", 9.99, "1951-01-01");
        Queries.createBook(conn, 1, "I Robot",    "sci-fi", 7.99, "1950-01-01");
        Queries.createBook(conn, 2, "Dune",       "sci-fi", 12.99, "1965-01-01");
        Queries.createBook(conn, 3, "Earthsea",   "fantasy", 8.99, "1968-01-01");

        Queries.createCustomer(conn, "Alice", "alice@example.com");
        Queries.createSale(conn, 1);
        Queries.addSaleItem(conn, 1, 1, 2, 9.99);   // Foundation qty 2
        Queries.addSaleItem(conn, 1, 3, 1, 12.99);  // Dune qty 1
    }

    // ─── :one tests ───────────────────────────────────────────────────────────

    @Test
    void testGetAuthor() throws SQLException {
        seed();
        var author = Queries.getAuthor(conn, 1).orElseThrow();
        assertEquals("Asimov", author.name());
        assertEquals("Sci-fi master", author.bio());
        assertEquals(1920, author.birthYear());
    }

    @Test
    void testGetAuthorNotFound() throws SQLException {
        assertTrue(Queries.getAuthor(conn, 999).isEmpty());
    }

    @Test
    void testGetBook() throws SQLException {
        seed();
        var book = Queries.getBook(conn, 1).orElseThrow();
        assertEquals("Foundation", book.title());
        assertEquals("sci-fi", book.genre());
        assertEquals(1, book.authorId());
    }

    // ─── :many tests ──────────────────────────────────────────────────────────

    @Test
    void testListAuthors() throws SQLException {
        seed();
        var authors = Queries.listAuthors(conn);
        assertEquals(3, authors.size());
        assertEquals("Asimov", authors.get(0).name());
        assertEquals("Herbert", authors.get(1).name());
        assertEquals("Le Guin", authors.get(2).name());
    }

    @Test
    void testListBooksByGenre() throws SQLException {
        seed();
        assertEquals(3, Queries.listBooksByGenre(conn, "sci-fi").size());
        var fantasy = Queries.listBooksByGenre(conn, "fantasy");
        assertEquals(1, fantasy.size());
        assertEquals("Earthsea", fantasy.get(0).title());
    }

    @Test
    void testListBooksByGenreOrAll() throws SQLException {
        seed();
        assertEquals(4, Queries.listBooksByGenreOrAll(conn, "all").size());
        assertEquals(3, Queries.listBooksByGenreOrAll(conn, "sci-fi").size());
    }

    // ─── :exec tests ──────────────────────────────────────────────────────────

    @Test
    void testCreateAuthorExec() throws SQLException {
        Queries.createAuthor(conn, "Test", null, null);
        var author = Queries.getAuthor(conn, 1).orElseThrow();
        assertEquals("Test", author.name());
        assertNull(author.bio());
        assertNull(author.birthYear());
    }

    @Test
    void testCreateBook() throws SQLException {
        seed();
        Queries.createBook(conn, 1, "New Book", "mystery", 14.50, null);
        var book = Queries.getBook(conn, 5).orElseThrow();
        assertEquals("New Book", book.title());
        assertEquals("mystery", book.genre());
        assertNull(book.publishedAt());
    }

    @Test
    void testCreateCustomer() throws SQLException {
        Queries.createCustomer(conn, "Bob", "bob@example.com");
        try (var s = conn.createStatement();
             var rs = s.executeQuery("SELECT COUNT(*) FROM customer WHERE name = 'Bob'")) {
            rs.next();
            assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    void testCreateSale() throws SQLException {
        seed();
        Queries.createSale(conn, 1);
        try (var s = conn.createStatement();
             var rs = s.executeQuery("SELECT COUNT(*) FROM sale WHERE customer_id = 1")) {
            rs.next();
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void testAddSaleItem() throws SQLException {
        seed();
        Queries.addSaleItem(conn, 1, 4, 1, 8.99);
        try (var s = conn.createStatement();
             var rs = s.executeQuery("SELECT COUNT(*) FROM sale_item WHERE sale_id = 1")) {
            rs.next();
            assertEquals(3, rs.getInt(1));
        }
    }

    @Test
    void testUpdateAuthorBio() throws SQLException {
        seed();
        Queries.updateAuthorBio(conn, "Updated bio", 1);
        var author = Queries.getAuthor(conn, 1).orElseThrow();
        assertEquals("Updated bio", author.bio());
    }

    @Test
    void testDeleteAuthor() throws SQLException {
        Queries.createAuthor(conn, "Temp", null, null);
        Queries.deleteAuthor(conn, 1);
        assertTrue(Queries.getAuthor(conn, 1).isEmpty());
    }

    // ─── :execrows tests ──────────────────────────────────────────────────────

    @Test
    void testDeleteBookById() throws SQLException {
        seed();
        // I Robot (id=2) has no sale_items
        assertEquals(1L, Queries.deleteBookById(conn, 2));
        assertEquals(0L, Queries.deleteBookById(conn, 999));
    }

    // ─── JOIN tests ───────────────────────────────────────────────────────────

    @Test
    void testListBooksWithAuthor() throws SQLException {
        seed();
        var rows = Queries.listBooksWithAuthor(conn);
        assertEquals(4, rows.size());

        var dune = rows.stream().filter(r -> r.title().equals("Dune")).findFirst().orElseThrow();
        assertEquals("Herbert", dune.authorName());
        assertNull(dune.authorBio());

        var found = rows.stream().filter(r -> r.title().equals("Foundation")).findFirst().orElseThrow();
        assertEquals("Asimov", found.authorName());
        assertEquals("Sci-fi master", found.authorBio());
    }

    @Test
    void testGetBooksNeverOrdered() throws SQLException {
        seed();
        // Seed has only Alice buying Foundation + Dune; I Robot and Earthsea were never ordered
        var books = Queries.getBooksNeverOrdered(conn);
        assertEquals(2, books.size());
        var titles = books.stream().map(Book::title).toList();
        assertTrue(titles.contains("I Robot"));
        assertTrue(titles.contains("Earthsea"));
    }

    // ─── CTE tests ────────────────────────────────────────────────────────────

    @Test
    void testGetTopSellingBooks() throws SQLException {
        seed();
        var rows = Queries.getTopSellingBooks(conn);
        assertFalse(rows.isEmpty());
        // Foundation: qty 2, Dune: qty 1
        assertEquals("Foundation", rows.get(0).title());
        assertEquals(2L, rows.get(0).unitsSold());
    }

    @Test
    void testGetBestCustomers() throws SQLException {
        seed();
        var rows = Queries.getBestCustomers(conn);
        assertEquals(1, rows.size());
        assertEquals("Alice", rows.get(0).name());
        assertNotNull(rows.get(0).totalSpent());
    }

    @Test
    void testGetAuthorStats() throws SQLException {
        seed();
        var rows = Queries.getAuthorStats(conn);
        assertEquals(3, rows.size());
        var asimov = rows.stream().filter(r -> r.name().equals("Asimov")).findFirst().orElseThrow();
        assertEquals(2L, asimov.numBooks());
    }

    // ─── Aggregate tests ──────────────────────────────────────────────────────

    @Test
    void testCountBooksByGenre() throws SQLException {
        seed();
        var rows = Queries.countBooksByGenre(conn);
        assertEquals(2, rows.size());
        var fantasy = rows.stream().filter(r -> r.genre().equals("fantasy")).findFirst().orElseThrow();
        assertEquals(1L, fantasy.bookCount());
        var scifi = rows.stream().filter(r -> r.genre().equals("sci-fi")).findFirst().orElseThrow();
        assertEquals(3L, scifi.bookCount());
    }

    // ─── LIMIT/OFFSET tests ───────────────────────────────────────────────────

    @Test
    void testListBooksWithLimit() throws SQLException {
        seed();
        var page1 = Queries.listBooksWithLimit(conn, 2, 0);
        assertEquals(2, page1.size());
        var page2 = Queries.listBooksWithLimit(conn, 2, 2);
        assertEquals(2, page2.size());
        var p1titles = page1.stream().map(Queries.ListBooksWithLimitRow::title).toList();
        var p2titles = page2.stream().map(Queries.ListBooksWithLimitRow::title).toList();
        assertTrue(p1titles.stream().noneMatch(p2titles::contains));
    }

    // ─── LIKE tests ───────────────────────────────────────────────────────────

    @Test
    void testSearchBooksByTitle() throws SQLException {
        seed();
        var results = Queries.searchBooksByTitle(conn, "%ound%");
        assertEquals(1, results.size());
        assertEquals("Foundation", results.get(0).title());
        assertTrue(Queries.searchBooksByTitle(conn, "NOPE%").isEmpty());
    }

    // ─── BETWEEN tests ────────────────────────────────────────────────────────

    @Test
    void testGetBooksByPriceRange() throws SQLException {
        seed();
        // Foundation (9.99) and Earthsea (8.99) in [8.00, 10.00]
        var results = Queries.getBooksByPriceRange(conn, 8.00, 10.00);
        assertEquals(2, results.size());
    }

    // ─── IN list tests ────────────────────────────────────────────────────────

    @Test
    void testGetBooksInGenres() throws SQLException {
        seed();
        var results = Queries.getBooksInGenres(conn, "sci-fi", "fantasy", "horror");
        assertEquals(4, results.size());
    }

    // ─── HAVING tests ─────────────────────────────────────────────────────────

    @Test
    void testGetGenresWithManyBooks() throws SQLException {
        seed();
        var results = Queries.getGenresWithManyBooks(conn, 1L);
        assertEquals(1, results.size());
        assertEquals("sci-fi", results.get(0).genre());
        assertEquals(3L, results.get(0).bookCount());
    }

    // ─── Subquery tests ───────────────────────────────────────────────────────

    @Test
    void testGetBooksNotByAuthor() throws SQLException {
        seed();
        var results = Queries.getBooksNotByAuthor(conn, "Asimov");
        assertEquals(2, results.size());
        assertTrue(results.stream().noneMatch(r -> r.title().equals("Foundation")));
        assertTrue(results.stream().noneMatch(r -> r.title().equals("I Robot")));
    }

    @Test
    void testGetBooksWithRecentSales() throws SQLException {
        seed();
        // Sales are current; use a past cutoff date
        var results = Queries.getBooksWithRecentSales(conn, java.time.LocalDateTime.of(2000, 1, 1, 0, 0));
        // Foundation (book 1) and Dune (book 3) have sale_items
        assertEquals(2, results.size());
    }

    // ─── Scalar subquery test ─────────────────────────────────────────────────

    @Test
    void testGetBookWithAuthorName() throws SQLException {
        seed();
        var rows = Queries.getBookWithAuthorName(conn);
        assertEquals(4, rows.size());
        var dune = rows.stream().filter(r -> r.title().equals("Dune")).findFirst().orElseThrow();
        assertEquals("Herbert", dune.authorName());
    }

    // ─── JOIN with param tests ────────────────────────────────────────────────

    @Test
    void testGetBooksByAuthorParam() throws SQLException {
        seed();
        // birth_year > 1925 → only Le Guin (1929) → Earthsea
        var results = Queries.getBooksByAuthorParam(conn, 1925);
        assertEquals(1, results.size());
        assertEquals("Earthsea", results.get(0).title());
    }

    // ─── Qualified wildcard tests ─────────────────────────────────────────────

    @Test
    void testGetAllBookFields() throws SQLException {
        seed();
        var books = Queries.getAllBookFields(conn);
        assertEquals(4, books.size());
        assertEquals(1, books.get(0).id());
        assertFalse(books.get(0).title().isEmpty());
    }

    // ─── List param tests ─────────────────────────────────────────────────────

    @Test
    void testGetBooksByIds() throws SQLException {
        seed();
        var books = Queries.getBooksByIds(conn, List.of(1L, 3L));
        assertEquals(2, books.size());
        var titles = books.stream().map(Book::title).toList();
        assertTrue(titles.contains("Foundation"));
        assertTrue(titles.contains("Dune"));
        assertTrue(Queries.getBooksByIds(conn, List.of()).isEmpty());
    }

    // ─── CASE / COALESCE tests ────────────────────────────────────────────────

    @Test
    void testGetBookPriceLabel() throws SQLException {
        seed();
        var rows = Queries.getBookPriceLabel(conn, 10.00);
        assertEquals(4, rows.size());
        var dune = rows.stream().filter(r -> r.title().equals("Dune")).findFirst().orElseThrow();
        assertEquals("expensive", dune.priceLabel());
        var earthsea = rows.stream().filter(r -> r.title().equals("Earthsea")).findFirst().orElseThrow();
        assertEquals("affordable", earthsea.priceLabel());
    }

    @Test
    void testGetBookPriceOrDefault() throws SQLException {
        seed();
        var rows = Queries.getBookPriceOrDefault(conn, 0.00);
        assertEquals(4, rows.size());
        assertTrue(rows.stream().allMatch(r -> r.effectivePrice() > 0.0));
    }

    // ─── Product type coverage ────────────────────────────────────────────────

    @Test
    void testGetProduct() throws SQLException {
        String pid = "prod-get-001";
        Queries.insertProduct(conn, pid, "SKU-GET", "Widget", 1, null, null, null, null, 5);
        var row = Queries.getProduct(conn, pid).orElseThrow();
        assertEquals(pid, row.id());
        assertEquals("Widget", row.name());
        assertEquals(5, row.stockCount());
    }

    @Test
    void testListActiveProducts() throws SQLException {
        Queries.insertProduct(conn, "act-1",   "ACT-1",   "Active",   1, null, null, null, null, 10);
        Queries.insertProduct(conn, "inact-1", "INACT-1", "Inactive", 0, null, null, null, null, 0);
        var active = Queries.listActiveProducts(conn, 1);
        assertEquals(1, active.size());
        assertEquals("Active", active.get(0).name());
        var inactive = Queries.listActiveProducts(conn, 0);
        assertEquals(1, inactive.size());
        assertEquals("Inactive", inactive.get(0).name());
    }

    @Test
    void testInsertProduct() throws SQLException {
        String pid = "prod-ins-001";
        Queries.insertProduct(conn, pid, "SKU-INS", "Gadget", 1, 1.5f, 4.2f, null, null, 7);
        var row = Queries.getProduct(conn, pid).orElseThrow();
        assertEquals(pid, row.id());
        assertEquals("Gadget", row.name());
        assertEquals(7, row.stockCount());
    }

    @Test
    void testUpsertProduct() throws SQLException {
        String pid = "prod-ups-001";
        Queries.upsertProduct(conn, pid, "SKU-UP", "Thing", 1, null, 10);
        var row = Queries.getProduct(conn, pid).orElseThrow();
        assertEquals("Thing", row.name());
        assertEquals(10, row.stockCount());

        Queries.upsertProduct(conn, pid, "SKU-UP", "Thing Pro", 1, null, 20);
        var updated = Queries.getProduct(conn, pid).orElseThrow();
        assertEquals("Thing Pro", updated.name());
        assertEquals(20, updated.stockCount());
    }

    // ─── IS NULL / IS NOT NULL tests ──────────────────────────────────────────

    @Test
    void testGetAuthorsWithNullBio() throws SQLException {
        seed();
        var rows = Queries.getAuthorsWithNullBio(conn);
        assertEquals(1, rows.size());
        assertEquals("Herbert", rows.get(0).name());
    }

    @Test
    void testGetAuthorsWithBio() throws SQLException {
        seed();
        var rows = Queries.getAuthorsWithBio(conn);
        assertEquals(2, rows.size());
        var names = rows.stream().map(Author::name).toList();
        assertTrue(names.contains("Asimov"));
        assertTrue(names.contains("Le Guin"));
    }

    // ─── Date BETWEEN tests ───────────────────────────────────────────────────

    @Test
    void testGetBooksPublishedBetween() throws SQLException {
        seed();
        // 1951-01-01 to 1966-01-01 → Foundation (1951) and Dune (1965)
        var rows = Queries.getBooksPublishedBetween(conn, "1951-01-01", "1966-01-01");
        assertEquals(2, rows.size());
        var titles = rows.stream().map(Queries.GetBooksPublishedBetweenRow::title).toList();
        assertTrue(titles.contains("Foundation"));
        assertTrue(titles.contains("Dune"));
    }

    // ─── DISTINCT tests ───────────────────────────────────────────────────────

    @Test
    void testGetDistinctGenres() throws SQLException {
        seed();
        var rows = Queries.getDistinctGenres(conn);
        assertEquals(2, rows.size());
        var genres = rows.stream().map(Queries.GetDistinctGenresRow::genre).toList();
        assertTrue(genres.contains("sci-fi"));
        assertTrue(genres.contains("fantasy"));
    }

    // ─── LEFT JOIN aggregate tests ────────────────────────────────────────────

    @Test
    void testGetBooksWithSalesCount() throws SQLException {
        seed();
        var rows = Queries.getBooksWithSalesCount(conn);
        assertEquals(4, rows.size());
        var foundation = rows.stream().filter(r -> r.title().equals("Foundation")).findFirst().orElseThrow();
        assertEquals(2L, foundation.totalQuantity());
        var dune = rows.stream().filter(r -> r.title().equals("Dune")).findFirst().orElseThrow();
        assertEquals(1L, dune.totalQuantity());
        var iRobot = rows.stream().filter(r -> r.title().equals("I Robot")).findFirst().orElseThrow();
        assertEquals(0L, iRobot.totalQuantity());
    }

    // ─── :one COUNT aggregate ─────────────────────────────────────────────────

    @Test
    void testCountSaleItems() throws SQLException {
        seed();
        var count1 = Queries.countSaleItems(conn, 1).orElseThrow();
        assertEquals(2L, count1.itemCount());
        // COUNT(*) always returns a row even for non-existent sale_id
        var count2 = Queries.countSaleItems(conn, 999).orElseThrow();
        assertEquals(0L, count2.itemCount());
    }

    // ─── MIN/MAX/SUM/AVG aggregate tests ─────────────────────────────────────

    @Test
    void testGetSaleItemQuantityAggregates() throws SQLException {
        seed();
        // Seed: Foundation qty 2 + Dune qty 1 → min=1, max=2, sum=3, avg=1.5
        var row = Queries.getSaleItemQuantityAggregates(conn).orElseThrow();
        assertEquals(1, row.minQty());
        assertEquals(2, row.maxQty());
        assertEquals(3L, row.sumQty());
        assertTrue(Math.abs(row.avgQty() - 1.5) < 0.01);
    }

    @Test
    void testGetBookPriceAggregates() throws SQLException {
        seed();
        // Book prices: 9.99, 7.99, 12.99, 8.99 → min=7.99, max=12.99, sum=39.96, avg≈9.99
        var row = Queries.getBookPriceAggregates(conn).orElseThrow();
        assertTrue(Math.abs(row.minPrice() - 7.99) < 0.01);
        assertTrue(Math.abs(row.maxPrice() - 12.99) < 0.01);
        assertTrue(Math.abs(row.sumPrice() - 39.96) < 0.01);
        assertTrue(Math.abs(row.avgPrice() - 9.99) < 0.01);
    }
}
