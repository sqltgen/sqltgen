package com.example.db;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end runtime tests for the generated Java/MySQL queries.
 *
 * Each test creates a dedicated MySQL database named test_&lt;uuid&gt; to provide
 * full isolation. Requires the docker-compose MySQL service on port 13306.
 */
class RuntimeTest {

    private static final String ROOT_URL =
        System.getenv().getOrDefault("MYSQL_ROOT_URL", "jdbc:mysql://localhost:13306/sqltgen_e2e");
    private static final String TEST_BASE_URL =
        System.getenv().getOrDefault("DATABASE_URL", "jdbc:mysql://localhost:13306/");
    private static final String USER      = "sqltgen";
    private static final String PASS      = "sqltgen";
    private static final String ROOT_USER = "sqltgen";
    private static final String ROOT_PASS = "sqltgen";

    private Connection conn;
    private String dbName;

    @BeforeEach
    void setUp() throws SQLException, IOException {
        dbName = "test_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection admin = DriverManager.getConnection(ROOT_URL, ROOT_USER, ROOT_PASS);
             Statement s = admin.createStatement()) {
            s.execute("CREATE DATABASE `" + dbName + "`");
            s.execute("GRANT ALL ON `" + dbName + "`.* TO 'sqltgen'@'%'");
        }
        conn = DriverManager.getConnection(
            TEST_BASE_URL + dbName + "?useSSL=false&allowPublicKeyRetrieval=true", USER, PASS);
        conn.setAutoCommit(true);
        String schemaSql = Files.readString(Path.of("../../../../fixtures/bookstore/mysql/schema.sql"));
        try (Statement s = conn.createStatement()) {
            for (String stmt : schemaSql.split(";")) {
                String t = stmt.strip();
                if (!t.isEmpty()) s.execute(t);
            }
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null) conn.close();
        try (Connection admin = DriverManager.getConnection(ROOT_URL, ROOT_USER, ROOT_PASS);
             Statement s = admin.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS `" + dbName + "`");
        }
    }

    /** Insert a consistent set of test fixtures. Known IDs: author 1=Asimov, 2=Herbert, 3=Le Guin;
     *  book 1=Foundation, 2=I Robot, 3=Dune, 4=Earthsea; customer 1=Alice; sale 1. */
    private void seed() throws SQLException {
        Queries.createAuthor(conn, "Asimov", "Sci-fi master", 1920);
        Queries.createAuthor(conn, "Herbert", null, 1920);
        Queries.createAuthor(conn, "Le Guin", "Earthsea", 1929);

        Queries.createBook(conn, 1L, "Foundation", "sci-fi", new BigDecimal("9.99"), LocalDate.of(1951, 1, 1));
        Queries.createBook(conn, 1L, "I Robot",    "sci-fi", new BigDecimal("7.99"), LocalDate.of(1950, 1, 1));
        Queries.createBook(conn, 2L, "Dune",       "sci-fi", new BigDecimal("12.99"), LocalDate.of(1965, 1, 1));
        Queries.createBook(conn, 3L, "Earthsea",   "fantasy", new BigDecimal("8.99"), LocalDate.of(1968, 1, 1));

        Queries.createCustomer(conn, "Alice", "alice@example.com");
        Queries.createSale(conn, 1L);
        Queries.addSaleItem(conn, 1L, 1L, 2, new BigDecimal("9.99"));   // Foundation qty 2
        Queries.addSaleItem(conn, 1L, 3L, 1, new BigDecimal("12.99"));  // Dune qty 1
    }

    // ─── :one tests ───────────────────────────────────────────────────────────

    @Test
    void testGetAuthor() throws SQLException {
        seed();
        var author = Queries.getAuthor(conn, 1L).orElseThrow();
        assertEquals("Asimov", author.name());
        assertEquals("Sci-fi master", author.bio());
        assertEquals(1920, author.birthYear());
    }

    @Test
    void testGetAuthorNotFound() throws SQLException {
        assertTrue(Queries.getAuthor(conn, 999L).isEmpty());
    }

    @Test
    void testGetBook() throws SQLException {
        seed();
        var book = Queries.getBook(conn, 1L).orElseThrow();
        assertEquals("Foundation", book.title());
        assertEquals("sci-fi", book.genre());
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
        var author = Queries.getAuthor(conn, 1L).orElseThrow();
        assertEquals("Test", author.name());
        assertNull(author.bio());
        assertNull(author.birthYear());
    }

    @Test
    void testCreateBook() throws SQLException {
        seed();
        Queries.createBook(conn, 1L, "New Book", "mystery", new BigDecimal("14.50"), null);
        var book = Queries.getBook(conn, 5L).orElseThrow();
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
            assertEquals(1, rs.getLong(1));
        }
    }

    @Test
    void testCreateSale() throws SQLException {
        seed();
        Queries.createSale(conn, 1L);
        try (var s = conn.createStatement();
             var rs = s.executeQuery("SELECT COUNT(*) FROM sale WHERE customer_id = 1")) {
            rs.next();
            assertEquals(2, rs.getLong(1));
        }
    }

    @Test
    void testAddSaleItem() throws SQLException {
        seed();
        Queries.addSaleItem(conn, 1L, 4L, 1, new BigDecimal("8.99"));
        try (var s = conn.createStatement();
             var rs = s.executeQuery("SELECT COUNT(*) FROM sale_item WHERE sale_id = 1")) {
            rs.next();
            assertEquals(3, rs.getLong(1));
        }
    }

    @Test
    void testUpdateAuthorBio() throws SQLException {
        seed();
        Queries.updateAuthorBio(conn, "Updated bio", 1L);
        var author = Queries.getAuthor(conn, 1L).orElseThrow();
        assertEquals("Updated bio", author.bio());
    }

    @Test
    void testDeleteAuthor() throws SQLException {
        Queries.createAuthor(conn, "Temp", null, null);
        Queries.deleteAuthor(conn, 1L);
        assertTrue(Queries.getAuthor(conn, 1L).isEmpty());
    }

    // ─── :execrows tests ──────────────────────────────────────────────────────

    @Test
    void testDeleteBookById() throws SQLException {
        seed();
        // I Robot (id=2) has no sale_items
        assertEquals(1L, Queries.deleteBookById(conn, 2L));
        assertEquals(0L, Queries.deleteBookById(conn, 999L));
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
        // Foundation: qty 2 is the top seller
        assertEquals("Foundation", rows.get(0).title());
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
        var page1 = Queries.listBooksWithLimit(conn, 2L, 0L);
        assertEquals(2, page1.size());
        var page2 = Queries.listBooksWithLimit(conn, 2L, 2L);
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
        var results = Queries.getBooksByPriceRange(conn,
            new BigDecimal("8.00"), new BigDecimal("10.00"));
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
        // Sales are current; use a far-past cutoff
        var results = Queries.getBooksWithRecentSales(conn, LocalDateTime.of(2000, 1, 1, 0, 0));
        // Foundation and Dune have sale_items
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
        var rows = Queries.getBookPriceLabel(conn, new BigDecimal("10.00"));
        assertEquals(4, rows.size());
        var dune = rows.stream().filter(r -> r.title().equals("Dune")).findFirst().orElseThrow();
        assertEquals("expensive", dune.priceLabel());
        var earthsea = rows.stream().filter(r -> r.title().equals("Earthsea")).findFirst().orElseThrow();
        assertEquals("affordable", earthsea.priceLabel());
    }

    @Test
    void testGetBookPriceOrDefault() throws SQLException {
        seed();
        var rows = Queries.getBookPriceOrDefault(conn, new BigDecimal("0.00"));
        assertEquals(4, rows.size());
        assertTrue(rows.stream().allMatch(r -> r.effectivePrice().compareTo(BigDecimal.ZERO) > 0));
    }

    // ─── Product type coverage ────────────────────────────────────────────────

    @Test
    void testGetProduct() throws SQLException {
        String pid = "prod-get-001";
        try (var s = conn.createStatement()) {
            s.execute("INSERT INTO product (id, sku, name, active, stock_count) VALUES ('"
                + pid + "', 'SKU-GET', 'Widget', TRUE, 5)");
        }
        var row = Queries.getProduct(conn, pid).orElseThrow();
        assertEquals(pid, row.id());
        assertEquals("Widget", row.name());
        assertEquals((short) 5, row.stockCount());
    }

    @Test
    void testListActiveProducts() throws SQLException {
        try (var s = conn.createStatement()) {
            s.execute("INSERT INTO product (id, sku, name, active, stock_count) VALUES ('act-1', 'ACT-1', 'Active', TRUE, 10)");
            s.execute("INSERT INTO product (id, sku, name, active, stock_count) VALUES ('inact-1', 'INACT-1', 'Inactive', FALSE, 0)");
        }
        var active = Queries.listActiveProducts(conn, true);
        assertEquals(1, active.size());
        assertEquals("Active", active.get(0).name());
        var inactive = Queries.listActiveProducts(conn, false);
        assertEquals(1, inactive.size());
        assertEquals("Inactive", inactive.get(0).name());
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
        var rows = Queries.getBooksPublishedBetween(conn,
            LocalDate.of(1951, 1, 1), LocalDate.of(1966, 1, 1));
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
        assertEquals(new BigDecimal("2"), foundation.totalQuantity());
        var dune = rows.stream().filter(r -> r.title().equals("Dune")).findFirst().orElseThrow();
        assertEquals(new BigDecimal("1"), dune.totalQuantity());
        var iRobot = rows.stream().filter(r -> r.title().equals("I Robot")).findFirst().orElseThrow();
        assertEquals(new BigDecimal("0"), iRobot.totalQuantity());
    }

    // ─── :one COUNT aggregate ─────────────────────────────────────────────────

    @Test
    void testCountSaleItems() throws SQLException {
        seed();
        // Sale 1 (Alice): Foundation + Dune = 2 items
        var count = Queries.countSaleItems(conn, 1L).orElseThrow();
        assertEquals(2L, count.itemCount());
    }

    // ─── MIN/MAX/SUM/AVG aggregate tests ─────────────────────────────────────

    @Test
    void testGetSaleItemQuantityAggregates() throws SQLException {
        seed();
        // Seed: Foundation qty 2 + Dune qty 1 → min=1, max=2, sum=3, avg=1.5
        var row = Queries.getSaleItemQuantityAggregates(conn).orElseThrow();
        assertEquals(1, row.minQty());
        assertEquals(2, row.maxQty());
        assertTrue(row.sumQty().compareTo(new BigDecimal("3")) == 0);
        assertTrue(row.avgQty().subtract(new BigDecimal("1.5")).abs().compareTo(new BigDecimal("0.01")) < 0);
    }

    @Test
    void testGetBookPriceAggregates() throws SQLException {
        seed();
        // Book prices: 9.99, 7.99, 12.99, 8.99 → min=7.99, max=12.99, sum=39.96, avg≈9.99
        var row = Queries.getBookPriceAggregates(conn).orElseThrow();
        assertTrue(row.minPrice().subtract(new BigDecimal("7.99")).abs()
            .compareTo(new BigDecimal("0.01")) < 0);
        assertTrue(row.maxPrice().subtract(new BigDecimal("12.99")).abs()
            .compareTo(new BigDecimal("0.01")) < 0);
        assertTrue(row.sumPrice().subtract(new BigDecimal("39.96")).abs()
            .compareTo(new BigDecimal("0.01")) < 0);
        assertTrue(row.avgPrice().subtract(new BigDecimal("9.99")).abs()
            .compareTo(new BigDecimal("0.01")) < 0);
    }
}
