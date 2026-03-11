package com.example.db;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * End-to-end runtime tests for the generated Java/PostgreSQL queries.
 *
 * Each test gets its own PostgreSQL schema so tests can run in parallel.
 * Requires the docker-compose postgres service on port 15432.
 */
class RuntimeTest {

    private static final String URL =
        System.getenv().getOrDefault("DATABASE_URL", "jdbc:postgresql://localhost:15432/sqltgen_e2e");
    private static final String USER = "sqltgen";
    private static final String PASS = "sqltgen";

    private Connection conn;
    private String schema;

    @BeforeEach
    void setUp() throws SQLException, IOException {
        conn = DriverManager.getConnection(URL, USER, PASS);
        conn.setAutoCommit(true);

        schema = "test_" + UUID.randomUUID().toString().replace("-", "");
        String schemaSql = Files.readString(Path.of("../../../fixtures/postgresql/schema.sql"));
        try (Statement s = conn.createStatement()) {
            s.execute("CREATE SCHEMA \"" + schema + "\"");
            s.execute("SET search_path TO \"" + schema + "\"");
            s.execute(schemaSql);
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null) {
            try (Statement s = conn.createStatement()) {
                s.execute("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
            }
            conn.close();
        }
    }

    private void seed() throws SQLException {
        Queries.createAuthor(conn, "Asimov", "Sci-fi master", 1920);
        Queries.createAuthor(conn, "Herbert", null, 1920);
        Queries.createAuthor(conn, "Le Guin", "Earthsea", 1929);

        Queries.createBook(conn, 1, "Foundation", "sci-fi", new BigDecimal("9.99"),
            LocalDate.of(1951, 1, 1));
        Queries.createBook(conn, 1, "I Robot", "sci-fi", new BigDecimal("7.99"),
            LocalDate.of(1950, 1, 1));
        Queries.createBook(conn, 2, "Dune", "sci-fi", new BigDecimal("12.99"),
            LocalDate.of(1965, 1, 1));
        Queries.createBook(conn, 3, "Earthsea", "fantasy", new BigDecimal("8.99"),
            LocalDate.of(1968, 1, 1));

        var cust = Queries.createCustomer(conn, "Alice", "alice@example.com").orElseThrow();
        var sale = Queries.createSale(conn, cust.id()).orElseThrow();
        Queries.addSaleItem(conn, sale.id(), 1, 2, new BigDecimal("9.99"));
        Queries.addSaleItem(conn, sale.id(), 3, 1, new BigDecimal("12.99"));
    }

    // ─── :one tests ────────────────────────────────────────────────────────

    @Test
    void testCreateAuthorReturning() throws SQLException {
        var author = Queries.createAuthor(conn, "Test", "bio", 1980).orElseThrow();
        assertEquals("Test", author.name());
        assertEquals("bio", author.bio());
        assertEquals(1980, author.birthYear());
        assertTrue(author.id() > 0);
    }

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
        var author = Queries.getAuthor(conn, 999);
        assertTrue(author.isEmpty());
    }

    @Test
    void testGetBook() throws SQLException {
        seed();
        var book = Queries.getBook(conn, 1).orElseThrow();
        assertEquals("Foundation", book.title());
        assertEquals("sci-fi", book.genre());
        assertEquals(new BigDecimal("9.99"), book.price());
        assertEquals(LocalDate.of(1951, 1, 1), book.publishedAt());
    }

    // ─── :one with RETURNING ──────────────────────────────────────────────

    @Test
    void testCreateBookReturning() throws SQLException {
        seed();
        var book = Queries.createBook(conn, 1, "New Book", "mystery",
            new BigDecimal("14.50"), null).orElseThrow();
        assertEquals("New Book", book.title());
        assertEquals("mystery", book.genre());
        assertEquals(new BigDecimal("14.50"), book.price());
        assertNull(book.publishedAt());
    }

    @Test
    void testUpdateAuthorBioReturning() throws SQLException {
        seed();
        var updated = Queries.updateAuthorBio(conn, "Updated bio", 1).orElseThrow();
        assertEquals("Asimov", updated.name());
        assertEquals("Updated bio", updated.bio());
    }

    @Test
    void testDeleteAuthorReturning() throws SQLException {
        Queries.createAuthor(conn, "Temp", null, null);
        var deleted = Queries.deleteAuthor(conn, 1).orElseThrow();
        assertEquals("Temp", deleted.name());

        assertTrue(Queries.getAuthor(conn, 1).isEmpty());
    }

    // ─── :many tests ──────────────────────────────────────────────────────

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

    // ─── :exec tests ──────────────────────────────────────────────────────

    @Test
    void testAddSaleItem() throws SQLException {
        seed();
        // Should not throw
        Queries.addSaleItem(conn, 1, 2, 5, new BigDecimal("7.99"));
    }

    // ─── :execrows tests ──────────────────────────────────────────────────

    @Test
    void testDeleteBookById() throws SQLException {
        seed();
        // Book 2 (I Robot) has no sale_items
        assertEquals(1, Queries.deleteBookById(conn, 2));
        assertEquals(0, Queries.deleteBookById(conn, 999));
    }

    // ─── JOIN tests ───────────────────────────────────────────────────────

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
        var books = Queries.getBooksNeverOrdered(conn);
        // I Robot and Earthsea were not ordered
        assertEquals(2, books.size());
        var titles = books.stream().map(Book::title).toList();
        assertTrue(titles.contains("I Robot"));
        assertTrue(titles.contains("Earthsea"));
    }

    // ─── CTE tests ────────────────────────────────────────────────────────

    @Test
    void testGetTopSellingBooks() throws SQLException {
        seed();
        var rows = Queries.getTopSellingBooks(conn);
        assertFalse(rows.isEmpty());
        // Foundation had qty 2, Dune had qty 1
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
        assertEquals(2, asimov.numBooks());
    }

    // ─── Data-modifying CTE ───────────────────────────────────────────────

    @Test
    void testArchiveAndReturnBooks() throws SQLException {
        seed();
        // Delete sale_items first so DELETE CTE can succeed
        try (Statement s = conn.createStatement()) {
            s.execute("DELETE FROM sale_item");
        }

        var archived = Queries.archiveAndReturnBooks(conn, LocalDate.of(1960, 1, 1));
        assertEquals(2, archived.size());
        var titles = archived.stream().map(Queries.ArchiveAndReturnBooksRow::title).toList();
        assertTrue(titles.contains("Foundation"));
        assertTrue(titles.contains("I Robot"));

        // Verify they're gone
        assertEquals(1, Queries.listBooksByGenre(conn, "sci-fi").size());
    }

    // ─── Aggregate tests ──────────────────────────────────────────────────

    @Test
    void testCountBooksByGenre() throws SQLException {
        seed();
        var rows = Queries.countBooksByGenre(conn);
        assertEquals(2, rows.size());

        var fantasy = rows.stream().filter(r -> r.genre().equals("fantasy")).findFirst().orElseThrow();
        assertEquals(1, fantasy.bookCount());

        var scifi = rows.stream().filter(r -> r.genre().equals("sci-fi")).findFirst().orElseThrow();
        assertEquals(3, scifi.bookCount());
    }

    // ─── LIMIT/OFFSET tests ──────────────────────────────────────────────

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

    // ─── LIKE tests ───────────────────────────────────────────────────────

    @Test
    void testSearchBooksByTitle() throws SQLException {
        seed();
        var results = Queries.searchBooksByTitle(conn, "%ound%");
        assertEquals(1, results.size());
        assertEquals("Foundation", results.get(0).title());

        assertTrue(Queries.searchBooksByTitle(conn, "NOPE%").isEmpty());
    }

    // ─── BETWEEN tests ───────────────────────────────────────────────────

    @Test
    void testGetBooksByPriceRange() throws SQLException {
        seed();
        var results = Queries.getBooksByPriceRange(conn,
            new BigDecimal("8.00"), new BigDecimal("10.00"));
        // Foundation (9.99) and Earthsea (8.99)
        assertEquals(2, results.size());
    }

    // ─── IN list tests ────────────────────────────────────────────────────

    @Test
    void testGetBooksInGenres() throws SQLException {
        seed();
        var results = Queries.getBooksInGenres(conn, "sci-fi", "fantasy", "horror");
        assertEquals(4, results.size());
    }

    // ─── HAVING tests ─────────────────────────────────────────────────────

    @Test
    void testGetGenresWithManyBooks() throws SQLException {
        seed();
        var results = Queries.getGenresWithManyBooks(conn, 1);
        assertEquals(1, results.size());
        assertEquals("sci-fi", results.get(0).genre());
        assertEquals(3, results.get(0).bookCount());
    }

    // ─── Subquery tests ──────────────────────────────────────────────────

    @Test
    void testGetBooksNotByAuthor() throws SQLException {
        seed();
        var results = Queries.getBooksNotByAuthor(conn, "Asimov");
        assertEquals(2, results.size());
        assertTrue(results.stream().noneMatch(r -> r.title().equals("Foundation")));
        assertTrue(results.stream().noneMatch(r -> r.title().equals("I Robot")));
    }

    // ─── JOIN with param tests ───────────────────────────────────────────

    @Test
    void testGetBooksByAuthorParam() throws SQLException {
        seed();
        // birth_year > 1925 → only Le Guin (1929)
        var results = Queries.getBooksByAuthorParam(conn, 1925);
        assertEquals(1, results.size());
        assertEquals("Earthsea", results.get(0).title());
    }

    // ─── Qualified wildcard tests ────────────────────────────────────────

    @Test
    void testGetAllBookFields() throws SQLException {
        seed();
        var books = Queries.getAllBookFields(conn);
        assertEquals(4, books.size());
        assertEquals(1, books.get(0).id());
        assertFalse(books.get(0).title().isEmpty());
    }

    // ─── List param tests (PG native ANY) ────────────────────────────────

    @Test
    void testGetBooksByIds() throws SQLException {
        seed();
        var books = Queries.getBooksByIds(conn, List.of(1L, 3L));
        assertEquals(2, books.size());
        var titles = books.stream().map(Book::title).toList();
        assertTrue(titles.contains("Foundation"));
        assertTrue(titles.contains("Dune"));

        // Empty list
        assertTrue(Queries.getBooksByIds(conn, List.of()).isEmpty());
    }

    // ─── CASE / COALESCE tests ──────────────────────────────────────────

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

    // ─── Product / ARRAY type tests ────────────────────────────────────

    @Test
    void testInsertAndGetProduct() throws SQLException {
        var id = UUID.randomUUID();
        var product = Queries.insertProduct(conn, id, "SKU-001", "Widget",
            true, 1.5f, 4.2, List.of("gadget", "tool"), "{\"color\":\"red\"}",
            new byte[]{0x01, 0x02}, (short) 10).orElseThrow();

        assertEquals(id, product.id());
        assertEquals("SKU-001", product.sku());
        assertEquals("Widget", product.name());
        assertTrue(product.active());
        assertEquals(1.5f, product.weightKg());
        assertEquals(4.2, product.rating());
        assertEquals(List.of("gadget", "tool"), product.tags());
        assertNotNull(product.metadata());
        assertArrayEquals(new byte[]{0x01, 0x02}, product.thumbnail());
        assertEquals((short) 10, product.stockCount());

        // Retrieve by id
        var fetched = Queries.getProduct(conn, id).orElseThrow();
        assertEquals("Widget", fetched.name());
        assertEquals(List.of("gadget", "tool"), fetched.tags());
    }

    @Test
    void testListActiveProducts() throws SQLException {
        var id1 = UUID.randomUUID();
        var id2 = UUID.randomUUID();
        Queries.insertProduct(conn, id1, "SKU-A", "Active", true, null, null,
            List.of(), null, null, (short) 0);
        Queries.insertProduct(conn, id2, "SKU-B", "Inactive", false, null, null,
            List.of("archived"), null, null, (short) 0);

        var active = Queries.listActiveProducts(conn, true);
        assertEquals(1, active.size());
        assertEquals("Active", active.get(0).name());

        var inactive = Queries.listActiveProducts(conn, false);
        assertEquals(1, inactive.size());
        assertEquals("Inactive", inactive.get(0).name());
    }

    @Test
    void testProductWithNullOptionalFields() throws SQLException {
        var id = UUID.randomUUID();
        var product = Queries.insertProduct(conn, id, "SKU-NULL", "Bare",
            true, null, null, List.of(), null, null, (short) 0).orElseThrow();

        assertNull(product.weightKg());
        assertNull(product.rating());
        assertEquals(List.of(), product.tags());
        assertNull(product.metadata());
        assertNull(product.thumbnail());
    }
}
