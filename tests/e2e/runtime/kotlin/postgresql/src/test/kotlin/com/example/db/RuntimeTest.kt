package com.example.db

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.UUID

/**
 * End-to-end runtime tests for the generated Kotlin/PostgreSQL queries.
 *
 * Each test runs in its own PostgreSQL schema to allow parallel execution.
 * Requires the docker-compose postgres service on port 15432.
 */
class RuntimeTest {

    private val url = System.getenv()
        .getOrDefault("DATABASE_URL", "jdbc:postgresql://localhost:15432/sqltgen_e2e")

    private lateinit var conn: Connection
    private lateinit var schema: String

    @BeforeEach
    fun setUp() {
        conn = DriverManager.getConnection(url, "sqltgen", "sqltgen")
        conn.autoCommit = true

        schema = "test_" + UUID.randomUUID().toString().replace("-", "")
        val schemaSql = Files.readString(Path.of("../../../fixtures/postgresql/schema.sql"))
        conn.createStatement().use { s ->
            s.execute("""CREATE SCHEMA "$schema"""")
            s.execute("""SET search_path TO "$schema"""")
            s.execute(schemaSql)
        }
    }

    @AfterEach
    fun tearDown() {
        conn.createStatement().use { s ->
            s.execute("""DROP SCHEMA IF EXISTS "$schema" CASCADE""")
        }
        conn.close()
    }

    private fun seed() {
        val a1 = Queries.createAuthor(conn, "Asimov", "Sci-fi master", 1920)!!
        val a2 = Queries.createAuthor(conn, "Herbert", null, 1920)!!
        val a3 = Queries.createAuthor(conn, "Le Guin", "Earthsea", 1929)!!

        val b1 = Queries.createBook(conn, a1.id, "Foundation", "sci-fi",
            BigDecimal("9.99"), LocalDate.of(1951, 1, 1))!!
        val b2 = Queries.createBook(conn, a1.id, "I Robot", "sci-fi",
            BigDecimal("7.99"), LocalDate.of(1950, 1, 1))!!
        val b3 = Queries.createBook(conn, a2.id, "Dune", "sci-fi",
            BigDecimal("12.99"), LocalDate.of(1965, 1, 1))!!
        val b4 = Queries.createBook(conn, a3.id, "Earthsea", "fantasy",
            BigDecimal("8.99"), LocalDate.of(1968, 1, 1))!!

        val alice = Queries.createCustomer(conn, "Alice", "alice@example.com")!!
        val bob = Queries.createCustomer(conn, "Bob", "bob@example.com")!!

        val sale1 = Queries.createSale(conn, alice.id)!!
        Queries.addSaleItem(conn, sale1.id, b1.id, 2, BigDecimal("9.99"))
        Queries.addSaleItem(conn, sale1.id, b3.id, 1, BigDecimal("12.99"))

        val sale2 = Queries.createSale(conn, bob.id)!!
        Queries.addSaleItem(conn, sale2.id, b4.id, 1, BigDecimal("8.99"))
    }

    // ─── :one tests ───────────────────────────────────────────────────────────

    @Test
    fun testGetAuthor() {
        seed()
        val author = Queries.getAuthor(conn, 1)!!
        assertEquals("Asimov", author.name)
        assertEquals("Sci-fi master", author.bio)
        assertEquals(1920, author.birthYear)
    }

    @Test
    fun testGetAuthorNotFound() {
        assertNull(Queries.getAuthor(conn, 999))
    }

    @Test
    fun testGetBook() {
        seed()
        val book = Queries.getBook(conn, 1)!!
        assertEquals("Foundation", book.title)
        assertEquals("sci-fi", book.genre)
    }

    // ─── :many tests ──────────────────────────────────────────────────────────

    @Test
    fun testListAuthors() {
        seed()
        val authors = Queries.listAuthors(conn)
        assertEquals(3, authors.size)
        assertEquals("Asimov", authors[0].name)
        assertEquals("Herbert", authors[1].name)
        assertEquals("Le Guin", authors[2].name)
    }

    @Test
    fun testListBooksByGenre() {
        seed()
        assertEquals(3, Queries.listBooksByGenre(conn, "sci-fi").size)
        val fantasy = Queries.listBooksByGenre(conn, "fantasy")
        assertEquals(1, fantasy.size)
        assertEquals("Earthsea", fantasy[0].title)
    }

    @Test
    fun testListBooksByGenreOrAll() {
        seed()
        assertEquals(4, Queries.listBooksByGenreOrAll(conn, "all").size)
        assertEquals(3, Queries.listBooksByGenreOrAll(conn, "sci-fi").size)
    }

    // ─── UpdateAuthorBio / DeleteAuthor tests ─────────────────────────────────

    @Test
    fun testUpdateAuthorBio() {
        seed()
        val updated = Queries.updateAuthorBio(conn, "Updated bio", 1)!!
        assertEquals("Asimov", updated.name)
        assertEquals("Updated bio", updated.bio)
    }

    @Test
    fun testDeleteAuthor() {
        // Create an author with no books so FK won't block delete
        Queries.createAuthor(conn, "Temp", null, null)!!
        val tempId = conn.createStatement().use { s ->
            s.executeQuery("SELECT id FROM author WHERE name = 'Temp'").use { rs ->
                rs.next(); rs.getLong(1)
            }
        }
        val deleted = Queries.deleteAuthor(conn, tempId)!!
        assertEquals("Temp", deleted.name)
        assertNull(Queries.getAuthor(conn, tempId))
    }

    // ─── CreateBook / AddSaleItem tests ───────────────────────────────────────

    @Test
    fun testCreateBook() {
        seed()
        val book = Queries.createBook(conn, 1L, "New Book", "mystery",
            BigDecimal("14.50"), null)!!
        assertEquals("New Book", book.title)
        assertEquals("mystery", book.genre)
        assertNull(book.publishedAt)
    }

    @Test
    fun testAddSaleItem() {
        seed()
        // Add an extra item to sale 1 (Earthsea, qty 1)
        Queries.addSaleItem(conn, 1L, 4L, 1, BigDecimal("8.99"))
        val count = conn.createStatement().use { s ->
            s.executeQuery("SELECT COUNT(*) FROM sale_item WHERE sale_id = 1").use { rs ->
                rs.next(); rs.getLong(1)
            }
        }
        assertEquals(3L, count)
    }

    // ─── CASE / COALESCE tests ────────────────────────────────────────────────

    @Test
    fun testGetBookPriceLabel() {
        seed()
        val rows = Queries.getBookPriceLabel(conn, BigDecimal("10.00"))
        assertEquals(4, rows.size)
        val dune = rows.first { it.title == "Dune" }
        assertEquals("expensive", dune.priceLabel)
        val earthsea = rows.first { it.title == "Earthsea" }
        assertEquals("affordable", earthsea.priceLabel)
    }

    @Test
    fun testGetBookPriceOrDefault() {
        seed()
        val rows = Queries.getBookPriceOrDefault(conn, BigDecimal("0.00"))
        assertEquals(4, rows.size)
        assertTrue(rows.all { it.effectivePrice > BigDecimal.ZERO })
    }

    // ─── Product type coverage ────────────────────────────────────────────────

    @Test
    fun testGetProduct() {
        val productId = UUID.fromString("00000000-0000-0000-0000-000000000002")
        Queries.insertProduct(conn, productId, "SKU-002", "Widget", true,
            1.5f, 4.7, listOf("tag1"), null, null, 5)
        val row = Queries.getProduct(conn, productId)!!
        assertEquals(productId, row.id)
        assertEquals("Widget", row.name)
        assertEquals(5.toShort(), row.stockCount)
    }

    @Test
    fun testListActiveProducts() {
        val id1 = UUID.randomUUID()
        val id2 = UUID.randomUUID()
        Queries.insertProduct(conn, id1, "ACT-1", "Active", true, null, null, listOf(), null, null, 10)
        Queries.insertProduct(conn, id2, "INACT-1", "Inactive", false, null, null, listOf(), null, null, 0)
        val active = Queries.listActiveProducts(conn, true)
        assertEquals(1, active.size)
        assertEquals("Active", active[0].name)
        val inactive = Queries.listActiveProducts(conn, false)
        assertEquals(1, inactive.size)
        assertEquals("Inactive", inactive[0].name)
    }

    @Test
    fun testInsertProduct() {
        val productId = UUID.fromString("00000000-0000-0000-0000-000000000003")
        val product = Queries.insertProduct(conn, productId, "SKU-003", "Gadget", true,
            null, null, listOf("electronics"), null, null, 20)!!
        assertEquals(productId, product.id)
        assertEquals("Gadget", product.name)
        assertEquals(20.toShort(), product.stockCount)
    }

    // ─── :exec tests ──────────────────────────────────────────────────────────

    @Test
    fun testCreateAuthorReturnsRow() {
        val author = Queries.createAuthor(conn, "Test", null, null)!!
        assertEquals("Test", author.name)
        assertNull(author.bio)
        assertNull(author.birthYear)
    }

    // ─── CreateCustomer / CreateSale tests ───────────────────────────────────

    @Test
    fun testCreateCustomer() {
        val cust = Queries.createCustomer(conn, "Solo", "solo@example.com")!!
        assertTrue(cust.id > 0)
    }

    @Test
    fun testCreateSale() {
        val cust = Queries.createCustomer(conn, "Solo", "solo@example.com")!!
        val sale = Queries.createSale(conn, cust.id)!!
        assertTrue(sale.id > 0)
    }

    // ─── :execrows tests ──────────────────────────────────────────────────────

    @Test
    fun testDeleteBookById() {
        seed()
        // I Robot (id=2) has no sale_items
        assertEquals(1L, Queries.deleteBookById(conn, 2))
        assertEquals(0L, Queries.deleteBookById(conn, 999))
    }

    // ─── JOIN tests ───────────────────────────────────────────────────────────

    @Test
    fun testListBooksWithAuthor() {
        seed()
        val rows = Queries.listBooksWithAuthor(conn)
        assertEquals(4, rows.size)

        val dune = rows.first { it.title == "Dune" }
        assertEquals("Herbert", dune.authorName)
        assertNull(dune.authorBio)

        val foundation = rows.first { it.title == "Foundation" }
        assertEquals("Asimov", foundation.authorName)
        assertEquals("Sci-fi master", foundation.authorBio)
    }

    @Test
    fun testGetBooksNeverOrdered() {
        seed()
        val books = Queries.getBooksNeverOrdered(conn)
        // Only I Robot was not ordered
        assertEquals(1, books.size)
        assertEquals("I Robot", books[0].title)
    }

    // ─── CTE tests ────────────────────────────────────────────────────────────

    @Test
    fun testGetTopSellingBooks() {
        seed()
        val rows = Queries.getTopSellingBooks(conn)
        assertTrue(rows.isNotEmpty())
        assertEquals("Foundation", rows[0].title)
    }

    @Test
    fun testGetBestCustomers() {
        seed()
        val rows = Queries.getBestCustomers(conn)
        assertEquals("Alice", rows[0].name)
    }

    @Test
    fun testGetAuthorStats() {
        seed()
        val rows = Queries.getAuthorStats(conn)
        assertEquals(3, rows.size)
        val asimov = rows.first { it.name == "Asimov" }
        assertEquals(2L, asimov.numBooks)
    }

    // ─── Aggregate tests ──────────────────────────────────────────────────────

    @Test
    fun testCountBooksByGenre() {
        seed()
        val rows = Queries.countBooksByGenre(conn)
        assertEquals(2, rows.size)
        val fantasy = rows.first { it.genre == "fantasy" }
        assertEquals(1L, fantasy.bookCount)
        val sciFi = rows.first { it.genre == "sci-fi" }
        assertEquals(3L, sciFi.bookCount)
    }

    // ─── LIMIT/OFFSET tests ───────────────────────────────────────────────────

    @Test
    fun testListBooksWithLimit() {
        seed()
        val page1 = Queries.listBooksWithLimit(conn, 2, 0)
        val page2 = Queries.listBooksWithLimit(conn, 2, 2)
        assertEquals(2, page1.size)
        assertEquals(2, page2.size)
        val titles1 = page1.map { it.title }.toSet()
        val titles2 = page2.map { it.title }.toSet()
        assertTrue(titles1.intersect(titles2).isEmpty())
    }

    // ─── LIKE tests ───────────────────────────────────────────────────────────

    @Test
    fun testSearchBooksByTitle() {
        seed()
        val results = Queries.searchBooksByTitle(conn, "%ound%")
        assertEquals(1, results.size)
        assertEquals("Foundation", results[0].title)
        assertTrue(Queries.searchBooksByTitle(conn, "NOPE%").isEmpty())
    }

    // ─── BETWEEN tests ────────────────────────────────────────────────────────

    @Test
    fun testGetBooksByPriceRange() {
        seed()
        // Foundation (9.99) and Earthsea (8.99) in [8.00, 10.00]
        val results = Queries.getBooksByPriceRange(conn, BigDecimal("8.00"), BigDecimal("10.00"))
        assertEquals(2, results.size)
    }

    // ─── IN list tests ────────────────────────────────────────────────────────

    @Test
    fun testGetBooksInGenres() {
        seed()
        val results = Queries.getBooksInGenres(conn, "sci-fi", "fantasy", "horror")
        assertEquals(4, results.size)
    }

    // ─── HAVING tests ─────────────────────────────────────────────────────────

    @Test
    fun testGetGenresWithManyBooks() {
        seed()
        val results = Queries.getGenresWithManyBooks(conn, 1)
        assertEquals(1, results.size)
        assertEquals("sci-fi", results[0].genre)
        assertEquals(3L, results[0].bookCount)
    }

    // ─── Subquery tests ───────────────────────────────────────────────────────

    @Test
    fun testGetBooksNotByAuthor() {
        seed()
        val results = Queries.getBooksNotByAuthor(conn, "Asimov")
        assertEquals(2, results.size)
        val titles = results.map { it.title }
        assertFalse(titles.contains("Foundation"))
        assertFalse(titles.contains("I Robot"))
    }

    @Test
    fun testGetBooksWithRecentSales() {
        seed()
        val results = Queries.getBooksWithRecentSales(conn, LocalDateTime.of(2000, 1, 1, 0, 0))
        // Foundation, Dune, Earthsea all have sale_items
        assertEquals(3, results.size)
    }

    // ─── Scalar subquery test ─────────────────────────────────────────────────

    @Test
    fun testGetBookWithAuthorName() {
        seed()
        val rows = Queries.getBookWithAuthorName(conn)
        assertEquals(4, rows.size)
        val dune = rows.first { it.title == "Dune" }
        assertEquals("Herbert", dune.authorName)
    }

    // ─── JOIN with param tests ────────────────────────────────────────────────

    @Test
    fun testGetBooksByAuthorParam() {
        seed()
        // birth_year > 1925 → only Le Guin (1929) → Earthsea
        val results = Queries.getBooksByAuthorParam(conn, 1925)
        assertEquals(1, results.size)
        assertEquals("Earthsea", results[0].title)
    }

    // ─── Qualified wildcard tests ─────────────────────────────────────────────

    @Test
    fun testGetAllBookFields() {
        seed()
        val books = Queries.getAllBookFields(conn)
        assertEquals(4, books.size)
        assertEquals("Foundation", books[0].title)
    }

    // ─── List param tests ─────────────────────────────────────────────────────

    @Test
    fun testGetBooksByIds() {
        seed()
        val books = Queries.getBooksByIds(conn, listOf(1L, 3L))
        assertEquals(2, books.size)
        val titles = books.map { it.title }.toSet()
        assertTrue(titles.contains("Foundation"))
        assertTrue(titles.contains("Dune"))
    }

    // ─── IS NULL / IS NOT NULL tests ─────────────────────────────────────────

    @Test
    fun testGetAuthorsWithNullBio() {
        seed()
        val rows = Queries.getAuthorsWithNullBio(conn)
        assertEquals(1, rows.size)
        assertEquals("Herbert", rows[0].name)
    }

    @Test
    fun testGetAuthorsWithBio() {
        seed()
        val rows = Queries.getAuthorsWithBio(conn)
        assertEquals(2, rows.size)
        val names = rows.map { it.name }.toSet()
        assertTrue(names.contains("Asimov"))
        assertTrue(names.contains("Le Guin"))
    }

    // ─── Date range tests ─────────────────────────────────────────────────────

    @Test
    fun testGetBooksPublishedBetween() {
        seed()
        val rows = Queries.getBooksPublishedBetween(
            conn, LocalDate.of(1951, 1, 1), LocalDate.of(1966, 1, 1)
        )
        assertEquals(2, rows.size)
        val titles = rows.map { it.title }.toSet()
        assertTrue(titles.contains("Foundation"))
        assertTrue(titles.contains("Dune"))
    }

    // ─── DISTINCT tests ───────────────────────────────────────────────────────

    @Test
    fun testGetDistinctGenres() {
        seed()
        val rows = Queries.getDistinctGenres(conn)
        assertEquals(2, rows.size)
        val genres = rows.map { it.genre }.toSet()
        assertTrue(genres.contains("sci-fi"))
        assertTrue(genres.contains("fantasy"))
    }

    // ─── LEFT JOIN aggregate tests ────────────────────────────────────────────

    @Test
    fun testGetBooksWithSalesCount() {
        seed()
        val rows = Queries.getBooksWithSalesCount(conn)
        assertEquals(4, rows.size)

        val foundation = rows.first { it.title == "Foundation" }
        assertEquals(2L, foundation.totalQuantity)

        val dune = rows.first { it.title == "Dune" }
        assertEquals(1L, dune.totalQuantity)

        val iRobot = rows.first { it.title == "I Robot" }
        assertEquals(0L, iRobot.totalQuantity)
    }

    // ─── Scalar aggregate tests ───────────────────────────────────────────────

    @Test
    fun testCountSaleItems() {
        seed()
        val row = Queries.countSaleItems(conn, 1)!!
        assertEquals(2L, row.itemCount)
    }

    // ─── Upsert tests (PostgreSQL-specific) ───────────────────────────────────

    @Test
    fun testUpsertProduct() {
        val productId = UUID.fromString("00000000-0000-0000-0000-000000000001")

        val inserted = Queries.upsertProduct(conn, productId, "SKU-001", "Widget",
            true, listOf("tag1"), 10)!!
        assertEquals("Widget", inserted.name)
        assertEquals(10.toShort(), inserted.stockCount)

        val updated = Queries.upsertProduct(conn, productId, "SKU-001", "Widget Pro",
            true, listOf("tag1", "tag2"), 25)!!
        assertEquals("Widget Pro", updated.name)
        assertEquals(25.toShort(), updated.stockCount)
    }

    // ─── CTE DELETE tests (PostgreSQL-specific) ───────────────────────────────

    @Test
    fun testArchiveAndReturnBooks() {
        seed()
        // Archive books published before 1951-01-01.
        // Only I Robot (1950-01-01) qualifies; it has no sale_items.
        val archived = Queries.archiveAndReturnBooks(conn, LocalDate.of(1951, 1, 1))
        assertEquals(1, archived.size)
        assertEquals("I Robot", archived[0].title)

        val remaining = Queries.listBooksByGenre(conn, "sci-fi")
        assertFalse(remaining.any { it.title == "I Robot" })
    }

    // ─── MIN/MAX/SUM/AVG aggregate tests ─────────────────────────────────────

    @Test
    fun testGetSaleItemQuantityAggregates() {
        seed()
        // Sale items: Foundation qty 2 (Alice), Dune qty 1 (Alice), Earthsea qty 1 (Bob)
        // → min=1, max=2, sum=4, avg≈1.33
        val row = Queries.getSaleItemQuantityAggregates(conn)!!
        assertEquals(1, row.minQty)
        assertEquals(2, row.maxQty)
        assertEquals(4L, row.sumQty)
        assertTrue(Math.abs(row.avgQty!!.toDouble() - (4.0 / 3.0)) < 0.01)
    }

    @Test
    fun testGetBookPriceAggregates() {
        seed()
        // Book prices: 9.99, 7.99, 12.99, 8.99 → min=7.99, max=12.99, sum=39.96, avg=9.99
        val row = Queries.getBookPriceAggregates(conn)!!
        assertEquals(BigDecimal("7.99"), row.minPrice)
        assertEquals(BigDecimal("12.99"), row.maxPrice)
        assertEquals(BigDecimal("39.96"), row.sumPrice)
        assertTrue(row.avgPrice!!.subtract(BigDecimal("9.99")).abs() < BigDecimal("0.01"))
    }
}
