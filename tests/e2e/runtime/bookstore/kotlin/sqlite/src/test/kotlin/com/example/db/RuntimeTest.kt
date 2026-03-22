package com.example.db

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager

/**
 * End-to-end runtime tests for the generated Kotlin/SQLite queries.
 *
 * Uses an in-memory SQLite database — no external services required.
 * Each test method gets a fresh database via setUp().
 */
class RuntimeTest {

    private lateinit var conn: Connection

    @BeforeEach
    fun setUp() {
        conn = DriverManager.getConnection("jdbc:sqlite::memory:")
        conn.autoCommit = true
        conn.createStatement().use { s ->
            s.execute("PRAGMA foreign_keys = ON")
        }
        val schemaSql = Files.readString(Path.of("../../../../fixtures/bookstore/sqlite/schema.sql"))
        conn.createStatement().use { s ->
            for (stmt in schemaSql.split(";")) {
                val t = stmt.trim()
                if (t.isNotEmpty()) s.execute(t)
            }
        }
    }

    @AfterEach
    fun tearDown() {
        conn.close()
    }

    /** Insert a consistent set of test fixtures. Known IDs: author 1=Asimov, 2=Herbert, 3=Le Guin;
     *  book 1=Foundation, 2=I Robot, 3=Dune, 4=Earthsea; customer 1=Alice; sale 1. */
    private fun seed() {
        Queries.createAuthor(conn, "Asimov", "Sci-fi master", 1920)
        Queries.createAuthor(conn, "Herbert", null, 1920)
        Queries.createAuthor(conn, "Le Guin", "Earthsea", 1929)

        Queries.createBook(conn, 1, "Foundation", "sci-fi", 9.99, "1951-01-01")
        Queries.createBook(conn, 1, "I Robot",    "sci-fi", 7.99, "1950-01-01")
        Queries.createBook(conn, 2, "Dune",       "sci-fi", 12.99, "1965-01-01")
        Queries.createBook(conn, 3, "Earthsea",   "fantasy", 8.99, "1968-01-01")

        Queries.createCustomer(conn, "Alice", "alice@example.com")
        Queries.createSale(conn, 1)
        Queries.addSaleItem(conn, 1, 1, 2, 9.99)   // Foundation qty 2
        Queries.addSaleItem(conn, 1, 3, 1, 12.99)  // Dune qty 1
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
        assertEquals(1, book.authorId)
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

    // ─── :exec tests ──────────────────────────────────────────────────────────

    @Test
    fun testCreateAuthorExec() {
        Queries.createAuthor(conn, "Test", null, null)
        val author = Queries.getAuthor(conn, 1)!!
        assertEquals("Test", author.name)
        assertNull(author.bio)
        assertNull(author.birthYear)
    }

    @Test
    fun testCreateBook() {
        seed()
        Queries.createBook(conn, 1, "New Book", "mystery", 14.50, null)
        val book = Queries.getBook(conn, 5)!!
        assertEquals("New Book", book.title)
        assertEquals("mystery", book.genre)
        assertNull(book.publishedAt)
    }

    @Test
    fun testCreateCustomer() {
        Queries.createCustomer(conn, "Bob", "bob@example.com")
        val count = conn.createStatement().use { s ->
            s.executeQuery("SELECT COUNT(*) FROM customer WHERE name = 'Bob'").use { rs ->
                rs.next(); rs.getInt(1)
            }
        }
        assertEquals(1, count)
    }

    @Test
    fun testCreateSale() {
        seed()
        Queries.createSale(conn, 1)
        val count = conn.createStatement().use { s ->
            s.executeQuery("SELECT COUNT(*) FROM sale WHERE customer_id = 1").use { rs ->
                rs.next(); rs.getInt(1)
            }
        }
        assertEquals(2, count)
    }

    @Test
    fun testAddSaleItem() {
        seed()
        Queries.addSaleItem(conn, 1, 4, 1, 8.99)
        val count = conn.createStatement().use { s ->
            s.executeQuery("SELECT COUNT(*) FROM sale_item WHERE sale_id = 1").use { rs ->
                rs.next(); rs.getInt(1)
            }
        }
        assertEquals(3, count)
    }

    @Test
    fun testUpdateAuthorBio() {
        seed()
        Queries.updateAuthorBio(conn, "Updated bio", 1)
        val author = Queries.getAuthor(conn, 1)!!
        assertEquals("Updated bio", author.bio)
    }

    @Test
    fun testDeleteAuthor() {
        Queries.createAuthor(conn, "Temp", null, null)
        Queries.deleteAuthor(conn, 1)
        assertNull(Queries.getAuthor(conn, 1))
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
        // Seed has only Alice buying Foundation + Dune; I Robot and Earthsea were never ordered
        val books = Queries.getBooksNeverOrdered(conn)
        assertEquals(2, books.size)
        val titles = books.map { it.title }.toSet()
        assertTrue(titles.contains("I Robot"))
        assertTrue(titles.contains("Earthsea"))
    }

    // ─── CTE tests ────────────────────────────────────────────────────────────

    @Test
    fun testGetTopSellingBooks() {
        seed()
        val rows = Queries.getTopSellingBooks(conn)
        assertTrue(rows.isNotEmpty())
        // Foundation: qty 2 is the top seller
        assertEquals("Foundation", rows[0].title)
        assertEquals(2L, rows[0].unitsSold)
    }

    @Test
    fun testGetBestCustomers() {
        seed()
        val rows = Queries.getBestCustomers(conn)
        assertEquals(1, rows.size)
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
        val results = Queries.getBooksByPriceRange(conn, 8.00, 10.00)
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
        assertFalse(results.any { it.title == "Foundation" })
        assertFalse(results.any { it.title == "I Robot" })
    }

    @Test
    fun testGetBooksWithRecentSales() {
        seed()
        // Sales are current; use a past cutoff date string
        val results = Queries.getBooksWithRecentSales(conn, java.time.LocalDateTime.of(2000, 1, 1, 0, 0))
        // Foundation and Dune have sale_items
        assertEquals(2, results.size)
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

    // ─── CASE / COALESCE tests ────────────────────────────────────────────────

    @Test
    fun testGetBookPriceLabel() {
        seed()
        val rows = Queries.getBookPriceLabel(conn, 10.00)
        assertEquals(4, rows.size)
        val dune = rows.first { it.title == "Dune" }
        assertEquals("expensive", dune.priceLabel)
        val earthsea = rows.first { it.title == "Earthsea" }
        assertEquals("affordable", earthsea.priceLabel)
    }

    @Test
    fun testGetBookPriceOrDefault() {
        seed()
        val rows = Queries.getBookPriceOrDefault(conn, 0.00)
        assertEquals(4, rows.size)
        assertTrue(rows.all { it.effectivePrice > 0.0 })
    }

    // ─── Product type coverage ────────────────────────────────────────────────

    @Test
    fun testGetProduct() {
        val pid = "prod-get-001"
        Queries.insertProduct(conn, pid, "SKU-GET", "Widget", 1, null, null, null, null, 5)
        val row = Queries.getProduct(conn, pid)!!
        assertEquals(pid, row.id)
        assertEquals("Widget", row.name)
        assertEquals(5, row.stockCount)
    }

    @Test
    fun testListActiveProducts() {
        Queries.insertProduct(conn, "act-1",   "ACT-1",   "Active",   1, null, null, null, null, 10)
        Queries.insertProduct(conn, "inact-1", "INACT-1", "Inactive", 0, null, null, null, null, 0)
        val active = Queries.listActiveProducts(conn, 1)
        assertEquals(1, active.size)
        assertEquals("Active", active[0].name)
        val inactive = Queries.listActiveProducts(conn, 0)
        assertEquals(1, inactive.size)
        assertEquals("Inactive", inactive[0].name)
    }

    @Test
    fun testInsertProduct() {
        val pid = "prod-ins-001"
        Queries.insertProduct(conn, pid, "SKU-INS", "Gadget", 1, 1.5f, null, null, null, 7)
        val row = Queries.getProduct(conn, pid)!!
        assertEquals(pid, row.id)
        assertEquals("Gadget", row.name)
        assertEquals(7, row.stockCount)
    }

    @Test
    fun testUpsertProduct() {
        val pid = "prod-ups-001"
        Queries.upsertProduct(conn, pid, "SKU-UP", "Thing", 1, null, 10)
        val row = Queries.getProduct(conn, pid)!!
        assertEquals("Thing", row.name)
        assertEquals(10, row.stockCount)

        Queries.upsertProduct(conn, pid, "SKU-UP", "Thing Pro", 1, null, 20)
        val updated = Queries.getProduct(conn, pid)!!
        assertEquals("Thing Pro", updated.name)
        assertEquals(20, updated.stockCount)
    }

    // ─── IS NULL / IS NOT NULL tests ──────────────────────────────────────────

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

    // ─── Date BETWEEN tests ───────────────────────────────────────────────────

    @Test
    fun testGetBooksPublishedBetween() {
        seed()
        // 1951-01-01 to 1966-01-01 → Foundation (1951) and Dune (1965)
        val rows = Queries.getBooksPublishedBetween(conn, "1951-01-01", "1966-01-01")
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

    // ─── :one COUNT aggregate ─────────────────────────────────────────────────

    @Test
    fun testCountSaleItems() {
        seed()
        val row = Queries.countSaleItems(conn, 1)!!
        assertEquals(2L, row.itemCount)
        // COUNT(*) always returns a row
        val zero = Queries.countSaleItems(conn, 999)!!
        assertEquals(0L, zero.itemCount)
    }

    // ─── MIN/MAX/SUM/AVG aggregate tests ─────────────────────────────────────

    @Test
    fun testGetSaleItemQuantityAggregates() {
        seed()
        // Seed: Foundation qty 2 + Dune qty 1 → min=1, max=2, sum=3, avg=1.5
        val row = Queries.getSaleItemQuantityAggregates(conn)!!
        assertEquals(1, row.minQty)
        assertEquals(2, row.maxQty)
        assertEquals(3L, row.sumQty)
        assertTrue(Math.abs(row.avgQty!! - 1.5) < 0.01)
    }

    @Test
    fun testGetBookPriceAggregates() {
        seed()
        // Book prices: 9.99, 7.99, 12.99, 8.99 → min=7.99, max=12.99, sum=39.96, avg≈9.99
        val row = Queries.getBookPriceAggregates(conn)!!
        assertTrue(Math.abs(row.minPrice!! - 7.99) < 0.01)
        assertTrue(Math.abs(row.maxPrice!! - 12.99) < 0.01)
        assertTrue(Math.abs(row.sumPrice!! - 39.96) < 0.01)
        assertTrue(Math.abs(row.avgPrice!! - 9.99) < 0.01)
    }
}
