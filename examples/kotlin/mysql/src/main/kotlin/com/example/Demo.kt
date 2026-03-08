package com.example

import com.example.db.QueriesDs
import java.math.BigDecimal
import com.mysql.cj.jdbc.MysqlDataSource

object Demo {

    private const val MYSQL_USER = "sqltgen"
    private const val MYSQL_PASS = "sqltgen"

    fun run(url: String) {
        val ds = MysqlDataSource().apply {
            setURL(url)
            setUser(MYSQL_USER)
            setPassword(MYSQL_PASS)
        }
        val q = QueriesDs(ds)
        seed(q)
        query(q)
    }

    private fun seed(q: QueriesDs) {
        // MySQL has no RETURNING, so INSERT returns void and IDs are sequential
        // starting from 1 on a fresh database (which docker compose always provides).
        q.createAuthor("Ursula K. Le Guin", "Science fiction and fantasy author", 1929)
        q.createAuthor("Frank Herbert",     "Author of the Dune series",           1920)
        q.createAuthor("Isaac Asimov",      null,                                  1920)
        println("[mysql] inserted 3 authors")

        q.createBook(1L, "The Left Hand of Darkness", "sci-fi", BigDecimal("12.99"), null)
        q.createBook(1L, "The Dispossessed",           "sci-fi", BigDecimal("11.50"), null)
        q.createBook(2L, "Dune",                       "sci-fi", BigDecimal("14.99"), null)
        q.createBook(3L, "Foundation",                 "sci-fi", BigDecimal("10.99"), null)
        q.createBook(3L, "The Caves of Steel",         "sci-fi", BigDecimal("9.99"),  null)
        println("[mysql] inserted 5 books")

        q.createCustomer("Ed",   "ed@example.com")
        q.createCustomer("Faye", "faye@example.com")
        println("[mysql] inserted 2 customers")

        q.createSale(1L)
        q.addSaleItem(1L, 3L, 2, BigDecimal("14.99"))  // Ed buys 2x Dune
        q.addSaleItem(1L, 4L, 1, BigDecimal("10.99"))  // Ed buys 1x Foundation
        q.createSale(2L)
        q.addSaleItem(2L, 3L, 1, BigDecimal("14.99"))  // Faye buys 1x Dune
        q.addSaleItem(2L, 1L, 1, BigDecimal("12.99"))  // Faye buys 1x Left Hand
        println("[mysql] inserted 2 sales with items")

        // Insert a temp author (no books) so we can demo update/delete without FK violations.
        // Docker always provides a fresh DB, so sequential IDs are predictable.
        q.createAuthor("Temp Author", null, null)
    }

    private fun query(q: QueriesDs) {
        val authors = q.listAuthors()
        println("[mysql] listAuthors: ${authors.size} row(s)")

        // Books inserted in seed have IDs 1–5; 1=Left Hand, 3=Dune.
        val byIds = q.getBooksByIds(listOf(1L, 3L))
        println("[mysql] getBooksByIds([1,3]): ${byIds.size} row(s)")
        byIds.forEach { println("  \"${it.title}\"") }

        val scifi = q.listBooksByGenre("sci-fi")
        println("[mysql] listBooksByGenre(sci-fi): ${scifi.size} row(s)")

        val allBooks = q.listBooksByGenreOrAll("all")
        println("[mysql] listBooksByGenreOrAll(all): ${allBooks.size} row(s) (repeated-param demo)")
        val scifi2 = q.listBooksByGenreOrAll("sci-fi")
        println("[mysql] listBooksByGenreOrAll(sci-fi): ${scifi2.size} row(s)")

        println("[mysql] listBooksWithAuthor:")
        q.listBooksWithAuthor().forEach { r ->
            println("  \"${r.title}\" by ${r.authorName}")
        }

        val neverOrdered = q.getBooksNeverOrdered()
        println("[mysql] getBooksNeverOrdered: ${neverOrdered.size} book(s)")
        neverOrdered.forEach { println("  \"${it.title}\"") }

        println("[mysql] getTopSellingBooks:")
        q.getTopSellingBooks().forEach { r ->
            println("  \"${r.title}\" sold ${r.unitsSold}")
        }

        println("[mysql] getBestCustomers:")
        q.getBestCustomers().forEach { r ->
            println("  ${r.name} spent ${r.totalSpent}")
        }

        // Demonstrate UPDATE and DELETE (no RETURNING in MySQL).
        // Uses author 4 (the temp author with no books) to avoid FK constraint violations.
        q.updateAuthorBio("Updated bio", 4L)
        println("[mysql] updateAuthorBio: updated temp author")
        q.deleteAuthor(4L)
        println("[mysql] deleteAuthor: deleted temp author")
    }
}
