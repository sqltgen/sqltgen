package com.example

import com.example.db.Queries
import java.math.BigDecimal
import java.sql.Connection
import java.sql.DriverManager

object Demo {

    // MySQL container runs on 3307 to avoid conflicts with any local MySQL instance.
    // allowPublicKeyRetrieval and useSSL=false are required by MySQL Connector/J 8+ for
    // plain-password auth when connecting without a client certificate.
    private const val MYSQL_URL  = "jdbc:mysql://localhost:3307/sqltgen?allowPublicKeyRetrieval=true&useSSL=false"
    private const val MYSQL_USER = "sqltgen"
    private const val MYSQL_PASS = "sqltgen"

    fun run() {
        DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASS).use { conn ->
            seed(conn)
            query(conn)
        }
    }

    private fun seed(conn: Connection) {
        // MySQL has no RETURNING, so INSERT returns void and IDs are sequential
        // starting from 1 on a fresh database (which docker compose always provides).
        Queries.createAuthor(conn, "Ursula K. Le Guin", "Science fiction and fantasy author", 1929)
        Queries.createAuthor(conn, "Frank Herbert",     "Author of the Dune series",           1920)
        Queries.createAuthor(conn, "Isaac Asimov",      null,                                  1920)
        println("[mysql] inserted 3 authors")

        Queries.createBook(conn, 1L, "The Left Hand of Darkness", "sci-fi", BigDecimal("12.99"), null)
        Queries.createBook(conn, 1L, "The Dispossessed",           "sci-fi", BigDecimal("11.50"), null)
        Queries.createBook(conn, 2L, "Dune",                       "sci-fi", BigDecimal("14.99"), null)
        Queries.createBook(conn, 3L, "Foundation",                 "sci-fi", BigDecimal("10.99"), null)
        Queries.createBook(conn, 3L, "The Caves of Steel",         "sci-fi", BigDecimal("9.99"),  null)
        println("[mysql] inserted 5 books")

        Queries.createCustomer(conn, "Ed",   "ed@example.com")
        Queries.createCustomer(conn, "Faye", "faye@example.com")
        println("[mysql] inserted 2 customers")

        Queries.createSale(conn, 1L)
        Queries.addSaleItem(conn, 1L, 3L, 2, BigDecimal("14.99"))  // Ed buys 2x Dune
        Queries.addSaleItem(conn, 1L, 4L, 1, BigDecimal("10.99"))  // Ed buys 1x Foundation
        Queries.createSale(conn, 2L)
        Queries.addSaleItem(conn, 2L, 3L, 1, BigDecimal("14.99"))  // Faye buys 1x Dune
        Queries.addSaleItem(conn, 2L, 1L, 1, BigDecimal("12.99"))  // Faye buys 1x Left Hand
        println("[mysql] inserted 2 sales with items")

        // Insert a temp author (no books) so we can demo update/delete without FK violations.
        // Docker always provides a fresh DB, so sequential IDs are predictable.
        Queries.createAuthor(conn, "Temp Author", null, null)
    }

    private fun query(conn: Connection) {
        val authors = Queries.listAuthors(conn)
        println("[mysql] listAuthors: ${authors.size} row(s)")

        val scifi = Queries.listBooksByGenre(conn, "sci-fi")
        println("[mysql] listBooksByGenre(sci-fi): ${scifi.size} row(s)")

        println("[mysql] listBooksWithAuthor:")
        Queries.listBooksWithAuthor(conn).forEach { r ->
            println("  \"${r.title}\" by ${r.authorName}")
        }

        val neverOrdered = Queries.getBooksNeverOrdered(conn)
        println("[mysql] getBooksNeverOrdered: ${neverOrdered.size} book(s)")
        neverOrdered.forEach { println("  \"${it.title}\"") }

        println("[mysql] getTopSellingBooks:")
        Queries.getTopSellingBooks(conn).forEach { r ->
            println("  \"${r.title}\" sold ${r.unitsSold}")
        }

        println("[mysql] getBestCustomers:")
        Queries.getBestCustomers(conn).forEach { r ->
            println("  ${r.name} spent ${r.totalSpent}")
        }

        // Demonstrate UPDATE and DELETE (no RETURNING in MySQL).
        // Uses author 4 (the temp author with no books) to avoid FK constraint violations.
        Queries.updateAuthorBio(conn, "Updated bio", 4L)
        println("[mysql] updateAuthorBio: updated temp author")
        Queries.deleteAuthor(conn, 4L)
        println("[mysql] deleteAuthor: deleted temp author")
    }
}
