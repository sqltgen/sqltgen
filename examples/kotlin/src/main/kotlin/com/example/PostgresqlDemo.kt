package com.example

import com.example.db.pg.Queries
import java.math.BigDecimal
import java.sql.Connection
import java.sql.DriverManager

object PostgresqlDemo {

    private const val PG_URL  = "jdbc:postgresql://localhost:5433/sqltgen"
    private const val PG_USER = "sqltgen"
    private const val PG_PASS = "sqltgen"

    fun run() {
        DriverManager.getConnection(PG_URL, PG_USER, PG_PASS).use { conn ->
            seed(conn)
            query(conn)
        }
    }

    private fun seed(conn: Connection) {
        val leGuin  = Queries.createAuthor(conn, "Ursula K. Le Guin", "Science fiction and fantasy author", 1929)!!
        val herbert = Queries.createAuthor(conn, "Frank Herbert",     "Author of the Dune series",           1920)!!
        val asimov  = Queries.createAuthor(conn, "Isaac Asimov",      null,                                  1920)!!
        println("[pg] inserted 3 authors (ids: ${leGuin.id}, ${herbert.id}, ${asimov.id})")

        val lhod  = Queries.createBook(conn, leGuin.id,  "The Left Hand of Darkness", "sci-fi", BigDecimal("12.99"), null)!!
        val disp  = Queries.createBook(conn, leGuin.id,  "The Dispossessed",           "sci-fi", BigDecimal("11.50"), null)!!
        val dune  = Queries.createBook(conn, herbert.id, "Dune",                       "sci-fi", BigDecimal("14.99"), null)!!
        val found = Queries.createBook(conn, asimov.id,  "Foundation",                 "sci-fi", BigDecimal("10.99"), null)!!
        val caves = Queries.createBook(conn, asimov.id,  "The Caves of Steel",         "sci-fi", BigDecimal("9.99"),  null)!!
        println("[pg] inserted 5 books")

        val alice = Queries.createCustomer(conn, "Alice", "alice@example.com")!!
        val bob   = Queries.createCustomer(conn, "Bob",   "bob@example.com")!!
        println("[pg] inserted 2 customers")

        val sale1 = Queries.createSale(conn, alice.id)!!
        Queries.addSaleItem(conn, sale1.id, dune.id,  2, BigDecimal("14.99"))
        Queries.addSaleItem(conn, sale1.id, found.id, 1, BigDecimal("10.99"))
        val sale2 = Queries.createSale(conn, bob.id)!!
        Queries.addSaleItem(conn, sale2.id, dune.id, 1, BigDecimal("14.99"))
        Queries.addSaleItem(conn, sale2.id, lhod.id, 1, BigDecimal("12.99"))
        println("[pg] inserted 2 sales with items")
    }

    private fun query(conn: Connection) {
        val authors = Queries.listAuthors(conn)
        println("[pg] listAuthors: ${authors.size} row(s)")

        val scifi = Queries.listBooksByGenre(conn, "sci-fi")
        println("[pg] listBooksByGenre(sci-fi): ${scifi.size} row(s)")

        println("[pg] listBooksWithAuthor:")
        Queries.listBooksWithAuthor(conn).forEach { r ->
            println("  \"${r.title}\" by ${r.authorName}")
        }

        val neverOrdered = Queries.getBooksNeverOrdered(conn)
        println("[pg] getBooksNeverOrdered: ${neverOrdered.size} book(s)")
        neverOrdered.forEach { println("  \"${it.title}\"") }

        println("[pg] getTopSellingBooks:")
        Queries.getTopSellingBooks(conn).forEach { r ->
            println("  \"${r.title}\" sold ${r.unitsSold}")
        }

        println("[pg] getBestCustomers:")
        Queries.getBestCustomers(conn).forEach { r ->
            println("  ${r.name} spent ${r.totalSpent}")
        }

        // Demonstrate UPDATE RETURNING and DELETE RETURNING with a transient author
        val temp = Queries.createAuthor(conn, "Temp Author", null, null)!!
        Queries.updateAuthorBio(conn, "Updated via UPDATE RETURNING", temp.id)
            ?.let { println("[pg] updateAuthorBio: updated \"${it.name}\" — bio: ${it.bio}") }
        Queries.deleteAuthor(conn, temp.id)
            ?.let { println("[pg] deleteAuthor: deleted \"${it.name}\" (id=${it.id})") }
    }
}
