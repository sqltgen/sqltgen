package com.example

import com.example.db.QueriesDs
import java.math.BigDecimal
import org.postgresql.ds.PGSimpleDataSource

object Demo {

    private const val PG_URL  = "jdbc:postgresql://localhost:5433/sqltgen"
    private const val PG_USER = "sqltgen"
    private const val PG_PASS = "sqltgen"

    fun run() {
        val ds = PGSimpleDataSource().apply {
            setURL(PG_URL)
            setUser(PG_USER)
            setPassword(PG_PASS)
        }
        val q = QueriesDs(ds)
        seed(q)
        query(q)
    }

    private fun seed(q: QueriesDs) {
        val leGuin  = q.createAuthor("Ursula K. Le Guin", "Science fiction and fantasy author", 1929)!!
        val herbert = q.createAuthor("Frank Herbert",     "Author of the Dune series",           1920)!!
        val asimov  = q.createAuthor("Isaac Asimov",      null,                                  1920)!!
        println("[pg] inserted 3 authors (ids: ${leGuin.id}, ${herbert.id}, ${asimov.id})")

        val lhod  = q.createBook(leGuin.id,  "The Left Hand of Darkness", "sci-fi", BigDecimal("12.99"), null)!!
        val disp  = q.createBook(leGuin.id,  "The Dispossessed",           "sci-fi", BigDecimal("11.50"), null)!!
        val dune  = q.createBook(herbert.id, "Dune",                       "sci-fi", BigDecimal("14.99"), null)!!
        val found = q.createBook(asimov.id,  "Foundation",                 "sci-fi", BigDecimal("10.99"), null)!!
        val caves = q.createBook(asimov.id,  "The Caves of Steel",         "sci-fi", BigDecimal("9.99"),  null)!!
        println("[pg] inserted 5 books")

        val alice = q.createCustomer("Alice", "alice@example.com")!!
        val bob   = q.createCustomer("Bob",   "bob@example.com")!!
        println("[pg] inserted 2 customers")

        val sale1 = q.createSale(alice.id)!!
        q.addSaleItem(sale1.id, dune.id,  2, BigDecimal("14.99"))
        q.addSaleItem(sale1.id, found.id, 1, BigDecimal("10.99"))
        val sale2 = q.createSale(bob.id)!!
        q.addSaleItem(sale2.id, dune.id, 1, BigDecimal("14.99"))
        q.addSaleItem(sale2.id, lhod.id, 1, BigDecimal("12.99"))
        println("[pg] inserted 2 sales with items")
    }

    private fun query(q: QueriesDs) {
        val authors = q.listAuthors()
        println("[pg] listAuthors: ${authors.size} row(s)")

        val scifi = q.listBooksByGenre("sci-fi")
        println("[pg] listBooksByGenre(sci-fi): ${scifi.size} row(s)")

        val allBooks = q.listBooksByGenreOrAll("all")
        println("[pg] listBooksByGenreOrAll(all): ${allBooks.size} row(s) (repeated-param demo)")
        val scifi2 = q.listBooksByGenreOrAll("sci-fi")
        println("[pg] listBooksByGenreOrAll(sci-fi): ${scifi2.size} row(s)")

        println("[pg] listBooksWithAuthor:")
        q.listBooksWithAuthor().forEach { r ->
            println("  \"${r.title}\" by ${r.authorName}")
        }

        val neverOrdered = q.getBooksNeverOrdered()
        println("[pg] getBooksNeverOrdered: ${neverOrdered.size} book(s)")
        neverOrdered.forEach { println("  \"${it.title}\"") }

        println("[pg] getTopSellingBooks:")
        q.getTopSellingBooks().forEach { r ->
            println("  \"${r.title}\" sold ${r.unitsSold}")
        }

        println("[pg] getBestCustomers:")
        q.getBestCustomers().forEach { r ->
            println("  ${r.name} spent ${r.totalSpent}")
        }

        // Demonstrate UPDATE RETURNING and DELETE RETURNING with a transient author
        val temp = q.createAuthor("Temp Author", null, null)!!
        q.updateAuthorBio("Updated via UPDATE RETURNING", temp.id)
            ?.let { println("[pg] updateAuthorBio: updated \"${it.name}\" — bio: ${it.bio}") }
        q.deleteAuthor(temp.id)
            ?.let { println("[pg] deleteAuthor: deleted \"${it.name}\" (id=${it.id})") }
    }
}
