package com.example

import com.example.db.QueriesDs
import java.math.BigDecimal
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.Statement
import org.sqlite.SQLiteDataSource

object Demo {

    // file::memory:?cache=shared allows multiple connections to share the same
    // in-memory database. The keeper connection in run() holds it open for the
    // full demo lifetime so the data survives across QueriesDs method calls.
    private const val SQLITE_URL = "jdbc:sqlite:file::memory:?cache=shared"

    fun run() {
        val ds = SQLiteDataSource().apply { setUrl(SQLITE_URL) }
        // Keep one connection open so the in-memory DB is not dropped between calls.
        ds.connection.use { keeper ->
            applyMigrations(keeper)
            val q = QueriesDs(ds)
            seed(q)
            query(q)
        }
    }

    private fun applyMigrations(conn: Connection) {
        val migrationsDir = Path.of("../../common/sqlite/migrations")
        val files = Files.list(migrationsDir)
            .filter { it.toString().endsWith(".sql") }
            .sorted()
            .toList()

        conn.createStatement().use { st: Statement ->
            for (file in files) {
                val sql = Files.readString(file)
                for (stmt in sql.split(";")) {
                    val s = stmt.trim()
                    if (s.isNotEmpty()) st.execute(s)
                }
            }
        }
    }

    private fun seed(q: QueriesDs) {
        q.createAuthor("Ursula K. Le Guin", "Science fiction and fantasy author", 1929)
        q.createAuthor("Frank Herbert",     "Author of the Dune series",           1920)
        q.createAuthor("Isaac Asimov",      null,                                  1920)
        println("[sqlite] inserted 3 authors")

        q.createBook(1, "The Left Hand of Darkness", "sci-fi", BigDecimal("12.99"), null)
        q.createBook(1, "The Dispossessed",           "sci-fi", BigDecimal("11.50"), null)
        q.createBook(2, "Dune",                       "sci-fi", BigDecimal("14.99"), null)
        q.createBook(3, "Foundation",                 "sci-fi", BigDecimal("10.99"), null)
        q.createBook(3, "The Caves of Steel",         "sci-fi", BigDecimal("9.99"),  null)
        println("[sqlite] inserted 5 books")

        q.createCustomer("Carol", "carol@example.com")
        q.createCustomer("Dave",  "dave@example.com")
        println("[sqlite] inserted 2 customers")

        q.createSale(1)
        q.addSaleItem(1, 3, 2, BigDecimal("14.99"))
        q.addSaleItem(1, 4, 1, BigDecimal("10.99"))
        q.createSale(2)
        q.addSaleItem(2, 3, 1, BigDecimal("14.99"))
        q.addSaleItem(2, 1, 1, BigDecimal("12.99"))
        println("[sqlite] inserted 2 sales with items")
    }

    private fun query(q: QueriesDs) {
        val authors = q.listAuthors()
        println("[sqlite] listAuthors: ${authors.size} row(s)")

        val scifi = q.listBooksByGenre("sci-fi")
        println("[sqlite] listBooksByGenre(sci-fi): ${scifi.size} row(s)")

        println("[sqlite] listBooksWithAuthor:")
        q.listBooksWithAuthor().forEach { r ->
            println("  \"${r.title}\" by ${r.authorName}")
        }

        val neverOrdered = q.getBooksNeverOrdered()
        println("[sqlite] getBooksNeverOrdered: ${neverOrdered.size} book(s)")
        neverOrdered.forEach { println("  \"${it.title}\"") }

        println("[sqlite] getTopSellingBooks:")
        q.getTopSellingBooks().forEach { r ->
            println("  \"${r.title}\" sold ${r.unitsSold}")
        }

        println("[sqlite] getBestCustomers:")
        q.getBestCustomers().forEach { r ->
            println("  ${r.name} spent ${r.totalSpent}")
        }
    }
}
