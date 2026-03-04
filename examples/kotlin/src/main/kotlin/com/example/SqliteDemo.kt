package com.example

import com.example.db.sqlite.Queries
import java.math.BigDecimal
import java.nio.file.Files
import java.nio.file.Path
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement

object SqliteDemo {

    fun run() {
        DriverManager.getConnection("jdbc:sqlite::memory:").use { conn ->
            applyMigrations(conn)
            seed(conn)
            query(conn)
        }
    }

    private fun applyMigrations(conn: Connection) {
        val migrationsDir = Path.of("../common/sqlite/migrations")
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

    private fun seed(conn: Connection) {
        Queries.createAuthor(conn, "Ursula K. Le Guin", "Science fiction and fantasy author", 1929)
        Queries.createAuthor(conn, "Frank Herbert",     "Author of the Dune series",           1920)
        Queries.createAuthor(conn, "Isaac Asimov",      null,                                  1920)
        println("[sqlite] inserted 3 authors")

        Queries.createBook(conn, 1, "The Left Hand of Darkness", "sci-fi", BigDecimal("12.99"), null)
        Queries.createBook(conn, 1, "The Dispossessed",           "sci-fi", BigDecimal("11.50"), null)
        Queries.createBook(conn, 2, "Dune",                       "sci-fi", BigDecimal("14.99"), null)
        Queries.createBook(conn, 3, "Foundation",                 "sci-fi", BigDecimal("10.99"), null)
        Queries.createBook(conn, 3, "The Caves of Steel",         "sci-fi", BigDecimal("9.99"),  null)
        println("[sqlite] inserted 5 books")

        Queries.createCustomer(conn, "Carol", "carol@example.com")
        Queries.createCustomer(conn, "Dave",  "dave@example.com")
        println("[sqlite] inserted 2 customers")

        Queries.createSale(conn, 1)
        Queries.addSaleItem(conn, 1, 3, 2, BigDecimal("14.99"))
        Queries.addSaleItem(conn, 1, 4, 1, BigDecimal("10.99"))
        Queries.createSale(conn, 2)
        Queries.addSaleItem(conn, 2, 3, 1, BigDecimal("14.99"))
        Queries.addSaleItem(conn, 2, 1, 1, BigDecimal("12.99"))
        println("[sqlite] inserted 2 sales with items")
    }

    private fun query(conn: Connection) {
        val authors = Queries.listAuthors(conn)
        println("[sqlite] listAuthors: ${authors.size} row(s)")

        val scifi = Queries.listBooksByGenre(conn, "sci-fi")
        println("[sqlite] listBooksByGenre(sci-fi): ${scifi.size} row(s)")

        println("[sqlite] listBooksWithAuthor:")
        Queries.listBooksWithAuthor(conn).forEach { r ->
            println("  \"${r.title}\" by ${r.authorName}")
        }

        val neverOrdered = Queries.getBooksNeverOrdered(conn)
        println("[sqlite] getBooksNeverOrdered: ${neverOrdered.size} book(s)")
        neverOrdered.forEach { println("  \"${it.title}\"") }

        println("[sqlite] getTopSellingBooks:")
        Queries.getTopSellingBooks(conn).forEach { r ->
            println("  \"${r.title}\" sold ${r.unitsSold}")
        }

        println("[sqlite] getBestCustomers:")
        Queries.getBestCustomers(conn).forEach { r ->
            println("  ${r.name} spent ${r.totalSpent}")
        }
    }
}
