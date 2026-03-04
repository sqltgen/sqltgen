package com.example

import com.example.db.pg.Queries
import java.math.BigDecimal
import java.sql.Connection
import java.sql.DriverManager
import java.sql.Statement

object PostgresqlDemo {

    fun run() {
        // H2 in PostgreSQL-compatible mode stands in for PostgreSQL in this demo.
        DriverManager.getConnection("jdbc:h2:mem:pg_demo;DB_CLOSE_DELAY=-1", "sa", "").use { conn ->
            createSchema(conn)
            seed(conn)
            query(conn)
        }
    }

    private fun createSchema(conn: Connection) {
        conn.createStatement().use { st: Statement ->
            st.execute("""
                CREATE TABLE author (
                    id         BIGINT         AUTO_INCREMENT PRIMARY KEY,
                    name       VARCHAR(255)   NOT NULL,
                    bio        VARCHAR(1024),
                    birth_year INTEGER
                )""".trimIndent())
            st.execute("""
                CREATE TABLE book (
                    id           BIGINT         AUTO_INCREMENT PRIMARY KEY,
                    author_id    BIGINT         NOT NULL,
                    title        VARCHAR(255)   NOT NULL,
                    genre        VARCHAR(100)   NOT NULL,
                    price        NUMERIC(10, 2) NOT NULL,
                    published_at DATE,
                    FOREIGN KEY (author_id) REFERENCES author(id)
                )""".trimIndent())
            st.execute("""
                CREATE TABLE customer (
                    id    BIGINT       AUTO_INCREMENT PRIMARY KEY,
                    name  VARCHAR(255) NOT NULL,
                    email VARCHAR(255) NOT NULL UNIQUE
                )""".trimIndent())
            st.execute("""
                CREATE TABLE sale (
                    id          BIGINT    AUTO_INCREMENT PRIMARY KEY,
                    customer_id BIGINT    NOT NULL,
                    ordered_at  TIMESTAMP NOT NULL DEFAULT NOW(),
                    FOREIGN KEY (customer_id) REFERENCES customer(id)
                )""".trimIndent())
            st.execute("""
                CREATE TABLE sale_item (
                    id         BIGINT         AUTO_INCREMENT PRIMARY KEY,
                    sale_id    BIGINT         NOT NULL,
                    book_id    BIGINT         NOT NULL,
                    quantity   INTEGER        NOT NULL,
                    unit_price NUMERIC(10, 2) NOT NULL,
                    FOREIGN KEY (sale_id) REFERENCES sale(id),
                    FOREIGN KEY (book_id) REFERENCES book(id)
                )""".trimIndent())
        }
    }

    private fun seed(conn: Connection) {
        Queries.createAuthor(conn, "Ursula K. Le Guin", "Science fiction and fantasy author", 1929)
        Queries.createAuthor(conn, "Frank Herbert",     "Author of the Dune series",           1920)
        Queries.createAuthor(conn, "Isaac Asimov",      null,                                  1920)
        println("[pg] inserted 3 authors")

        Queries.createBook(conn, 1L, "The Left Hand of Darkness", "sci-fi", BigDecimal("12.99"), null)
        Queries.createBook(conn, 1L, "The Dispossessed",           "sci-fi", BigDecimal("11.50"), null)
        Queries.createBook(conn, 2L, "Dune",                       "sci-fi", BigDecimal("14.99"), null)
        Queries.createBook(conn, 3L, "Foundation",                 "sci-fi", BigDecimal("10.99"), null)
        Queries.createBook(conn, 3L, "The Caves of Steel",         "sci-fi", BigDecimal("9.99"),  null)
        println("[pg] inserted 5 books")

        Queries.createCustomer(conn, "Alice", "alice@example.com")
        Queries.createCustomer(conn, "Bob",   "bob@example.com")
        println("[pg] inserted 2 customers")

        Queries.createSale(conn, 1L)
        Queries.addSaleItem(conn, 1L, 3L, 2, BigDecimal("14.99"))
        Queries.addSaleItem(conn, 1L, 4L, 1, BigDecimal("10.99"))
        Queries.createSale(conn, 2L)
        Queries.addSaleItem(conn, 2L, 3L, 1, BigDecimal("14.99"))
        Queries.addSaleItem(conn, 2L, 1L, 1, BigDecimal("12.99"))
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
    }
}
