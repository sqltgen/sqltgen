package com.example.db.queries

import javax.sql.DataSource

class Querier(private val dataSource: DataSource) {

    fun createAuthor(name: String, bio: String?, birthYear: Int?): Unit =
        dataSource.connection.use { conn -> Queries.createAuthor(conn, name, bio, birthYear) }

    fun getAuthor(id: Int): Author? =
        dataSource.connection.use { conn -> Queries.getAuthor(conn, id) }

    fun listAuthors(): List<Author> =
        dataSource.connection.use { conn -> Queries.listAuthors(conn) }

    fun createBook(authorId: Int, title: String, genre: String, price: Double, publishedAt: String?): Unit =
        dataSource.connection.use { conn -> Queries.createBook(conn, authorId, title, genre, price, publishedAt) }

    fun getBook(id: Int): Book? =
        dataSource.connection.use { conn -> Queries.getBook(conn, id) }

    fun getBooksByIds(ids: List<Long>): List<Book> =
        dataSource.connection.use { conn -> Queries.getBooksByIds(conn, ids) }

    fun listBooksByGenre(genre: String): List<Book> =
        dataSource.connection.use { conn -> Queries.listBooksByGenre(conn, genre) }

    fun listBooksByGenreOrAll(genre: String): List<Book> =
        dataSource.connection.use { conn -> Queries.listBooksByGenreOrAll(conn, genre) }

    fun createCustomer(name: String, email: String): Unit =
        dataSource.connection.use { conn -> Queries.createCustomer(conn, name, email) }

    fun createSale(customerId: Int): Unit =
        dataSource.connection.use { conn -> Queries.createSale(conn, customerId) }

    fun addSaleItem(saleId: Int, bookId: Int, quantity: Int, unitPrice: Double): Unit =
        dataSource.connection.use { conn -> Queries.addSaleItem(conn, saleId, bookId, quantity, unitPrice) }

    fun listBooksWithAuthor(): List<Queries.ListBooksWithAuthorRow> =
        dataSource.connection.use { conn -> Queries.listBooksWithAuthor(conn) }

    fun getBooksNeverOrdered(): List<Book> =
        dataSource.connection.use { conn -> Queries.getBooksNeverOrdered(conn) }

    fun getTopSellingBooks(): List<Queries.GetTopSellingBooksRow> =
        dataSource.connection.use { conn -> Queries.getTopSellingBooks(conn) }

    fun getBestCustomers(): List<Queries.GetBestCustomersRow> =
        dataSource.connection.use { conn -> Queries.getBestCustomers(conn) }
}
