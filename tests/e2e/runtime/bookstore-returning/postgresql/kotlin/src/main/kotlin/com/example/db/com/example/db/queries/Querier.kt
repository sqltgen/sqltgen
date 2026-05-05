package com.example.db.queries

import com.example.db.models.Author
import com.example.db.models.Book
import javax.sql.DataSource

class Querier(private val dataSource: DataSource) {

    fun createAuthor(name: String, bio: String?, birthYear: Int?): Author? =
        dataSource.connection.use { conn -> Queries.createAuthor(conn, name, bio, birthYear) }

    fun getAuthor(id: Long): Author? =
        dataSource.connection.use { conn -> Queries.getAuthor(conn, id) }

    fun updateAuthorBio(bio: String?, id: Long): Author? =
        dataSource.connection.use { conn -> Queries.updateAuthorBio(conn, bio, id) }

    fun deleteAuthor(id: Long): Queries.DeleteAuthorRow? =
        dataSource.connection.use { conn -> Queries.deleteAuthor(conn, id) }

    fun createBook(authorId: Long, title: String, genre: String, price: java.math.BigDecimal, publishedAt: java.time.LocalDate?): Book? =
        dataSource.connection.use { conn -> Queries.createBook(conn, authorId, title, genre, price, publishedAt) }
}
