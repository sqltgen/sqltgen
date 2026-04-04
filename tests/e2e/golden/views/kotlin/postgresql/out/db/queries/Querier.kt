package db.queries

import javax.sql.DataSource

class Querier(private val dataSource: DataSource) {

    fun listBookSummaries(): List<BookSummaries> =
        dataSource.connection.use { conn -> Queries.listBookSummaries(conn) }

    fun listBookSummariesByGenre(genre: String): List<BookSummaries> =
        dataSource.connection.use { conn -> Queries.listBookSummariesByGenre(conn, genre) }

    fun listSciFiBooks(): List<SciFiBooks> =
        dataSource.connection.use { conn -> Queries.listSciFiBooks(conn) }
}
