package db.queries

import java.sql.Connection
import db.models.BookSummaries
import db.models.SciFiBooks

object Queries {

    private val SQL_LIST_BOOK_SUMMARIES = """
        SELECT id, title, genre, author_name
        FROM book_summaries
        ORDER BY title;
    """.trimIndent()
    fun listBookSummaries(conn: Connection): List<BookSummaries> {
        conn.prepareStatement(SQL_LIST_BOOK_SUMMARIES).use { ps ->
            val rows = mutableListOf<BookSummaries>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(BookSummaries(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4)))
            }
            return rows
        }
    }

    private val SQL_LIST_BOOK_SUMMARIES_BY_GENRE = """
        SELECT id, title, genre, author_name
        FROM book_summaries
        WHERE genre = ?
        ORDER BY title;
    """.trimIndent()
    fun listBookSummariesByGenre(conn: Connection, genre: String): List<BookSummaries> {
        conn.prepareStatement(SQL_LIST_BOOK_SUMMARIES_BY_GENRE).use { ps ->
            ps.setString(1, genre)
            val rows = mutableListOf<BookSummaries>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(BookSummaries(rs.getLong(1), rs.getString(2), rs.getString(3), rs.getString(4)))
            }
            return rows
        }
    }

    private val SQL_LIST_SCI_FI_BOOKS = """
        SELECT id, title, author_name
        FROM sci_fi_books
        ORDER BY title;
    """.trimIndent()
    fun listSciFiBooks(conn: Connection): List<SciFiBooks> {
        conn.prepareStatement(SQL_LIST_SCI_FI_BOOKS).use { ps ->
            val rows = mutableListOf<SciFiBooks>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(SciFiBooks(rs.getLong(1), rs.getString(2), rs.getString(3)))
            }
            return rows
        }
    }

    private fun getNullableBoolean(rs: java.sql.ResultSet, col: Int): Boolean? {
        val v = rs.getBoolean(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableShort(rs: java.sql.ResultSet, col: Int): Short? {
        val v = rs.getShort(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableInt(rs: java.sql.ResultSet, col: Int): Int? {
        val v = rs.getInt(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableLong(rs: java.sql.ResultSet, col: Int): Long? {
        val v = rs.getLong(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableFloat(rs: java.sql.ResultSet, col: Int): Float? {
        val v = rs.getFloat(col)
        return if (rs.wasNull()) null else v
    }
    private fun getNullableDouble(rs: java.sql.ResultSet, col: Int): Double? {
        val v = rs.getDouble(col)
        return if (rs.wasNull()) null else v
    }

    @Suppress("UNCHECKED_CAST")
    private fun <T> jdbcArrayToList(arr: java.sql.Array): List<T> =
        (arr.array as Array<T>).toList()
}
