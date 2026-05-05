package com.example.db.queries

import java.sql.Connection
import com.example.db.models.Author
import com.example.db.models.Book

object Queries {

    private val SQL_CREATE_AUTHOR = """
        INSERT INTO author (name, bio, birth_year)
        VALUES (?, ?, ?)
        RETURNING *;
    """.trimIndent()
    fun createAuthor(conn: Connection, name: String, bio: String?, birthYear: Int?): Author? {
        conn.prepareStatement(SQL_CREATE_AUTHOR).use { ps ->
            ps.setString(1, name)
            ps.setObject(2, bio)
            ps.setObject(3, birthYear)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Author(rs.getLong(1), rs.getString(2), rs.getString(3), getNullableInt(rs, 4))
            }
        }
    }

    private val SQL_GET_AUTHOR = """
        SELECT id, name, bio, birth_year
        FROM author
        WHERE id = ?;
    """.trimIndent()
    fun getAuthor(conn: Connection, id: Long): Author? {
        conn.prepareStatement(SQL_GET_AUTHOR).use { ps ->
            ps.setLong(1, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Author(rs.getLong(1), rs.getString(2), rs.getString(3), getNullableInt(rs, 4))
            }
        }
    }

    private val SQL_UPDATE_AUTHOR_BIO = """
        UPDATE author SET bio = ? WHERE id = ?
        RETURNING *;
    """.trimIndent()
    fun updateAuthorBio(conn: Connection, bio: String?, id: Long): Author? {
        conn.prepareStatement(SQL_UPDATE_AUTHOR_BIO).use { ps ->
            ps.setObject(1, bio)
            ps.setLong(2, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Author(rs.getLong(1), rs.getString(2), rs.getString(3), getNullableInt(rs, 4))
            }
        }
    }

    data class DeleteAuthorRow(
        val id: Long,
        val name: String
    )

    private val SQL_DELETE_AUTHOR = """
        DELETE FROM author WHERE id = ?
        RETURNING id, name;
    """.trimIndent()
    fun deleteAuthor(conn: Connection, id: Long): DeleteAuthorRow? {
        conn.prepareStatement(SQL_DELETE_AUTHOR).use { ps ->
            ps.setLong(1, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return DeleteAuthorRow(rs.getLong(1), rs.getString(2))
            }
        }
    }

    private val SQL_CREATE_BOOK = """
        INSERT INTO book (author_id, title, genre, price, published_at)
        VALUES (?, ?, ?, ?, ?)
        RETURNING *;
    """.trimIndent()
    fun createBook(conn: Connection, authorId: Long, title: String, genre: String, price: java.math.BigDecimal, publishedAt: java.time.LocalDate?): Book? {
        conn.prepareStatement(SQL_CREATE_BOOK).use { ps ->
            ps.setLong(1, authorId)
            ps.setString(2, title)
            ps.setString(3, genre)
            ps.setBigDecimal(4, price)
            ps.setObject(5, publishedAt)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Book(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getString(4), rs.getBigDecimal(5), rs.getObject(6, java.time.LocalDate::class.java))
            }
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
