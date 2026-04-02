package com.example.db

import java.sql.Connection
import java.time.LocalDateTime
import java.util.UUID

object Queries {

    private val SQL_INSERT_RECORD = """
        INSERT INTO record (label, timestamps, uuids)
        VALUES (?, ?, ?);
    """.trimIndent()
    fun insertRecord(conn: Connection, label: String, timestamps: List<LocalDateTime>, uuids: List<UUID>): Unit {
        conn.prepareStatement(SQL_INSERT_RECORD).use { ps ->
            ps.setString(1, label)
            ps.setArray(2, conn.createArrayOf("timestamp", timestamps.toTypedArray()))
            ps.setArray(3, conn.createArrayOf("uuid", uuids.toTypedArray()))
            ps.executeUpdate()
        }
    }

    private val SQL_GET_RECORD = """
        SELECT id, label, timestamps, uuids
        FROM record
        WHERE id = ?;
    """.trimIndent()
    fun getRecord(conn: Connection, id: Long): Record? {
        conn.prepareStatement(SQL_GET_RECORD).use { ps ->
            ps.setLong(1, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Record(rs.getLong(1), rs.getString(2), (rs.getArray(3).array as Array<*>).map { (it as java.sql.Timestamp).toLocalDateTime() }.toList(), (rs.getArray(4).array as Array<*>).map { (it as java.util.UUID) }.toList())
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
