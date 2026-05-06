package com.example.db.queries

import java.sql.Connection
import com.example.db.models.UnsignedValues

object Queries {

    private val SQL_INSERT_UNSIGNED_ROW = """
        INSERT INTO unsigned_values (u8_val, u16_val, u24_val, u32_val, u64_val)
        VALUES (?, ?, ?, ?, ?);
    """.trimIndent()
    fun insertUnsignedRow(conn: Connection, u8Val: Short, u16Val: Int, u24Val: Long, u32Val: Long, u64Val: java.math.BigInteger): Unit {
        conn.prepareStatement(SQL_INSERT_UNSIGNED_ROW).use { ps ->
            ps.setShort(1, u8Val)
            ps.setInt(2, u16Val)
            ps.setLong(3, u24Val)
            ps.setLong(4, u32Val)
            ps.setBigDecimal(5, java.math.BigDecimal(u64Val))
            ps.executeUpdate()
        }
    }

    private val SQL_GET_UNSIGNED_ROWS = """
        SELECT id, u8_val, u16_val, u24_val, u32_val, u64_val
        FROM unsigned_values
        ORDER BY id;
    """.trimIndent()
    fun getUnsignedRows(conn: Connection): List<UnsignedValues> {
        conn.prepareStatement(SQL_GET_UNSIGNED_ROWS).use { ps ->
            val rows = mutableListOf<UnsignedValues>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(UnsignedValues(rs.getObject(1, java.math.BigInteger::class.java), rs.getShort(2), rs.getInt(3), rs.getLong(4), rs.getLong(5), rs.getObject(6, java.math.BigInteger::class.java)))
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
