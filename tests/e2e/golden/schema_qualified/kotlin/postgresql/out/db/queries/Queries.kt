package db.queries

import java.sql.Connection
import db.models.Internal_AuditLog
import db.models.Users

object Queries {

    private val SQL_GET_USER = """
        SELECT * FROM public.users WHERE id = ?;
    """.trimIndent()
    fun getUser(conn: Connection, id: Long): Users? {
        conn.prepareStatement(SQL_GET_USER).use { ps ->
            ps.setLong(1, id)
            ps.executeQuery().use { rs ->
                if (!rs.next()) return null
                return Users(rs.getLong(1), rs.getString(2), rs.getString(3))
            }
        }
    }

    private val SQL_LIST_AUDIT_LOGS = """
        SELECT * FROM internal.audit_log ORDER BY created_at DESC;
    """.trimIndent()
    fun listAuditLogs(conn: Connection): List<Internal_AuditLog> {
        conn.prepareStatement(SQL_LIST_AUDIT_LOGS).use { ps ->
            val rows = mutableListOf<Internal_AuditLog>()
            ps.executeQuery().use { rs ->
                while (rs.next()) rows.add(Internal_AuditLog(rs.getLong(1), rs.getLong(2), rs.getString(3), rs.getObject(4, java.time.LocalDateTime::class.java)))
            }
            return rows
        }
    }

    private val SQL_CREATE_AUDIT_LOG = """
        INSERT INTO internal.audit_log (user_id, action) VALUES (?, ?);
    """.trimIndent()
    fun createAuditLog(conn: Connection, userId: Long, action: String): Unit {
        conn.prepareStatement(SQL_CREATE_AUDIT_LOG).use { ps ->
            ps.setLong(1, userId)
            ps.setString(2, action)
            ps.executeUpdate()
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
