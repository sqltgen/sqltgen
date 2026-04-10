package db.queries

import db.models.Internal_AuditLog
import db.models.Users
import javax.sql.DataSource

class Querier(private val dataSource: DataSource) {

    fun getUser(id: Long): Users? =
        dataSource.connection.use { conn -> Queries.getUser(conn, id) }

    fun listAuditLogs(): List<Internal_AuditLog> =
        dataSource.connection.use { conn -> Queries.listAuditLogs(conn) }

    fun createAuditLog(userId: Long, action: String): Unit =
        dataSource.connection.use { conn -> Queries.createAuditLog(conn, userId, action) }
}
