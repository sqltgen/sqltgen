package db

import javax.sql.DataSource

class QueriesDs(private val dataSource: DataSource) {

    fun getEvent(id: Long): Event? =
        dataSource.connection.use { conn -> Queries.getEvent(conn, id) }

    fun listEvents(): List<Event> =
        dataSource.connection.use { conn -> Queries.listEvents(conn) }

    fun insertEvent(name: String, payload: String, meta: String?, docId: java.util.UUID, createdAt: java.time.LocalDateTime, scheduledAt: java.time.OffsetDateTime?, eventDate: java.time.LocalDate?, eventTime: java.time.LocalTime?): Unit =
        dataSource.connection.use { conn -> Queries.insertEvent(conn, name, payload, meta, docId, createdAt, scheduledAt, eventDate, eventTime) }

    fun updatePayload(payload: String, meta: String?, id: Long): Unit =
        dataSource.connection.use { conn -> Queries.updatePayload(conn, payload, meta, id) }

    fun findByDate(eventDate: java.time.LocalDate?): Queries.FindByDateRow? =
        dataSource.connection.use { conn -> Queries.findByDate(conn, eventDate) }

    fun findByUuid(docId: java.util.UUID): Queries.FindByUuidRow? =
        dataSource.connection.use { conn -> Queries.findByUuid(conn, docId) }
}
