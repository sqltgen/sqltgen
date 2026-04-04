package com.example.db.queries

import com.example.db.models.Record
import java.time.LocalDateTime
import java.util.UUID
import javax.sql.DataSource

class Querier(private val dataSource: DataSource) {

    fun insertRecord(label: String, timestamps: List<LocalDateTime>, uuids: List<UUID>): Unit =
        dataSource.connection.use { conn -> Queries.insertRecord(conn, label, timestamps, uuids) }

    fun getRecord(id: Long): Record? =
        dataSource.connection.use { conn -> Queries.getRecord(conn, id) }
}
