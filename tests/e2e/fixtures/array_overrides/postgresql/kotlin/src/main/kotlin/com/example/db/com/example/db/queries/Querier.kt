package com.example.db.queries

import com.example.db.models.Record
import javax.sql.DataSource

class Querier(private val dataSource: DataSource) {

    fun insertRecord(label: String, timestamps: List<java.time.LocalDateTime>, uuids: List<java.util.UUID>): Unit =
        dataSource.connection.use { conn -> Queries.insertRecord(conn, label, timestamps, uuids) }

    fun getRecord(id: Long): Record? =
        dataSource.connection.use { conn -> Queries.getRecord(conn, id) }
}
