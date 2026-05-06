package com.example.db.queries

import com.example.db.models.UnsignedValues
import javax.sql.DataSource

class Querier(private val dataSource: DataSource) {

    fun insertUnsignedRow(u8Val: Short, u16Val: Int, u24Val: Long, u32Val: Long, u64Val: java.math.BigInteger): Unit =
        dataSource.connection.use { conn -> Queries.insertUnsignedRow(conn, u8Val, u16Val, u24Val, u32Val, u64Val) }

    fun getUnsignedRows(): List<UnsignedValues> =
        dataSource.connection.use { conn -> Queries.getUnsignedRows(conn) }
}
