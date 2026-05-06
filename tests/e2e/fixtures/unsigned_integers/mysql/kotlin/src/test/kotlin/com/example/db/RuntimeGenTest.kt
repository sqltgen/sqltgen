package com.example.db

import com.example.db.models.UnsignedValues
import com.example.db.queries.Queries
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigInteger
import java.sql.Connection
import java.sql.DriverManager
import java.util.UUID

class RuntimeGenTest {
    private val rootUrl = System.getenv()
        .getOrDefault("MYSQL_ROOT_URL", "jdbc:mysql://localhost:13306/sqltgen_e2e")
    private val testBaseUrl = System.getenv()
        .getOrDefault("DATABASE_URL", "jdbc:mysql://localhost:13306/")

    private lateinit var conn: Connection
    private lateinit var dbName: String

    @BeforeEach
    fun setUp() {
        dbName = "test_" + UUID.randomUUID().toString().replace("-", "")
        DriverManager.getConnection(rootUrl, "root", "sqltgen").use { admin ->
            admin.createStatement().use { s ->
                s.execute("CREATE DATABASE `$dbName`")
                s.execute("GRANT ALL ON `$dbName`.* TO 'sqltgen'@'%'")
            }
        }
        conn = DriverManager.getConnection(
            "${testBaseUrl}${dbName}?useSSL=false&allowPublicKeyRetrieval=true",
            "sqltgen", "sqltgen"
        )
        conn.autoCommit = true
        val ddl = java.nio.file.Files.readString(java.nio.file.Path.of("../schema.sql"))
        conn.createStatement().use { s ->
            for (stmt in ddl.split(";")) {
                val t = stmt.trim()
                if (t.isNotEmpty()) s.execute(t)
            }
        }
    }

    @AfterEach
    fun tearDown() {
        conn.close()
        DriverManager.getConnection(rootUrl, "root", "sqltgen").use { admin ->
            admin.createStatement().use { s ->
                s.execute("DROP DATABASE IF EXISTS `$dbName`")
            }
        }
    }

    @Test
    fun unsignedIntegersRoundTripThroughFullRange() {
        // Row 1: zero. Row 2: small. Row 3: each column at its maximum unsigned value.
        Queries.insertUnsignedRow(conn, 0.toShort(), 0, 0L, 0L, BigInteger.ZERO)
        Queries.insertUnsignedRow(conn, 1.toShort(), 1, 1L, 1L, BigInteger.ONE)
        // BIGINT UNSIGNED max = 2^64 - 1 = 18446744073709551615 — exceeds Long.
        val u64Max = BigInteger("18446744073709551615")
        Queries.insertUnsignedRow(conn, 255.toShort(), 65_535, 16_777_215L, 4_294_967_295L, u64Max)

        val rows: List<UnsignedValues> = Queries.getUnsignedRows(conn)
        assertEquals(3, rows.size)

        val r0 = rows[0]
        assertEquals(0.toShort(), r0.u8Val)
        assertEquals(0, r0.u16Val)
        assertEquals(0L, r0.u24Val)
        assertEquals(0L, r0.u32Val)
        assertEquals(BigInteger.ZERO, r0.u64Val)

        val r1 = rows[1]
        assertEquals(1.toShort(), r1.u8Val)
        assertEquals(1, r1.u16Val)
        assertEquals(1L, r1.u24Val)
        assertEquals(1L, r1.u32Val)
        assertEquals(BigInteger.ONE, r1.u64Val)

        val r2 = rows[2]
        assertEquals(255.toShort(), r2.u8Val)
        assertEquals(65_535, r2.u16Val)
        assertEquals(16_777_215L, r2.u24Val)
        assertEquals(4_294_967_295L, r2.u32Val)
        // The critical correctness gate: 2^64-1 must round-trip without truncation.
        assertEquals(u64Max, r2.u64Val)

        // The id column itself is BIGINT UNSIGNED.
        assertEquals(BigInteger.ONE, r0.id)
        assertEquals(BigInteger.valueOf(3), r2.id)
    }
}
