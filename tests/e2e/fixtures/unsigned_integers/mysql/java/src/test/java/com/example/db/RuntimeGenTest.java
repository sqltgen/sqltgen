package com.example.db;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import com.example.db.queries.Queries;
import com.example.db.models.UnsignedValues;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RuntimeGenTest {

    private static final String ROOT_URL =
        System.getenv().getOrDefault("MYSQL_ROOT_URL",
            "jdbc:mysql://localhost:13306/sqltgen_e2e");
    private static final String TEST_BASE_URL =
        System.getenv().getOrDefault("DATABASE_URL", "jdbc:mysql://localhost:13306/");
    private static final String USER = "sqltgen";
    private static final String PASS = "sqltgen";
    private static final String ROOT_USER = "root";
    private static final String ROOT_PASS = "sqltgen";

    private Connection conn;
    private String dbName;

    @BeforeEach
    void setUp() throws Exception {
        dbName = "test_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection admin = DriverManager.getConnection(ROOT_URL, ROOT_USER, ROOT_PASS);
             Statement s = admin.createStatement()) {
            s.execute("CREATE DATABASE `" + dbName + "`");
            s.execute("GRANT ALL ON `" + dbName + "`.* TO 'sqltgen'@'%'");
        }
        conn = DriverManager.getConnection(
            TEST_BASE_URL + dbName + "?useSSL=false&allowPublicKeyRetrieval=true", USER, PASS);
        conn.setAutoCommit(true);
        String ddl = java.nio.file.Files.readString(java.nio.file.Path.of("../schema.sql"));
        try (Statement s = conn.createStatement()) {
            for (String stmt : ddl.split(";")) {
                String t = stmt.strip();
                if (!t.isEmpty()) s.execute(t);
            }
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (conn != null) conn.close();
        try (Connection admin = DriverManager.getConnection(ROOT_URL, ROOT_USER, ROOT_PASS);
             Statement s = admin.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS `" + dbName + "`");
        }
    }

    @Test
    void unsignedIntegersRoundTripThroughFullRange() throws Exception {
        // Row 1: zero. Row 2: small. Row 3: each column at its maximum unsigned value.
        Queries.insertUnsignedRow(conn, (short) 0, 0, 0L, 0L, BigInteger.ZERO);
        Queries.insertUnsignedRow(conn, (short) 1, 1, 1L, 1L, BigInteger.ONE);
        // BIGINT UNSIGNED max = 2^64 - 1 = 18446744073709551615 — exceeds Java long.
        BigInteger u64Max = new BigInteger("18446744073709551615");
        Queries.insertUnsignedRow(conn, (short) 255, 65_535, 16_777_215L, 4_294_967_295L, u64Max);

        List<UnsignedValues> rows = Queries.getUnsignedRows(conn);
        assertEquals(3, rows.size());

        UnsignedValues r0 = rows.get(0);
        assertEquals((short) 0, r0.u8Val());
        assertEquals(0, r0.u16Val());
        assertEquals(0L, r0.u24Val());
        assertEquals(0L, r0.u32Val());
        assertEquals(BigInteger.ZERO, r0.u64Val());

        UnsignedValues r1 = rows.get(1);
        assertEquals((short) 1, r1.u8Val());
        assertEquals(1, r1.u16Val());
        assertEquals(1L, r1.u24Val());
        assertEquals(1L, r1.u32Val());
        assertEquals(BigInteger.ONE, r1.u64Val());

        UnsignedValues r2 = rows.get(2);
        assertEquals((short) 255, r2.u8Val());
        assertEquals(65_535, r2.u16Val());
        assertEquals(16_777_215L, r2.u24Val());
        assertEquals(4_294_967_295L, r2.u32Val());
        // The critical correctness gate: 2^64-1 must round-trip without truncation.
        assertEquals(u64Max, r2.u64Val());

        // The id column itself is BIGINT UNSIGNED and round-trips as BigInteger.
        assertEquals(BigInteger.ONE, r0.id());
        assertEquals(BigInteger.valueOf(3), r2.id());
    }
}
