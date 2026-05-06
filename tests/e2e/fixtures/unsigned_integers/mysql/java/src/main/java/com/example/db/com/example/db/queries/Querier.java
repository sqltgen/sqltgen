package com.example.db.queries;

import com.example.db.models.UnsignedValues;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;

public final class Querier {
    private final DataSource dataSource;

    public Querier(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void insertUnsignedRow(short u8Val, int u16Val, long u24Val, long u32Val, java.math.BigInteger u64Val) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            Queries.insertUnsignedRow(conn, u8Val, u16Val, u24Val, u32Val, u64Val);
        }
    }

    public List<UnsignedValues> getUnsignedRows() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            return Queries.getUnsignedRows(conn);
        }
    }
}
