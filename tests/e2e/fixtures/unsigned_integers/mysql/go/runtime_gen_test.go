package main

import (
	"context"
	crand "crypto/rand"
	"database/sql"
	"encoding/hex"
	"math"
	"os"
	"strings"
	"testing"

	gen "e2e-unsigned-integers-go-mysql/gen"

	_ "github.com/go-sql-driver/mysql"
)

func mysqlDSN() string {
	if v := os.Getenv("MYSQL_DSN"); v != "" {
		return v
	}
	return "sqltgen:sqltgen@tcp(localhost:13306)/sqltgen_e2e?parseTime=true"
}

func mysqlAdminDSN() string {
	d := mysqlDSN()
	if idx := strings.LastIndex(d, "/"); idx != -1 {
		rest := d[idx+1:]
		if qIdx := strings.Index(rest, "?"); qIdx != -1 {
			return d[:idx+1] + rest[qIdx:]
		}
		return d[:idx+1]
	}
	return d
}

func randomDBName() string {
	b := make([]byte, 8)
	_, _ = crand.Read(b)
	return "test_" + hex.EncodeToString(b)
}

func setupDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	admin, err := sql.Open("mysql", mysqlAdminDSN())
	if err != nil {
		t.Fatal(err)
	}

	dbName := randomDBName()
	if _, err := admin.Exec("CREATE DATABASE " + dbName); err != nil {
		admin.Close()
		t.Fatal(err)
	}

	d := mysqlDSN()
	var testDSN string
	if idx := strings.LastIndex(d, "/"); idx != -1 {
		rest := d[idx+1:]
		if qIdx := strings.Index(rest, "?"); qIdx != -1 {
			testDSN = d[:idx+1] + dbName + rest[qIdx:]
		} else {
			testDSN = d[:idx+1] + dbName
		}
	} else {
		testDSN = d + "/" + dbName
	}

	db, err := sql.Open("mysql", testDSN)
	if err != nil {
		admin.Exec("DROP DATABASE IF EXISTS " + dbName)
		admin.Close()
		t.Fatal(err)
	}

	ddl, err := os.ReadFile("../schema.sql")
	if err != nil {
		db.Close()
		admin.Exec("DROP DATABASE IF EXISTS " + dbName)
		admin.Close()
		t.Fatal(err)
	}
	for _, stmt := range strings.Split(string(ddl), ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			admin.Exec("DROP DATABASE IF EXISTS " + dbName)
			admin.Close()
			t.Fatal(err)
		}
	}

	return db, func() {
		db.Close()
		admin.Exec("DROP DATABASE IF EXISTS " + dbName)
		admin.Close()
	}
}

func TestUnsignedIntegersRoundTripThroughFullRange(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	if err := gen.InsertUnsignedRow(ctx, db, 0, 0, 0, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := gen.InsertUnsignedRow(ctx, db, 1, 1, 1, 1, 1); err != nil {
		t.Fatal(err)
	}
	// BIGINT UNSIGNED max = 2^64 - 1 = math.MaxUint64.
	if err := gen.InsertUnsignedRow(ctx, db, math.MaxUint8, math.MaxUint16, 16_777_215, math.MaxUint32, math.MaxUint64); err != nil {
		t.Fatal(err)
	}

	rows, err := gen.GetUnsignedRows(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	for i, want := range [...]struct {
		u8  uint8
		u16 uint16
		u24 uint32
		u32 uint32
		u64 uint64
	}{
		{0, 0, 0, 0, 0},
		{1, 1, 1, 1, 1},
		{math.MaxUint8, math.MaxUint16, 16_777_215, math.MaxUint32, math.MaxUint64},
	} {
		r := rows[i]
		if r.U8Val != want.u8 || r.U16Val != want.u16 || r.U24Val != want.u24 || r.U32Val != want.u32 || r.U64Val != want.u64 {
			t.Errorf("row %d mismatch: got (%d, %d, %d, %d, %d), want (%d, %d, %d, %d, %d)",
				i, r.U8Val, r.U16Val, r.U24Val, r.U32Val, r.U64Val,
				want.u8, want.u16, want.u24, want.u32, want.u64)
		}
	}

	// The id column itself is BIGINT UNSIGNED.
	if rows[0].Id != 1 || rows[2].Id != 3 {
		t.Errorf("Id round-trip failed: got %d/%d, want 1/3", rows[0].Id, rows[2].Id)
	}
}
