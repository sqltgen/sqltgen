package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
)

const defaultDSN = "postgres://sqltgen:sqltgen@localhost:15432/sqltgen_e2e"

func dsn() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return defaultDSN
}

// setupDB creates an isolated database, applies the DDL, and returns a connected *sql.DB
// plus a cleanup function that drops the database.
func setupDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()
	ctx := context.Background()

	dbName := fmt.Sprintf("test_%d", rand.Int63())
	admin, err := sql.Open("pgx", dsn())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE "%s"`, dbName)); err != nil {
		t.Fatal(err)
	}
	admin.Close()

	dbURL := replaceLastSegment(dsn(), dbName)
	db, err := sql.Open("pgx", dbURL)
	if err != nil {
		t.Fatal(err)
	}

	ddl, err := os.ReadFile("../../../../fixtures/type_overrides/postgresql/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range splitStatements(string(ddl)) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}

	cleanup := func() {
		db.Close()
		adm, _ := sql.Open("pgx", dsn())
		adm.ExecContext(ctx, fmt.Sprintf(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s' AND pid <> pg_backend_pid()`, dbName))
		adm.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS "%s"`, dbName))
		adm.Close()
	}

	return db, cleanup
}

func replaceLastSegment(url, replacement string) string {
	i := strings.LastIndex(url, "/")
	if i < 0 {
		return url
	}
	return url[:i+1] + replacement
}

func splitStatements(ddl string) []string {
	var stmts []string
	for _, s := range strings.Split(ddl, ";") {
		s = strings.TrimSpace(s)
		if s != "" {
			stmts = append(stmts, s)
		}
	}
	return stmts
}

func mustJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func ptrBytes(b []byte) *[]byte {
	return &b
}

func testUUID() string {
	return fmt.Sprintf("%08x-0000-4000-8000-%012x", rand.Int63()&0xFFFFFFFF, rand.Int63()&0xFFFFFFFFFFFF)
}

var nilMeta *[]byte
