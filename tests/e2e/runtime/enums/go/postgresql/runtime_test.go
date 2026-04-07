package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
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

// setupDB creates an isolated schema, applies the DDL, and returns a connected *sql.DB.
// The schema is dropped when the test completes.
func setupDB(t *testing.T) (*sql.DB, context.Context) {
	t.Helper()
	ctx := context.Background()

	db, err := sql.Open("pgx", dsn())
	if err != nil {
		t.Fatal(err)
	}

	schema := fmt.Sprintf("test_%d", rand.Int63())
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE SCHEMA "%s"`, schema)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`SET search_path TO "%s"`, schema)); err != nil {
		t.Fatal(err)
	}

	ddl, err := os.ReadFile("../../../../fixtures/enums/postgresql/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, string(ddl)); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		db.ExecContext(ctx, fmt.Sprintf(`DROP SCHEMA IF EXISTS "%s" CASCADE`, schema))
		db.Close()
	})

	return db, ctx
}
