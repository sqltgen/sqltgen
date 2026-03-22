package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	gen "e2e-type-overrides-go-mysql/gen"
	_ "github.com/go-sql-driver/mysql"
)

const defaultDSN = "sqltgen:sqltgen@tcp(localhost:13306)/sqltgen_e2e?parseTime=true"

func dsn() string {
	if v := os.Getenv("MYSQL_DSN"); v != "" {
		return v
	}
	return defaultDSN
}

func adminDSN() string {
	d := dsn()
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
	_, _ = rand.Read(b)
	return "test_" + hex.EncodeToString(b)
}

func setupDB(t *testing.T) (*sql.DB, func()) {
	t.Helper()

	admin, err := sql.Open("mysql", adminDSN())
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}

	dbName := randomDBName()
	if _, err := admin.Exec("CREATE DATABASE " + dbName); err != nil {
		admin.Close()
		t.Fatalf("create database %s: %v", dbName, err)
	}

	d := dsn()
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
		t.Fatalf("open test database: %v", err)
	}

	ddl, err := os.ReadFile("../../../../fixtures/type_overrides/mysql/schema.sql")
	if err != nil {
		db.Close()
		admin.Exec("DROP DATABASE IF EXISTS " + dbName)
		admin.Close()
		t.Fatalf("read schema: %v", err)
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
			t.Fatalf("apply schema statement: %v", err)
		}
	}

	cleanup := func() {
		db.Close()
		admin.Exec("DROP DATABASE IF EXISTS " + dbName)
		admin.Close()
	}
	return db, cleanup
}

func mustJSON(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func ts(year, month, day, hour, min, sec int) time.Time {
	return time.Date(year, time.Month(month), day, hour, min, sec, 0, time.UTC)
}

var nilTime sql.NullTime

// ─── :one tests ───────────────────────────────────────────────────────────────

func TestInsertAndGetEvent(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	payload := mustJSON(map[string]interface{}{"type": "click", "x": 10})
	meta := mustJSON(map[string]interface{}{"source": "web"})

	if err := gen.InsertEvent(ctx, db, "login", payload, sql.NullString{String: meta, Valid: true},
		"doc-001", ts(2024, 6, 1, 12, 0, 0), nilTime,
		sql.NullTime{Time: ts(2024, 6, 1, 0, 0, 0), Valid: true},
		nilTime); err != nil {
		t.Fatal(err)
	}

	ev, err := gen.GetEvent(ctx, db, 1)
	if err != nil {
		t.Fatal(err)
	}
	if ev == nil {
		t.Fatal("expected event, got nil")
	}
	if ev.Name != "login" {
		t.Errorf("expected name=login, got %s", ev.Name)
	}
	if ev.DocId != "doc-001" {
		t.Errorf("expected doc_id=doc-001, got %s", ev.DocId)
	}
}

func TestGetEventNotFound(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	ev, err := gen.GetEvent(ctx, db, 999)
	if err != nil {
		t.Fatal(err)
	}
	if ev != nil {
		t.Errorf("expected nil, got %+v", ev)
	}
}

// ─── :many tests ──────────────────────────────────────────────────────────────

func TestListEvents(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	for _, name := range []string{"alpha", "beta", "gamma"} {
		docID := fmt.Sprintf("doc-%s", name)
		if err := gen.InsertEvent(ctx, db, name, "{}", sql.NullString{}, docID, ts(2024, 6, 1, 12, 0, 0), nilTime, nilTime, nilTime); err != nil {
			t.Fatal(err)
		}
	}

	events, err := gen.ListEvents(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events, got %d", len(events))
	}
	if events[0].Name != "alpha" || events[1].Name != "beta" || events[2].Name != "gamma" {
		t.Errorf("unexpected order")
	}
}

func TestGetEventsByDateRange(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	insert := func(name string, created time.Time) {
		t.Helper()
		if err := gen.InsertEvent(ctx, db, name, "{}", sql.NullString{}, "doc-"+name, created, nilTime, nilTime, nilTime); err != nil {
			t.Fatal(err)
		}
	}
	insert("early", ts(2024, 1, 1, 10, 0, 0))
	insert("mid", ts(2024, 6, 1, 12, 0, 0))
	insert("late", ts(2024, 12, 1, 15, 0, 0))

	events, err := gen.GetEventsByDateRange(ctx, db, ts(2024, 1, 1, 0, 0, 0), ts(2024, 7, 1, 0, 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].Name != "early" || events[1].Name != "mid" {
		t.Errorf("unexpected order")
	}
}

// ─── :exec tests ──────────────────────────────────────────────────────────────

func TestUpdatePayload(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	if err := gen.InsertEvent(ctx, db, "test", `{"v":1}`, sql.NullString{}, "doc-1",
		ts(2024, 6, 1, 12, 0, 0), nilTime, nilTime, nilTime); err != nil {
		t.Fatal(err)
	}

	if err := gen.UpdatePayload(ctx, db, `{"v":2}`, sql.NullString{}, 1); err != nil {
		t.Fatal(err)
	}

	ev, err := gen.GetEvent(ctx, db, 1)
	if err != nil {
		t.Fatal(err)
	}
	if ev == nil {
		t.Fatal("expected event")
	}
	if ev.Meta.Valid {
		t.Errorf("expected nil meta")
	}
}

func TestUpdateEventDate(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	if err := gen.InsertEvent(ctx, db, "dated", "{}", sql.NullString{}, "doc-1",
		ts(2024, 6, 1, 12, 0, 0), nilTime, sql.NullTime{Time: ts(2024, 1, 1, 0, 0, 0), Valid: true}, nilTime); err != nil {
		t.Fatal(err)
	}

	if err := gen.UpdateEventDate(ctx, db, sql.NullTime{Time: ts(2024, 12, 31, 0, 0, 0), Valid: true}, 1); err != nil {
		t.Fatal(err)
	}
}

// ─── :execrows tests ──────────────────────────────────────────────────────────

func TestInsertEventRows(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	n, err := gen.InsertEventRows(ctx, db, "rowtest", "{}", sql.NullString{}, "doc-1",
		ts(2024, 6, 1, 12, 0, 0), nilTime, nilTime, nilTime)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1 row, got %d", n)
	}
}

// ─── projection tests ─────────────────────────────────────────────────────────

func TestFindByDate(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	if err := gen.InsertEvent(ctx, db, "dated", "{}", sql.NullString{}, "doc-1",
		ts(2024, 6, 1, 12, 0, 0), nilTime, sql.NullTime{Time: ts(2024, 6, 15, 0, 0, 0), Valid: true}, nilTime); err != nil {
		t.Fatal(err)
	}

	row, err := gen.FindByDate(ctx, db, sql.NullTime{Time: ts(2024, 6, 15, 0, 0, 0), Valid: true})
	if err != nil {
		t.Fatal(err)
	}
	if row == nil {
		t.Fatal("expected row, got nil")
	}
	if row.Name != "dated" {
		t.Errorf("expected dated, got %s", row.Name)
	}
}

func TestFindByDocId(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	if err := gen.InsertEvent(ctx, db, "doctest", "{}", sql.NullString{}, "unique-doc-id",
		ts(2024, 6, 1, 12, 0, 0), nilTime, nilTime, nilTime); err != nil {
		t.Fatal(err)
	}

	row, err := gen.FindByDocId(ctx, db, "unique-doc-id")
	if err != nil {
		t.Fatal(err)
	}
	if row == nil {
		t.Fatal("expected row, got nil")
	}
	if row.Name != "doctest" {
		t.Errorf("expected doctest, got %s", row.Name)
	}
}

// ─── count tests ──────────────────────────────────────────────────────────────

func TestCountEvents(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	for i := 1; i <= 3; i++ {
		docID := fmt.Sprintf("doc-%d", i)
		if err := gen.InsertEvent(ctx, db, fmt.Sprintf("ev%d", i), "{}", sql.NullString{}, docID, ts(2024, 6, i, 0, 0, 0), nilTime, nilTime, nilTime); err != nil {
			t.Fatal(err)
		}
	}

	row, err := gen.CountEvents(ctx, db, ts(2024, 1, 1, 0, 0, 0))
	if err != nil {
		t.Fatal(err)
	}
	if row == nil {
		t.Fatal("expected row")
	}
	if row.Total != 3 {
		t.Errorf("expected 3, got %d", row.Total)
	}
}
