package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	gen "e2e-type-overrides-go-postgresql/gen"

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

	ddl, err := os.ReadFile("../../../../fixtures/type_overrides/schema.sql")
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

func mustJSON(v interface{}) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// ─── :one tests ───────────────────────────────────────────────────────────────

func TestInsertAndGetEvent(t *testing.T) {
	db, ctx := setupDB(t)

	payload := mustJSON(map[string]interface{}{"type": "click", "x": 10})
	meta := mustJSON(map[string]interface{}{"source": "web"})
	docID := "aaaaaaaa-0000-0000-0000-000000000001"
	createdAt := sql.NullTime{Time: time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC), Valid: true}
	eventDate := sql.NullTime{Time: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC), Valid: true}

	if err := gen.InsertEvent(ctx, db, "login", payload, meta, docID, createdAt, sql.NullTime{}, eventDate, sql.NullTime{}); err != nil {
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
}

func TestGetEventNotFound(t *testing.T) {
	db, ctx := setupDB(t)

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
	db, ctx := setupDB(t)

	ts := sql.NullTime{Time: time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC), Valid: true}
	for _, name := range []string{"alpha", "beta", "gamma"} {
		docID := fmt.Sprintf("doc-%s", name)
		if err := gen.InsertEvent(ctx, db, name, mustJSON(map[string]interface{}{}), nil, docID, ts, sql.NullTime{}, sql.NullTime{}, sql.NullTime{}); err != nil {
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
		t.Errorf("unexpected order: %v %v %v", events[0].Name, events[1].Name, events[2].Name)
	}
}

func TestGetEventsByDateRange(t *testing.T) {
	db, ctx := setupDB(t)

	insertAt := func(name, docID string, ts time.Time) {
		t.Helper()
		if err := gen.InsertEvent(ctx, db, name, mustJSON(map[string]interface{}{}), nil, docID, sql.NullTime{Time: ts, Valid: true}, sql.NullTime{}, sql.NullTime{}, sql.NullTime{}); err != nil {
			t.Fatal(err)
		}
	}
	insertAt("early", "doc-1", time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC))
	insertAt("mid", "doc-2", time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC))
	insertAt("late", "doc-3", time.Date(2024, 12, 1, 15, 0, 0, 0, time.UTC))

	start := sql.NullTime{Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}
	end := sql.NullTime{Time: time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC), Valid: true}
	events, err := gen.GetEventsByDateRange(ctx, db, start, end)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].Name != "early" || events[1].Name != "mid" {
		t.Errorf("unexpected: %v %v", events[0].Name, events[1].Name)
	}
}

// ─── :exec tests ──────────────────────────────────────────────────────────────

func TestUpdatePayload(t *testing.T) {
	db, ctx := setupDB(t)

	ts := sql.NullTime{Time: time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC), Valid: true}
	if err := gen.InsertEvent(ctx, db, "test", mustJSON(map[string]interface{}{"v": 1}), nil, "doc-1", ts, sql.NullTime{}, sql.NullTime{}, sql.NullTime{}); err != nil {
		t.Fatal(err)
	}

	updated := mustJSON(map[string]interface{}{"v": 2, "changed": true})
	if err := gen.UpdatePayload(ctx, db, updated, nil, 1); err != nil {
		t.Fatal(err)
	}

	ev, err := gen.GetEvent(ctx, db, 1)
	if err != nil {
		t.Fatal(err)
	}
	if ev == nil {
		t.Fatal("expected event")
	}
	if ev.Meta != nil {
		t.Errorf("expected nil meta, got %v", ev.Meta)
	}
}

func TestUpdateEventDate(t *testing.T) {
	db, ctx := setupDB(t)

	ts := sql.NullTime{Time: time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC), Valid: true}
	ed := sql.NullTime{Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}
	if err := gen.InsertEvent(ctx, db, "dated", mustJSON(map[string]interface{}{}), nil, "doc-1", ts, sql.NullTime{}, ed, sql.NullTime{}); err != nil {
		t.Fatal(err)
	}

	newDate := sql.NullTime{Time: time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true}
	if err := gen.UpdateEventDate(ctx, db, newDate, 1); err != nil {
		t.Fatal(err)
	}
}

// ─── :execrows tests ──────────────────────────────────────────────────────────

func TestInsertEventRows(t *testing.T) {
	db, ctx := setupDB(t)

	ts := sql.NullTime{Time: time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC), Valid: true}
	n, err := gen.InsertEventRows(ctx, db, "rowtest", mustJSON(map[string]interface{}{}), nil, "doc-1", ts, sql.NullTime{}, sql.NullTime{}, sql.NullTime{})
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("expected 1 row, got %d", n)
	}
}

// ─── projection tests ─────────────────────────────────────────────────────────

func TestFindByDate(t *testing.T) {
	db, ctx := setupDB(t)

	ts := sql.NullTime{Time: time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC), Valid: true}
	target := sql.NullTime{Time: time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC), Valid: true}
	if err := gen.InsertEvent(ctx, db, "dated", mustJSON(map[string]interface{}{}), nil, "doc-1", ts, sql.NullTime{}, target, sql.NullTime{}); err != nil {
		t.Fatal(err)
	}

	row, err := gen.FindByDate(ctx, db, target)
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

func TestFindByUuid(t *testing.T) {
	db, ctx := setupDB(t)

	docID := "bbbbbbbb-0000-0000-0000-000000000001"
	ts := sql.NullTime{Time: time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC), Valid: true}
	if err := gen.InsertEvent(ctx, db, "uuid-test", mustJSON(map[string]interface{}{}), nil, docID, ts, sql.NullTime{}, sql.NullTime{}, sql.NullTime{}); err != nil {
		t.Fatal(err)
	}

	row, err := gen.FindByUuid(ctx, db, docID)
	if err != nil {
		t.Fatal(err)
	}
	if row == nil {
		t.Fatal("expected row, got nil")
	}
	if row.Name != "uuid-test" {
		t.Errorf("expected uuid-test, got %s", row.Name)
	}
}

// ─── count tests ──────────────────────────────────────────────────────────────

func TestCountEvents(t *testing.T) {
	db, ctx := setupDB(t)

	for i := 1; i <= 3; i++ {
		ts := sql.NullTime{Time: time.Date(2024, 6, i, 0, 0, 0, 0, time.UTC), Valid: true}
		docID := fmt.Sprintf("doc-%d", i)
		if err := gen.InsertEvent(ctx, db, fmt.Sprintf("ev%d", i), mustJSON(map[string]interface{}{}), nil, docID, ts, sql.NullTime{}, sql.NullTime{}, sql.NullTime{}); err != nil {
			t.Fatal(err)
		}
	}

	cutoff := sql.NullTime{Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}
	row, err := gen.CountEvents(ctx, db, cutoff)
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
