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

	ddl, err := os.ReadFile("../../../../fixtures/type_overrides/postgresql/schema.sql")
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

func ptrBytes(b []byte) *[]byte {
	return &b
}

func testUUID() string {
	return fmt.Sprintf("%08x-0000-4000-8000-%012x", rand.Int63()&0xFFFFFFFF, rand.Int63()&0xFFFFFFFFFFFF)
}

var nilMeta *[]byte

// ─── :one tests ───────────────────────────────────────────────────────────────

func TestInsertAndGetEvent(t *testing.T) {
	db, ctx := setupDB(t)

	payload := mustJSON(map[string]interface{}{"type": "click", "x": 10})
	meta := mustJSON(map[string]interface{}{"source": "web"})
	docID := "aaaaaaaa-0000-0000-0000-000000000001"
	createdAt := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	eventDate := sql.NullTime{Time: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC), Valid: true}

	if err := gen.InsertEvent(ctx, db, "login", payload, ptrBytes(meta), docID, createdAt, sql.NullTime{}, eventDate, sql.NullTime{}); err != nil {
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
	if ev.DocId != docID {
		t.Errorf("expected doc_id=%s, got %s", docID, ev.DocId)
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

	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	for _, name := range []string{"alpha", "beta", "gamma"} {
		docID := testUUID()
		if err := gen.InsertEvent(ctx, db, name, mustJSON(map[string]interface{}{}), nilMeta, docID, ts, sql.NullTime{}, sql.NullTime{}, sql.NullTime{}); err != nil {
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
		if err := gen.InsertEvent(ctx, db, name, mustJSON(map[string]interface{}{}), nilMeta, docID, ts, sql.NullTime{}, sql.NullTime{}, sql.NullTime{}); err != nil {
			t.Fatal(err)
		}
	}
	insertAt("early", testUUID(), time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC))
	insertAt("mid", testUUID(), time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC))
	insertAt("late", testUUID(), time.Date(2024, 12, 1, 15, 0, 0, 0, time.UTC))

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
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

	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	initialMeta := mustJSON(map[string]interface{}{"source": "web"})
	if err := gen.InsertEvent(ctx, db, "test", mustJSON(map[string]interface{}{"v": 1}), ptrBytes(initialMeta), testUUID(), ts, sql.NullTime{}, sql.NullTime{}, sql.NullTime{}); err != nil {
		t.Fatal(err)
	}

	updated := mustJSON(map[string]interface{}{"v": 2, "changed": true})
	if err := gen.UpdatePayload(ctx, db, updated, nilMeta, 1); err != nil {
		t.Fatal(err)
	}

	ev, err := gen.GetEvent(ctx, db, 1)
	if err != nil {
		t.Fatal(err)
	}
	if ev == nil {
		t.Fatal("expected event")
	}
	var gotPayload, wantPayload interface{}
	_ = json.Unmarshal(ev.Payload, &gotPayload)
	_ = json.Unmarshal(updated, &wantPayload)
	if fmt.Sprintf("%v", gotPayload) != fmt.Sprintf("%v", wantPayload) {
		t.Errorf("expected payload=%s, got %s", string(updated), string(ev.Payload))
	}
	if ev.Meta != nil {
		t.Errorf("expected nil meta, got %v", ev.Meta)
	}
}

func TestUpdateEventDate(t *testing.T) {
	db, ctx := setupDB(t)

	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	ed := sql.NullTime{Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}
	if err := gen.InsertEvent(ctx, db, "dated", mustJSON(map[string]interface{}{}), nilMeta, testUUID(), ts, sql.NullTime{}, ed, sql.NullTime{}); err != nil {
		t.Fatal(err)
	}

	newDate := sql.NullTime{Time: time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true}
	if err := gen.UpdateEventDate(ctx, db, newDate, 1); err != nil {
		t.Fatal(err)
	}

	ev, err := gen.GetEvent(ctx, db, 1)
	if err != nil {
		t.Fatal(err)
	}
	if ev == nil {
		t.Fatal("expected event after update")
	}
	if !ev.EventDate.Valid {
		t.Fatal("expected event_date to be valid after update")
	}
	if !ev.EventDate.Time.Equal(newDate.Time) {
		t.Errorf("expected event_date=%v, got %v", newDate.Time, ev.EventDate.Time)
	}
}

// ─── :execrows tests ──────────────────────────────────────────────────────────

func TestInsertEventRows(t *testing.T) {
	db, ctx := setupDB(t)

	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	n, err := gen.InsertEventRows(ctx, db, "rowtest", mustJSON(map[string]interface{}{}), nilMeta, testUUID(), ts, sql.NullTime{}, sql.NullTime{}, sql.NullTime{})
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

	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	target := sql.NullTime{Time: time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC), Valid: true}
	if err := gen.InsertEvent(ctx, db, "dated", mustJSON(map[string]interface{}{}), nilMeta, testUUID(), ts, sql.NullTime{}, target, sql.NullTime{}); err != nil {
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
	ts := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	if err := gen.InsertEvent(ctx, db, "uuid-test", mustJSON(map[string]interface{}{}), nilMeta, docID, ts, sql.NullTime{}, sql.NullTime{}, sql.NullTime{}); err != nil {
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
		ts := time.Date(2024, 6, i, 0, 0, 0, 0, time.UTC)
		docID := testUUID()
		if err := gen.InsertEvent(ctx, db, fmt.Sprintf("ev%d", i), mustJSON(map[string]interface{}{}), nilMeta, docID, ts, sql.NullTime{}, sql.NullTime{}, sql.NullTime{}); err != nil {
			t.Fatal(err)
		}
	}

	cutoff := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
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
