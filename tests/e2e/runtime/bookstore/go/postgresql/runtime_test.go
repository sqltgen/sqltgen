package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	gen "e2e-go-postgresql/gen"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const defaultDSN = "postgres://sqltgen:sqltgen@localhost:15432/sqltgen_e2e"

func dsn() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return defaultDSN
}

// setupDB creates an isolated database, applies the DDL, and returns a connected *pgxpool.Pool
// plus a cleanup function that drops the database.
func setupDB(t *testing.T) (*pgxpool.Pool, func()) {
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
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatal(err)
	}

	ddl, err := os.ReadFile("../../../../fixtures/bookstore/postgresql/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range splitStatements(string(ddl)) {
		if _, err := pool.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}

	cleanup := func() {
		pool.Close()
		adm, _ := sql.Open("pgx", dsn())
		adm.ExecContext(ctx, fmt.Sprintf(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '%s' AND pid <> pg_backend_pid()`, dbName))
		adm.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS "%s"`, dbName))
		adm.Close()
	}

	return pool, cleanup
}

func replaceLastSegment(url, replacement string) string {
	i := strings.LastIndex(url, "/")
	if i < 0 {
		return url
	}
	return url[:i+1] + replacement
}

// splitStatements splits a SQL string on semicolons, trimming whitespace.
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

// seed populates the database with a standard dataset.
func seed(t *testing.T, ctx context.Context, db *pgxpool.Pool) {
	t.Helper()

	// 3 authors
	_, err := gen.CreateAuthor(ctx, db, "Asimov", sql.NullString{String: "Sci-fi master", Valid: true}, sql.NullInt32{Int32: 1920, Valid: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = gen.CreateAuthor(ctx, db, "Herbert", sql.NullString{}, sql.NullInt32{Int32: 1920, Valid: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = gen.CreateAuthor(ctx, db, "Le Guin", sql.NullString{String: "Earthsea", Valid: true}, sql.NullInt32{Int32: 1929, Valid: true})
	if err != nil {
		t.Fatal(err)
	}

	// 4 books
	_, err = gen.CreateBook(ctx, db, 1, "Foundation", "sci-fi", "9.99",
		sql.NullTime{Time: time.Date(1951, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = gen.CreateBook(ctx, db, 1, "I Robot", "sci-fi", "7.99",
		sql.NullTime{Time: time.Date(1950, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = gen.CreateBook(ctx, db, 2, "Dune", "sci-fi", "12.99",
		sql.NullTime{Time: time.Date(1965, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true})
	if err != nil {
		t.Fatal(err)
	}
	_, err = gen.CreateBook(ctx, db, 3, "Earthsea", "fantasy", "8.99",
		sql.NullTime{Time: time.Date(1968, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true})
	if err != nil {
		t.Fatal(err)
	}

	// 2 customers
	alice, err := gen.CreateCustomer(ctx, db, "Alice", "alice@example.com")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := gen.CreateCustomer(ctx, db, "Bob", "bob@example.com")
	if err != nil {
		t.Fatal(err)
	}

	// Sale 1: Alice buys Foundation (qty 2) + Dune (qty 1)
	sale1, err := gen.CreateSale(ctx, db, alice.Id)
	if err != nil {
		t.Fatal(err)
	}
	if err := gen.AddSaleItem(ctx, db, sale1.Id, 1, 2, "9.99"); err != nil {
		t.Fatal(err)
	}
	if err := gen.AddSaleItem(ctx, db, sale1.Id, 3, 1, "12.99"); err != nil {
		t.Fatal(err)
	}

	// Sale 2: Bob buys Earthsea (qty 1) + Foundation (qty 1)
	sale2, err := gen.CreateSale(ctx, db, bob.Id)
	if err != nil {
		t.Fatal(err)
	}
	if err := gen.AddSaleItem(ctx, db, sale2.Id, 4, 1, "8.99"); err != nil {
		t.Fatal(err)
	}
	if err := gen.AddSaleItem(ctx, db, sale2.Id, 1, 1, "9.99"); err != nil {
		t.Fatal(err)
	}
}

// ─── :one tests ────────────────────────────────────────────────────────

func TestCreateAuthor(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	author, err := gen.CreateAuthor(ctx, db, "Test", sql.NullString{String: "bio", Valid: true}, sql.NullInt32{Int32: 1980, Valid: true})
	if err != nil {
		t.Fatal(err)
	}
	if author.Name != "Test" {
		t.Fatalf("expected name Test, got %s", author.Name)
	}
	if !author.Bio.Valid || author.Bio.String != "bio" {
		t.Fatalf("expected bio 'bio', got %v", author.Bio)
	}
	if !author.BirthYear.Valid || author.BirthYear.Int32 != 1980 {
		t.Fatalf("expected birth_year 1980, got %v", author.BirthYear)
	}
	if author.Id <= 0 {
		t.Fatalf("expected positive id, got %d", author.Id)
	}
}

func TestGetAuthor(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	author, err := gen.GetAuthor(ctx, db, 1)
	if err != nil {
		t.Fatal(err)
	}
	if author == nil {
		t.Fatal("expected author, got nil")
	}
	if author.Name != "Asimov" {
		t.Fatalf("expected Asimov, got %s", author.Name)
	}
	if !author.Bio.Valid || author.Bio.String != "Sci-fi master" {
		t.Fatalf("expected bio 'Sci-fi master', got %v", author.Bio)
	}
	if !author.BirthYear.Valid || author.BirthYear.Int32 != 1920 {
		t.Fatalf("expected birth_year 1920, got %v", author.BirthYear)
	}

	// Not found
	missing, err := gen.GetAuthor(ctx, db, 999)
	if err != nil {
		t.Fatal(err)
	}
	if missing != nil {
		t.Fatal("expected nil for missing author")
	}
}

func TestUpdateAuthorBio(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	updated, err := gen.UpdateAuthorBio(ctx, db, sql.NullString{String: "Updated bio", Valid: true}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if updated == nil {
		t.Fatal("expected updated author, got nil")
	}
	if updated.Name != "Asimov" {
		t.Fatalf("expected Asimov, got %s", updated.Name)
	}
	if !updated.Bio.Valid || updated.Bio.String != "Updated bio" {
		t.Fatalf("expected bio 'Updated bio', got %v", updated.Bio)
	}
}

func TestDeleteAuthor(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	_, err := gen.CreateAuthor(ctx, db, "Temp", sql.NullString{}, sql.NullInt32{})
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := gen.DeleteAuthor(ctx, db, 1)
	if err != nil {
		t.Fatal(err)
	}
	if deleted == nil {
		t.Fatal("expected deleted row, got nil")
	}
	if deleted.Name != "Temp" {
		t.Fatalf("expected Temp, got %s", deleted.Name)
	}

	// Verify gone
	got, err := gen.GetAuthor(ctx, db, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatal("expected nil after delete")
	}
}

func TestGetBook(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	book, err := gen.GetBook(ctx, db, 1)
	if err != nil {
		t.Fatal(err)
	}
	if book == nil {
		t.Fatal("expected book, got nil")
	}
	if book.Title != "Foundation" {
		t.Fatalf("expected Foundation, got %s", book.Title)
	}
	if book.Genre != "sci-fi" {
		t.Fatalf("expected sci-fi, got %s", book.Genre)
	}
	if book.Price != "9.99" {
		t.Fatalf("expected 9.99, got %s", book.Price)
	}
	if !book.PublishedAt.Valid {
		t.Fatal("expected published_at to be valid")
	}
}

func TestCreateBook(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	book, err := gen.CreateBook(ctx, db, 1, "New Book", "mystery", "14.50", sql.NullTime{})
	if err != nil {
		t.Fatal(err)
	}
	if book.Title != "New Book" {
		t.Fatalf("expected New Book, got %s", book.Title)
	}
	if book.Genre != "mystery" {
		t.Fatalf("expected mystery, got %s", book.Genre)
	}
	if book.Price != "14.50" {
		t.Fatalf("expected 14.50, got %s", book.Price)
	}
	if book.PublishedAt.Valid {
		t.Fatal("expected published_at to be null")
	}
}

// ─── :many tests ──────────────────────────────────────────────────────

func TestListAuthors(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	authors, err := gen.ListAuthors(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(authors) != 3 {
		t.Fatalf("expected 3 authors, got %d", len(authors))
	}
	// Ordered by name
	if authors[0].Name != "Asimov" {
		t.Fatalf("expected Asimov first, got %s", authors[0].Name)
	}
	if authors[1].Name != "Herbert" {
		t.Fatalf("expected Herbert second, got %s", authors[1].Name)
	}
	if authors[2].Name != "Le Guin" {
		t.Fatalf("expected Le Guin third, got %s", authors[2].Name)
	}
}

func TestListBooksByGenre(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	scifi, err := gen.ListBooksByGenre(ctx, db, "sci-fi")
	if err != nil {
		t.Fatal(err)
	}
	if len(scifi) != 3 {
		t.Fatalf("expected 3 sci-fi books, got %d", len(scifi))
	}

	fantasy, err := gen.ListBooksByGenre(ctx, db, "fantasy")
	if err != nil {
		t.Fatal(err)
	}
	if len(fantasy) != 1 {
		t.Fatalf("expected 1 fantasy book, got %d", len(fantasy))
	}
	if fantasy[0].Title != "Earthsea" {
		t.Fatalf("expected Earthsea, got %s", fantasy[0].Title)
	}
}

func TestListBooksByGenreOrAll(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	all, err := gen.ListBooksByGenreOrAll(ctx, db, "all")
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 4 {
		t.Fatalf("expected 4 books for 'all', got %d", len(all))
	}

	scifi, err := gen.ListBooksByGenreOrAll(ctx, db, "sci-fi")
	if err != nil {
		t.Fatal(err)
	}
	if len(scifi) != 3 {
		t.Fatalf("expected 3 sci-fi books, got %d", len(scifi))
	}
}

func TestGetBooksByIds(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	books, err := gen.GetBooksByIds(ctx, db, []int64{1, 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(books) != 2 {
		t.Fatalf("expected 2 books, got %d", len(books))
	}
	titles := map[string]bool{}
	for _, b := range books {
		titles[b.Title] = true
	}
	if !titles["Foundation"] {
		t.Fatal("expected Foundation in results")
	}
	if !titles["Dune"] {
		t.Fatal("expected Dune in results")
	}

	// Empty list
	empty, err := gen.GetBooksByIds(ctx, db, []int64{})
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("expected 0 books for empty ids, got %d", len(empty))
	}
}

// ─── :exec tests ──────────────────────────────────────────────────────

func TestAddSaleItem(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	// Should not return error
	err := gen.AddSaleItem(ctx, db, 1, 2, 5, "7.99")
	if err != nil {
		t.Fatal(err)
	}
}

// ─── :execrows tests ──────────────────────────────────────────────────

func TestDeleteBookById(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	// Book 2 (I Robot) has no sale_items
	n, err := gen.DeleteBookById(ctx, db, 2)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 row deleted, got %d", n)
	}

	n, err = gen.DeleteBookById(ctx, db, 999)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("expected 0 rows deleted, got %d", n)
	}
}

// ─── JOIN tests ───────────────────────────────────────────────────────

func TestListBooksWithAuthor(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.ListBooksWithAuthor(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}

	var dune, foundation *gen.ListBooksWithAuthorRow
	for i := range rows {
		if rows[i].Title == "Dune" {
			dune = &rows[i]
		}
		if rows[i].Title == "Foundation" {
			foundation = &rows[i]
		}
	}
	if dune == nil {
		t.Fatal("expected Dune in results")
	}
	if dune.AuthorName != "Herbert" {
		t.Fatalf("expected Herbert for Dune, got %s", dune.AuthorName)
	}
	if dune.AuthorBio.Valid {
		t.Fatal("expected null bio for Herbert")
	}
	if foundation == nil {
		t.Fatal("expected Foundation in results")
	}
	if foundation.AuthorName != "Asimov" {
		t.Fatalf("expected Asimov for Foundation, got %s", foundation.AuthorName)
	}
	if !foundation.AuthorBio.Valid || foundation.AuthorBio.String != "Sci-fi master" {
		t.Fatalf("expected bio 'Sci-fi master' for Foundation, got %v", foundation.AuthorBio)
	}
}

func TestListBookSummariesView(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.ListBookSummariesView(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	if rows[0].Title != "Dune" {
		t.Fatalf("expected first title Dune, got %s", rows[0].Title)
	}
	if rows[0].AuthorName != "Herbert" {
		t.Fatalf("expected first author Herbert, got %s", rows[0].AuthorName)
	}
}

func TestGetBooksNeverOrdered(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	books, err := gen.GetBooksNeverOrdered(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	// Only I Robot was never ordered
	if len(books) != 1 {
		t.Fatalf("expected 1 book never ordered, got %d", len(books))
	}
	if books[0].Title != "I Robot" {
		t.Fatalf("expected I Robot, got %s", books[0].Title)
	}
}

// ─── CTE tests ────────────────────────────────────────────────────────

func TestGetTopSellingBooks(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetTopSellingBooks(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		t.Fatal("expected at least one row")
	}
	// Foundation: qty 2 (Alice) + qty 1 (Bob) = 3
	if rows[0].Title != "Foundation" {
		t.Fatalf("expected Foundation as top seller, got %s", rows[0].Title)
	}
	if !rows[0].UnitsSold.Valid || rows[0].UnitsSold.Int64 != 3 {
		t.Fatalf("expected 3 units sold, got %v", rows[0].UnitsSold)
	}
}

func TestGetBestCustomers(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetBestCustomers(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 customers, got %d", len(rows))
	}
	// Alice: 2*9.99 + 12.99 = 32.97 → first
	if rows[0].Name != "Alice" {
		t.Fatalf("expected Alice first, got %s", rows[0].Name)
	}
	if !rows[0].TotalSpent.Valid {
		t.Fatal("expected total_spent to be non-null")
	}
}

func TestGetAuthorStats(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetAuthorStats(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	var asimov *gen.GetAuthorStatsRow
	for i := range rows {
		if rows[i].Name == "Asimov" {
			asimov = &rows[i]
		}
	}
	if asimov == nil {
		t.Fatal("expected Asimov in results")
	}
	if asimov.NumBooks != 2 {
		t.Fatalf("expected 2 books for Asimov, got %d", asimov.NumBooks)
	}
}

// ─── Data-modifying CTE ───────────────────────────────────────────────

func TestArchiveAndReturnBooks(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	// Delete sale_items first so the CTE DELETE on book can succeed
	if _, err := db.Exec(ctx, "DELETE FROM sale_item"); err != nil {
		t.Fatal(err)
	}

	archived, err := gen.ArchiveAndReturnBooks(ctx, db,
		sql.NullTime{Time: time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(archived) != 2 {
		t.Fatalf("expected 2 archived books, got %d", len(archived))
	}
	titles := map[string]bool{}
	for _, r := range archived {
		titles[r.Title] = true
	}
	if !titles["Foundation"] {
		t.Fatal("expected Foundation in archived")
	}
	if !titles["I Robot"] {
		t.Fatal("expected I Robot in archived")
	}

	// Verify remaining sci-fi books
	remaining, err := gen.ListBooksByGenre(ctx, db, "sci-fi")
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 1 {
		t.Fatalf("expected 1 remaining sci-fi book, got %d", len(remaining))
	}
}

// ─── Aggregate tests ──────────────────────────────────────────────────

func TestCountBooksByGenre(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.CountBooksByGenre(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 genres, got %d", len(rows))
	}

	genreMap := map[string]int64{}
	for _, r := range rows {
		genreMap[r.Genre] = r.BookCount
	}
	if genreMap["fantasy"] != 1 {
		t.Fatalf("expected 1 fantasy book, got %d", genreMap["fantasy"])
	}
	if genreMap["sci-fi"] != 3 {
		t.Fatalf("expected 3 sci-fi books, got %d", genreMap["sci-fi"])
	}
}

// ─── LIMIT/OFFSET tests ──────────────────────────────────────────────

func TestListBooksWithLimit(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	page1, err := gen.ListBooksWithLimit(ctx, db, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 2 {
		t.Fatalf("expected 2 books on page 1, got %d", len(page1))
	}

	page2, err := gen.ListBooksWithLimit(ctx, db, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 2 {
		t.Fatalf("expected 2 books on page 2, got %d", len(page2))
	}

	// Pages should not overlap
	p1Titles := map[string]bool{}
	for _, r := range page1 {
		p1Titles[r.Title] = true
	}
	for _, r := range page2 {
		if p1Titles[r.Title] {
			t.Fatalf("page 2 title %s overlaps with page 1", r.Title)
		}
	}
}

// ─── LIKE tests ───────────────────────────────────────────────────────

func TestSearchBooksByTitle(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	results, err := gen.SearchBooksByTitle(ctx, db, "%ound%")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].Title != "Foundation" {
		t.Fatalf("expected Foundation, got %s", results[0].Title)
	}

	empty, err := gen.SearchBooksByTitle(ctx, db, "NOPE%")
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("expected 0 results, got %d", len(empty))
	}
}

// ─── BETWEEN tests ───────────────────────────────────────────────────

func TestGetBooksByPriceRange(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	results, err := gen.GetBooksByPriceRange(ctx, db, "8.00", "10.00")
	if err != nil {
		t.Fatal(err)
	}
	// Foundation (9.99) and Earthsea (8.99)
	if len(results) != 2 {
		t.Fatalf("expected 2 books in range, got %d", len(results))
	}
}

// ─── IN list tests ────────────────────────────────────────────────────

func TestGetBooksInGenres(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	results, err := gen.GetBooksInGenres(ctx, db, "sci-fi", "fantasy", "horror")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 4 {
		t.Fatalf("expected 4 books, got %d", len(results))
	}
}

// ─── CASE / COALESCE tests ──────────────────────────────────────────

func TestGetBookPriceLabel(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetBookPriceLabel(ctx, db, "10.00")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}

	labelMap := map[string]string{}
	for _, r := range rows {
		labelMap[r.Title] = r.PriceLabel
	}
	if labelMap["Dune"] != "expensive" {
		t.Fatalf("expected Dune to be expensive, got %s", labelMap["Dune"])
	}
	if labelMap["Earthsea"] != "affordable" {
		t.Fatalf("expected Earthsea to be affordable, got %s", labelMap["Earthsea"])
	}
}

func TestGetBookPriceOrDefault(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetBookPriceOrDefault(ctx, db, sql.NullString{String: "0.00", Valid: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}

	var dune *gen.GetBookPriceOrDefaultRow
	for i := range rows {
		if rows[i].Title == "Dune" {
			dune = &rows[i]
		}
	}
	if dune == nil {
		t.Fatal("expected Dune in results")
	}
	if dune.EffectivePrice != "12.99" {
		t.Fatalf("expected 12.99, got %s", dune.EffectivePrice)
	}
}

// ─── HAVING tests ─────────────────────────────────────────────────────

func TestGetGenresWithManyBooks(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	results, err := gen.GetGenresWithManyBooks(ctx, db, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 genre, got %d", len(results))
	}
	if results[0].Genre != "sci-fi" {
		t.Fatalf("expected sci-fi, got %s", results[0].Genre)
	}
	if results[0].BookCount != 3 {
		t.Fatalf("expected 3 books, got %d", results[0].BookCount)
	}
}

// ─── Subquery tests ──────────────────────────────────────────────────

func TestGetBooksNotByAuthor(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	results, err := gen.GetBooksNotByAuthor(ctx, db, "Asimov")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 books, got %d", len(results))
	}
	for _, r := range results {
		if r.Title == "Foundation" || r.Title == "I Robot" {
			t.Fatalf("unexpected Asimov book %s in results", r.Title)
		}
	}
}

// ─── JOIN with param tests ───────────────────────────────────────────

func TestGetBooksByAuthorParam(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	// birth_year > 1925 → only Le Guin (1929)
	results, err := gen.GetBooksByAuthorParam(ctx, db, sql.NullInt32{Int32: 1925, Valid: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 book, got %d", len(results))
	}
	if results[0].Title != "Earthsea" {
		t.Fatalf("expected Earthsea, got %s", results[0].Title)
	}
}

// ─── Qualified wildcard tests ────────────────────────────────────────

func TestGetAllBookFields(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	books, err := gen.GetAllBookFields(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(books) != 4 {
		t.Fatalf("expected 4 books, got %d", len(books))
	}
	if books[0].Id != 1 {
		t.Fatalf("expected first book id 1, got %d", books[0].Id)
	}
	if books[0].Title == "" {
		t.Fatal("expected non-empty title")
	}
}

// ─── EXISTS subquery tests ──────────────────────────────────────────

func TestGetBooksWithRecentSales(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	// All sales happened just now; epoch cutoff → Foundation, Dune, Earthsea
	rows, err := gen.GetBooksWithRecentSales(ctx, db, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 books with recent sales, got %d", len(rows))
	}
	titles := map[string]bool{}
	for _, r := range rows {
		titles[r.Title] = true
	}
	if !titles["Foundation"] {
		t.Fatal("expected Foundation")
	}
	if !titles["Dune"] {
		t.Fatal("expected Dune")
	}
	if !titles["Earthsea"] {
		t.Fatal("expected Earthsea")
	}
}

// ─── Scalar subquery tests ──────────────────────────────────────────

func TestGetBookWithAuthorName(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetBookWithAuthorName(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}

	var foundation *gen.GetBookWithAuthorNameRow
	for i := range rows {
		if rows[i].Title == "Foundation" {
			foundation = &rows[i]
		}
	}
	if foundation == nil {
		t.Fatal("expected Foundation in results")
	}
	if !foundation.AuthorName.Valid || foundation.AuthorName.String != "Asimov" {
		t.Fatalf("expected author_name Asimov, got %v", foundation.AuthorName)
	}
}

// ─── Customer / Sale creation tests ──────────────────────────────────

func TestCreateCustomer(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	cust, err := gen.CreateCustomer(ctx, db, "Solo", "solo@example.com")
	if err != nil {
		t.Fatal(err)
	}
	if cust == nil {
		t.Fatal("expected customer, got nil")
	}
	if cust.Id <= 0 {
		t.Fatalf("expected positive id, got %d", cust.Id)
	}
}

func TestCreateSale(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	cust, err := gen.CreateCustomer(ctx, db, "Solo", "solo@example.com")
	if err != nil {
		t.Fatal(err)
	}
	sale, err := gen.CreateSale(ctx, db, cust.Id)
	if err != nil {
		t.Fatal(err)
	}
	if sale == nil {
		t.Fatal("expected sale, got nil")
	}
	if sale.Id <= 0 {
		t.Fatalf("expected positive id, got %d", sale.Id)
	}
}

// ─── Product tests ──────────────────────────────────────────────────

func TestInsertProduct(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	id := "550e8400-e29b-41d4-a716-446655440000"
	weightKg := float32(1.5)
	product, err := gen.InsertProduct(ctx, db, id, "SKU-INS", "InsWidget",
		true, &weightKg, sql.NullFloat64{Float64: 3.5, Valid: true},
		[]string{"tag"}, nil, nil, 7)
	if err != nil {
		t.Fatal(err)
	}
	if product == nil {
		t.Fatal("expected product, got nil")
	}
	if product.Id != id {
		t.Fatalf("expected id %s, got %s", id, product.Id)
	}
	if product.Name != "InsWidget" {
		t.Fatalf("expected InsWidget, got %s", product.Name)
	}
	if product.StockCount != 7 {
		t.Fatalf("expected stock_count 7, got %d", product.StockCount)
	}
}

func TestGetProduct(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	id := "550e8400-e29b-41d4-a716-446655440001"
	_, err := gen.InsertProduct(ctx, db, id, "SKU-GET", "GetWidget",
		true, nil, sql.NullFloat64{}, []string{}, nil, nil, 3)
	if err != nil {
		t.Fatal(err)
	}

	product, err := gen.GetProduct(ctx, db, id)
	if err != nil {
		t.Fatal(err)
	}
	if product == nil {
		t.Fatal("expected product, got nil")
	}
	if product.Name != "GetWidget" {
		t.Fatalf("expected GetWidget, got %s", product.Name)
	}
	if product.StockCount != 3 {
		t.Fatalf("expected stock_count 3, got %d", product.StockCount)
	}
}

func TestListActiveProducts(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	id1 := "550e8400-e29b-41d4-a716-446655440010"
	id2 := "550e8400-e29b-41d4-a716-446655440011"
	_, err := gen.InsertProduct(ctx, db, id1, "SKU-A", "Active",
		true, nil, sql.NullFloat64{}, []string{}, nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = gen.InsertProduct(ctx, db, id2, "SKU-B", "Inactive",
		false, nil, sql.NullFloat64{}, []string{"archived"}, nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	active, err := gen.ListActiveProducts(ctx, db, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 1 {
		t.Fatalf("expected 1 active product, got %d", len(active))
	}
	if active[0].Name != "Active" {
		t.Fatalf("expected Active, got %s", active[0].Name)
	}

	inactive, err := gen.ListActiveProducts(ctx, db, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(inactive) != 1 {
		t.Fatalf("expected 1 inactive product, got %d", len(inactive))
	}
	if inactive[0].Name != "Inactive" {
		t.Fatalf("expected Inactive, got %s", inactive[0].Name)
	}
}

func TestUpsertProduct(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	id := "550e8400-e29b-41d4-a716-446655440020"

	// Initial insert
	_, err := gen.UpsertProduct(ctx, db, id, "SKU-U1", "Original", true, []string{"a"}, 5)
	if err != nil {
		t.Fatal(err)
	}

	// Upsert with changed name and stock
	result, err := gen.UpsertProduct(ctx, db, id, "SKU-U1", "Updated", true, []string{"a", "b"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected result, got nil")
	}
	if result.Id != id {
		t.Fatalf("expected id %s, got %s", id, result.Id)
	}
	if result.Name != "Updated" {
		t.Fatalf("expected Updated, got %s", result.Name)
	}
	if result.StockCount != 10 {
		t.Fatalf("expected stock_count 10, got %d", result.StockCount)
	}
	if len(result.Tags) != 2 || result.Tags[0] != "a" || result.Tags[1] != "b" {
		t.Fatalf("expected tags [a, b], got %v", result.Tags)
	}
}

// ─── IS NULL / IS NOT NULL tests ─────────────────────────────────────

func TestGetAuthorsWithNullBio(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	authors, err := gen.GetAuthorsWithNullBio(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(authors) != 1 {
		t.Fatalf("expected 1 author with null bio, got %d", len(authors))
	}
	if authors[0].Name != "Herbert" {
		t.Fatalf("expected Herbert, got %s", authors[0].Name)
	}
}

func TestGetAuthorsWithBio(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	authors, err := gen.GetAuthorsWithBio(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(authors) != 2 {
		t.Fatalf("expected 2 authors with bio, got %d", len(authors))
	}
	// Ordered by name
	if authors[0].Name != "Asimov" {
		t.Fatalf("expected Asimov first, got %s", authors[0].Name)
	}
	if authors[1].Name != "Le Guin" {
		t.Fatalf("expected Le Guin second, got %s", authors[1].Name)
	}
}

// ─── Date BETWEEN tests ───────────────────────────────────────────────

func TestGetBooksPublishedBetween(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	early, err := gen.GetBooksPublishedBetween(ctx, db,
		sql.NullTime{Time: time.Date(1950, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true},
		sql.NullTime{Time: time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(early) != 2 {
		t.Fatalf("expected 2 early books, got %d", len(early))
	}
	earlyTitles := map[string]bool{}
	for _, r := range early {
		earlyTitles[r.Title] = true
	}
	if !earlyTitles["Foundation"] {
		t.Fatal("expected Foundation in early range")
	}
	if !earlyTitles["I Robot"] {
		t.Fatal("expected I Robot in early range")
	}

	later, err := gen.GetBooksPublishedBetween(ctx, db,
		sql.NullTime{Time: time.Date(1961, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true},
		sql.NullTime{Time: time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(later) != 2 {
		t.Fatalf("expected 2 later books, got %d", len(later))
	}
	laterTitles := map[string]bool{}
	for _, r := range later {
		laterTitles[r.Title] = true
	}
	if !laterTitles["Dune"] {
		t.Fatal("expected Dune in later range")
	}
	if !laterTitles["Earthsea"] {
		t.Fatal("expected Earthsea in later range")
	}
}

// ─── DISTINCT tests ───────────────────────────────────────────────────

func TestGetDistinctGenres(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	genres, err := gen.GetDistinctGenres(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(genres) != 2 {
		t.Fatalf("expected 2 genres, got %d", len(genres))
	}
	if genres[0].Genre != "fantasy" {
		t.Fatalf("expected fantasy first, got %s", genres[0].Genre)
	}
	if genres[1].Genre != "sci-fi" {
		t.Fatalf("expected sci-fi second, got %s", genres[1].Genre)
	}
}

// ─── LEFT JOIN aggregate tests ────────────────────────────────────────

func TestGetBooksWithSalesCount(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetBooksWithSalesCount(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(rows))
	}
	// Foundation: qty 2 (Alice) + qty 1 (Bob) = 3, should be first (DESC)
	if rows[0].Title != "Foundation" {
		t.Fatalf("expected Foundation first, got %s", rows[0].Title)
	}
	if rows[0].TotalQuantity != 3 {
		t.Fatalf("expected total_quantity 3, got %d", rows[0].TotalQuantity)
	}

	// I Robot: never sold → 0
	var iRobot *gen.GetBooksWithSalesCountRow
	for i := range rows {
		if rows[i].Title == "I Robot" {
			iRobot = &rows[i]
		}
	}
	if iRobot == nil {
		t.Fatal("expected I Robot in results")
	}
	if iRobot.TotalQuantity != 0 {
		t.Fatalf("expected total_quantity 0 for I Robot, got %d", iRobot.TotalQuantity)
	}
}

// ─── :one COUNT aggregate ─────────────────────────────────────────────

func TestCountSaleItems(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	// Sale 1 (Alice): Foundation + Dune = 2 items
	count1, err := gen.CountSaleItems(ctx, db, 1)
	if err != nil {
		t.Fatal(err)
	}
	if count1 == nil {
		t.Fatal("expected count, got nil")
	}
	if count1.ItemCount != 2 {
		t.Fatalf("expected 2 items in sale 1, got %d", count1.ItemCount)
	}

	// Sale 2 (Bob): Earthsea + Foundation = 2 items
	count2, err := gen.CountSaleItems(ctx, db, 2)
	if err != nil {
		t.Fatal(err)
	}
	if count2 == nil {
		t.Fatal("expected count, got nil")
	}
	if count2.ItemCount != 2 {
		t.Fatalf("expected 2 items in sale 2, got %d", count2.ItemCount)
	}
}

// ─── MIN/MAX/SUM/AVG aggregate tests ─────────────────────────────────

func TestGetSaleItemQuantityAggregates(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	// Sale items: Foundation qty 2 (Alice), Dune qty 1 (Alice),
	//             Earthsea qty 1 (Bob), Foundation qty 1 (Bob)
	// → min=1, max=2, sum=5
	row, err := gen.GetSaleItemQuantityAggregates(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if row == nil {
		t.Fatal("expected row, got nil")
	}
	if !row.MinQty.Valid || row.MinQty.Int32 != 1 {
		t.Fatalf("expected min_qty 1, got %v", row.MinQty)
	}
	if !row.MaxQty.Valid || row.MaxQty.Int32 != 2 {
		t.Fatalf("expected max_qty 2, got %v", row.MaxQty)
	}
	if !row.SumQty.Valid || row.SumQty.Int64 != 5 {
		t.Fatalf("expected sum_qty 5, got %v", row.SumQty)
	}
	if !row.AvgQty.Valid {
		t.Fatal("expected avg_qty to be non-null")
	}
}

func TestGetBookPriceAggregates(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	seed(t, ctx, db)

	// Book prices: 9.99, 7.99, 12.99, 8.99
	// → min=7.99, max=12.99, sum=39.96
	row, err := gen.GetBookPriceAggregates(ctx, db)
	if err != nil {
		t.Fatal(err)
	}
	if row == nil {
		t.Fatal("expected row, got nil")
	}
	if !row.MinPrice.Valid || row.MinPrice.String != "7.99" {
		t.Fatalf("expected min_price 7.99, got %v", row.MinPrice)
	}
	if !row.MaxPrice.Valid || row.MaxPrice.String != "12.99" {
		t.Fatalf("expected max_price 12.99, got %v", row.MaxPrice)
	}
	if !row.SumPrice.Valid || row.SumPrice.String != "39.96" {
		t.Fatalf("expected sum_price 39.96, got %v", row.SumPrice)
	}
	if !row.AvgPrice.Valid {
		t.Fatal("expected avg_price to be non-null")
	}
}

// ─── Product with all fields ──────────────────────────────────────────

func TestInsertProductAllFields(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	id := "550e8400-e29b-41d4-a716-446655440030"
	weightKg := float32(1.5)
	metadata := []byte(`{"color":"red"}`)
	thumbnail := []byte{0x01, 0x02}

	product, err := gen.InsertProduct(ctx, db, id, "SKU-001", "Widget",
		true, &weightKg, sql.NullFloat64{Float64: 4.2, Valid: true},
		[]string{"gadget", "tool"}, &metadata, thumbnail, 10)
	if err != nil {
		t.Fatal(err)
	}
	if product == nil {
		t.Fatal("expected product, got nil")
	}
	if product.Id != id {
		t.Fatalf("expected id %s, got %s", id, product.Id)
	}
	if product.Sku != "SKU-001" {
		t.Fatalf("expected SKU-001, got %s", product.Sku)
	}
	if product.Name != "Widget" {
		t.Fatalf("expected Widget, got %s", product.Name)
	}
	if !product.Active {
		t.Fatal("expected active to be true")
	}
	if product.WeightKg == nil || *product.WeightKg != 1.5 {
		t.Fatalf("expected weight_kg 1.5, got %v", product.WeightKg)
	}
	if !product.Rating.Valid || product.Rating.Float64 != 4.2 {
		t.Fatalf("expected rating 4.2, got %v", product.Rating)
	}
	if len(product.Tags) != 2 || product.Tags[0] != "gadget" || product.Tags[1] != "tool" {
		t.Fatalf("expected tags [gadget, tool], got %v", product.Tags)
	}
	if product.Metadata == nil {
		t.Fatal("expected metadata to be non-nil")
	}
	if len(product.Thumbnail) != 2 || product.Thumbnail[0] != 0x01 || product.Thumbnail[1] != 0x02 {
		t.Fatalf("expected thumbnail [0x01, 0x02], got %v", product.Thumbnail)
	}
	if product.StockCount != 10 {
		t.Fatalf("expected stock_count 10, got %d", product.StockCount)
	}

	// Retrieve and verify
	fetched, err := gen.GetProduct(ctx, db, id)
	if err != nil {
		t.Fatal(err)
	}
	if fetched == nil {
		t.Fatal("expected fetched product, got nil")
	}
	if fetched.Name != "Widget" {
		t.Fatalf("expected Widget, got %s", fetched.Name)
	}
	if len(fetched.Tags) != 2 || fetched.Tags[0] != "gadget" || fetched.Tags[1] != "tool" {
		t.Fatalf("expected tags [gadget, tool], got %v", fetched.Tags)
	}
}

func TestProductWithNullOptionalFields(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	id := "550e8400-e29b-41d4-a716-446655440031"
	product, err := gen.InsertProduct(ctx, db, id, "SKU-NULL", "Bare",
		true, nil, sql.NullFloat64{}, []string{}, nil, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if product == nil {
		t.Fatal("expected product, got nil")
	}
	if product.WeightKg != nil {
		t.Fatalf("expected nil weight_kg, got %v", product.WeightKg)
	}
	if product.Rating.Valid {
		t.Fatal("expected null rating")
	}
	if len(product.Tags) != 0 {
		t.Fatalf("expected empty tags, got %v", product.Tags)
	}
	if product.Metadata != nil {
		t.Fatalf("expected nil metadata, got %v", product.Metadata)
	}
	if product.Thumbnail != nil {
		t.Fatalf("expected nil thumbnail, got %v", product.Thumbnail)
	}
}
