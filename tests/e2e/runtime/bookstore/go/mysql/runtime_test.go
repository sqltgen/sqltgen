package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	gen "e2e-go-mysql/gen"
	_ "github.com/go-sql-driver/mysql"
)

const defaultDSN = "sqltgen:sqltgen@tcp(localhost:13306)/sqltgen_e2e?parseTime=true"

func dsn() string {
	if v := os.Getenv("MYSQL_DSN"); v != "" {
		return v
	}
	return defaultDSN
}

// adminDSN returns a DSN without a database name so we can CREATE/DROP databases.
func adminDSN() string {
	d := dsn()
	// Strip the database name from the DSN (everything between the last / and ?)
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

	// Build a DSN pointing at the new database.
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

	// Apply schema DDL.
	schema, err := os.ReadFile("../../../../fixtures/bookstore/mysql/schema.sql")
	if err != nil {
		db.Close()
		admin.Exec("DROP DATABASE IF EXISTS " + dbName)
		admin.Close()
		t.Fatalf("read schema file: %v", err)
	}

	// Split by semicolon and execute each statement individually.
	stmts := strings.Split(string(schema), ";")
	for _, stmt := range stmts {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			admin.Exec("DROP DATABASE IF EXISTS " + dbName)
			admin.Close()
			t.Fatalf("apply schema: %v\nstatement: %s", err, stmt)
		}
	}

	cleanup := func() {
		db.Close()
		admin.Exec("DROP DATABASE IF EXISTS " + dbName)
		admin.Close()
	}
	return db, cleanup
}

// seedIDs holds the auto-increment IDs assigned during seeding.
type seedIDs struct {
	authorAlice   int64
	authorBob     int64
	authorCharlie int64
	bookAlice1    int64
	bookAlice2    int64
	bookBob1      int64
	bookCharlie1  int64
	customerDan   int64
	customerEve   int64
	sale1         int64
	sale2         int64
}

func lastInsertID(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var id int64
	if err := db.QueryRow("SELECT LAST_INSERT_ID()").Scan(&id); err != nil {
		t.Fatalf("LAST_INSERT_ID: %v", err)
	}
	return id
}

func seed(t *testing.T, ctx context.Context, db *sql.DB) seedIDs {
	t.Helper()
	var ids seedIDs

	// Authors
	err := gen.CreateAuthor(ctx, db, "Alice", sql.NullString{String: "Alice writes fiction", Valid: true}, sql.NullInt32{Int32: 1980, Valid: true})
	if err != nil {
		t.Fatalf("create author Alice: %v", err)
	}
	ids.authorAlice = lastInsertID(t, db)

	err = gen.CreateAuthor(ctx, db, "Bob", sql.NullString{Valid: false}, sql.NullInt32{Int32: 1975, Valid: true})
	if err != nil {
		t.Fatalf("create author Bob: %v", err)
	}
	ids.authorBob = lastInsertID(t, db)

	err = gen.CreateAuthor(ctx, db, "Charlie", sql.NullString{String: "Charlie writes nonfiction", Valid: true}, sql.NullInt32{Valid: false})
	if err != nil {
		t.Fatalf("create author Charlie: %v", err)
	}
	ids.authorCharlie = lastInsertID(t, db)

	// Books
	err = gen.CreateBook(ctx, db, ids.authorAlice, "Alpha Book", "fiction", "19.99",
		sql.NullTime{Time: time.Date(2020, 6, 15, 0, 0, 0, 0, time.UTC), Valid: true})
	if err != nil {
		t.Fatalf("create book Alpha: %v", err)
	}
	ids.bookAlice1 = lastInsertID(t, db)

	err = gen.CreateBook(ctx, db, ids.authorAlice, "Beta Book", "science", "29.99",
		sql.NullTime{Time: time.Date(2021, 3, 10, 0, 0, 0, 0, time.UTC), Valid: true})
	if err != nil {
		t.Fatalf("create book Beta: %v", err)
	}
	ids.bookAlice2 = lastInsertID(t, db)

	err = gen.CreateBook(ctx, db, ids.authorBob, "Gamma Book", "fiction", "9.99",
		sql.NullTime{Time: time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true})
	if err != nil {
		t.Fatalf("create book Gamma: %v", err)
	}
	ids.bookBob1 = lastInsertID(t, db)

	err = gen.CreateBook(ctx, db, ids.authorCharlie, "Delta Book", "history", "14.99",
		sql.NullTime{Valid: false})
	if err != nil {
		t.Fatalf("create book Delta: %v", err)
	}
	ids.bookCharlie1 = lastInsertID(t, db)

	// Customers
	err = gen.CreateCustomer(ctx, db, "Dan", "dan@example.com")
	if err != nil {
		t.Fatalf("create customer Dan: %v", err)
	}
	ids.customerDan = lastInsertID(t, db)

	err = gen.CreateCustomer(ctx, db, "Eve", "eve@example.com")
	if err != nil {
		t.Fatalf("create customer Eve: %v", err)
	}
	ids.customerEve = lastInsertID(t, db)

	// Sales
	err = gen.CreateSale(ctx, db, ids.customerDan)
	if err != nil {
		t.Fatalf("create sale 1: %v", err)
	}
	ids.sale1 = lastInsertID(t, db)

	err = gen.CreateSale(ctx, db, ids.customerEve)
	if err != nil {
		t.Fatalf("create sale 2: %v", err)
	}
	ids.sale2 = lastInsertID(t, db)

	// Sale items: Dan buys Alpha (qty 2) and Gamma (qty 1)
	err = gen.AddSaleItem(ctx, db, ids.sale1, ids.bookAlice1, 2, "19.99")
	if err != nil {
		t.Fatalf("add sale item 1: %v", err)
	}
	err = gen.AddSaleItem(ctx, db, ids.sale1, ids.bookBob1, 1, "9.99")
	if err != nil {
		t.Fatalf("add sale item 2: %v", err)
	}

	// Eve buys Beta (qty 3)
	err = gen.AddSaleItem(ctx, db, ids.sale2, ids.bookAlice2, 3, "29.99")
	if err != nil {
		t.Fatalf("add sale item 3: %v", err)
	}

	return ids
}

// --- Author CRUD tests ---

func TestCreateAuthor(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	err := gen.CreateAuthor(ctx, db, "Zara", sql.NullString{String: "A test author", Valid: true}, sql.NullInt32{Int32: 1990, Valid: true})
	if err != nil {
		t.Fatalf("CreateAuthor: %v", err)
	}

	// Verify the author was inserted.
	id := lastInsertID(t, db)
	a, err := gen.GetAuthor(ctx, db, id)
	if err != nil {
		t.Fatalf("GetAuthor: %v", err)
	}
	if a == nil {
		t.Fatal("expected author, got nil")
	}
	if a.Name != "Zara" {
		t.Errorf("name = %q, want %q", a.Name, "Zara")
	}
}

func TestGetAuthor(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	ids := seed(t, ctx, db)

	a, err := gen.GetAuthor(ctx, db, ids.authorAlice)
	if err != nil {
		t.Fatalf("GetAuthor: %v", err)
	}
	if a == nil {
		t.Fatal("expected author, got nil")
	}
	if a.Name != "Alice" {
		t.Errorf("name = %q, want %q", a.Name, "Alice")
	}
	if !a.Bio.Valid || a.Bio.String != "Alice writes fiction" {
		t.Errorf("bio = %v, want 'Alice writes fiction'", a.Bio)
	}
	if !a.BirthYear.Valid || a.BirthYear.Int32 != 1980 {
		t.Errorf("birth_year = %v, want 1980", a.BirthYear)
	}

	// Non-existent author returns nil.
	a2, err := gen.GetAuthor(ctx, db, 999999)
	if err != nil {
		t.Fatalf("GetAuthor non-existent: %v", err)
	}
	if a2 != nil {
		t.Errorf("expected nil for non-existent author, got %+v", a2)
	}
}

func TestListAuthors(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	authors, err := gen.ListAuthors(ctx, db)
	if err != nil {
		t.Fatalf("ListAuthors: %v", err)
	}
	if len(authors) != 3 {
		t.Fatalf("len = %d, want 3", len(authors))
	}
	// Ordered by name: Alice, Bob, Charlie
	if authors[0].Name != "Alice" {
		t.Errorf("authors[0].Name = %q, want Alice", authors[0].Name)
	}
	if authors[1].Name != "Bob" {
		t.Errorf("authors[1].Name = %q, want Bob", authors[1].Name)
	}
	if authors[2].Name != "Charlie" {
		t.Errorf("authors[2].Name = %q, want Charlie", authors[2].Name)
	}
}

func TestUpdateAuthorBio(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	ids := seed(t, ctx, db)

	newBio := sql.NullString{String: "Bob now has a bio", Valid: true}
	err := gen.UpdateAuthorBio(ctx, db, newBio, ids.authorBob)
	if err != nil {
		t.Fatalf("UpdateAuthorBio: %v", err)
	}

	a, err := gen.GetAuthor(ctx, db, ids.authorBob)
	if err != nil {
		t.Fatalf("GetAuthor: %v", err)
	}
	if !a.Bio.Valid || a.Bio.String != "Bob now has a bio" {
		t.Errorf("bio = %v, want 'Bob now has a bio'", a.Bio)
	}

	// Set bio to NULL.
	err = gen.UpdateAuthorBio(ctx, db, sql.NullString{Valid: false}, ids.authorBob)
	if err != nil {
		t.Fatalf("UpdateAuthorBio to NULL: %v", err)
	}
	a, err = gen.GetAuthor(ctx, db, ids.authorBob)
	if err != nil {
		t.Fatalf("GetAuthor after null: %v", err)
	}
	if a.Bio.Valid {
		t.Errorf("bio should be NULL, got %v", a.Bio)
	}
}

func TestDeleteAuthor(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	// Create a standalone author (no books) so FK constraints don't block deletion.
	err := gen.CreateAuthor(ctx, db, "Temp", sql.NullString{Valid: false}, sql.NullInt32{Valid: false})
	if err != nil {
		t.Fatalf("CreateAuthor: %v", err)
	}
	id := lastInsertID(t, db)

	err = gen.DeleteAuthor(ctx, db, id)
	if err != nil {
		t.Fatalf("DeleteAuthor: %v", err)
	}

	a, err := gen.GetAuthor(ctx, db, id)
	if err != nil {
		t.Fatalf("GetAuthor: %v", err)
	}
	if a != nil {
		t.Errorf("expected nil after delete, got %+v", a)
	}
}

// --- Book CRUD tests ---

func TestCreateBook(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	ids := seed(t, ctx, db)

	err := gen.CreateBook(ctx, db, ids.authorAlice, "New Book", "mystery", "24.99",
		sql.NullTime{Time: time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true})
	if err != nil {
		t.Fatalf("CreateBook: %v", err)
	}
	bookID := lastInsertID(t, db)

	b, err := gen.GetBook(ctx, db, bookID)
	if err != nil {
		t.Fatalf("GetBook: %v", err)
	}
	if b == nil {
		t.Fatal("expected book, got nil")
	}
	if b.Title != "New Book" {
		t.Errorf("title = %q, want 'New Book'", b.Title)
	}
	if b.Genre != "mystery" {
		t.Errorf("genre = %q, want 'mystery'", b.Genre)
	}
	if b.Price != "24.99" {
		t.Errorf("price = %q, want '24.99'", b.Price)
	}
}

func TestGetBook(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	ids := seed(t, ctx, db)

	b, err := gen.GetBook(ctx, db, ids.bookAlice1)
	if err != nil {
		t.Fatalf("GetBook: %v", err)
	}
	if b == nil {
		t.Fatal("expected book, got nil")
	}
	if b.Title != "Alpha Book" {
		t.Errorf("title = %q, want 'Alpha Book'", b.Title)
	}
	if b.AuthorId != ids.authorAlice {
		t.Errorf("author_id = %d, want %d", b.AuthorId, ids.authorAlice)
	}

	// Non-existent book returns nil.
	b2, err := gen.GetBook(ctx, db, 999999)
	if err != nil {
		t.Fatalf("GetBook non-existent: %v", err)
	}
	if b2 != nil {
		t.Errorf("expected nil for non-existent book, got %+v", b2)
	}
}

func TestGetBooksByIds(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	ids := seed(t, ctx, db)

	books, err := gen.GetBooksByIds(ctx, db, []int64{ids.bookAlice1, ids.bookBob1})
	if err != nil {
		t.Fatalf("GetBooksByIds: %v", err)
	}
	if len(books) != 2 {
		t.Fatalf("len = %d, want 2", len(books))
	}
	// Ordered by title: Alpha Book, Gamma Book
	if books[0].Title != "Alpha Book" {
		t.Errorf("books[0].Title = %q, want 'Alpha Book'", books[0].Title)
	}
	if books[1].Title != "Gamma Book" {
		t.Errorf("books[1].Title = %q, want 'Gamma Book'", books[1].Title)
	}

	// Empty slice returns no results.
	empty, err := gen.GetBooksByIds(ctx, db, []int64{})
	if err != nil {
		t.Fatalf("GetBooksByIds empty: %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("expected 0 results for empty ids, got %d", len(empty))
	}
}

func TestListBooksByGenre(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	books, err := gen.ListBooksByGenre(ctx, db, "fiction")
	if err != nil {
		t.Fatalf("ListBooksByGenre: %v", err)
	}
	if len(books) != 2 {
		t.Fatalf("len = %d, want 2", len(books))
	}
	// Ordered by title: Alpha Book, Gamma Book
	if books[0].Title != "Alpha Book" {
		t.Errorf("books[0].Title = %q, want 'Alpha Book'", books[0].Title)
	}
	if books[1].Title != "Gamma Book" {
		t.Errorf("books[1].Title = %q, want 'Gamma Book'", books[1].Title)
	}

	// Non-existent genre returns empty.
	none, err := gen.ListBooksByGenre(ctx, db, "romance")
	if err != nil {
		t.Fatalf("ListBooksByGenre romance: %v", err)
	}
	if len(none) != 0 {
		t.Errorf("expected 0 for romance, got %d", len(none))
	}
}

func TestListBooksByGenreOrAll(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	// Specific genre
	books, err := gen.ListBooksByGenreOrAll(ctx, db, "fiction")
	if err != nil {
		t.Fatalf("ListBooksByGenreOrAll fiction: %v", err)
	}
	if len(books) != 2 {
		t.Fatalf("fiction len = %d, want 2", len(books))
	}

	// "all" returns everything
	all, err := gen.ListBooksByGenreOrAll(ctx, db, "all")
	if err != nil {
		t.Fatalf("ListBooksByGenreOrAll all: %v", err)
	}
	if len(all) != 4 {
		t.Errorf("all len = %d, want 4", len(all))
	}
}

// --- Customer tests ---

func TestCreateCustomer(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	err := gen.CreateCustomer(ctx, db, "Frank", "frank@example.com")
	if err != nil {
		t.Fatalf("CreateCustomer: %v", err)
	}
	// Verify via direct query.
	var name string
	err = db.QueryRowContext(ctx, "SELECT name FROM customer WHERE email = ?", "frank@example.com").Scan(&name)
	if err != nil {
		t.Fatalf("verify customer: %v", err)
	}
	if name != "Frank" {
		t.Errorf("name = %q, want Frank", name)
	}
}

// --- Sale tests ---

func TestCreateSale(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	err := gen.CreateCustomer(ctx, db, "Grace", "grace@example.com")
	if err != nil {
		t.Fatalf("CreateCustomer: %v", err)
	}
	custID := lastInsertID(t, db)

	err = gen.CreateSale(ctx, db, custID)
	if err != nil {
		t.Fatalf("CreateSale: %v", err)
	}
	saleID := lastInsertID(t, db)

	var customerID int64
	err = db.QueryRowContext(ctx, "SELECT customer_id FROM sale WHERE id = ?", saleID).Scan(&customerID)
	if err != nil {
		t.Fatalf("verify sale: %v", err)
	}
	if customerID != custID {
		t.Errorf("customer_id = %d, want %d", customerID, custID)
	}
}

func TestAddSaleItem(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	ids := seed(t, ctx, db)

	// Add another item to sale1.
	err := gen.AddSaleItem(ctx, db, ids.sale1, ids.bookCharlie1, 5, "14.99")
	if err != nil {
		t.Fatalf("AddSaleItem: %v", err)
	}

	var qty int32
	err = db.QueryRowContext(ctx,
		"SELECT quantity FROM sale_item WHERE sale_id = ? AND book_id = ?",
		ids.sale1, ids.bookCharlie1).Scan(&qty)
	if err != nil {
		t.Fatalf("verify sale item: %v", err)
	}
	if qty != 5 {
		t.Errorf("quantity = %d, want 5", qty)
	}
}

// --- Join / multi-table query tests ---

func TestListBooksWithAuthor(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	rows, err := gen.ListBooksWithAuthor(ctx, db)
	if err != nil {
		t.Fatalf("ListBooksWithAuthor: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("len = %d, want 4", len(rows))
	}
	// Ordered by title: Alpha, Beta, Delta, Gamma
	if rows[0].Title != "Alpha Book" || rows[0].AuthorName != "Alice" {
		t.Errorf("rows[0] = {%q, %q}, want {Alpha Book, Alice}", rows[0].Title, rows[0].AuthorName)
	}
	if rows[2].Title != "Delta Book" || rows[2].AuthorName != "Charlie" {
		t.Errorf("rows[2] = {%q, %q}, want {Delta Book, Charlie}", rows[2].Title, rows[2].AuthorName)
	}
}

func TestGetBooksNeverOrdered(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	// Delta Book has no sale items.
	books, err := gen.GetBooksNeverOrdered(ctx, db)
	if err != nil {
		t.Fatalf("GetBooksNeverOrdered: %v", err)
	}
	if len(books) != 1 {
		t.Fatalf("len = %d, want 1", len(books))
	}
	if books[0].Title != "Delta Book" {
		t.Errorf("title = %q, want 'Delta Book'", books[0].Title)
	}
}

func TestGetTopSellingBooks(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	rows, err := gen.GetTopSellingBooks(ctx, db)
	if err != nil {
		t.Fatalf("GetTopSellingBooks: %v", err)
	}
	// 3 books have sales: Beta (3), Alpha (2), Gamma (1)
	if len(rows) != 3 {
		t.Fatalf("len = %d, want 3", len(rows))
	}
	if rows[0].Title != "Beta Book" {
		t.Errorf("rows[0].Title = %q, want 'Beta Book'", rows[0].Title)
	}
	if !rows[0].UnitsSold.Valid || rows[0].UnitsSold.String != "3" {
		t.Errorf("rows[0].UnitsSold = %v, want 3", rows[0].UnitsSold)
	}
}

func TestGetBestCustomers(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	rows, err := gen.GetBestCustomers(ctx, db)
	if err != nil {
		t.Fatalf("GetBestCustomers: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("len = %d, want 2", len(rows))
	}
	// Eve: 3*29.99 = 89.97; Dan: 2*19.99 + 1*9.99 = 49.97
	if rows[0].Name != "Eve" {
		t.Errorf("rows[0].Name = %q, want Eve", rows[0].Name)
	}
	if rows[1].Name != "Dan" {
		t.Errorf("rows[1].Name = %q, want Dan", rows[1].Name)
	}
}

func TestCountBooksByGenre(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	rows, err := gen.CountBooksByGenre(ctx, db)
	if err != nil {
		t.Fatalf("CountBooksByGenre: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("len = %d, want 3", len(rows))
	}
	// Ordered by genre: fiction (2), history (1), science (1)
	found := map[string]int64{}
	for _, r := range rows {
		found[r.Genre] = r.BookCount
	}
	if found["fiction"] != 2 {
		t.Errorf("fiction count = %d, want 2", found["fiction"])
	}
	if found["history"] != 1 {
		t.Errorf("history count = %d, want 1", found["history"])
	}
	if found["science"] != 1 {
		t.Errorf("science count = %d, want 1", found["science"])
	}
}

func TestListBooksWithLimit(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	// First page: 2 books.
	page1, err := gen.ListBooksWithLimit(ctx, db, 2, 0)
	if err != nil {
		t.Fatalf("ListBooksWithLimit page1: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("page1 len = %d, want 2", len(page1))
	}
	if page1[0].Title != "Alpha Book" {
		t.Errorf("page1[0].Title = %q, want 'Alpha Book'", page1[0].Title)
	}
	if page1[1].Title != "Beta Book" {
		t.Errorf("page1[1].Title = %q, want 'Beta Book'", page1[1].Title)
	}

	// Second page: 2 books.
	page2, err := gen.ListBooksWithLimit(ctx, db, 2, 2)
	if err != nil {
		t.Fatalf("ListBooksWithLimit page2: %v", err)
	}
	if len(page2) != 2 {
		t.Fatalf("page2 len = %d, want 2", len(page2))
	}
	if page2[0].Title != "Delta Book" {
		t.Errorf("page2[0].Title = %q, want 'Delta Book'", page2[0].Title)
	}
}

func TestSearchBooksByTitle(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	rows, err := gen.SearchBooksByTitle(ctx, db, "%Alpha%")
	if err != nil {
		t.Fatalf("SearchBooksByTitle: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("len = %d, want 1", len(rows))
	}
	if rows[0].Title != "Alpha Book" {
		t.Errorf("title = %q, want 'Alpha Book'", rows[0].Title)
	}

	// No match.
	none, err := gen.SearchBooksByTitle(ctx, db, "%ZZZ%")
	if err != nil {
		t.Fatalf("SearchBooksByTitle no match: %v", err)
	}
	if len(none) != 0 {
		t.Errorf("expected 0, got %d", len(none))
	}
}

func TestGetBooksByPriceRange(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	// Range 10.00-20.00 should include Alpha (19.99) and Delta (14.99)
	rows, err := gen.GetBooksByPriceRange(ctx, db, "10.00", "20.00")
	if err != nil {
		t.Fatalf("GetBooksByPriceRange: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("len = %d, want 2", len(rows))
	}
	// Ordered by price: 14.99 (Delta), 19.99 (Alpha)
	if rows[0].Title != "Delta Book" {
		t.Errorf("rows[0].Title = %q, want 'Delta Book'", rows[0].Title)
	}
	if rows[1].Title != "Alpha Book" {
		t.Errorf("rows[1].Title = %q, want 'Alpha Book'", rows[1].Title)
	}
}

func TestGetBooksInGenres(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	rows, err := gen.GetBooksInGenres(ctx, db, "fiction", "history", "mystery")
	if err != nil {
		t.Fatalf("GetBooksInGenres: %v", err)
	}
	// fiction: Alpha, Gamma; history: Delta => 3 books
	if len(rows) != 3 {
		t.Fatalf("len = %d, want 3", len(rows))
	}
	// Ordered by title: Alpha, Delta, Gamma
	if rows[0].Title != "Alpha Book" {
		t.Errorf("rows[0].Title = %q, want 'Alpha Book'", rows[0].Title)
	}
	if rows[1].Title != "Delta Book" {
		t.Errorf("rows[1].Title = %q, want 'Delta Book'", rows[1].Title)
	}
	if rows[2].Title != "Gamma Book" {
		t.Errorf("rows[2].Title = %q, want 'Gamma Book'", rows[2].Title)
	}
}

func TestGetBookPriceLabel(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	rows, err := gen.GetBookPriceLabel(ctx, db, "15.00")
	if err != nil {
		t.Fatalf("GetBookPriceLabel: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("len = %d, want 4", len(rows))
	}
	// Ordered by title: Alpha (19.99 > 15 => expensive), Beta (29.99 => expensive),
	// Delta (14.99 <= 15 => affordable), Gamma (9.99 => affordable)
	labels := map[string]string{}
	for _, r := range rows {
		labels[r.Title] = r.PriceLabel
	}
	if labels["Alpha Book"] != "expensive" {
		t.Errorf("Alpha label = %q, want expensive", labels["Alpha Book"])
	}
	if labels["Gamma Book"] != "affordable" {
		t.Errorf("Gamma label = %q, want affordable", labels["Gamma Book"])
	}
}

func TestGetBookPriceOrDefault(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	rows, err := gen.GetBookPriceOrDefault(ctx, db, sql.NullString{String: "0.00", Valid: true})
	if err != nil {
		t.Fatalf("GetBookPriceOrDefault: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("len = %d, want 4", len(rows))
	}
	// All books have non-null prices, so COALESCE returns the actual price.
	for _, r := range rows {
		if r.EffectivePrice == "0.00" {
			t.Errorf("book %q should have actual price, got default 0.00", r.Title)
		}
	}
}

func TestDeleteBookById(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	// Create a standalone author + book with no FK references from sale_item.
	err := gen.CreateAuthor(ctx, db, "Temp", sql.NullString{Valid: false}, sql.NullInt32{Valid: false})
	if err != nil {
		t.Fatalf("CreateAuthor: %v", err)
	}
	authorID := lastInsertID(t, db)

	err = gen.CreateBook(ctx, db, authorID, "Ephemeral", "temp", "1.00", sql.NullTime{Valid: false})
	if err != nil {
		t.Fatalf("CreateBook: %v", err)
	}
	bookID := lastInsertID(t, db)

	affected, err := gen.DeleteBookById(ctx, db, bookID)
	if err != nil {
		t.Fatalf("DeleteBookById: %v", err)
	}
	if affected != 1 {
		t.Errorf("affected = %d, want 1", affected)
	}

	// Verify deletion.
	b, err := gen.GetBook(ctx, db, bookID)
	if err != nil {
		t.Fatalf("GetBook after delete: %v", err)
	}
	if b != nil {
		t.Errorf("expected nil after delete, got %+v", b)
	}

	// Deleting non-existent book returns 0 rows affected.
	affected2, err := gen.DeleteBookById(ctx, db, 999999)
	if err != nil {
		t.Fatalf("DeleteBookById non-existent: %v", err)
	}
	if affected2 != 0 {
		t.Errorf("affected = %d, want 0", affected2)
	}
}

func TestGetGenresWithManyBooks(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	// Only fiction has 2 books, the rest have 1.
	rows, err := gen.GetGenresWithManyBooks(ctx, db, 1)
	if err != nil {
		t.Fatalf("GetGenresWithManyBooks: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("len = %d, want 1", len(rows))
	}
	if rows[0].Genre != "fiction" {
		t.Errorf("genre = %q, want fiction", rows[0].Genre)
	}
	if rows[0].BookCount != 2 {
		t.Errorf("count = %d, want 2", rows[0].BookCount)
	}
}

func TestGetBooksByAuthorParam(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	// Authors with birth_year > 1976: Alice (1980) => Alpha Book, Beta Book
	rows, err := gen.GetBooksByAuthorParam(ctx, db, sql.NullInt32{Int32: 1976, Valid: true})
	if err != nil {
		t.Fatalf("GetBooksByAuthorParam: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("len = %d, want 2", len(rows))
	}
	if rows[0].Title != "Alpha Book" {
		t.Errorf("rows[0].Title = %q, want 'Alpha Book'", rows[0].Title)
	}
	if rows[1].Title != "Beta Book" {
		t.Errorf("rows[1].Title = %q, want 'Beta Book'", rows[1].Title)
	}
}

func TestGetAllBookFields(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	ids := seed(t, ctx, db)

	books, err := gen.GetAllBookFields(ctx, db)
	if err != nil {
		t.Fatalf("GetAllBookFields: %v", err)
	}
	if len(books) != 4 {
		t.Fatalf("len = %d, want 4", len(books))
	}
	// Ordered by id
	if books[0].Id != ids.bookAlice1 {
		t.Errorf("books[0].Id = %d, want %d", books[0].Id, ids.bookAlice1)
	}
	if books[0].Title != "Alpha Book" {
		t.Errorf("books[0].Title = %q, want 'Alpha Book'", books[0].Title)
	}
	if books[0].Price != "19.99" {
		t.Errorf("books[0].Price = %q, want '19.99'", books[0].Price)
	}
}

func TestGetBooksNotByAuthor(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	// Books NOT by Alice => Gamma (Bob), Delta (Charlie)
	rows, err := gen.GetBooksNotByAuthor(ctx, db, "Alice")
	if err != nil {
		t.Fatalf("GetBooksNotByAuthor: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("len = %d, want 2", len(rows))
	}
	// Ordered by title: Delta, Gamma
	if rows[0].Title != "Delta Book" {
		t.Errorf("rows[0].Title = %q, want 'Delta Book'", rows[0].Title)
	}
	if rows[1].Title != "Gamma Book" {
		t.Errorf("rows[1].Title = %q, want 'Gamma Book'", rows[1].Title)
	}
}

func TestGetBooksWithRecentSales(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	// Sales were created "just now" via DEFAULT CURRENT_TIMESTAMP.
	// Anything after a year ago should match all sold books.
	cutoff := time.Now().Add(-365 * 24 * time.Hour)
	rows, err := gen.GetBooksWithRecentSales(ctx, db, cutoff)
	if err != nil {
		t.Fatalf("GetBooksWithRecentSales: %v", err)
	}
	// Alpha, Beta, Gamma have sales; Delta does not.
	if len(rows) != 3 {
		t.Fatalf("len = %d, want 3", len(rows))
	}

	// Far future cutoff: no results.
	future := time.Now().Add(365 * 24 * time.Hour)
	none, err := gen.GetBooksWithRecentSales(ctx, db, future)
	if err != nil {
		t.Fatalf("GetBooksWithRecentSales future: %v", err)
	}
	if len(none) != 0 {
		t.Errorf("expected 0 with future cutoff, got %d", len(none))
	}
}

func TestGetBookWithAuthorName(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	rows, err := gen.GetBookWithAuthorName(ctx, db)
	if err != nil {
		t.Fatalf("GetBookWithAuthorName: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("len = %d, want 4", len(rows))
	}
	// Ordered by title: Alpha, Beta, Delta, Gamma
	if rows[0].Title != "Alpha Book" {
		t.Errorf("rows[0].Title = %q, want 'Alpha Book'", rows[0].Title)
	}
	if !rows[0].AuthorName.Valid || rows[0].AuthorName.String != "Alice" {
		t.Errorf("rows[0].AuthorName = %v, want Alice", rows[0].AuthorName)
	}
	if rows[2].Title != "Delta Book" {
		t.Errorf("rows[2].Title = %q, want 'Delta Book'", rows[2].Title)
	}
	if !rows[2].AuthorName.Valid || rows[2].AuthorName.String != "Charlie" {
		t.Errorf("rows[2].AuthorName = %v, want Charlie", rows[2].AuthorName)
	}
}

func TestGetAuthorStats(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	rows, err := gen.GetAuthorStats(ctx, db)
	if err != nil {
		t.Fatalf("GetAuthorStats: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("len = %d, want 3", len(rows))
	}
	// Ordered by name: Alice, Bob, Charlie
	stats := map[string]gen.GetAuthorStatsRow{}
	for _, r := range rows {
		stats[r.Name] = r
	}
	if stats["Alice"].NumBooks != 2 {
		t.Errorf("Alice num_books = %d, want 2", stats["Alice"].NumBooks)
	}
	// Alice total_sold: Alpha (2) + Beta (3) = 5
	if stats["Alice"].TotalSold != "5" {
		t.Errorf("Alice total_sold = %q, want '5'", stats["Alice"].TotalSold)
	}
	if stats["Bob"].NumBooks != 1 {
		t.Errorf("Bob num_books = %d, want 1", stats["Bob"].NumBooks)
	}
	if stats["Charlie"].NumBooks != 1 {
		t.Errorf("Charlie num_books = %d, want 1", stats["Charlie"].NumBooks)
	}
	if stats["Charlie"].TotalSold != "0" {
		t.Errorf("Charlie total_sold = %q, want '0'", stats["Charlie"].TotalSold)
	}
}

// --- Product tests ---

func insertProduct(t *testing.T, ctx context.Context, db *sql.DB, id, sku, name string, active bool, weightKg *float32, rating sql.NullFloat64, metadata sql.NullString, thumbnail []byte, stockCount int16) {
	t.Helper()
	_, err := db.ExecContext(ctx,
		"INSERT INTO product (id, sku, name, active, weight_kg, rating, metadata, thumbnail, stock_count) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
		id, sku, name, active, weightKg, rating, metadata, thumbnail, stockCount)
	if err != nil {
		t.Fatalf("insert product %s: %v", id, err)
	}
}

func TestGetProduct(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	wt := float32(1.5)
	insertProduct(t, ctx, db,
		"prod-001", "SKU-001", "Widget", true,
		&wt,
		sql.NullFloat64{Float64: 4.5, Valid: true},
		sql.NullString{String: `{"color":"red"}`, Valid: true},
		[]byte{0xDE, 0xAD},
		10,
	)

	p, err := gen.GetProduct(ctx, db, "prod-001")
	if err != nil {
		t.Fatalf("GetProduct: %v", err)
	}
	if p == nil {
		t.Fatal("expected product, got nil")
	}
	if p.Name != "Widget" {
		t.Errorf("name = %q, want Widget", p.Name)
	}
	if p.Sku != "SKU-001" {
		t.Errorf("sku = %q, want SKU-001", p.Sku)
	}
	if !p.Active {
		t.Error("active = false, want true")
	}
	if p.WeightKg == nil || *p.WeightKg != 1.5 {
		t.Errorf("weight_kg = %v, want 1.5", p.WeightKg)
	}
	if !p.Rating.Valid || p.Rating.Float64 != 4.5 {
		t.Errorf("rating = %v, want 4.5", p.Rating)
	}
	if p.StockCount != 10 {
		t.Errorf("stock_count = %d, want 10", p.StockCount)
	}

	// Non-existent product returns nil.
	p2, err := gen.GetProduct(ctx, db, "nonexistent")
	if err != nil {
		t.Fatalf("GetProduct non-existent: %v", err)
	}
	if p2 != nil {
		t.Errorf("expected nil, got %+v", p2)
	}
}

func TestListActiveProducts(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()

	insertProduct(t, ctx, db, "p1", "SKU-A", "Active One", true, nil, sql.NullFloat64{}, sql.NullString{}, nil, 5)
	insertProduct(t, ctx, db, "p2", "SKU-B", "Active Two", true, nil, sql.NullFloat64{}, sql.NullString{}, nil, 3)
	insertProduct(t, ctx, db, "p3", "SKU-C", "Inactive", false, nil, sql.NullFloat64{}, sql.NullString{}, nil, 0)

	active, err := gen.ListActiveProducts(ctx, db, true)
	if err != nil {
		t.Fatalf("ListActiveProducts true: %v", err)
	}
	if len(active) != 2 {
		t.Fatalf("active len = %d, want 2", len(active))
	}

	inactive, err := gen.ListActiveProducts(ctx, db, false)
	if err != nil {
		t.Fatalf("ListActiveProducts false: %v", err)
	}
	if len(inactive) != 1 {
		t.Fatalf("inactive len = %d, want 1", len(inactive))
	}
	if inactive[0].Name != "Inactive" {
		t.Errorf("name = %q, want 'Inactive'", inactive[0].Name)
	}
}

// --- NULL-oriented tests ---

func TestGetAuthorsWithNullBio(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	rows, err := gen.GetAuthorsWithNullBio(ctx, db)
	if err != nil {
		t.Fatalf("GetAuthorsWithNullBio: %v", err)
	}
	// Bob has NULL bio.
	if len(rows) != 1 {
		t.Fatalf("len = %d, want 1", len(rows))
	}
	if rows[0].Name != "Bob" {
		t.Errorf("name = %q, want Bob", rows[0].Name)
	}
}

func TestGetAuthorsWithBio(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	rows, err := gen.GetAuthorsWithBio(ctx, db)
	if err != nil {
		t.Fatalf("GetAuthorsWithBio: %v", err)
	}
	// Alice and Charlie have bios.
	if len(rows) != 2 {
		t.Fatalf("len = %d, want 2", len(rows))
	}
	if rows[0].Name != "Alice" {
		t.Errorf("rows[0].Name = %q, want Alice", rows[0].Name)
	}
	if rows[1].Name != "Charlie" {
		t.Errorf("rows[1].Name = %q, want Charlie", rows[1].Name)
	}
}

// --- Date range test ---

func TestGetBooksPublishedBetween(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	start := sql.NullTime{Time: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC), Valid: true}
	end := sql.NullTime{Time: time.Date(2021, 12, 31, 0, 0, 0, 0, time.UTC), Valid: true}

	rows, err := gen.GetBooksPublishedBetween(ctx, db, start, end)
	if err != nil {
		t.Fatalf("GetBooksPublishedBetween: %v", err)
	}
	// Alpha (2020-06-15) and Beta (2021-03-10) are in range.
	if len(rows) != 2 {
		t.Fatalf("len = %d, want 2", len(rows))
	}
	if rows[0].Title != "Alpha Book" {
		t.Errorf("rows[0].Title = %q, want 'Alpha Book'", rows[0].Title)
	}
	if rows[1].Title != "Beta Book" {
		t.Errorf("rows[1].Title = %q, want 'Beta Book'", rows[1].Title)
	}
}

// --- Distinct genres ---

func TestGetDistinctGenres(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	rows, err := gen.GetDistinctGenres(ctx, db)
	if err != nil {
		t.Fatalf("GetDistinctGenres: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("len = %d, want 3", len(rows))
	}
	// Ordered: fiction, history, science
	if rows[0].Genre != "fiction" {
		t.Errorf("rows[0].Genre = %q, want fiction", rows[0].Genre)
	}
	if rows[1].Genre != "history" {
		t.Errorf("rows[1].Genre = %q, want history", rows[1].Genre)
	}
	if rows[2].Genre != "science" {
		t.Errorf("rows[2].Genre = %q, want science", rows[2].Genre)
	}
}

// --- Aggregate tests ---

func TestGetBooksWithSalesCount(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	rows, err := gen.GetBooksWithSalesCount(ctx, db)
	if err != nil {
		t.Fatalf("GetBooksWithSalesCount: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("len = %d, want 4", len(rows))
	}
	// Ordered by total_quantity DESC, title: Beta (3), Alpha (2), Gamma (1), Delta (0)
	counts := map[string]string{}
	for _, r := range rows {
		counts[r.Title] = r.TotalQuantity
	}
	if counts["Beta Book"] != "3" {
		t.Errorf("Beta total = %q, want '3'", counts["Beta Book"])
	}
	if counts["Alpha Book"] != "2" {
		t.Errorf("Alpha total = %q, want '2'", counts["Alpha Book"])
	}
	if counts["Delta Book"] != "0" {
		t.Errorf("Delta total = %q, want '0'", counts["Delta Book"])
	}
}

func TestCountSaleItems(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	ids := seed(t, ctx, db)

	// Sale 1 has 2 items (Alpha, Gamma).
	row, err := gen.CountSaleItems(ctx, db, ids.sale1)
	if err != nil {
		t.Fatalf("CountSaleItems: %v", err)
	}
	if row == nil {
		t.Fatal("expected row, got nil")
	}
	if row.ItemCount != 2 {
		t.Errorf("item_count = %d, want 2", row.ItemCount)
	}

	// Sale 2 has 1 item (Beta).
	row2, err := gen.CountSaleItems(ctx, db, ids.sale2)
	if err != nil {
		t.Fatalf("CountSaleItems sale2: %v", err)
	}
	if row2 == nil {
		t.Fatal("expected row, got nil")
	}
	if row2.ItemCount != 1 {
		t.Errorf("item_count = %d, want 1", row2.ItemCount)
	}
}

func TestGetSaleItemQuantityAggregates(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	row, err := gen.GetSaleItemQuantityAggregates(ctx, db)
	if err != nil {
		t.Fatalf("GetSaleItemQuantityAggregates: %v", err)
	}
	if row == nil {
		t.Fatal("expected row, got nil")
	}
	// Quantities: 2, 1, 3 => min=1, max=3, sum=6
	if !row.MinQty.Valid || row.MinQty.Int32 != 1 {
		t.Errorf("min_qty = %v, want 1", row.MinQty)
	}
	if !row.MaxQty.Valid || row.MaxQty.Int32 != 3 {
		t.Errorf("max_qty = %v, want 3", row.MaxQty)
	}
	if !row.SumQty.Valid || row.SumQty.String != "6" {
		t.Errorf("sum_qty = %v, want '6'", row.SumQty)
	}
}

func TestGetBookPriceAggregates(t *testing.T) {
	db, cleanup := setupDB(t)
	defer cleanup()
	ctx := context.Background()
	_ = seed(t, ctx, db)

	row, err := gen.GetBookPriceAggregates(ctx, db)
	if err != nil {
		t.Fatalf("GetBookPriceAggregates: %v", err)
	}
	if row == nil {
		t.Fatal("expected row, got nil")
	}
	// Prices: 19.99, 29.99, 9.99, 14.99 => min=9.99, max=29.99, sum=74.96
	if !row.MinPrice.Valid || row.MinPrice.String != "9.99" {
		t.Errorf("min_price = %v, want '9.99'", row.MinPrice)
	}
	if !row.MaxPrice.Valid || row.MaxPrice.String != "29.99" {
		t.Errorf("max_price = %v, want '29.99'", row.MaxPrice)
	}
	if !row.SumPrice.Valid || row.SumPrice.String != "74.96" {
		t.Errorf("sum_price = %v, want '74.96'", row.SumPrice)
	}
	// avg = 74.96 / 4 = 18.740000
	if !row.AvgPrice.Valid {
		t.Error("avg_price is NULL, expected non-null")
	}
}

// TestMain ensures all test functions are registered in this file.
// This is just a standard entry point; Go's testing framework discovers Test* functions automatically.
func TestMain(m *testing.M) {
	fmt.Println("Running MySQL runtime e2e tests")
	os.Exit(m.Run())
}
