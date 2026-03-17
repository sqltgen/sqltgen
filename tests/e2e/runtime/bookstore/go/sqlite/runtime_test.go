package main

import (
	"context"
	"database/sql"
	"math"
	"os"
	"testing"

	gen "e2e-go-sqlite/gen"

	_ "modernc.org/sqlite"
)

func setupDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	ddl, err := os.ReadFile("../../../../fixtures/bookstore/sqlite/schema.sql")
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	if _, err := db.Exec(string(ddl)); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func seed(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()

	// Authors: IDs will be 1, 2, 3
	if err := gen.CreateAuthor(ctx, db, "Alice Author", sql.NullString{String: "Alice writes fiction", Valid: true}, sql.NullInt32{Int32: 1970, Valid: true}); err != nil {
		t.Fatalf("create author Alice: %v", err)
	}
	if err := gen.CreateAuthor(ctx, db, "Bob Biographer", sql.NullString{}, sql.NullInt32{Int32: 1985, Valid: true}); err != nil {
		t.Fatalf("create author Bob: %v", err)
	}
	if err := gen.CreateAuthor(ctx, db, "Carol Coder", sql.NullString{String: "Carol writes tech books", Valid: true}, sql.NullInt32{}); err != nil {
		t.Fatalf("create author Carol: %v", err)
	}

	// Books: IDs will be 1, 2, 3, 4
	if err := gen.CreateBook(ctx, db, 1, "Adventures in SQL", "fiction", 19.99, sql.NullString{String: "2023-01-15", Valid: true}); err != nil {
		t.Fatalf("create book 1: %v", err)
	}
	if err := gen.CreateBook(ctx, db, 1, "More SQL Adventures", "fiction", 24.99, sql.NullString{String: "2023-06-01", Valid: true}); err != nil {
		t.Fatalf("create book 2: %v", err)
	}
	if err := gen.CreateBook(ctx, db, 2, "Biography of Data", "nonfiction", 14.99, sql.NullString{String: "2022-11-20", Valid: true}); err != nil {
		t.Fatalf("create book 3: %v", err)
	}
	if err := gen.CreateBook(ctx, db, 3, "Code Complete Guide", "technology", 39.99, sql.NullString{}); err != nil {
		t.Fatalf("create book 4: %v", err)
	}

	// Customers: IDs will be 1, 2
	if err := gen.CreateCustomer(ctx, db, "Dave", "dave@example.com"); err != nil {
		t.Fatalf("create customer Dave: %v", err)
	}
	if err := gen.CreateCustomer(ctx, db, "Eve", "eve@example.com"); err != nil {
		t.Fatalf("create customer Eve: %v", err)
	}

	// Sales: IDs will be 1, 2
	if err := gen.CreateSale(ctx, db, 1); err != nil {
		t.Fatalf("create sale 1: %v", err)
	}
	if err := gen.CreateSale(ctx, db, 2); err != nil {
		t.Fatalf("create sale 2: %v", err)
	}

	// Sale items for sale 1: books 1 and 2
	if err := gen.AddSaleItem(ctx, db, 1, 1, 2, 19.99); err != nil {
		t.Fatalf("add sale item 1: %v", err)
	}
	if err := gen.AddSaleItem(ctx, db, 1, 2, 1, 24.99); err != nil {
		t.Fatalf("add sale item 2: %v", err)
	}

	// Sale items for sale 2: book 1
	if err := gen.AddSaleItem(ctx, db, 2, 1, 3, 19.99); err != nil {
		t.Fatalf("add sale item 3: %v", err)
	}
}

func TestCreateAuthor(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()

	err := gen.CreateAuthor(ctx, db, "Test Author", sql.NullString{String: "A bio", Valid: true}, sql.NullInt32{Int32: 1990, Valid: true})
	if err != nil {
		t.Fatalf("CreateAuthor: %v", err)
	}

	author, err := gen.GetAuthor(ctx, db, 1)
	if err != nil {
		t.Fatalf("GetAuthor: %v", err)
	}
	if author == nil {
		t.Fatal("expected author, got nil")
	}
	if author.Name != "Test Author" {
		t.Errorf("name = %q, want %q", author.Name, "Test Author")
	}
}

func TestGetAuthor(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	author, err := gen.GetAuthor(ctx, db, 1)
	if err != nil {
		t.Fatalf("GetAuthor: %v", err)
	}
	if author == nil {
		t.Fatal("expected author, got nil")
	}
	if author.Name != "Alice Author" {
		t.Errorf("name = %q, want %q", author.Name, "Alice Author")
	}
	if !author.Bio.Valid || author.Bio.String != "Alice writes fiction" {
		t.Errorf("bio = %v, want %q", author.Bio, "Alice writes fiction")
	}
	if !author.BirthYear.Valid || author.BirthYear.Int32 != 1970 {
		t.Errorf("birth_year = %v, want 1970", author.BirthYear)
	}

	// Non-existent
	missing, err := gen.GetAuthor(ctx, db, 999)
	if err != nil {
		t.Fatalf("GetAuthor(999): %v", err)
	}
	if missing != nil {
		t.Errorf("expected nil for missing author, got %+v", missing)
	}
}

func TestListAuthors(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	authors, err := gen.ListAuthors(ctx, db)
	if err != nil {
		t.Fatalf("ListAuthors: %v", err)
	}
	if len(authors) != 3 {
		t.Fatalf("len = %d, want 3", len(authors))
	}
	// Ordered by name
	if authors[0].Name != "Alice Author" {
		t.Errorf("authors[0].Name = %q, want %q", authors[0].Name, "Alice Author")
	}
	if authors[1].Name != "Bob Biographer" {
		t.Errorf("authors[1].Name = %q, want %q", authors[1].Name, "Bob Biographer")
	}
	if authors[2].Name != "Carol Coder" {
		t.Errorf("authors[2].Name = %q, want %q", authors[2].Name, "Carol Coder")
	}
}

func TestCreateBook(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()

	err := gen.CreateAuthor(ctx, db, "Author", sql.NullString{}, sql.NullInt32{})
	if err != nil {
		t.Fatalf("CreateAuthor: %v", err)
	}
	err = gen.CreateBook(ctx, db, 1, "Test Book", "fiction", 9.99, sql.NullString{String: "2024-01-01", Valid: true})
	if err != nil {
		t.Fatalf("CreateBook: %v", err)
	}

	book, err := gen.GetBook(ctx, db, 1)
	if err != nil {
		t.Fatalf("GetBook: %v", err)
	}
	if book == nil {
		t.Fatal("expected book, got nil")
	}
	if book.Title != "Test Book" {
		t.Errorf("title = %q, want %q", book.Title, "Test Book")
	}
	if book.Price != 9.99 {
		t.Errorf("price = %f, want 9.99", book.Price)
	}
}

func TestGetBook(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	book, err := gen.GetBook(ctx, db, 1)
	if err != nil {
		t.Fatalf("GetBook: %v", err)
	}
	if book == nil {
		t.Fatal("expected book, got nil")
	}
	if book.Title != "Adventures in SQL" {
		t.Errorf("title = %q, want %q", book.Title, "Adventures in SQL")
	}
	if book.AuthorId != 1 {
		t.Errorf("author_id = %d, want 1", book.AuthorId)
	}
	if book.Genre != "fiction" {
		t.Errorf("genre = %q, want %q", book.Genre, "fiction")
	}
	if book.Price != 19.99 {
		t.Errorf("price = %f, want 19.99", book.Price)
	}
	if !book.PublishedAt.Valid || book.PublishedAt.String != "2023-01-15" {
		t.Errorf("published_at = %v, want 2023-01-15", book.PublishedAt)
	}

	// Non-existent
	missing, err := gen.GetBook(ctx, db, 999)
	if err != nil {
		t.Fatalf("GetBook(999): %v", err)
	}
	if missing != nil {
		t.Errorf("expected nil for missing book, got %+v", missing)
	}
}

func TestGetBooksByIds(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	books, err := gen.GetBooksByIds(ctx, db, []int64{1, 3})
	if err != nil {
		t.Fatalf("GetBooksByIds: %v", err)
	}
	if len(books) != 2 {
		t.Fatalf("len = %d, want 2", len(books))
	}
	// Ordered by title
	if books[0].Title != "Adventures in SQL" {
		t.Errorf("books[0].Title = %q, want %q", books[0].Title, "Adventures in SQL")
	}
	if books[1].Title != "Biography of Data" {
		t.Errorf("books[1].Title = %q, want %q", books[1].Title, "Biography of Data")
	}
}

func TestListBooksByGenre(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	books, err := gen.ListBooksByGenre(ctx, db, "fiction")
	if err != nil {
		t.Fatalf("ListBooksByGenre: %v", err)
	}
	if len(books) != 2 {
		t.Fatalf("len = %d, want 2", len(books))
	}
	for _, b := range books {
		if b.Genre != "fiction" {
			t.Errorf("genre = %q, want fiction", b.Genre)
		}
	}

	// No results
	empty, err := gen.ListBooksByGenre(ctx, db, "romance")
	if err != nil {
		t.Fatalf("ListBooksByGenre(romance): %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("len = %d, want 0", len(empty))
	}
}

func TestListBooksByGenreOrAll(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	// Specific genre
	books, err := gen.ListBooksByGenreOrAll(ctx, db, "fiction")
	if err != nil {
		t.Fatalf("ListBooksByGenreOrAll(fiction): %v", err)
	}
	if len(books) != 2 {
		t.Fatalf("len = %d, want 2", len(books))
	}

	// All
	all, err := gen.ListBooksByGenreOrAll(ctx, db, "all")
	if err != nil {
		t.Fatalf("ListBooksByGenreOrAll(all): %v", err)
	}
	if len(all) != 4 {
		t.Fatalf("len = %d, want 4", len(all))
	}
}

func TestCreateCustomer(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()

	err := gen.CreateCustomer(ctx, db, "Test Customer", "test@example.com")
	if err != nil {
		t.Fatalf("CreateCustomer: %v", err)
	}

	// Verify via raw query
	var name string
	if err := db.QueryRowContext(ctx, "SELECT name FROM customer WHERE id = 1").Scan(&name); err != nil {
		t.Fatalf("verify customer: %v", err)
	}
	if name != "Test Customer" {
		t.Errorf("name = %q, want %q", name, "Test Customer")
	}
}

func TestCreateSale(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()

	err := gen.CreateCustomer(ctx, db, "Buyer", "buyer@example.com")
	if err != nil {
		t.Fatalf("CreateCustomer: %v", err)
	}
	err = gen.CreateSale(ctx, db, 1)
	if err != nil {
		t.Fatalf("CreateSale: %v", err)
	}

	var custID int32
	if err := db.QueryRowContext(ctx, "SELECT customer_id FROM sale WHERE id = 1").Scan(&custID); err != nil {
		t.Fatalf("verify sale: %v", err)
	}
	if custID != 1 {
		t.Errorf("customer_id = %d, want 1", custID)
	}
}

func TestAddSaleItem(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()

	// Create prerequisite data
	_ = gen.CreateAuthor(ctx, db, "A", sql.NullString{}, sql.NullInt32{})
	_ = gen.CreateBook(ctx, db, 1, "B", "g", 10.0, sql.NullString{})
	_ = gen.CreateCustomer(ctx, db, "C", "c@example.com")
	_ = gen.CreateSale(ctx, db, 1)

	err := gen.AddSaleItem(ctx, db, 1, 1, 5, 10.0)
	if err != nil {
		t.Fatalf("AddSaleItem: %v", err)
	}

	var qty int32
	if err := db.QueryRowContext(ctx, "SELECT quantity FROM sale_item WHERE id = 1").Scan(&qty); err != nil {
		t.Fatalf("verify sale_item: %v", err)
	}
	if qty != 5 {
		t.Errorf("quantity = %d, want 5", qty)
	}
}

func TestListBooksWithAuthor(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.ListBooksWithAuthor(ctx, db)
	if err != nil {
		t.Fatalf("ListBooksWithAuthor: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("len = %d, want 4", len(rows))
	}
	// Ordered by title
	if rows[0].Title != "Adventures in SQL" {
		t.Errorf("rows[0].Title = %q, want %q", rows[0].Title, "Adventures in SQL")
	}
	if rows[0].AuthorName != "Alice Author" {
		t.Errorf("rows[0].AuthorName = %q, want %q", rows[0].AuthorName, "Alice Author")
	}
	if !rows[0].AuthorBio.Valid || rows[0].AuthorBio.String != "Alice writes fiction" {
		t.Errorf("rows[0].AuthorBio = %v, want %q", rows[0].AuthorBio, "Alice writes fiction")
	}
}

func TestGetBooksNeverOrdered(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	books, err := gen.GetBooksNeverOrdered(ctx, db)
	if err != nil {
		t.Fatalf("GetBooksNeverOrdered: %v", err)
	}
	// Books 1 and 2 have sale items; books 3 and 4 do not
	if len(books) != 2 {
		t.Fatalf("len = %d, want 2", len(books))
	}
	titles := map[string]bool{}
	for _, b := range books {
		titles[b.Title] = true
	}
	if !titles["Biography of Data"] {
		t.Error("expected 'Biography of Data' in never-ordered")
	}
	if !titles["Code Complete Guide"] {
		t.Error("expected 'Code Complete Guide' in never-ordered")
	}
}

func TestGetTopSellingBooks(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetTopSellingBooks(ctx, db)
	if err != nil {
		t.Fatalf("GetTopSellingBooks: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("len = %d, want 2", len(rows))
	}
	// Book 1 has 2+3=5 units sold, Book 2 has 1 unit sold
	if rows[0].Title != "Adventures in SQL" {
		t.Errorf("rows[0].Title = %q, want %q", rows[0].Title, "Adventures in SQL")
	}
	if !rows[0].UnitsSold.Valid || rows[0].UnitsSold.Int64 != 5 {
		t.Errorf("rows[0].UnitsSold = %v, want 5", rows[0].UnitsSold)
	}
	if rows[1].Title != "More SQL Adventures" {
		t.Errorf("rows[1].Title = %q, want %q", rows[1].Title, "More SQL Adventures")
	}
	if !rows[1].UnitsSold.Valid || rows[1].UnitsSold.Int64 != 1 {
		t.Errorf("rows[1].UnitsSold = %v, want 1", rows[1].UnitsSold)
	}
}

func TestGetBestCustomers(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetBestCustomers(ctx, db)
	if err != nil {
		t.Fatalf("GetBestCustomers: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("len = %d, want 2", len(rows))
	}
	// Dave (sale 1): 2*19.99 + 1*24.99 = 64.97
	// Eve (sale 2): 3*19.99 = 59.97
	if rows[0].Name != "Dave" {
		t.Errorf("rows[0].Name = %q, want %q", rows[0].Name, "Dave")
	}
	if !rows[0].TotalSpent.Valid || math.Abs(rows[0].TotalSpent.Float64-64.97) > 0.01 {
		t.Errorf("rows[0].TotalSpent = %v, want ~64.97", rows[0].TotalSpent)
	}
	if rows[1].Name != "Eve" {
		t.Errorf("rows[1].Name = %q, want %q", rows[1].Name, "Eve")
	}
	if !rows[1].TotalSpent.Valid || math.Abs(rows[1].TotalSpent.Float64-59.97) > 0.01 {
		t.Errorf("rows[1].TotalSpent = %v, want ~59.97", rows[1].TotalSpent)
	}
}

func TestCountBooksByGenre(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.CountBooksByGenre(ctx, db)
	if err != nil {
		t.Fatalf("CountBooksByGenre: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("len = %d, want 3", len(rows))
	}
	// Ordered by genre: fiction(2), nonfiction(1), technology(1)
	expected := map[string]int64{
		"fiction":    2,
		"nonfiction": 1,
		"technology": 1,
	}
	for _, r := range rows {
		if want, ok := expected[r.Genre]; !ok {
			t.Errorf("unexpected genre %q", r.Genre)
		} else if r.BookCount != want {
			t.Errorf("genre %q: count = %d, want %d", r.Genre, r.BookCount, want)
		}
	}
}

func TestListBooksWithLimit(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	// First page
	page1, err := gen.ListBooksWithLimit(ctx, db, 2, 0)
	if err != nil {
		t.Fatalf("ListBooksWithLimit: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("page1 len = %d, want 2", len(page1))
	}
	if page1[0].Title != "Adventures in SQL" {
		t.Errorf("page1[0].Title = %q, want %q", page1[0].Title, "Adventures in SQL")
	}

	// Second page
	page2, err := gen.ListBooksWithLimit(ctx, db, 2, 2)
	if err != nil {
		t.Fatalf("ListBooksWithLimit page2: %v", err)
	}
	if len(page2) != 2 {
		t.Fatalf("page2 len = %d, want 2", len(page2))
	}
}

func TestSearchBooksByTitle(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.SearchBooksByTitle(ctx, db, "%SQL%")
	if err != nil {
		t.Fatalf("SearchBooksByTitle: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("len = %d, want 2", len(rows))
	}
	for _, r := range rows {
		if r.Title != "Adventures in SQL" && r.Title != "More SQL Adventures" {
			t.Errorf("unexpected title %q", r.Title)
		}
	}

	// No match
	empty, err := gen.SearchBooksByTitle(ctx, db, "%Nonexistent%")
	if err != nil {
		t.Fatalf("SearchBooksByTitle(no match): %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("len = %d, want 0", len(empty))
	}
}

func TestGetBooksByPriceRange(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetBooksByPriceRange(ctx, db, 10.0, 20.0)
	if err != nil {
		t.Fatalf("GetBooksByPriceRange: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("len = %d, want 2", len(rows))
	}
	// Ordered by price: 14.99, 19.99
	if rows[0].Title != "Biography of Data" {
		t.Errorf("rows[0].Title = %q, want %q", rows[0].Title, "Biography of Data")
	}
	if rows[1].Title != "Adventures in SQL" {
		t.Errorf("rows[1].Title = %q, want %q", rows[1].Title, "Adventures in SQL")
	}
}

func TestGetBooksInGenres(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetBooksInGenres(ctx, db, "fiction", "technology", "romance")
	if err != nil {
		t.Fatalf("GetBooksInGenres: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("len = %d, want 3", len(rows))
	}
	// Ordered by title
	if rows[0].Title != "Adventures in SQL" {
		t.Errorf("rows[0].Title = %q, want %q", rows[0].Title, "Adventures in SQL")
	}
}

func TestGetBookPriceLabel(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetBookPriceLabel(ctx, db, 20.0)
	if err != nil {
		t.Fatalf("GetBookPriceLabel: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("len = %d, want 4", len(rows))
	}
	labels := map[string]string{}
	for _, r := range rows {
		labels[r.Title] = r.PriceLabel
	}
	if labels["Adventures in SQL"] != "affordable" {
		t.Errorf("Adventures in SQL label = %q, want affordable", labels["Adventures in SQL"])
	}
	if labels["More SQL Adventures"] != "expensive" {
		t.Errorf("More SQL Adventures label = %q, want expensive", labels["More SQL Adventures"])
	}
	if labels["Code Complete Guide"] != "expensive" {
		t.Errorf("Code Complete Guide label = %q, want expensive", labels["Code Complete Guide"])
	}
}

func TestGetBookPriceOrDefault(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetBookPriceOrDefault(ctx, db, sql.NullFloat64{Float64: 0.0, Valid: true})
	if err != nil {
		t.Fatalf("GetBookPriceOrDefault: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("len = %d, want 4", len(rows))
	}
	// All books have prices, so effective_price should equal actual price
	for _, r := range rows {
		if r.EffectivePrice <= 0 {
			t.Errorf("book %q: effective_price = %f, want > 0", r.Title, r.EffectivePrice)
		}
	}
}

func TestDeleteBookById(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	// Delete book 4 (no sale items referencing it)
	affected, err := gen.DeleteBookById(ctx, db, 4)
	if err != nil {
		t.Fatalf("DeleteBookById: %v", err)
	}
	if affected != 1 {
		t.Errorf("affected = %d, want 1", affected)
	}

	// Verify deleted
	book, err := gen.GetBook(ctx, db, 4)
	if err != nil {
		t.Fatalf("GetBook after delete: %v", err)
	}
	if book != nil {
		t.Error("expected nil after delete")
	}

	// Delete non-existent
	affected, err = gen.DeleteBookById(ctx, db, 999)
	if err != nil {
		t.Fatalf("DeleteBookById(999): %v", err)
	}
	if affected != 0 {
		t.Errorf("affected = %d, want 0", affected)
	}
}

func TestGetGenresWithManyBooks(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	// Only fiction has 2 books, threshold > 1
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
		t.Errorf("book_count = %d, want 2", rows[0].BookCount)
	}

	// Threshold > 0 should return all 3 genres
	rows2, err := gen.GetGenresWithManyBooks(ctx, db, 0)
	if err != nil {
		t.Fatalf("GetGenresWithManyBooks(0): %v", err)
	}
	if len(rows2) != 3 {
		t.Errorf("len = %d, want 3", len(rows2))
	}
}

func TestGetBooksByAuthorParam(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	// Authors with birth_year > 1980: Bob (1985) => book 3
	rows, err := gen.GetBooksByAuthorParam(ctx, db, sql.NullInt32{Int32: 1980, Valid: true})
	if err != nil {
		t.Fatalf("GetBooksByAuthorParam: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("len = %d, want 1", len(rows))
	}
	if rows[0].Title != "Biography of Data" {
		t.Errorf("title = %q, want %q", rows[0].Title, "Biography of Data")
	}
}

func TestGetAllBookFields(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	books, err := gen.GetAllBookFields(ctx, db)
	if err != nil {
		t.Fatalf("GetAllBookFields: %v", err)
	}
	if len(books) != 4 {
		t.Fatalf("len = %d, want 4", len(books))
	}
	// Ordered by id
	if books[0].Id != 1 {
		t.Errorf("books[0].Id = %d, want 1", books[0].Id)
	}
	if books[3].Id != 4 {
		t.Errorf("books[3].Id = %d, want 4", books[3].Id)
	}
}

func TestGetBooksNotByAuthor(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	// Exclude Alice Author's books (books 1 & 2)
	rows, err := gen.GetBooksNotByAuthor(ctx, db, "Alice Author")
	if err != nil {
		t.Fatalf("GetBooksNotByAuthor: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("len = %d, want 2", len(rows))
	}
	titles := map[string]bool{}
	for _, r := range rows {
		titles[r.Title] = true
	}
	if !titles["Biography of Data"] {
		t.Error("expected 'Biography of Data'")
	}
	if !titles["Code Complete Guide"] {
		t.Error("expected 'Code Complete Guide'")
	}
}

func TestGetBooksWithRecentSales(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	// Sales use CURRENT_TIMESTAMP, so anything before "2000-01-01" should match all sold books
	rows, err := gen.GetBooksWithRecentSales(ctx, db, "2000-01-01")
	if err != nil {
		t.Fatalf("GetBooksWithRecentSales: %v", err)
	}
	// Books 1 and 2 have sale items
	if len(rows) != 2 {
		t.Fatalf("len = %d, want 2", len(rows))
	}

	// Far future should return nothing
	empty, err := gen.GetBooksWithRecentSales(ctx, db, "2099-01-01")
	if err != nil {
		t.Fatalf("GetBooksWithRecentSales(future): %v", err)
	}
	if len(empty) != 0 {
		t.Errorf("len = %d, want 0", len(empty))
	}
}

func TestGetBookWithAuthorName(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetBookWithAuthorName(ctx, db)
	if err != nil {
		t.Fatalf("GetBookWithAuthorName: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("len = %d, want 4", len(rows))
	}
	// Ordered by title
	if rows[0].Title != "Adventures in SQL" {
		t.Errorf("rows[0].Title = %q, want %q", rows[0].Title, "Adventures in SQL")
	}
	if !rows[0].AuthorName.Valid || rows[0].AuthorName.String != "Alice Author" {
		t.Errorf("rows[0].AuthorName = %v, want %q", rows[0].AuthorName, "Alice Author")
	}
}

func TestGetAuthorStats(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetAuthorStats(ctx, db)
	if err != nil {
		t.Fatalf("GetAuthorStats: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("len = %d, want 3", len(rows))
	}
	// Ordered by name: Alice, Bob, Carol
	stats := map[string]gen.GetAuthorStatsRow{}
	for _, r := range rows {
		stats[r.Name] = r
	}
	// Alice: 2 books, 5+1=6 total sold
	if stats["Alice Author"].NumBooks != 2 {
		t.Errorf("Alice num_books = %d, want 2", stats["Alice Author"].NumBooks)
	}
	if stats["Alice Author"].TotalSold != 6 {
		t.Errorf("Alice total_sold = %d, want 6", stats["Alice Author"].TotalSold)
	}
	// Bob: 1 book, 0 sold
	if stats["Bob Biographer"].NumBooks != 1 {
		t.Errorf("Bob num_books = %d, want 1", stats["Bob Biographer"].NumBooks)
	}
	if stats["Bob Biographer"].TotalSold != 0 {
		t.Errorf("Bob total_sold = %d, want 0", stats["Bob Biographer"].TotalSold)
	}
	// Carol: 1 book, 0 sold
	if stats["Carol Coder"].NumBooks != 1 {
		t.Errorf("Carol num_books = %d, want 1", stats["Carol Coder"].NumBooks)
	}
}

func TestGetProduct(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()

	wkg := float32(1.5)
	rating := float32(4.5)
	err := gen.InsertProduct(ctx, db, "prod-1", "SKU001", "Widget", 1, &wkg, &rating,
		sql.NullString{String: `{"color":"red"}`, Valid: true}, []byte{0xDE, 0xAD}, 100)
	if err != nil {
		t.Fatalf("InsertProduct: %v", err)
	}

	product, err := gen.GetProduct(ctx, db, "prod-1")
	if err != nil {
		t.Fatalf("GetProduct: %v", err)
	}
	if product == nil {
		t.Fatal("expected product, got nil")
	}
	if product.Name != "Widget" {
		t.Errorf("name = %q, want Widget", product.Name)
	}
	if product.Sku != "SKU001" {
		t.Errorf("sku = %q, want SKU001", product.Sku)
	}
	if product.Active != 1 {
		t.Errorf("active = %d, want 1", product.Active)
	}
	if product.WeightKg == nil || *product.WeightKg != 1.5 {
		t.Errorf("weight_kg = %v, want 1.5", product.WeightKg)
	}
	if product.Rating == nil || *product.Rating != 4.5 {
		t.Errorf("rating = %v, want 4.5", product.Rating)
	}
	if !product.Metadata.Valid || product.Metadata.String != `{"color":"red"}` {
		t.Errorf("metadata = %v, want {\"color\":\"red\"}", product.Metadata)
	}
	if len(product.Thumbnail) != 2 || product.Thumbnail[0] != 0xDE || product.Thumbnail[1] != 0xAD {
		t.Errorf("thumbnail = %v, want [0xDE 0xAD]", product.Thumbnail)
	}
	if product.StockCount != 100 {
		t.Errorf("stock_count = %d, want 100", product.StockCount)
	}

	// Non-existent
	missing, err := gen.GetProduct(ctx, db, "no-such-id")
	if err != nil {
		t.Fatalf("GetProduct(missing): %v", err)
	}
	if missing != nil {
		t.Errorf("expected nil for missing product, got %+v", missing)
	}
}

func TestListActiveProducts(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()

	wkg := float32(0.5)
	_ = gen.InsertProduct(ctx, db, "p1", "S1", "Active Widget", 1, &wkg, nil, sql.NullString{}, nil, 10)
	_ = gen.InsertProduct(ctx, db, "p2", "S2", "Inactive Gadget", 0, nil, nil, sql.NullString{}, nil, 0)
	_ = gen.InsertProduct(ctx, db, "p3", "S3", "Another Active", 1, nil, nil, sql.NullString{}, nil, 5)

	active, err := gen.ListActiveProducts(ctx, db, 1)
	if err != nil {
		t.Fatalf("ListActiveProducts: %v", err)
	}
	if len(active) != 2 {
		t.Fatalf("len = %d, want 2", len(active))
	}

	inactive, err := gen.ListActiveProducts(ctx, db, 0)
	if err != nil {
		t.Fatalf("ListActiveProducts(0): %v", err)
	}
	if len(inactive) != 1 {
		t.Fatalf("len = %d, want 1", len(inactive))
	}
	if inactive[0].Name != "Inactive Gadget" {
		t.Errorf("name = %q, want %q", inactive[0].Name, "Inactive Gadget")
	}
}

func TestGetAuthorsWithNullBio(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetAuthorsWithNullBio(ctx, db)
	if err != nil {
		t.Fatalf("GetAuthorsWithNullBio: %v", err)
	}
	// Bob has NULL bio
	if len(rows) != 1 {
		t.Fatalf("len = %d, want 1", len(rows))
	}
	if rows[0].Name != "Bob Biographer" {
		t.Errorf("name = %q, want %q", rows[0].Name, "Bob Biographer")
	}
}

func TestGetAuthorsWithBio(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetAuthorsWithBio(ctx, db)
	if err != nil {
		t.Fatalf("GetAuthorsWithBio: %v", err)
	}
	// Alice and Carol have bios
	if len(rows) != 2 {
		t.Fatalf("len = %d, want 2", len(rows))
	}
	names := map[string]bool{}
	for _, r := range rows {
		names[r.Name] = true
	}
	if !names["Alice Author"] {
		t.Error("expected Alice Author")
	}
	if !names["Carol Coder"] {
		t.Error("expected Carol Coder")
	}
}

func TestGetBooksPublishedBetween(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetBooksPublishedBetween(ctx, db,
		sql.NullString{String: "2023-01-01", Valid: true},
		sql.NullString{String: "2023-12-31", Valid: true})
	if err != nil {
		t.Fatalf("GetBooksPublishedBetween: %v", err)
	}
	// Books 1 (2023-01-15) and 2 (2023-06-01)
	if len(rows) != 2 {
		t.Fatalf("len = %d, want 2", len(rows))
	}
	if rows[0].Title != "Adventures in SQL" {
		t.Errorf("rows[0].Title = %q, want %q", rows[0].Title, "Adventures in SQL")
	}
}

func TestGetDistinctGenres(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetDistinctGenres(ctx, db)
	if err != nil {
		t.Fatalf("GetDistinctGenres: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("len = %d, want 3", len(rows))
	}
	genres := map[string]bool{}
	for _, r := range rows {
		genres[r.Genre] = true
	}
	if !genres["fiction"] || !genres["nonfiction"] || !genres["technology"] {
		t.Errorf("genres = %v, want fiction/nonfiction/technology", genres)
	}
}

func TestGetBooksWithSalesCount(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	rows, err := gen.GetBooksWithSalesCount(ctx, db)
	if err != nil {
		t.Fatalf("GetBooksWithSalesCount: %v", err)
	}
	if len(rows) != 4 {
		t.Fatalf("len = %d, want 4", len(rows))
	}
	// Ordered by total_quantity DESC, title
	// Book 1: 2+3=5, Book 2: 1, Book 3: 0, Book 4: 0
	counts := map[string]int64{}
	for _, r := range rows {
		counts[r.Title] = r.TotalQuantity
	}
	if counts["Adventures in SQL"] != 5 {
		t.Errorf("Adventures in SQL qty = %d, want 5", counts["Adventures in SQL"])
	}
	if counts["More SQL Adventures"] != 1 {
		t.Errorf("More SQL Adventures qty = %d, want 1", counts["More SQL Adventures"])
	}
	if counts["Biography of Data"] != 0 {
		t.Errorf("Biography of Data qty = %d, want 0", counts["Biography of Data"])
	}
}

func TestCountSaleItems(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	row, err := gen.CountSaleItems(ctx, db, 1)
	if err != nil {
		t.Fatalf("CountSaleItems: %v", err)
	}
	if row == nil {
		t.Fatal("expected row, got nil")
	}
	// Sale 1 has 2 items
	if row.ItemCount != 2 {
		t.Errorf("item_count = %d, want 2", row.ItemCount)
	}

	row2, err := gen.CountSaleItems(ctx, db, 2)
	if err != nil {
		t.Fatalf("CountSaleItems(2): %v", err)
	}
	if row2 == nil {
		t.Fatal("expected row, got nil")
	}
	// Sale 2 has 1 item
	if row2.ItemCount != 1 {
		t.Errorf("item_count = %d, want 1", row2.ItemCount)
	}
}

func TestUpdateAuthorBio(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	// Update Bob's bio (was NULL)
	err := gen.UpdateAuthorBio(ctx, db, sql.NullString{String: "Updated bio", Valid: true}, 2)
	if err != nil {
		t.Fatalf("UpdateAuthorBio: %v", err)
	}
	author, err := gen.GetAuthor(ctx, db, 2)
	if err != nil {
		t.Fatalf("GetAuthor: %v", err)
	}
	if author == nil {
		t.Fatal("expected author, got nil")
	}
	if !author.Bio.Valid || author.Bio.String != "Updated bio" {
		t.Errorf("bio = %v, want %q", author.Bio, "Updated bio")
	}

	// Set bio to NULL
	err = gen.UpdateAuthorBio(ctx, db, sql.NullString{}, 2)
	if err != nil {
		t.Fatalf("UpdateAuthorBio(null): %v", err)
	}
	author2, err := gen.GetAuthor(ctx, db, 2)
	if err != nil {
		t.Fatalf("GetAuthor after null: %v", err)
	}
	if author2.Bio.Valid {
		t.Errorf("bio = %v, want NULL", author2.Bio)
	}
}

func TestDeleteAuthor(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()

	// Create standalone author with no books (to avoid FK violation)
	_ = gen.CreateAuthor(ctx, db, "Temp", sql.NullString{}, sql.NullInt32{})

	err := gen.DeleteAuthor(ctx, db, 1)
	if err != nil {
		t.Fatalf("DeleteAuthor: %v", err)
	}

	author, err := gen.GetAuthor(ctx, db, 1)
	if err != nil {
		t.Fatalf("GetAuthor after delete: %v", err)
	}
	if author != nil {
		t.Error("expected nil after delete")
	}
}

func TestInsertProduct(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()

	wkg := float32(2.0)
	rating := float32(3.5)
	err := gen.InsertProduct(ctx, db, "ip-1", "SKU-IP1", "Inserted Product", 1,
		&wkg, &rating, sql.NullString{String: `{"key":"val"}`, Valid: true},
		[]byte{0x01, 0x02}, 50)
	if err != nil {
		t.Fatalf("InsertProduct: %v", err)
	}

	p, err := gen.GetProduct(ctx, db, "ip-1")
	if err != nil {
		t.Fatalf("GetProduct: %v", err)
	}
	if p == nil {
		t.Fatal("expected product, got nil")
	}
	if p.Name != "Inserted Product" {
		t.Errorf("name = %q, want %q", p.Name, "Inserted Product")
	}
	if p.StockCount != 50 {
		t.Errorf("stock_count = %d, want 50", p.StockCount)
	}
}

func TestUpsertProduct(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()

	// Insert via upsert
	err := gen.UpsertProduct(ctx, db, "up-1", "SKU-UP1", "Original", 1,
		sql.NullString{}, 10)
	if err != nil {
		t.Fatalf("UpsertProduct insert: %v", err)
	}

	p, err := gen.GetProduct(ctx, db, "up-1")
	if err != nil {
		t.Fatalf("GetProduct: %v", err)
	}
	if p == nil {
		t.Fatal("expected product, got nil")
	}
	if p.Name != "Original" {
		t.Errorf("name = %q, want Original", p.Name)
	}

	// Update via upsert (conflict on id)
	err = gen.UpsertProduct(ctx, db, "up-1", "SKU-UP1", "Updated", 0,
		sql.NullString{String: `{"updated":true}`, Valid: true}, 20)
	if err != nil {
		t.Fatalf("UpsertProduct update: %v", err)
	}

	p2, err := gen.GetProduct(ctx, db, "up-1")
	if err != nil {
		t.Fatalf("GetProduct after upsert: %v", err)
	}
	if p2 == nil {
		t.Fatal("expected product, got nil")
	}
	if p2.Name != "Updated" {
		t.Errorf("name = %q, want Updated", p2.Name)
	}
	if p2.Active != 0 {
		t.Errorf("active = %d, want 0", p2.Active)
	}
	if p2.StockCount != 20 {
		t.Errorf("stock_count = %d, want 20", p2.StockCount)
	}
}

func TestGetSaleItemQuantityAggregates(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	row, err := gen.GetSaleItemQuantityAggregates(ctx, db)
	if err != nil {
		t.Fatalf("GetSaleItemQuantityAggregates: %v", err)
	}
	if row == nil {
		t.Fatal("expected row, got nil")
	}
	// Sale items: qty 2, 1, 3 => min=1, max=3, sum=6, avg=2.0
	if !row.MinQty.Valid || row.MinQty.Int32 != 1 {
		t.Errorf("min_qty = %v, want 1", row.MinQty)
	}
	if !row.MaxQty.Valid || row.MaxQty.Int32 != 3 {
		t.Errorf("max_qty = %v, want 3", row.MaxQty)
	}
	if !row.SumQty.Valid || row.SumQty.Int64 != 6 {
		t.Errorf("sum_qty = %v, want 6", row.SumQty)
	}
	if !row.AvgQty.Valid || row.AvgQty.Float64 != 2.0 {
		t.Errorf("avg_qty = %v, want 2.0", row.AvgQty)
	}
}

func TestGetBookPriceAggregates(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	seed(t, ctx, db)

	row, err := gen.GetBookPriceAggregates(ctx, db)
	if err != nil {
		t.Fatalf("GetBookPriceAggregates: %v", err)
	}
	if row == nil {
		t.Fatal("expected row, got nil")
	}
	// Prices: 19.99, 24.99, 14.99, 39.99 => min=14.99, max=39.99, sum=99.96
	if !row.MinPrice.Valid || math.Abs(row.MinPrice.Float64-14.99) > 0.01 {
		t.Errorf("min_price = %v, want ~14.99", row.MinPrice)
	}
	if !row.MaxPrice.Valid || math.Abs(row.MaxPrice.Float64-39.99) > 0.01 {
		t.Errorf("max_price = %v, want ~39.99", row.MaxPrice)
	}
	if !row.SumPrice.Valid || math.Abs(row.SumPrice.Float64-99.96) > 0.01 {
		t.Errorf("sum_price = %v, want ~99.96", row.SumPrice)
	}
	if !row.AvgPrice.Valid || math.Abs(row.AvgPrice.Float64-24.99) > 0.01 {
		t.Errorf("avg_price = %v, want ~24.99", row.AvgPrice)
	}
}
