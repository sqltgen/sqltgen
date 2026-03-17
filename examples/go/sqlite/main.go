package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"

	"example-go-sqlite/gen"

	_ "modernc.org/sqlite"
)

func applyMigrations(ctx context.Context, db *sql.DB) {
	dir := "../../common/sqlite/migrations"
	entries, err := os.ReadDir(dir)
	must(err)
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".sql" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		must(err)
		_, err = db.ExecContext(ctx, string(data))
		must(err)
	}
}

func seed(ctx context.Context, db *sql.DB) {
	must(gen.CreateAuthor(ctx, db, "Ursula K. Le Guin", sql.NullString{String: "Science fiction and fantasy author", Valid: true}, sql.NullInt32{Int32: 1929, Valid: true}))
	must(gen.CreateAuthor(ctx, db, "Frank Herbert", sql.NullString{String: "Author of the Dune series", Valid: true}, sql.NullInt32{Int32: 1920, Valid: true}))
	must(gen.CreateAuthor(ctx, db, "Isaac Asimov", sql.NullString{}, sql.NullInt32{Int32: 1920, Valid: true}))
	fmt.Println("[sqlite] inserted 3 authors")

	// Author IDs are 1, 2, 3 from autoincrement.
	must(gen.CreateBook(ctx, db, 1, "The Left Hand of Darkness", "sci-fi", 12.99, sql.NullString{}))
	must(gen.CreateBook(ctx, db, 1, "The Dispossessed", "sci-fi", 11.50, sql.NullString{}))
	must(gen.CreateBook(ctx, db, 2, "Dune", "sci-fi", 14.99, sql.NullString{}))
	must(gen.CreateBook(ctx, db, 3, "Foundation", "sci-fi", 10.99, sql.NullString{}))
	must(gen.CreateBook(ctx, db, 3, "The Caves of Steel", "sci-fi", 9.99, sql.NullString{}))
	fmt.Println("[sqlite] inserted 5 books")

	must(gen.CreateCustomer(ctx, db, "Carol", "carol@example.com"))
	must(gen.CreateCustomer(ctx, db, "Dave", "dave@example.com"))
	fmt.Println("[sqlite] inserted 2 customers")

	// Book IDs: 1=Left Hand, 2=Dispossessed, 3=Dune, 4=Foundation, 5=Caves of Steel
	// Customer IDs: 1=Carol, 2=Dave
	must(gen.CreateSale(ctx, db, 1))
	must(gen.AddSaleItem(ctx, db, 1, 3, 2, 14.99))
	must(gen.AddSaleItem(ctx, db, 1, 4, 1, 10.99))
	must(gen.CreateSale(ctx, db, 2))
	must(gen.AddSaleItem(ctx, db, 2, 3, 1, 14.99))
	must(gen.AddSaleItem(ctx, db, 2, 1, 1, 12.99))
	fmt.Println("[sqlite] inserted 2 sales with items")
}

func query(ctx context.Context, db *sql.DB) {
	authors, err := gen.ListAuthors(ctx, db)
	must(err)
	fmt.Printf("[sqlite] listAuthors: %d row(s)\n", len(authors))

	byIds, err := gen.GetBooksByIds(ctx, db, []int64{1, 3})
	must(err)
	fmt.Printf("[sqlite] getBooksByIds([1,3]): %d row(s)\n", len(byIds))
	for _, b := range byIds {
		fmt.Printf("  \"%s\"\n", b.Title)
	}

	scifi, err := gen.ListBooksByGenre(ctx, db, "sci-fi")
	must(err)
	fmt.Printf("[sqlite] listBooksByGenre(sci-fi): %d row(s)\n", len(scifi))

	allBooks, err := gen.ListBooksByGenreOrAll(ctx, db, "all")
	must(err)
	fmt.Printf("[sqlite] listBooksByGenreOrAll(all): %d row(s) (repeated-param demo)\n", len(allBooks))
	scifi2, err := gen.ListBooksByGenreOrAll(ctx, db, "sci-fi")
	must(err)
	fmt.Printf("[sqlite] listBooksByGenreOrAll(sci-fi): %d row(s)\n", len(scifi2))

	booksWithAuthor, err := gen.ListBooksWithAuthor(ctx, db)
	must(err)
	fmt.Println("[sqlite] listBooksWithAuthor:")
	for _, r := range booksWithAuthor {
		fmt.Printf("  \"%s\" by %s\n", r.Title, r.AuthorName)
	}

	neverOrdered, err := gen.GetBooksNeverOrdered(ctx, db)
	must(err)
	fmt.Printf("[sqlite] getBooksNeverOrdered: %d book(s)\n", len(neverOrdered))
	for _, b := range neverOrdered {
		fmt.Printf("  \"%s\"\n", b.Title)
	}

	topSelling, err := gen.GetTopSellingBooks(ctx, db)
	must(err)
	fmt.Println("[sqlite] getTopSellingBooks:")
	for _, r := range topSelling {
		fmt.Printf("  \"%s\" sold %d\n", r.Title, r.UnitsSold.Int64)
	}

	bestCustomers, err := gen.GetBestCustomers(ctx, db)
	must(err)
	fmt.Println("[sqlite] getBestCustomers:")
	for _, r := range bestCustomers {
		fmt.Printf("  %s spent %.2f\n", r.Name, r.TotalSpent.Float64)
	}
}

func main() {
	ctx := context.Background()

	db, err := sql.Open("sqlite", ":memory:")
	must(err)
	defer db.Close()

	_, err = db.ExecContext(ctx, "PRAGMA foreign_keys = ON")
	must(err)

	applyMigrations(ctx, db)
	seed(ctx, db)
	query(ctx, db)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
