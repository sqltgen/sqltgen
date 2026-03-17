package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"

	"example-go-postgresql/gen"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/lib/pq"
)

const dbURL = "postgres://sqltgen:sqltgen@localhost:5433/sqltgen"
const adminURL = "postgres://sqltgen:sqltgen@localhost:5433/postgres"

func seed(ctx context.Context, db *sql.DB) {
	leGuin, err := gen.CreateAuthor(ctx, db, "Ursula K. Le Guin", sql.NullString{String: "Science fiction and fantasy author", Valid: true}, sql.NullInt32{Int32: 1929, Valid: true})
	must(err)
	herbert, err := gen.CreateAuthor(ctx, db, "Frank Herbert", sql.NullString{String: "Author of the Dune series", Valid: true}, sql.NullInt32{Int32: 1920, Valid: true})
	must(err)
	asimov, err := gen.CreateAuthor(ctx, db, "Isaac Asimov", sql.NullString{}, sql.NullInt32{Int32: 1920, Valid: true})
	must(err)
	fmt.Printf("[pg] inserted 3 authors (ids: %d, %d, %d)\n", leGuin.Id, herbert.Id, asimov.Id)

	lhod, err := gen.CreateBook(ctx, db, leGuin.Id, "The Left Hand of Darkness", "sci-fi", "12.99", sql.NullTime{})
	must(err)
	_, err = gen.CreateBook(ctx, db, leGuin.Id, "The Dispossessed", "sci-fi", "11.50", sql.NullTime{})
	must(err)
	dune, err := gen.CreateBook(ctx, db, herbert.Id, "Dune", "sci-fi", "14.99", sql.NullTime{})
	must(err)
	found, err := gen.CreateBook(ctx, db, asimov.Id, "Foundation", "sci-fi", "10.99", sql.NullTime{})
	must(err)
	_, err = gen.CreateBook(ctx, db, asimov.Id, "The Caves of Steel", "sci-fi", "9.99", sql.NullTime{})
	must(err)
	fmt.Println("[pg] inserted 5 books")

	alice, err := gen.CreateCustomer(ctx, db, "Alice", "alice@example.com")
	must(err)
	bob, err := gen.CreateCustomer(ctx, db, "Bob", "bob@example.com")
	must(err)
	fmt.Println("[pg] inserted 2 customers")

	sale1, err := gen.CreateSale(ctx, db, alice.Id)
	must(err)
	must(gen.AddSaleItem(ctx, db, sale1.Id, dune.Id, 2, "14.99"))
	must(gen.AddSaleItem(ctx, db, sale1.Id, found.Id, 1, "10.99"))
	sale2, err := gen.CreateSale(ctx, db, bob.Id)
	must(err)
	must(gen.AddSaleItem(ctx, db, sale2.Id, dune.Id, 1, "14.99"))
	must(gen.AddSaleItem(ctx, db, sale2.Id, lhod.Id, 1, "12.99"))
	fmt.Println("[pg] inserted 2 sales with items")
}

func query(ctx context.Context, db *sql.DB) {
	authors, err := gen.ListAuthors(ctx, db)
	must(err)
	fmt.Printf("[pg] listAuthors: %d row(s)\n", len(authors))

	byIds, err := gen.GetBooksByIds(ctx, db, []int64{1, 3})
	must(err)
	fmt.Printf("[pg] getBooksByIds([1,3]): %d row(s)\n", len(byIds))
	for _, b := range byIds {
		fmt.Printf("  \"%s\"\n", b.Title)
	}

	scifi, err := gen.ListBooksByGenre(ctx, db, "sci-fi")
	must(err)
	fmt.Printf("[pg] listBooksByGenre(sci-fi): %d row(s)\n", len(scifi))

	allBooks, err := gen.ListBooksByGenreOrAll(ctx, db, "all")
	must(err)
	fmt.Printf("[pg] listBooksByGenreOrAll(all): %d row(s) (repeated-param demo)\n", len(allBooks))
	scifi2, err := gen.ListBooksByGenreOrAll(ctx, db, "sci-fi")
	must(err)
	fmt.Printf("[pg] listBooksByGenreOrAll(sci-fi): %d row(s)\n", len(scifi2))

	booksWithAuthor, err := gen.ListBooksWithAuthor(ctx, db)
	must(err)
	fmt.Println("[pg] listBooksWithAuthor:")
	for _, r := range booksWithAuthor {
		fmt.Printf("  \"%s\" by %s\n", r.Title, r.AuthorName)
	}

	neverOrdered, err := gen.GetBooksNeverOrdered(ctx, db)
	must(err)
	fmt.Printf("[pg] getBooksNeverOrdered: %d book(s)\n", len(neverOrdered))
	for _, b := range neverOrdered {
		fmt.Printf("  \"%s\"\n", b.Title)
	}

	topSelling, err := gen.GetTopSellingBooks(ctx, db)
	must(err)
	fmt.Println("[pg] getTopSellingBooks:")
	for _, r := range topSelling {
		fmt.Printf("  \"%s\" sold %d\n", r.Title, r.UnitsSold.Int64)
	}

	bestCustomers, err := gen.GetBestCustomers(ctx, db)
	must(err)
	fmt.Println("[pg] getBestCustomers:")
	for _, r := range bestCustomers {
		fmt.Printf("  %s spent %s\n", r.Name, r.TotalSpent.String)
	}

	// Demonstrate UPDATE RETURNING and DELETE RETURNING with a transient author
	temp, err := gen.CreateAuthor(ctx, db, "Temp Author", sql.NullString{}, sql.NullInt32{})
	must(err)
	updated, err := gen.UpdateAuthorBio(ctx, db, sql.NullString{String: "Updated via UPDATE RETURNING", Valid: true}, temp.Id)
	must(err)
	if updated != nil {
		fmt.Printf("[pg] updateAuthorBio: updated \"%s\" — bio: %s\n", updated.Name, updated.Bio.String)
	}
	deleted, err := gen.DeleteAuthor(ctx, db, temp.Id)
	must(err)
	if deleted != nil {
		fmt.Printf("[pg] deleteAuthor: deleted \"%s\" (id=%d)\n", deleted.Name, deleted.Id)
	}
}

func run(ctx context.Context, connStr string) {
	db, err := sql.Open("pgx", connStr)
	must(err)
	defer db.Close()
	must(db.PingContext(ctx))

	seed(ctx, db)
	query(ctx, db)
}

func main() {
	// Suppress unused import error for pq.Array — it is used by the generated code.
	_ = pq.Array

	ctx := context.Background()

	migrationsDir := os.Getenv("MIGRATIONS_DIR")
	if migrationsDir == "" {
		run(ctx, dbURL)
		return
	}

	dbName := "sqltgen_" + randomHex(4)
	adminDB, err := sql.Open("pgx", adminURL)
	must(err)
	_, err = adminDB.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE "%s"`, dbName))
	must(err)
	adminDB.Close()

	testURL := fmt.Sprintf("postgres://sqltgen:sqltgen@localhost:5433/%s", dbName)

	defer func() {
		adminDB, err := sql.Open("pgx", adminURL)
		if err != nil {
			fmt.Printf("[pg] warning: could not drop database %s: %v\n", dbName, err)
			return
		}
		defer adminDB.Close()
		if _, err := adminDB.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS "%s"`, dbName)); err != nil {
			fmt.Printf("[pg] warning: could not drop database %s: %v\n", dbName, err)
		}
	}()

	applyMigrations(ctx, testURL, migrationsDir)
	run(ctx, testURL)
}

func applyMigrations(ctx context.Context, connStr, dir string) {
	db, err := sql.Open("pgx", connStr)
	must(err)
	defer db.Close()

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

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
