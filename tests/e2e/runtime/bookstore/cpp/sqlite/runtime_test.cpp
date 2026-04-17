#include <algorithm>
#include <cstdint>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include <sqlite3.h>
#include <gtest/gtest.h>

#include "gen/queries/queries.hpp"

using namespace db;

namespace fs = std::filesystem;

static std::string read_file(const fs::path& path) {
    std::ifstream f(path);
    if (!f) throw std::runtime_error("cannot open: " + path.string());
    return {std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
}

static void exec_sql(sqlite3* db, const std::string& sql) {
    char* err = nullptr;
    if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
        std::string msg = err ? err : "unknown error";
        sqlite3_free(err);
        throw std::runtime_error(msg);
    }
}

static std::int32_t last_id(sqlite3* db) {
    return static_cast<std::int32_t>(sqlite3_last_insert_rowid(db));
}

class BookstoreTest : public ::testing::Test {
protected:
    sqlite3* db_ = nullptr;

    void SetUp() override {
        ASSERT_EQ(sqlite3_open(":memory:", &db_), SQLITE_OK);
        exec_sql(db_, "PRAGMA foreign_keys = ON");
        exec_sql(db_, read_file(SCHEMA_PATH));
    }

    void TearDown() override {
        sqlite3_close(db_);
        db_ = nullptr;
    }

    // Seed the database with a fixed, predictable dataset.
    void seed() {
        create_author(db_, "Asimov",  std::optional<std::string>{"Sci-fi master"}, std::optional<std::int32_t>{1920});
        create_author(db_, "Herbert", std::nullopt,                                  std::optional<std::int32_t>{1920});
        create_author(db_, "Le Guin", std::optional<std::string>{"Earthsea"},       std::optional<std::int32_t>{1929});
        // ids: 1=Asimov, 2=Herbert, 3=Le Guin

        create_book(db_, 1, "Foundation", "sci-fi",  9.99,  std::optional<std::string>{"1951-01-01"});
        create_book(db_, 1, "I Robot",    "sci-fi",  7.99,  std::optional<std::string>{"1950-01-01"});
        create_book(db_, 2, "Dune",       "sci-fi",  12.99, std::optional<std::string>{"1965-01-01"});
        create_book(db_, 3, "Earthsea",   "fantasy", 8.99,  std::optional<std::string>{"1968-01-01"});
        // ids: 1=Foundation, 2=I Robot, 3=Dune, 4=Earthsea

        create_customer(db_, "Alice", "alice@example.com");
        // id: 1=Alice

        create_sale(db_, 1);
        // id: 1 (Alice's sale)
        add_sale_item(db_, 1, 1, 2, 9.99);   // Foundation x2
        add_sale_item(db_, 1, 3, 1, 12.99);  // Dune x1
    }
};

// ─── :one ────────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetAuthor) {
    seed();
    auto row = get_author(db_, 1);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->name, "Asimov");
    EXPECT_EQ(row->bio, std::optional<std::string>{"Sci-fi master"});
    EXPECT_EQ(row->birth_year, std::optional<std::int32_t>{1920});
}

TEST_F(BookstoreTest, GetAuthorNotFound) {
    auto row = get_author(db_, 999);
    EXPECT_FALSE(row.has_value());
}

TEST_F(BookstoreTest, GetBook) {
    seed();
    auto row = get_book(db_, 1);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->title, "Foundation");
    EXPECT_EQ(row->genre, "sci-fi");
    EXPECT_EQ(row->author_id, 1);
}

TEST_F(BookstoreTest, GetBookNotFound) {
    auto row = get_book(db_, 999);
    EXPECT_FALSE(row.has_value());
}

// ─── :many ───────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, ListAuthors) {
    seed();
    auto rows = list_authors(db_);
    ASSERT_EQ(rows.size(), 3u);
    // Sorted by name
    EXPECT_EQ(rows[0].name, "Asimov");
    EXPECT_EQ(rows[1].name, "Herbert");
    EXPECT_EQ(rows[2].name, "Le Guin");
}

TEST_F(BookstoreTest, ListBooksByGenre) {
    seed();
    auto scifi = list_books_by_genre(db_, "sci-fi");
    EXPECT_EQ(scifi.size(), 3u);

    auto fantasy = list_books_by_genre(db_, "fantasy");
    ASSERT_EQ(fantasy.size(), 1u);
    EXPECT_EQ(fantasy[0].title, "Earthsea");

    auto none = list_books_by_genre(db_, "horror");
    EXPECT_TRUE(none.empty());
}

TEST_F(BookstoreTest, ListBooksByGenreOrAll) {
    seed();
    auto all = list_books_by_genre_or_all(db_, "all");
    EXPECT_EQ(all.size(), 4u);

    auto scifi = list_books_by_genre_or_all(db_, "sci-fi");
    EXPECT_EQ(scifi.size(), 3u);
}

TEST_F(BookstoreTest, GetBooksByIds) {
    seed();
    auto rows = get_books_by_ids(db_, {1, 3});
    ASSERT_EQ(rows.size(), 2u);

    std::vector<std::string> titles;
    for (const auto& b : rows) titles.push_back(b.title);
    std::sort(titles.begin(), titles.end());
    EXPECT_EQ(titles[0], "Dune");
    EXPECT_EQ(titles[1], "Foundation");
}

TEST_F(BookstoreTest, GetBooksByIdsEmpty) {
    seed();
    auto rows = get_books_by_ids(db_, {});
    EXPECT_TRUE(rows.empty());
}

// ─── :exec ───────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, CreateAuthorNullableBio) {
    create_author(db_, "NewAuthor", std::nullopt, std::nullopt);
    auto row = get_author(db_, 1);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->name, "NewAuthor");
    EXPECT_FALSE(row->bio.has_value());
    EXPECT_FALSE(row->birth_year.has_value());
}

TEST_F(BookstoreTest, CreateBookNullPublishedAt) {
    seed();
    create_book(db_, 1, "New Book", "mystery", 14.50, std::nullopt);
    auto row = get_book(db_, 5);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->title, "New Book");
    EXPECT_EQ(row->genre, "mystery");
    EXPECT_FALSE(row->published_at.has_value());
}

TEST_F(BookstoreTest, UpdateAuthorBio) {
    seed();
    update_author_bio(db_, std::optional<std::string>{"Updated bio"}, 1);
    auto row = get_author(db_, 1);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->bio, std::optional<std::string>{"Updated bio"});
}

TEST_F(BookstoreTest, UpdateAuthorBioNull) {
    seed();
    update_author_bio(db_, std::nullopt, 1);
    auto row = get_author(db_, 1);
    ASSERT_TRUE(row.has_value());
    EXPECT_FALSE(row->bio.has_value());
}

TEST_F(BookstoreTest, DeleteAuthor) {
    // Author with no books; FK enforcement won't block the delete.
    create_author(db_, "Temp", std::nullopt, std::nullopt);
    delete_author(db_, 1);
    EXPECT_FALSE(get_author(db_, 1).has_value());
}

// ─── :execrows ───────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, DeleteBookById) {
    seed();
    // Book 2 (I Robot) has no sale_items, safe to delete.
    std::int64_t affected = delete_book_by_id(db_, 2);
    EXPECT_EQ(affected, 1);

    std::int64_t miss = delete_book_by_id(db_, 999);
    EXPECT_EQ(miss, 0);
}

// ─── JOIN ─────────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, ListBooksWithAuthor) {
    seed();
    auto rows = list_books_with_author(db_);
    ASSERT_EQ(rows.size(), 4u);

    auto dune = std::find_if(rows.begin(), rows.end(), [](const auto& r){ return r.title == "Dune"; });
    ASSERT_NE(dune, rows.end());
    EXPECT_EQ(dune->author_name, "Herbert");
    EXPECT_FALSE(dune->author_bio.has_value());

    auto foundation = std::find_if(rows.begin(), rows.end(), [](const auto& r){ return r.title == "Foundation"; });
    ASSERT_NE(foundation, rows.end());
    EXPECT_EQ(foundation->author_name, "Asimov");
    EXPECT_EQ(foundation->author_bio, std::optional<std::string>{"Sci-fi master"});
}

// ─── LEFT JOIN ────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksNeverOrdered) {
    seed();
    auto rows = get_books_never_ordered(db_);
    // Foundation (1) and Dune (3) were ordered; I Robot (2) and Earthsea (4) were not.
    ASSERT_EQ(rows.size(), 2u);

    std::vector<std::string> titles;
    for (const auto& b : rows) titles.push_back(b.title);
    std::sort(titles.begin(), titles.end());
    EXPECT_EQ(titles[0], "Earthsea");
    EXPECT_EQ(titles[1], "I Robot");
}

// ─── CTE ──────────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetTopSellingBooks) {
    seed();
    auto rows = get_top_selling_books(db_);
    ASSERT_FALSE(rows.empty());
    // Foundation had qty 2, Dune had qty 1 → Foundation is first.
    EXPECT_EQ(rows[0].title, "Foundation");
    EXPECT_EQ(rows[0].units_sold, std::optional<std::int64_t>{2});
}

TEST_F(BookstoreTest, GetBestCustomers) {
    seed();
    auto rows = get_best_customers(db_);
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].name, "Alice");
    // Alice spent: 2*9.99 + 1*12.99 = 32.97
    ASSERT_TRUE(rows[0].total_spent.has_value());
    EXPECT_NEAR(*rows[0].total_spent, 32.97, 0.01);
}

// ─── Aggregate (COUNT GROUP BY) ──────────────────────────────────────────────

TEST_F(BookstoreTest, CountBooksByGenre) {
    seed();
    auto rows = count_books_by_genre(db_);
    ASSERT_EQ(rows.size(), 2u);

    auto fantasy = std::find_if(rows.begin(), rows.end(), [](const auto& r){ return r.genre == "fantasy"; });
    ASSERT_NE(fantasy, rows.end());
    EXPECT_EQ(fantasy->book_count, 1);

    auto scifi = std::find_if(rows.begin(), rows.end(), [](const auto& r){ return r.genre == "sci-fi"; });
    ASSERT_NE(scifi, rows.end());
    EXPECT_EQ(scifi->book_count, 3);
}

// ─── Product: REAL, BLOB, TEXT primary key ────────────────────────────────────

TEST_F(BookstoreTest, InsertProduct) {
    insert_product(db_, "prod-1", "SKU-001", "Widget", 1,
                   std::optional<float>{1.5f},
                   std::optional<float>{4.7f},
                   std::optional<std::string>{R"({"color":"red"})"},
                   std::nullopt,
                   42);

    auto row = get_product(db_, "prod-1");
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->id, "prod-1");
    EXPECT_EQ(row->name, "Widget");
    EXPECT_EQ(row->stock_count, 42);
    EXPECT_TRUE(row->weight_kg.has_value());
    EXPECT_NEAR(*row->weight_kg, 1.5f, 0.001f);
}

TEST_F(BookstoreTest, UpsertProduct) {
    upsert_product(db_, "prod-2", "SKU-002", "Gadget", 1, std::nullopt, 10);

    auto row = get_product(db_, "prod-2");
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->name, "Gadget");
    EXPECT_EQ(row->stock_count, 10);

    // Upsert again — should update.
    upsert_product(db_, "prod-2", "SKU-002", "Gadget Pro", 1, std::nullopt, 20);

    auto updated = get_product(db_, "prod-2");
    ASSERT_TRUE(updated.has_value());
    EXPECT_EQ(updated->name, "Gadget Pro");
    EXPECT_EQ(updated->stock_count, 20);
}

TEST_F(BookstoreTest, GetProductNotFound) {
    auto row = get_product(db_, "no-such-id");
    EXPECT_FALSE(row.has_value());
}

// ─── CreateCustomer / CreateSale / AddSaleItem ────────────────────────────────

TEST_F(BookstoreTest, CreateCustomer) {
    create_customer(db_, "Bob", "bob@example.com");
    EXPECT_EQ(last_id(db_), 1);
}

TEST_F(BookstoreTest, CreateSale) {
    create_customer(db_, "Bob", "bob@example.com");
    create_sale(db_, last_id(db_));
    EXPECT_EQ(last_id(db_), 1);
}

TEST_F(BookstoreTest, AddSaleItem) {
    seed();
    // Add I Robot (book 2) to sale 1 — it has no items yet.
    add_sale_item(db_, 1, 2, 3, 7.99);
    // Sale 1 now has 3 items (Foundation×2, Dune×1, I Robot×3).
    auto cnt = count_sale_items(db_, 1);
    ASSERT_TRUE(cnt.has_value());
    EXPECT_EQ(cnt->item_count, 3);
}

// ─── ListBooksWithLimit ───────────────────────────────────────────────────────

TEST_F(BookstoreTest, ListBooksWithLimit) {
    seed();
    // Books ordered by title: Dune, Earthsea, Foundation, I Robot
    auto page1 = list_books_with_limit(db_, 2, 0);
    ASSERT_EQ(page1.size(), 2u);
    EXPECT_EQ(page1[0].title, "Dune");
    EXPECT_EQ(page1[1].title, "Earthsea");

    auto page2 = list_books_with_limit(db_, 2, 2);
    ASSERT_EQ(page2.size(), 2u);
    EXPECT_EQ(page2[0].title, "Foundation");
    EXPECT_EQ(page2[1].title, "I Robot");

    auto empty = list_books_with_limit(db_, 2, 10);
    EXPECT_TRUE(empty.empty());
}

// ─── SearchBooksByTitle ───────────────────────────────────────────────────────

TEST_F(BookstoreTest, SearchBooksByTitle) {
    seed();
    auto rows = search_books_by_title(db_, "%un%");
    // Matches "Foundation" and "Dune"
    ASSERT_EQ(rows.size(), 2u);

    auto none = search_books_by_title(db_, "%zzz%");
    EXPECT_TRUE(none.empty());
}

// ─── GetBooksByPriceRange ─────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksByPriceRange) {
    seed();
    // Prices: 7.99 (I Robot), 8.99 (Earthsea), 9.99 (Foundation), 12.99 (Dune)
    auto rows = get_books_by_price_range(db_, 8.0, 10.0);
    ASSERT_EQ(rows.size(), 2u);
    // Ordered by price: Earthsea, Foundation
    EXPECT_EQ(rows[0].title, "Earthsea");
    EXPECT_EQ(rows[1].title, "Foundation");
}

// ─── GetBooksInGenres ─────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksInGenres) {
    seed();
    auto rows = get_books_in_genres(db_, "sci-fi", "fantasy", "sci-fi");
    EXPECT_EQ(rows.size(), 4u);

    auto sci_only = get_books_in_genres(db_, "sci-fi", "sci-fi", "sci-fi");
    EXPECT_EQ(sci_only.size(), 3u);
}

// ─── GetBookPriceLabel ────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBookPriceLabel) {
    seed();
    // Threshold 10.0 — only Dune (12.99) is expensive.
    auto rows = get_book_price_label(db_, 10.0);
    ASSERT_EQ(rows.size(), 4u);

    auto dune = std::find_if(rows.begin(), rows.end(),
                             [](const auto& r){ return r.title == "Dune"; });
    ASSERT_NE(dune, rows.end());
    EXPECT_EQ(dune->price_label, "expensive");

    auto foundation = std::find_if(rows.begin(), rows.end(),
                                   [](const auto& r){ return r.title == "Foundation"; });
    ASSERT_NE(foundation, rows.end());
    EXPECT_EQ(foundation->price_label, "affordable");
}

// ─── GetBookPriceOrDefault ────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBookPriceOrDefault) {
    seed();
    // All books have prices, so the default is never used.
    auto rows = get_book_price_or_default(db_, std::optional<double>{0.0});
    ASSERT_EQ(rows.size(), 4u);

    auto dune = std::find_if(rows.begin(), rows.end(),
                             [](const auto& r){ return r.title == "Dune"; });
    ASSERT_NE(dune, rows.end());
    EXPECT_NEAR(dune->effective_price, 12.99, 0.001);
}

// ─── GetGenresWithManyBooks ───────────────────────────────────────────────────

TEST_F(BookstoreTest, GetGenresWithManyBooks) {
    seed();
    // sci-fi has 3 books, fantasy has 1. HAVING COUNT(*) > 2 → only sci-fi.
    auto rows = get_genres_with_many_books(db_, 2);
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].genre, "sci-fi");
    EXPECT_EQ(rows[0].book_count, 3);

    // HAVING COUNT(*) > 0 → both genres.
    auto both = get_genres_with_many_books(db_, 0);
    EXPECT_EQ(both.size(), 2u);
}

// ─── GetBooksByAuthorParam ────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksByAuthorParam) {
    seed();
    // birth_year > 1920 → Le Guin (1929) → Earthsea.
    auto rows = get_books_by_author_param(db_, std::optional<std::int32_t>{1920});
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].title, "Earthsea");

    // NULL birth_year → NULL comparison → no matches.
    auto none = get_books_by_author_param(db_, std::nullopt);
    EXPECT_TRUE(none.empty());
}

// ─── GetAllBookFields ─────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetAllBookFields) {
    seed();
    auto rows = get_all_book_fields(db_);
    ASSERT_EQ(rows.size(), 4u);
    // Ordered by id: Foundation first.
    EXPECT_EQ(rows[0].title, "Foundation");
    EXPECT_EQ(rows[0].genre, "sci-fi");
    EXPECT_NEAR(rows[0].price, 9.99, 0.001);
}

// ─── GetBooksNotByAuthor ──────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksNotByAuthor) {
    seed();
    auto rows = get_books_not_by_author(db_, "Asimov");
    ASSERT_EQ(rows.size(), 2u);

    std::vector<std::string> titles;
    for (const auto& b : rows) titles.push_back(b.title);
    std::sort(titles.begin(), titles.end());
    EXPECT_EQ(titles[0], "Dune");
    EXPECT_EQ(titles[1], "Earthsea");
}

// ─── GetBooksWithRecentSales ──────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksWithRecentSales) {
    seed();
    // A date in the past should catch the seeded sale.
    auto rows = get_books_with_recent_sales(db_, "2000-01-01");
    ASSERT_EQ(rows.size(), 2u);

    std::vector<std::string> titles;
    for (const auto& b : rows) titles.push_back(b.title);
    std::sort(titles.begin(), titles.end());
    EXPECT_EQ(titles[0], "Dune");
    EXPECT_EQ(titles[1], "Foundation");

    // A date in the far future → no results.
    auto none = get_books_with_recent_sales(db_, "2999-01-01");
    EXPECT_TRUE(none.empty());
}

// ─── GetBookWithAuthorName ────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBookWithAuthorName) {
    seed();
    auto rows = get_book_with_author_name(db_);
    ASSERT_EQ(rows.size(), 4u);

    auto foundation = std::find_if(rows.begin(), rows.end(),
                                   [](const auto& r){ return r.title == "Foundation"; });
    ASSERT_NE(foundation, rows.end());
    ASSERT_TRUE(foundation->author_name.has_value());
    EXPECT_EQ(*foundation->author_name, "Asimov");
}

// ─── GetAuthorStats ───────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetAuthorStats) {
    seed();
    auto rows = get_author_stats(db_);
    ASSERT_EQ(rows.size(), 3u);
    // Ordered by name: Asimov, Herbert, Le Guin.
    auto asimov = rows[0];
    EXPECT_EQ(asimov.name, "Asimov");
    EXPECT_EQ(asimov.num_books, 2);
    EXPECT_EQ(asimov.total_sold, 2);  // Foundation×2

    auto herbert = rows[1];
    EXPECT_EQ(herbert.name, "Herbert");
    EXPECT_EQ(herbert.num_books, 1);
    EXPECT_EQ(herbert.total_sold, 1);  // Dune×1

    auto leguin = rows[2];
    EXPECT_EQ(leguin.name, "Le Guin");
    EXPECT_EQ(leguin.num_books, 1);
    EXPECT_EQ(leguin.total_sold, 0);
}

// ─── ListActiveProducts ───────────────────────────────────────────────────────

TEST_F(BookstoreTest, ListActiveProducts) {
    insert_product(db_, "p1", "SKU-1", "Widget", 1,
                   std::nullopt, std::nullopt, std::nullopt, std::nullopt, 5);
    insert_product(db_, "p2", "SKU-2", "Gadget", 0,
                   std::nullopt, std::nullopt, std::nullopt, std::nullopt, 3);

    auto active = list_active_products(db_, 1);
    ASSERT_EQ(active.size(), 1u);
    EXPECT_EQ(active[0].name, "Widget");

    auto inactive = list_active_products(db_, 0);
    ASSERT_EQ(inactive.size(), 1u);
    EXPECT_EQ(inactive[0].name, "Gadget");
}

// ─── GetAuthorsWithNullBio / GetAuthorsWithBio ────────────────────────────────

TEST_F(BookstoreTest, GetAuthorsWithNullBio) {
    seed();
    // Herbert has no bio.
    auto rows = get_authors_with_null_bio(db_);
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].name, "Herbert");
    EXPECT_FALSE(rows[0].birth_year.has_value() && rows[0].birth_year.value() == 0);
    EXPECT_EQ(rows[0].birth_year, std::optional<std::int32_t>{1920});
}

TEST_F(BookstoreTest, GetAuthorsWithBio) {
    seed();
    // Asimov and Le Guin have bios.
    auto rows = get_authors_with_bio(db_);
    ASSERT_EQ(rows.size(), 2u);
    // Ordered by name: Asimov, Le Guin.
    EXPECT_EQ(rows[0].name, "Asimov");
    EXPECT_EQ(rows[0].bio, std::optional<std::string>{"Sci-fi master"});
    EXPECT_EQ(rows[1].name, "Le Guin");
}

// ─── GetBooksPublishedBetween ─────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksPublishedBetween) {
    seed();
    // "1950-01-01" to "1960-01-01" → I Robot (1950) + Foundation (1951).
    auto rows = get_books_published_between(
        db_,
        std::optional<std::string>{"1950-01-01"},
        std::optional<std::string>{"1960-01-01"});
    ASSERT_EQ(rows.size(), 2u);
    // Ordered by published_at.
    EXPECT_EQ(rows[0].title, "I Robot");
    EXPECT_EQ(rows[1].title, "Foundation");
}

// ─── GetDistinctGenres ────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetDistinctGenres) {
    seed();
    auto rows = get_distinct_genres(db_);
    ASSERT_EQ(rows.size(), 2u);
    // Ordered alphabetically.
    EXPECT_EQ(rows[0].genre, "fantasy");
    EXPECT_EQ(rows[1].genre, "sci-fi");
}

// ─── GetBooksWithSalesCount ───────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksWithSalesCount) {
    seed();
    auto rows = get_books_with_sales_count(db_);
    ASSERT_EQ(rows.size(), 4u);
    // Ordered by total_quantity DESC, title.
    // Foundation: 2, Dune: 1, Earthsea: 0, I Robot: 0.
    EXPECT_EQ(rows[0].title, "Foundation");
    EXPECT_EQ(rows[0].total_quantity, 2);
    EXPECT_EQ(rows[1].title, "Dune");
    EXPECT_EQ(rows[1].total_quantity, 1);
    EXPECT_EQ(rows[2].total_quantity, 0);
    EXPECT_EQ(rows[3].total_quantity, 0);
}

// ─── CountSaleItems ───────────────────────────────────────────────────────────

TEST_F(BookstoreTest, CountSaleItems) {
    seed();
    // Sale 1 has 2 items (Foundation + Dune).
    auto row = count_sale_items(db_, 1);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->item_count, 2);

    // Non-existent sale → 0.
    auto none = count_sale_items(db_, 999);
    ASSERT_TRUE(none.has_value());
    EXPECT_EQ(none->item_count, 0);
}

// ─── GetSaleItemQuantityAggregates ────────────────────────────────────────────

TEST_F(BookstoreTest, GetSaleItemQuantityAggregates) {
    seed();
    // Quantities: Foundation×2, Dune×1.
    auto row = get_sale_item_quantity_aggregates(db_);
    ASSERT_TRUE(row.has_value());
    ASSERT_TRUE(row->min_qty.has_value());
    ASSERT_TRUE(row->max_qty.has_value());
    ASSERT_TRUE(row->sum_qty.has_value());
    ASSERT_TRUE(row->avg_qty.has_value());
    EXPECT_EQ(*row->min_qty, 1);
    EXPECT_EQ(*row->max_qty, 2);
    EXPECT_EQ(*row->sum_qty, 3);
    EXPECT_NEAR(*row->avg_qty, 1.5, 0.001);
}

// ─── GetBookPriceAggregates ───────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBookPriceAggregates) {
    seed();
    // Prices: 9.99 (Foundation), 7.99 (I Robot), 12.99 (Dune), 8.99 (Earthsea).
    auto row = get_book_price_aggregates(db_);
    ASSERT_TRUE(row.has_value());
    ASSERT_TRUE(row->min_price.has_value());
    ASSERT_TRUE(row->max_price.has_value());
    ASSERT_TRUE(row->sum_price.has_value());
    ASSERT_TRUE(row->avg_price.has_value());
    EXPECT_NEAR(*row->min_price, 7.99, 0.001);
    EXPECT_NEAR(*row->max_price, 12.99, 0.001);
    EXPECT_NEAR(*row->sum_price, 39.96, 0.01);
    EXPECT_NEAR(*row->avg_price, 9.99, 0.01);
}
