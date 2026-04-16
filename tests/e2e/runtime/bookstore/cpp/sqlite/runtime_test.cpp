#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
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

TEST_F(BookstoreTest, InsertAndGetProduct) {
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
