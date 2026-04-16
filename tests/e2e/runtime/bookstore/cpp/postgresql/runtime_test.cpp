#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

#include <pqxx/pqxx>
#include <gtest/gtest.h>

#include "gen/queries/queries.hpp"

using namespace db;

namespace fs = std::filesystem;

static std::string read_file(const fs::path& path) {
    std::ifstream f(path);
    if (!f) throw std::runtime_error("cannot open: " + path.string());
    return {std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
}

static std::string random_schema_name() {
    static const char hex[] = "0123456789abcdef";
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(0, 15);
    std::string s = "test_";
    for (int i = 0; i < 12; ++i) s += hex[dist(gen)];
    return s;
}

static std::string db_url() {
    const char* e = std::getenv("DATABASE_URL");
    return e ? e : "postgres://sqltgen:sqltgen@localhost:15432/sqltgen_e2e";
}

class BookstoreTest : public ::testing::Test {
    std::unique_ptr<pqxx::connection> conn_;
    std::string schema_name_;

protected:
    pqxx::connection& conn() { return *conn_; }

    void SetUp() override {
        schema_name_ = random_schema_name();
        conn_ = std::make_unique<pqxx::connection>(db_url());

        // Create schema, set search_path, and apply fixture schema in one transaction.
        pqxx::work txn(*conn_);
        txn.exec("CREATE SCHEMA " + txn.quote_name(schema_name_));
        txn.exec("SET search_path TO " + txn.quote_name(schema_name_));
        txn.exec(read_file(SCHEMA_PATH));
        txn.commit();
    }

    void TearDown() override {
        try {
            pqxx::work txn(*conn_);
            txn.exec("DROP SCHEMA IF EXISTS " + txn.quote_name(schema_name_) + " CASCADE");
            txn.commit();
        } catch (...) {}
        conn_.reset();
    }

    // Seed the database with a fixed, predictable dataset.
    // Returns the IDs of created rows so tests can reference them.
    struct SeedIds {
        std::int64_t asimov_id, herbert_id, le_guin_id;
        std::int64_t foundation_id, i_robot_id, dune_id, earthsea_id;
        std::int64_t alice_id, sale1_id;
    };

    SeedIds seed() {
        auto a1 = create_author(conn(), "Asimov",  std::optional<std::string>{"Sci-fi master"}, std::optional<std::int32_t>{1920}).value();
        auto a2 = create_author(conn(), "Herbert", std::nullopt,                                  std::optional<std::int32_t>{1920}).value();
        auto a3 = create_author(conn(), "Le Guin", std::optional<std::string>{"Earthsea"},       std::optional<std::int32_t>{1929}).value();

        auto b1 = create_book(conn(), a1.id, "Foundation", "sci-fi",  "9.99",  std::optional<std::string>{"1951-01-01"}).value();
        auto b2 = create_book(conn(), a1.id, "I Robot",    "sci-fi",  "7.99",  std::optional<std::string>{"1950-01-01"}).value();
        auto b3 = create_book(conn(), a2.id, "Dune",       "sci-fi",  "12.99", std::optional<std::string>{"1965-01-01"}).value();
        auto b4 = create_book(conn(), a3.id, "Earthsea",   "fantasy", "8.99",  std::optional<std::string>{"1968-01-01"}).value();

        auto c1 = create_customer(conn(), "Alice", "alice@example.com").value();
        auto s1 = create_sale(conn(), c1.id).value();

        add_sale_item(conn(), s1.id, b1.id, 2, "9.99");   // Foundation x2
        add_sale_item(conn(), s1.id, b3.id, 1, "12.99");  // Dune x1

        return {a1.id, a2.id, a3.id, b1.id, b2.id, b3.id, b4.id, c1.id, s1.id};
    }
};

// ─── RETURNING :one (create) ──────────────────────────────────────────────────

TEST_F(BookstoreTest, CreateAuthorReturning) {
    auto row = create_author(conn(), "Test", std::optional<std::string>{"bio"}, std::optional<std::int32_t>{1980});
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->name, "Test");
    EXPECT_EQ(row->bio, std::optional<std::string>{"bio"});
    EXPECT_EQ(row->birth_year, std::optional<std::int32_t>{1980});
    EXPECT_GT(row->id, 0);
}

TEST_F(BookstoreTest, CreateBookReturning) {
    auto a = create_author(conn(), "Author", std::nullopt, std::nullopt).value();
    auto row = create_book(conn(), a.id, "New Book", "fiction", "14.99", std::nullopt);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->title, "New Book");
    EXPECT_EQ(row->price, "14.99");
    EXPECT_FALSE(row->published_at.has_value());
}

// ─── :one ─────────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetAuthor) {
    auto ids = seed();
    auto row = get_author(conn(), ids.asimov_id);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->name, "Asimov");
    EXPECT_EQ(row->bio, std::optional<std::string>{"Sci-fi master"});
    EXPECT_EQ(row->birth_year, std::optional<std::int32_t>{1920});
}

TEST_F(BookstoreTest, GetAuthorNotFound) {
    auto row = get_author(conn(), 999999);
    EXPECT_FALSE(row.has_value());
}

TEST_F(BookstoreTest, GetBook) {
    auto ids = seed();
    auto row = get_book(conn(), ids.foundation_id);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->title, "Foundation");
    EXPECT_EQ(row->genre, "sci-fi");
    EXPECT_EQ(row->author_id, ids.asimov_id);
}

TEST_F(BookstoreTest, GetBookNotFound) {
    auto row = get_book(conn(), 999999);
    EXPECT_FALSE(row.has_value());
}

// ─── :many ───────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, ListAuthors) {
    seed();
    auto rows = list_authors(conn());
    ASSERT_EQ(rows.size(), 3u);
    // Sorted by name
    EXPECT_EQ(rows[0].name, "Asimov");
    EXPECT_EQ(rows[1].name, "Herbert");
    EXPECT_EQ(rows[2].name, "Le Guin");
}

TEST_F(BookstoreTest, ListBooksByGenre) {
    seed();
    auto scifi = list_books_by_genre(conn(), "sci-fi");
    EXPECT_EQ(scifi.size(), 3u);

    auto fantasy = list_books_by_genre(conn(), "fantasy");
    ASSERT_EQ(fantasy.size(), 1u);
    EXPECT_EQ(fantasy[0].title, "Earthsea");

    auto none = list_books_by_genre(conn(), "horror");
    EXPECT_TRUE(none.empty());
}

TEST_F(BookstoreTest, ListBooksByGenreOrAll) {
    seed();
    auto all = list_books_by_genre_or_all(conn(), "all");
    EXPECT_EQ(all.size(), 4u);

    auto scifi = list_books_by_genre_or_all(conn(), "sci-fi");
    EXPECT_EQ(scifi.size(), 3u);
}

TEST_F(BookstoreTest, GetBooksByIds) {
    auto ids = seed();
    auto rows = get_books_by_ids(conn(), {ids.foundation_id, ids.dune_id});
    ASSERT_EQ(rows.size(), 2u);

    std::vector<std::string> titles;
    for (const auto& b : rows) titles.push_back(b.title);
    std::sort(titles.begin(), titles.end());
    EXPECT_EQ(titles[0], "Dune");
    EXPECT_EQ(titles[1], "Foundation");
}

TEST_F(BookstoreTest, GetBooksByIdsEmpty) {
    seed();
    auto rows = get_books_by_ids(conn(), {});
    EXPECT_TRUE(rows.empty());
}

// ─── UPDATE RETURNING ─────────────────────────────────────────────────────────

TEST_F(BookstoreTest, UpdateAuthorBio) {
    auto ids = seed();
    auto row = update_author_bio(conn(), std::optional<std::string>{"Updated bio"}, ids.asimov_id);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->name, "Asimov");
    EXPECT_EQ(row->bio, std::optional<std::string>{"Updated bio"});
}

TEST_F(BookstoreTest, UpdateAuthorBioNull) {
    auto ids = seed();
    auto row = update_author_bio(conn(), std::nullopt, ids.asimov_id);
    ASSERT_TRUE(row.has_value());
    EXPECT_FALSE(row->bio.has_value());
}

// ─── DELETE RETURNING ─────────────────────────────────────────────────────────

TEST_F(BookstoreTest, DeleteAuthor) {
    // Author with no books — FK won't block deletion.
    auto a = create_author(conn(), "Temp", std::nullopt, std::nullopt).value();
    auto row = delete_author(conn(), a.id);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->name, "Temp");
    EXPECT_EQ(row->id, a.id);

    EXPECT_FALSE(get_author(conn(), a.id).has_value());
}

TEST_F(BookstoreTest, DeleteAuthorNotFound) {
    auto row = delete_author(conn(), 999999);
    EXPECT_FALSE(row.has_value());
}

// ─── :execrows ───────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, DeleteBookById) {
    auto ids = seed();
    // I Robot has no sale_items, safe to delete.
    std::int64_t affected = delete_book_by_id(conn(), ids.i_robot_id);
    EXPECT_EQ(affected, 1);

    std::int64_t miss = delete_book_by_id(conn(), 999999);
    EXPECT_EQ(miss, 0);
}

// ─── :exec ───────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, AddSaleItem) {
    auto ids = seed();
    // Add Earthsea to sale1 — it has no sale_items yet.
    add_sale_item(conn(), ids.sale1_id, ids.earthsea_id, 3, "8.99");

    auto rows = get_books_never_ordered(conn());
    // After adding Earthsea to the sale, only I Robot remains unordered.
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].title, "I Robot");
}

// ─── JOIN ─────────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, ListBooksWithAuthor) {
    seed();
    auto rows = list_books_with_author(conn());
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
    auto rows = get_books_never_ordered(conn());
    // Foundation and Dune were ordered; I Robot and Earthsea were not.
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
    auto rows = get_top_selling_books(conn());
    ASSERT_FALSE(rows.empty());
    // Foundation had qty 2, Dune had qty 1 → Foundation is first.
    EXPECT_EQ(rows[0].title, "Foundation");
    EXPECT_EQ(rows[0].units_sold, std::optional<std::int64_t>{2});
}

TEST_F(BookstoreTest, GetBestCustomers) {
    seed();
    auto rows = get_best_customers(conn());
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].name, "Alice");
    // Alice spent: 2*9.99 + 1*12.99 = 32.97 (stored as NUMERIC string)
    ASSERT_TRUE(rows[0].total_spent.has_value());
    EXPECT_FALSE(rows[0].total_spent->empty());
}

// ─── Aggregate (COUNT GROUP BY) ──────────────────────────────────────────────

TEST_F(BookstoreTest, CountBooksByGenre) {
    seed();
    auto rows = count_books_by_genre(conn());
    ASSERT_EQ(rows.size(), 2u);

    auto fantasy = std::find_if(rows.begin(), rows.end(), [](const auto& r){ return r.genre == "fantasy"; });
    ASSERT_NE(fantasy, rows.end());
    EXPECT_EQ(fantasy->book_count, 1);

    auto scifi = std::find_if(rows.begin(), rows.end(), [](const auto& r){ return r.genre == "sci-fi"; });
    ASSERT_NE(scifi, rows.end());
    EXPECT_EQ(scifi->book_count, 3);
}

// ─── View query ───────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, ListBookSummariesView) {
    seed();
    auto rows = list_book_summaries_view(conn());
    ASSERT_EQ(rows.size(), 4u);

    auto dune = std::find_if(rows.begin(), rows.end(), [](const auto& r){ return r.title == "Dune"; });
    ASSERT_NE(dune, rows.end());
    EXPECT_EQ(dune->author_name, "Herbert");
    EXPECT_EQ(dune->genre, "sci-fi");
}
