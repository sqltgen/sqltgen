#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
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
        std::int64_t alice_id, bob_id, sale1_id, sale2_id;
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
        auto c2 = create_customer(conn(), "Bob",   "bob@example.com").value();

        auto s1 = create_sale(conn(), c1.id).value();
        add_sale_item(conn(), s1.id, b1.id, 2, "9.99");   // Foundation x2
        add_sale_item(conn(), s1.id, b3.id, 1, "12.99");  // Dune x1

        auto s2 = create_sale(conn(), c2.id).value();
        add_sale_item(conn(), s2.id, b4.id, 1, "8.99");   // Earthsea x1
        add_sale_item(conn(), s2.id, b1.id, 1, "9.99");   // Foundation x1

        return {a1.id, a2.id, a3.id, b1.id, b2.id, b3.id, b4.id, c1.id, c2.id, s1.id, s2.id};
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
    // Foundation, Dune, Earthsea all ordered; only I Robot was not.
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].title, "I Robot");
}

// ─── CTE ──────────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetTopSellingBooks) {
    seed();
    auto rows = get_top_selling_books(conn());
    ASSERT_FALSE(rows.empty());
    // Foundation: qty 2 (Alice) + qty 1 (Bob) = 3 → first.
    EXPECT_EQ(rows[0].title, "Foundation");
    EXPECT_EQ(rows[0].units_sold, std::optional<std::int64_t>{3});
}

TEST_F(BookstoreTest, GetBestCustomers) {
    seed();
    auto rows = get_best_customers(conn());
    ASSERT_EQ(rows.size(), 2u);
    // Alice spent: 2*9.99 + 1*12.99 = 32.97 → first
    EXPECT_EQ(rows[0].name, "Alice");
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

// ─── Customer / Sale creation ─────────────────────────────────────────────────

TEST_F(BookstoreTest, CreateCustomer) {
    auto row = create_customer(conn(), "Solo", "solo@example.com");
    ASSERT_TRUE(row.has_value());
    EXPECT_GT(row->id, 0);
}

TEST_F(BookstoreTest, CreateSale) {
    auto c = create_customer(conn(), "Solo", "solo@example.com").value();
    auto row = create_sale(conn(), c.id);
    ASSERT_TRUE(row.has_value());
    EXPECT_GT(row->id, 0);
}

// ─── LIMIT / OFFSET ───────────────────────────────────────────────────────────

TEST_F(BookstoreTest, ListBooksWithLimit) {
    seed();
    auto page1 = list_books_with_limit(conn(), 2, 0);
    ASSERT_EQ(page1.size(), 2u);

    auto page2 = list_books_with_limit(conn(), 2, 2);
    ASSERT_EQ(page2.size(), 2u);

    // Pages must not overlap (ordered by title, so page1 != page2 titles)
    for (const auto& b2 : page2) {
        for (const auto& b1 : page1) {
            EXPECT_NE(b1.title, b2.title);
        }
    }
}

// ─── LIKE ─────────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, SearchBooksByTitle) {
    seed();
    auto results = search_books_by_title(conn(), "%ound%");
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].title, "Foundation");

    auto empty = search_books_by_title(conn(), "NOPE%");
    EXPECT_TRUE(empty.empty());
}

// ─── BETWEEN ──────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksByPriceRange) {
    seed();
    // Foundation (9.99) and Earthsea (8.99) are in [8.00, 10.00]
    auto rows = get_books_by_price_range(conn(), "8.00", "10.00");
    ASSERT_EQ(rows.size(), 2u);
}

// ─── IN ───────────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksInGenres) {
    seed();
    auto rows = get_books_in_genres(conn(), "sci-fi", "fantasy", "horror");
    ASSERT_EQ(rows.size(), 4u);
}

// ─── CASE ─────────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBookPriceLabel) {
    seed();
    auto rows = get_book_price_label(conn(), "10.00");
    ASSERT_EQ(rows.size(), 4u);

    std::map<std::string, std::string> labels;
    for (const auto& r : rows) labels[r.title] = r.price_label;

    EXPECT_EQ(labels["Dune"],      "expensive");
    EXPECT_EQ(labels["Earthsea"],  "affordable");
    EXPECT_EQ(labels["Foundation"],"affordable");
    EXPECT_EQ(labels["I Robot"],   "affordable");
}

// ─── COALESCE ─────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBookPriceOrDefault) {
    seed();
    auto rows = get_book_price_or_default(conn(), std::optional<std::string>{"0.00"});
    ASSERT_EQ(rows.size(), 4u);

    auto dune = std::find_if(rows.begin(), rows.end(), [](const auto& r){ return r.title == "Dune"; });
    ASSERT_NE(dune, rows.end());
    EXPECT_EQ(dune->effective_price, "12.99");
}

// ─── HAVING ───────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetGenresWithManyBooks) {
    seed();
    // count > 1 → only sci-fi (3 books); fantasy has only 1
    auto rows = get_genres_with_many_books(conn(), 1);
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].genre, "sci-fi");
    EXPECT_EQ(rows[0].book_count, 3);
}

// ─── JOIN with param ──────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksByAuthorParam) {
    seed();
    // birth_year > 1925 → only Le Guin (1929) → Earthsea
    auto rows = get_books_by_author_param(conn(), std::optional<std::int32_t>{1925});
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].title, "Earthsea");
}

// ─── Qualified wildcard ───────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetAllBookFields) {
    seed();
    auto rows = get_all_book_fields(conn());
    ASSERT_EQ(rows.size(), 4u);
    EXPECT_FALSE(rows[0].title.empty());
}

// ─── Subquery ─────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksNotByAuthor) {
    seed();
    auto rows = get_books_not_by_author(conn(), "Asimov");
    ASSERT_EQ(rows.size(), 2u);
    for (const auto& r : rows) {
        EXPECT_NE(r.title, "Foundation");
        EXPECT_NE(r.title, "I Robot");
    }
}

// ─── EXISTS subquery ──────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksWithRecentSales) {
    seed();
    // All sales happened very recently; epoch cutoff → Foundation, Dune, Earthsea
    auto rows = get_books_with_recent_sales(conn(), "1970-01-01");
    ASSERT_EQ(rows.size(), 3u);

    std::vector<std::string> titles;
    for (const auto& r : rows) titles.push_back(r.title);
    std::sort(titles.begin(), titles.end());
    EXPECT_EQ(titles[0], "Dune");
    EXPECT_EQ(titles[1], "Earthsea");
    EXPECT_EQ(titles[2], "Foundation");
}

// ─── Scalar subquery ──────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBookWithAuthorName) {
    seed();
    auto rows = get_book_with_author_name(conn());
    ASSERT_EQ(rows.size(), 4u);

    auto foundation = std::find_if(rows.begin(), rows.end(), [](const auto& r){ return r.title == "Foundation"; });
    ASSERT_NE(foundation, rows.end());
    ASSERT_TRUE(foundation->author_name.has_value());
    EXPECT_EQ(foundation->author_name.value(), "Asimov");
}

// ─── Multi-CTE ────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetAuthorStats) {
    seed();
    auto rows = get_author_stats(conn());
    ASSERT_EQ(rows.size(), 3u);

    auto asimov = std::find_if(rows.begin(), rows.end(), [](const auto& r){ return r.name == "Asimov"; });
    ASSERT_NE(asimov, rows.end());
    EXPECT_EQ(asimov->num_books, 2);
    EXPECT_EQ(asimov->total_sold, 3);  // Foundation: qty 2 (Alice) + qty 1 (Bob)

    auto le_guin = std::find_if(rows.begin(), rows.end(), [](const auto& r){ return r.name == "Le Guin"; });
    ASSERT_NE(le_guin, rows.end());
    EXPECT_EQ(le_guin->total_sold, 1);  // Earthsea: qty 1 (Bob)
}

// ─── Data-modifying CTE ───────────────────────────────────────────────────────

TEST_F(BookstoreTest, ArchiveAndReturnBooks) {
    seed();
    // Clear sale_items so the FK doesn't block deletion of Foundation / I Robot
    pqxx::work txn(conn());
    txn.exec("DELETE FROM sale_item");
    txn.commit();

    // Books published before 1960: Foundation (1951) and I Robot (1950)
    auto archived = archive_and_return_books(conn(), std::optional<std::string>{"1960-01-01"});
    ASSERT_EQ(archived.size(), 2u);

    std::vector<std::string> titles;
    for (const auto& r : archived) titles.push_back(r.title);
    std::sort(titles.begin(), titles.end());
    EXPECT_EQ(titles[0], "Foundation");
    EXPECT_EQ(titles[1], "I Robot");

    // Only Dune and Earthsea remain
    auto remaining = list_books_by_genre(conn(), "sci-fi");
    ASSERT_EQ(remaining.size(), 1u);
    EXPECT_EQ(remaining[0].title, "Dune");
}

// ─── Product ──────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, InsertProduct) {
    auto row = insert_product(conn(),
        "550e8400-e29b-41d4-a716-446655440000", "SKU-INS", "InsWidget",
        true, std::optional<float>{1.5f}, std::optional<double>{3.5},
        "{tag}", std::nullopt, std::nullopt, 7);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->id, "550e8400-e29b-41d4-a716-446655440000");
    EXPECT_EQ(row->name, "InsWidget");
    EXPECT_EQ(row->stock_count, 7);
}

TEST_F(BookstoreTest, GetProduct) {
    const std::string id = "550e8400-e29b-41d4-a716-446655440001";
    insert_product(conn(), id, "SKU-GET", "GetWidget",
        true, std::nullopt, std::nullopt, "{}", std::nullopt, std::nullopt, 3);

    auto row = get_product(conn(), id);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->name, "GetWidget");
    EXPECT_EQ(row->stock_count, 3);
}

TEST_F(BookstoreTest, ListActiveProducts) {
    insert_product(conn(), "550e8400-e29b-41d4-a716-446655440010",
        "SKU-A", "Active",   true,  std::nullopt, std::nullopt, "{}", std::nullopt, std::nullopt, 0);
    insert_product(conn(), "550e8400-e29b-41d4-a716-446655440011",
        "SKU-B", "Inactive", false, std::nullopt, std::nullopt, "{archived}", std::nullopt, std::nullopt, 0);

    auto active = list_active_products(conn(), true);
    ASSERT_EQ(active.size(), 1u);
    EXPECT_EQ(active[0].name, "Active");

    auto inactive = list_active_products(conn(), false);
    ASSERT_EQ(inactive.size(), 1u);
    EXPECT_EQ(inactive[0].name, "Inactive");
}

TEST_F(BookstoreTest, UpsertProduct) {
    const std::string id = "550e8400-e29b-41d4-a716-446655440020";

    // Initial insert via upsert
    upsert_product(conn(), id, "SKU-U1", "Original", true, "{a}", 5);

    // Upsert again — should update name, tags, stock_count
    auto row = upsert_product(conn(), id, "SKU-U1", "Updated", true, "{a,b}", 10);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->id, id);
    EXPECT_EQ(row->name, "Updated");
    EXPECT_EQ(row->stock_count, 10);
}

// ─── IS NULL / IS NOT NULL ────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetAuthorsWithNullBio) {
    seed();
    auto rows = get_authors_with_null_bio(conn());
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].name, "Herbert");
}

TEST_F(BookstoreTest, GetAuthorsWithBio) {
    seed();
    auto rows = get_authors_with_bio(conn());
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0].name, "Asimov");
    EXPECT_EQ(rows[1].name, "Le Guin");
}

// ─── Date BETWEEN ─────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksPublishedBetween) {
    seed();
    // 1950–1960: Foundation (1951) and I Robot (1950)
    auto early = get_books_published_between(conn(),
        std::optional<std::string>{"1950-01-01"},
        std::optional<std::string>{"1960-01-01"});
    ASSERT_EQ(early.size(), 2u);
    std::vector<std::string> earlyTitles;
    for (const auto& r : early) earlyTitles.push_back(r.title);
    std::sort(earlyTitles.begin(), earlyTitles.end());
    EXPECT_EQ(earlyTitles[0], "Foundation");
    EXPECT_EQ(earlyTitles[1], "I Robot");

    // 1961–1970: Dune (1965) and Earthsea (1968)
    auto later = get_books_published_between(conn(),
        std::optional<std::string>{"1961-01-01"},
        std::optional<std::string>{"1970-01-01"});
    ASSERT_EQ(later.size(), 2u);
    std::vector<std::string> laterTitles;
    for (const auto& r : later) laterTitles.push_back(r.title);
    std::sort(laterTitles.begin(), laterTitles.end());
    EXPECT_EQ(laterTitles[0], "Dune");
    EXPECT_EQ(laterTitles[1], "Earthsea");
}

// ─── DISTINCT ─────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetDistinctGenres) {
    seed();
    auto rows = get_distinct_genres(conn());
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0].genre, "fantasy");
    EXPECT_EQ(rows[1].genre, "sci-fi");
}

// ─── LEFT JOIN aggregate ──────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksWithSalesCount) {
    seed();
    auto rows = get_books_with_sales_count(conn());
    ASSERT_EQ(rows.size(), 4u);

    // Foundation: qty 2 (Alice) + qty 1 (Bob) = 3 → first
    EXPECT_EQ(rows[0].title, "Foundation");
    EXPECT_EQ(rows[0].total_quantity, 3);

    auto i_robot = std::find_if(rows.begin(), rows.end(), [](const auto& r){ return r.title == "I Robot"; });
    ASSERT_NE(i_robot, rows.end());
    EXPECT_EQ(i_robot->total_quantity, 0);
}

// ─── COUNT :one ───────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, CountSaleItems) {
    auto ids = seed();
    auto row = count_sale_items(conn(), ids.sale1_id);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->item_count, 2);  // Foundation + Dune
}

// ─── MIN / MAX / SUM / AVG aggregates ────────────────────────────────────────

TEST_F(BookstoreTest, GetSaleItemQuantityAggregates) {
    seed();
    // Items: Foundation qty 2 (Alice), Dune qty 1 (Alice), Earthsea qty 1 (Bob),
    //        Foundation qty 1 (Bob) → min=1, max=2, sum=5
    auto row = get_sale_item_quantity_aggregates(conn());
    ASSERT_TRUE(row.has_value());
    ASSERT_TRUE(row->min_qty.has_value());
    EXPECT_EQ(row->min_qty.value(), 1);
    ASSERT_TRUE(row->max_qty.has_value());
    EXPECT_EQ(row->max_qty.value(), 2);
    ASSERT_TRUE(row->sum_qty.has_value());
    EXPECT_EQ(row->sum_qty.value(), 5);
    EXPECT_TRUE(row->avg_qty.has_value());
}

TEST_F(BookstoreTest, GetBookPriceAggregates) {
    seed();
    // Prices: 9.99, 7.99, 12.99, 8.99 → min=7.99, max=12.99, sum=39.96
    auto row = get_book_price_aggregates(conn());
    ASSERT_TRUE(row.has_value());
    ASSERT_TRUE(row->min_price.has_value());
    EXPECT_EQ(row->min_price.value(), "7.99");
    ASSERT_TRUE(row->max_price.has_value());
    EXPECT_EQ(row->max_price.value(), "12.99");
    ASSERT_TRUE(row->sum_price.has_value());
    EXPECT_EQ(row->sum_price.value(), "39.96");
    EXPECT_TRUE(row->avg_price.has_value());
}
