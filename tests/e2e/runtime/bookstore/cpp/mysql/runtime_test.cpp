#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <random>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

#include <mysql.h>
#include <gtest/gtest.h>

#include "gen/queries/queries.hpp"

using namespace db;

namespace fs = std::filesystem;

static std::string read_file(const fs::path& path) {
    std::ifstream f(path);
    if (!f) throw std::runtime_error("cannot open: " + path.string());
    return {std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
}

static std::string random_db_name() {
    static const char hex[] = "0123456789abcdef";
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(0, 15);
    std::string s = "test_";
    for (int i = 0; i < 12; ++i) s += hex[dist(gen)];
    return s;
}

// Parse mysql://user:pass@host[:port]/db — returns {host, port, user, pass}.
struct DBParams { std::string host, user, pass; unsigned int port = 0; };

static DBParams parse_db_params() {
    const char* url = std::getenv("DATABASE_URL");
    if (!url) return {"localhost", "sqltgen", "sqltgen", 3306};
    std::string s(url);
    // strip scheme
    auto scheme_end = s.find("://");
    if (scheme_end != std::string::npos) s = s.substr(scheme_end + 3);
    // user:pass@host[:port]/db
    auto at = s.find('@');
    if (at == std::string::npos) return {"localhost", "sqltgen", "sqltgen", 3306};
    std::string userpass = s.substr(0, at);
    std::string hostdb = s.substr(at + 1);
    // strip /db from host[:port]
    auto slash = hostdb.find('/');
    std::string hostport = (slash != std::string::npos) ? hostdb.substr(0, slash) : hostdb;
    // split host and port
    std::string host;
    unsigned int port = 3306;
    auto colon2 = hostport.find(':');
    if (colon2 != std::string::npos) {
        host = hostport.substr(0, colon2);
        port = static_cast<unsigned int>(std::stoul(hostport.substr(colon2 + 1)));
    } else {
        host = hostport;
    }
    std::string user, pass;
    auto colon = userpass.find(':');
    if (colon != std::string::npos) {
        user = userpass.substr(0, colon);
        pass = userpass.substr(colon + 1);
    } else {
        user = userpass;
    }
    return {host, user, pass, port};
}

static void exec_sql(MYSQL* db, const std::string& sql) {
    if (mysql_query(db, sql.c_str()) != 0)
        throw std::runtime_error(mysql_error(db));
}

class BookstoreTest : public ::testing::Test {
    MYSQL* db_ = nullptr;
    std::string db_name_;

protected:
    MYSQL* db() { return db_; }

    std::int64_t last_insert_id() {
        return static_cast<std::int64_t>(mysql_insert_id(db_));
    }

    void SetUp() override {
        auto p = parse_db_params();
        // Retry to tolerate the race between depends_on: service_healthy and
        // MySQL's TCP listener being fully ready (healthcheck uses UNIX socket).
        constexpr int kMaxRetries = 30;
        for (int attempt = 0; ; ++attempt) {
            db_ = mysql_init(nullptr);
            if (!db_) throw std::runtime_error("mysql_init failed");
            if (mysql_real_connect(db_, p.host.c_str(), p.user.c_str(), p.pass.c_str(),
                                   nullptr, p.port, nullptr, 0))
                break;
            std::string err = mysql_error(db_);
            mysql_close(db_);
            db_ = nullptr;
            if (attempt >= kMaxRetries)
                throw std::runtime_error("connect: " + err);
            usleep(500'000); // 500ms
        }

        db_name_ = random_db_name();
        exec_sql(db_, "CREATE DATABASE `" + db_name_ + "`");
        exec_sql(db_, "USE `" + db_name_ + "`");

        // Apply schema: split on ';' and execute each statement individually
        // so the connection stays free of CLIENT_MULTI_STATEMENTS (required for
        // prepared statements used by the generated queries).
        std::string schema = read_file(SCHEMA_PATH);
        std::string stmt;
        for (char c : schema) {
            if (c == ';') {
                // trim whitespace
                auto start = stmt.find_first_not_of(" \t\r\n");
                if (start != std::string::npos) {
                    stmt = stmt.substr(start);
                    exec_sql(db_, stmt);
                }
                stmt.clear();
            } else {
                stmt += c;
            }
        }
    }

    void TearDown() override {
        if (db_) {
            try { exec_sql(db_, "DROP DATABASE IF EXISTS `" + db_name_ + "`"); }
            catch (...) {}
            mysql_close(db_);
            db_ = nullptr;
        }
    }

    struct SeedIds {
        std::int64_t alice_id, bob_id, charlie_id;
        std::int64_t book_alpha, book_beta, book_gamma, book_delta;
        std::int64_t customer_dan, customer_eve;
        std::int64_t sale1, sale2;
    };

    SeedIds seed() {
        // Authors
        create_author(db(), "Alice",   std::optional<std::string>{"Alice writes fiction"},     std::optional<std::int32_t>{1980});
        auto alice_id = last_insert_id();
        create_author(db(), "Bob",     std::nullopt,                                            std::optional<std::int32_t>{1975});
        auto bob_id = last_insert_id();
        create_author(db(), "Charlie", std::optional<std::string>{"Charlie writes nonfiction"}, std::nullopt);
        auto charlie_id = last_insert_id();

        // Books
        create_book(db(), alice_id,   "Alpha Book", "fiction", "19.99", std::optional<std::string>{"2020-06-15"});
        auto book_alpha = last_insert_id();
        create_book(db(), alice_id,   "Beta Book",  "science", "29.99", std::optional<std::string>{"2021-03-10"});
        auto book_beta = last_insert_id();
        create_book(db(), bob_id,     "Gamma Book", "fiction", "9.99",  std::optional<std::string>{"2019-01-01"});
        auto book_gamma = last_insert_id();
        create_book(db(), charlie_id, "Delta Book", "history", "14.99", std::nullopt);
        auto book_delta = last_insert_id();

        // Customers
        create_customer(db(), "Dan", "dan@example.com");
        auto customer_dan = last_insert_id();
        create_customer(db(), "Eve", "eve@example.com");
        auto customer_eve = last_insert_id();

        // Sales: Dan buys Alpha x2 and Gamma x1; Eve buys Beta x3
        create_sale(db(), customer_dan);
        auto sale1 = last_insert_id();
        create_sale(db(), customer_eve);
        auto sale2 = last_insert_id();

        add_sale_item(db(), sale1, book_alpha, 2, "19.99");
        add_sale_item(db(), sale1, book_gamma, 1, "9.99");
        add_sale_item(db(), sale2, book_beta,  3, "29.99");

        return {alice_id, bob_id, charlie_id,
                book_alpha, book_beta, book_gamma, book_delta,
                customer_dan, customer_eve,
                sale1, sale2};
    }

    // Insert a product row directly — the fixture has no generated InsertProduct query.
    void insert_product(const std::string& id, const std::string& sku,
                        const std::string& name, bool active,
                        const char* weight_kg, const char* rating,
                        const char* metadata, const char* thumbnail_hex,
                        int stock_count) {
        std::string sql = "INSERT INTO product (id, sku, name, active";
        std::string vals = "VALUES ('" + id + "', '" + sku + "', '" + name + "', " + (active ? "1" : "0");
        if (weight_kg) { sql += ", weight_kg"; vals += std::string(", ") + weight_kg; }
        if (rating)    { sql += ", rating";    vals += std::string(", ") + rating; }
        if (metadata)  { sql += ", metadata";  vals += std::string(", '") + metadata + "'"; }
        if (stock_count != 0 || true) { sql += ", stock_count"; vals += ", " + std::to_string(stock_count); }
        sql += ") " + vals + ")";
        exec_sql(db(), sql);
    }
};

// ─── Author CRUD ──────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, CreateAuthor) {
    create_author(db(), "Zara", std::optional<std::string>{"A test author"}, std::optional<std::int32_t>{1990});
    auto id = last_insert_id();
    auto row = get_author(db(), id);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->name, "Zara");
    EXPECT_EQ(row->bio, std::optional<std::string>{"A test author"});
    EXPECT_EQ(row->birth_year, std::optional<std::int32_t>{1990});
}

TEST_F(BookstoreTest, GetAuthor) {
    auto ids = seed();
    auto row = get_author(db(), ids.alice_id);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->name, "Alice");
    EXPECT_EQ(row->bio, std::optional<std::string>{"Alice writes fiction"});
    EXPECT_EQ(row->birth_year, std::optional<std::int32_t>{1980});
}

TEST_F(BookstoreTest, GetAuthorNotFound) {
    auto row = get_author(db(), 999999);
    EXPECT_FALSE(row.has_value());
}

TEST_F(BookstoreTest, ListAuthors) {
    seed();
    auto rows = list_authors(db());
    ASSERT_EQ(rows.size(), 3u);
    // Ordered by name: Alice, Bob, Charlie
    EXPECT_EQ(rows[0].name, "Alice");
    EXPECT_EQ(rows[1].name, "Bob");
    EXPECT_EQ(rows[2].name, "Charlie");
}

TEST_F(BookstoreTest, UpdateAuthorBio) {
    auto ids = seed();
    update_author_bio(db(), std::optional<std::string>{"Bob now has a bio"}, ids.bob_id);
    auto row = get_author(db(), ids.bob_id);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->bio, std::optional<std::string>{"Bob now has a bio"});
}

TEST_F(BookstoreTest, UpdateAuthorBioNull) {
    auto ids = seed();
    update_author_bio(db(), std::nullopt, ids.bob_id);
    auto row = get_author(db(), ids.bob_id);
    ASSERT_TRUE(row.has_value());
    EXPECT_FALSE(row->bio.has_value());
}

TEST_F(BookstoreTest, DeleteAuthor) {
    create_author(db(), "Temp", std::nullopt, std::nullopt);
    auto id = last_insert_id();
    delete_author(db(), id);
    EXPECT_FALSE(get_author(db(), id).has_value());
}

// ─── Book CRUD ────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, CreateBook) {
    create_author(db(), "Someone", std::nullopt, std::nullopt);
    auto author_id = last_insert_id();
    create_book(db(), author_id, "New Book", "mystery", "24.99",
                std::optional<std::string>{"2023-01-01"});
    auto book_id = last_insert_id();
    auto row = get_book(db(), book_id);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->title, "New Book");
    EXPECT_EQ(row->genre, "mystery");
    EXPECT_EQ(row->price, "24.99");
    EXPECT_TRUE(row->published_at.has_value());
}

TEST_F(BookstoreTest, GetBook) {
    auto ids = seed();
    auto row = get_book(db(), ids.book_alpha);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->title, "Alpha Book");
    EXPECT_EQ(row->author_id, ids.alice_id);
}

TEST_F(BookstoreTest, GetBookNotFound) {
    auto row = get_book(db(), 999999);
    EXPECT_FALSE(row.has_value());
}

TEST_F(BookstoreTest, GetBooksByIds) {
    auto ids = seed();
    auto rows = get_books_by_ids(db(), {ids.book_alpha, ids.book_gamma});
    ASSERT_EQ(rows.size(), 2u);
    // Ordered by title: Alpha Book, Gamma Book
    EXPECT_EQ(rows[0].title, "Alpha Book");
    EXPECT_EQ(rows[1].title, "Gamma Book");
}

TEST_F(BookstoreTest, GetBooksByIdsEmpty) {
    seed();
    auto rows = get_books_by_ids(db(), {});
    EXPECT_TRUE(rows.empty());
}

TEST_F(BookstoreTest, ListBooksByGenre) {
    seed();
    auto fiction = list_books_by_genre(db(), "fiction");
    ASSERT_EQ(fiction.size(), 2u);
    // Ordered by title: Alpha Book, Gamma Book
    EXPECT_EQ(fiction[0].title, "Alpha Book");
    EXPECT_EQ(fiction[1].title, "Gamma Book");

    auto none = list_books_by_genre(db(), "romance");
    EXPECT_TRUE(none.empty());
}

TEST_F(BookstoreTest, ListBooksByGenreOrAll) {
    seed();
    auto fiction = list_books_by_genre_or_all(db(), "fiction");
    ASSERT_EQ(fiction.size(), 2u);

    auto all = list_books_by_genre_or_all(db(), "all");
    ASSERT_EQ(all.size(), 4u);
}

// ─── Customer / Sale ─────────────────────────────────────────────────────────

TEST_F(BookstoreTest, CreateCustomer) {
    create_customer(db(), "Frank", "frank@example.com");
    auto id = last_insert_id();
    // Verify by querying back through a known query-less path: use direct mysql_query.
    exec_sql(db(), "SELECT name FROM customer WHERE id = " + std::to_string(id));
    MYSQL_RES* res = mysql_store_result(db());
    ASSERT_NE(res, nullptr);
    MYSQL_ROW row = mysql_fetch_row(res);
    ASSERT_NE(row, nullptr);
    EXPECT_STREQ(row[0], "Frank");
    mysql_free_result(res);
}

TEST_F(BookstoreTest, CreateSale) {
    create_customer(db(), "Grace", "grace@example.com");
    auto cust_id = last_insert_id();
    create_sale(db(), cust_id);
    auto sale_id = last_insert_id();

    exec_sql(db(), "SELECT customer_id FROM sale WHERE id = " + std::to_string(sale_id));
    MYSQL_RES* res = mysql_store_result(db());
    ASSERT_NE(res, nullptr);
    MYSQL_ROW row = mysql_fetch_row(res);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(std::stoll(row[0]), cust_id);
    mysql_free_result(res);
}

TEST_F(BookstoreTest, AddSaleItem) {
    auto ids = seed();
    add_sale_item(db(), ids.sale1, ids.book_delta, 5, "14.99");

    exec_sql(db(), "SELECT quantity FROM sale_item WHERE sale_id = " +
             std::to_string(ids.sale1) + " AND book_id = " + std::to_string(ids.book_delta));
    MYSQL_RES* res = mysql_store_result(db());
    ASSERT_NE(res, nullptr);
    MYSQL_ROW row = mysql_fetch_row(res);
    ASSERT_NE(row, nullptr);
    EXPECT_EQ(std::stoi(row[0]), 5);
    mysql_free_result(res);
}

// ─── JOIN / multi-table ───────────────────────────────────────────────────────

TEST_F(BookstoreTest, ListBooksWithAuthor) {
    seed();
    auto rows = list_books_with_author(db());
    ASSERT_EQ(rows.size(), 4u);
    // Ordered by title: Alpha, Beta, Delta, Gamma
    EXPECT_EQ(rows[0].title, "Alpha Book");
    EXPECT_EQ(rows[0].author_name, "Alice");
    EXPECT_EQ(rows[2].title, "Delta Book");
    EXPECT_EQ(rows[2].author_name, "Charlie");
}

TEST_F(BookstoreTest, GetBooksNeverOrdered) {
    seed();
    // Delta Book has no sale items.
    auto rows = get_books_never_ordered(db());
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].title, "Delta Book");
}

// ─── CTE / aggregate ─────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetTopSellingBooks) {
    seed();
    auto rows = get_top_selling_books(db());
    // 3 books have sales: Beta (3), Alpha (2), Gamma (1)
    ASSERT_EQ(rows.size(), 3u);
    EXPECT_EQ(rows[0].title, "Beta Book");
    EXPECT_EQ(rows[0].units_sold, std::optional<std::string>{"3"});
}

TEST_F(BookstoreTest, GetBestCustomers) {
    seed();
    auto rows = get_best_customers(db());
    ASSERT_EQ(rows.size(), 2u);
    // Eve: 3*29.99 = 89.97; Dan: 2*19.99 + 1*9.99 = 49.97
    EXPECT_EQ(rows[0].name, "Eve");
    EXPECT_EQ(rows[1].name, "Dan");
    ASSERT_TRUE(rows[0].total_spent.has_value());
    EXPECT_FALSE(rows[0].total_spent->empty());
}

TEST_F(BookstoreTest, CountBooksByGenre) {
    seed();
    auto rows = count_books_by_genre(db());
    ASSERT_EQ(rows.size(), 3u);
    // Ordered by genre: fiction, history, science
    std::map<std::string, std::int64_t> counts;
    for (const auto& r : rows) counts[r.genre] = r.book_count;
    EXPECT_EQ(counts["fiction"], 2);
    EXPECT_EQ(counts["history"], 1);
    EXPECT_EQ(counts["science"], 1);
}

// ─── Pagination ───────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, ListBooksWithLimit) {
    seed();
    // First page: 2 books ordered by title
    auto page1 = list_books_with_limit(db(), 2, 0);
    ASSERT_EQ(page1.size(), 2u);
    EXPECT_EQ(page1[0].title, "Alpha Book");
    EXPECT_EQ(page1[1].title, "Beta Book");

    // Second page
    auto page2 = list_books_with_limit(db(), 2, 2);
    ASSERT_EQ(page2.size(), 2u);
    EXPECT_EQ(page2[0].title, "Delta Book");
    EXPECT_EQ(page2[1].title, "Gamma Book");
}

// ─── LIKE search ─────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, SearchBooksByTitle) {
    seed();
    auto rows = search_books_by_title(db(), "%Alpha%");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].title, "Alpha Book");

    auto none = search_books_by_title(db(), "%ZZZ%");
    EXPECT_TRUE(none.empty());
}

// ─── BETWEEN ─────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksByPriceRange) {
    seed();
    // 10.00–20.00 includes Alpha (19.99) and Delta (14.99)
    auto rows = get_books_by_price_range(db(), "10.00", "20.00");
    ASSERT_EQ(rows.size(), 2u);
    // Ordered by price: Delta (14.99), Alpha (19.99)
    EXPECT_EQ(rows[0].title, "Delta Book");
    EXPECT_EQ(rows[1].title, "Alpha Book");
}

// ─── IN (fixed 3-param) ───────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksInGenres) {
    seed();
    // fiction: Alpha, Gamma; history: Delta => 3 books
    auto rows = get_books_in_genres(db(), "fiction", "history", "mystery");
    ASSERT_EQ(rows.size(), 3u);
    // Ordered by title: Alpha, Delta, Gamma
    EXPECT_EQ(rows[0].title, "Alpha Book");
    EXPECT_EQ(rows[1].title, "Delta Book");
    EXPECT_EQ(rows[2].title, "Gamma Book");
}

// ─── CASE WHEN ────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBookPriceLabel) {
    seed();
    auto rows = get_book_price_label(db(), "15.00");
    ASSERT_EQ(rows.size(), 4u);
    std::map<std::string, std::string> labels;
    for (const auto& r : rows) labels[r.title] = r.price_label;
    // Alpha (19.99) and Beta (29.99) > 15 => expensive
    EXPECT_EQ(labels["Alpha Book"], "expensive");
    EXPECT_EQ(labels["Beta Book"],  "expensive");
    // Delta (14.99) and Gamma (9.99) <= 15 => affordable
    EXPECT_EQ(labels["Delta Book"], "affordable");
    EXPECT_EQ(labels["Gamma Book"], "affordable");
}

// ─── COALESCE ─────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBookPriceOrDefault) {
    seed();
    auto rows = get_book_price_or_default(db(), std::optional<std::string>{"0.00"});
    ASSERT_EQ(rows.size(), 4u);
    // All books have non-null prices, so COALESCE returns the actual price.
    for (const auto& r : rows)
        EXPECT_NE(r.effective_price, "0.00") << "book: " << r.title;
}

// ─── :execrows ────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, DeleteBookById) {
    create_author(db(), "Temp", std::nullopt, std::nullopt);
    auto author_id = last_insert_id();
    create_book(db(), author_id, "Ephemeral", "temp", "1.00", std::nullopt);
    auto book_id = last_insert_id();

    std::int64_t affected = delete_book_by_id(db(), book_id);
    EXPECT_EQ(affected, 1);
    EXPECT_FALSE(get_book(db(), book_id).has_value());

    std::int64_t miss = delete_book_by_id(db(), 999999);
    EXPECT_EQ(miss, 0);
}

// ─── HAVING ───────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetGenresWithManyBooks) {
    seed();
    // Only fiction has 2 books; history and science have 1 each.
    auto rows = get_genres_with_many_books(db(), 1);
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].genre, "fiction");
    EXPECT_EQ(rows[0].book_count, 2);
}

// ─── JOIN with param ──────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksByAuthorParam) {
    seed();
    // Authors with birth_year > 1976: Alice (1980) => Alpha Book, Beta Book
    auto rows = get_books_by_author_param(db(), std::optional<std::int32_t>{1976});
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0].title, "Alpha Book");
    EXPECT_EQ(rows[1].title, "Beta Book");
}

// ─── SELECT * ─────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetAllBookFields) {
    auto ids = seed();
    auto rows = get_all_book_fields(db());
    ASSERT_EQ(rows.size(), 4u);
    // Ordered by id: Alpha, Beta, Gamma, Delta (insertion order)
    EXPECT_EQ(rows[0].id, ids.book_alpha);
    EXPECT_EQ(rows[0].title, "Alpha Book");
    EXPECT_EQ(rows[0].price, "19.99");
}

// ─── NOT IN subquery ──────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksNotByAuthor) {
    seed();
    // Books NOT by Alice => Gamma (Bob), Delta (Charlie)
    auto rows = get_books_not_by_author(db(), "Alice");
    ASSERT_EQ(rows.size(), 2u);
    // Ordered by title: Delta, Gamma
    EXPECT_EQ(rows[0].title, "Delta Book");
    EXPECT_EQ(rows[1].title, "Gamma Book");
}

// ─── EXISTS subquery ──────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksWithRecentSales) {
    seed();
    // Sales were just inserted; one year ago should capture all sold books.
    auto rows = get_books_with_recent_sales(db(), "2025-01-01 00:00:00");
    // Alpha, Beta, Gamma have sales; Delta does not.
    ASSERT_EQ(rows.size(), 3u);

    // Far-future cutoff: no results.
    auto none = get_books_with_recent_sales(db(), "2099-01-01 00:00:00");
    EXPECT_TRUE(none.empty());
}

// ─── Scalar subquery ──────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBookWithAuthorName) {
    seed();
    auto rows = get_book_with_author_name(db());
    ASSERT_EQ(rows.size(), 4u);
    // Ordered by title: Alpha, Beta, Delta, Gamma
    EXPECT_EQ(rows[0].title, "Alpha Book");
    ASSERT_TRUE(rows[0].author_name.has_value());
    EXPECT_EQ(*rows[0].author_name, "Alice");
    EXPECT_EQ(rows[2].title, "Delta Book");
    ASSERT_TRUE(rows[2].author_name.has_value());
    EXPECT_EQ(*rows[2].author_name, "Charlie");
}

// ─── Multi-CTE ────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetAuthorStats) {
    seed();
    auto rows = get_author_stats(db());
    ASSERT_EQ(rows.size(), 3u);
    // Ordered by name: Alice, Bob, Charlie
    std::map<std::string, GetAuthorStatsRow> stats;
    for (const auto& r : rows) stats[r.name] = r;

    EXPECT_EQ(stats["Alice"].num_books, std::int64_t{2});
    // Alice total_sold: Alpha (2) + Beta (3) = 5
    EXPECT_EQ(stats["Alice"].total_sold, "5");
    EXPECT_EQ(stats["Bob"].num_books, std::int64_t{1});
    EXPECT_EQ(stats["Charlie"].num_books, std::int64_t{1});
    EXPECT_EQ(stats["Charlie"].total_sold, "0");
}

// ─── Product: BOOLEAN, FLOAT, DOUBLE, JSON, BLOB, SMALLINT ───────────────────

TEST_F(BookstoreTest, GetProduct) {
    insert_product("prod-001", "SKU-001", "Widget", true, "1.5", "4.5",
                   "{\"color\":\"red\"}", nullptr, 10);

    auto row = get_product(db(), "prod-001");
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->name, "Widget");
    EXPECT_EQ(row->sku, "SKU-001");
    EXPECT_TRUE(row->active);
    ASSERT_TRUE(row->weight_kg.has_value());
    EXPECT_NEAR(*row->weight_kg, 1.5f, 0.001f);
    ASSERT_TRUE(row->rating.has_value());
    EXPECT_NEAR(*row->rating, 4.5, 0.001);
    EXPECT_EQ(row->stock_count, std::int16_t{10});

    // Non-existent product returns nullopt.
    EXPECT_FALSE(get_product(db(), "nonexistent").has_value());
}

TEST_F(BookstoreTest, ListActiveProducts) {
    insert_product("p1", "SKU-A", "Active One",  true,  nullptr, nullptr, nullptr, nullptr, 5);
    insert_product("p2", "SKU-B", "Active Two",  true,  nullptr, nullptr, nullptr, nullptr, 3);
    insert_product("p3", "SKU-C", "Inactive One", false, nullptr, nullptr, nullptr, nullptr, 0);

    auto active = list_active_products(db(), true);
    ASSERT_EQ(active.size(), 2u);

    auto inactive = list_active_products(db(), false);
    ASSERT_EQ(inactive.size(), 1u);
    EXPECT_EQ(inactive[0].name, "Inactive One");
}

// ─── NULL filter queries ──────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetAuthorsWithNullBio) {
    seed();
    auto rows = get_authors_with_null_bio(db());
    // Bob has NULL bio.
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0].name, "Bob");
}

TEST_F(BookstoreTest, GetAuthorsWithBio) {
    seed();
    auto rows = get_authors_with_bio(db());
    // Alice and Charlie have bios; ordered by name.
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0].name, "Alice");
    EXPECT_EQ(rows[1].name, "Charlie");
}

// ─── Date range ───────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksPublishedBetween) {
    seed();
    // Alpha (2020-06-15) and Beta (2021-03-10) are in range 2020-01-01 – 2021-12-31.
    auto rows = get_books_published_between(db(), "2020-01-01", "2021-12-31");
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0].title, "Alpha Book");
    EXPECT_EQ(rows[1].title, "Beta Book");
}

// ─── DISTINCT ─────────────────────────────────────────────────────────────────

TEST_F(BookstoreTest, GetDistinctGenres) {
    seed();
    auto rows = get_distinct_genres(db());
    ASSERT_EQ(rows.size(), 3u);
    // Ordered: fiction, history, science
    EXPECT_EQ(rows[0].genre, "fiction");
    EXPECT_EQ(rows[1].genre, "history");
    EXPECT_EQ(rows[2].genre, "science");
}

// ─── Aggregate: GROUP BY with SUM ────────────────────────────────────────────

TEST_F(BookstoreTest, GetBooksWithSalesCount) {
    seed();
    auto rows = get_books_with_sales_count(db());
    ASSERT_EQ(rows.size(), 4u);
    // Ordered by total_quantity DESC, title: Beta (3), Alpha (2), Gamma (1), Delta (0)
    std::map<std::string, std::string> counts;
    for (const auto& r : rows) counts[r.title] = r.total_quantity;
    EXPECT_EQ(counts["Beta Book"],  "3");
    EXPECT_EQ(counts["Alpha Book"], "2");
    EXPECT_EQ(counts["Gamma Book"], "1");
    EXPECT_EQ(counts["Delta Book"], "0");
}

// ─── Aggregate: COUNT :one ────────────────────────────────────────────────────

TEST_F(BookstoreTest, CountSaleItems) {
    auto ids = seed();
    // Sale 1 has 2 items (Alpha, Gamma).
    auto row = count_sale_items(db(), ids.sale1);
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->item_count, std::int64_t{2});

    // Sale 2 has 1 item (Beta).
    auto row2 = count_sale_items(db(), ids.sale2);
    ASSERT_TRUE(row2.has_value());
    EXPECT_EQ(row2->item_count, std::int64_t{1});
}

// ─── Aggregate: MIN/MAX/SUM/AVG :one ─────────────────────────────────────────

TEST_F(BookstoreTest, GetSaleItemQuantityAggregates) {
    seed();
    auto row = get_sale_item_quantity_aggregates(db());
    ASSERT_TRUE(row.has_value());
    // Quantities inserted: 2, 1, 3 => min=1, max=3, sum=6
    ASSERT_TRUE(row->min_qty.has_value());
    EXPECT_EQ(*row->min_qty, std::int32_t{1});
    ASSERT_TRUE(row->max_qty.has_value());
    EXPECT_EQ(*row->max_qty, std::int32_t{3});
    ASSERT_TRUE(row->sum_qty.has_value());
    EXPECT_EQ(*row->sum_qty, "6");
}

TEST_F(BookstoreTest, GetBookPriceAggregates) {
    seed();
    auto row = get_book_price_aggregates(db());
    ASSERT_TRUE(row.has_value());
    // Prices: 9.99, 14.99, 19.99, 29.99 => min=9.99, max=29.99, sum=74.96
    ASSERT_TRUE(row->min_price.has_value());
    EXPECT_EQ(*row->min_price, "9.99");
    ASSERT_TRUE(row->max_price.has_value());
    EXPECT_EQ(*row->max_price, "29.99");
    ASSERT_TRUE(row->sum_price.has_value());
    EXPECT_EQ(*row->sum_price, "74.96");
    EXPECT_TRUE(row->avg_price.has_value());
}
