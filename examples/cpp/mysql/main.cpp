#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include <algorithm>

#include <mysql/mysql.h>

#include "gen/queries/queries.hpp"

namespace fs = std::filesystem;

/// Read a file into a string.
static std::string read_file(const fs::path& path) {
    std::ifstream f(path);
    return {std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
}

/// Generate a short random hex suffix for the temp database name.
static std::string random_hex(int len = 8) {
    static const char hex[] = "0123456789abcdef";
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(0, 15);
    std::string s;
    for (int i = 0; i < len; ++i) s += hex[dist(gen)];
    return s;
}

static void run_demo(MYSQL* db) {
    // Fresh database — auto-increment IDs are deterministic:
    //   authors 1–3, books 1–5, customers 1–2, sales 1–2

    // ── Insert authors (ids: 1, 2, 3) ───────────────────────────────────
    create_author(db, "Ursula K. Le Guin",
                  std::optional<std::string>{"Science fiction and fantasy author"},
                  std::optional<std::int32_t>{1929});
    create_author(db, "Frank Herbert",
                  std::optional<std::string>{"Author of the Dune series"},
                  std::optional<std::int32_t>{1920});
    create_author(db, "Isaac Asimov",
                  std::nullopt,
                  std::optional<std::int32_t>{1920});
    std::cout << "[cpp/mysql] inserted 3 authors (ids: 1, 2, 3)\n";

    // ── Insert books (ids: 1–5) ─────────────────────────────────────────
    create_book(db, 1, "The Left Hand of Darkness", "sci-fi", "12.99", std::optional<std::string>{"1969-03-01"});
    create_book(db, 1, "The Dispossessed",          "sci-fi", "11.50", std::optional<std::string>{"1974-05-01"});
    create_book(db, 2, "Dune",                      "sci-fi", "14.99", std::optional<std::string>{"1965-08-01"});
    create_book(db, 3, "Foundation",                "sci-fi", "10.99", std::optional<std::string>{"1951-06-01"});
    create_book(db, 3, "The Caves of Steel",        "sci-fi", "9.99",  std::optional<std::string>{"1954-02-01"});
    std::cout << "[cpp/mysql] inserted 5 books\n";

    // ── Insert customers (ids: 1, 2) ────────────────────────────────────
    create_customer(db, "Alice", "alice@example.com");
    create_customer(db, "Bob",   "bob@example.com");
    std::cout << "[cpp/mysql] inserted 2 customers\n";

    // ── Insert sales + items (sale ids: 1, 2; book ids: dune=3, found=4, lhod=1) ──
    create_sale(db, 1);
    add_sale_item(db, 1, 3, 2, "14.99");
    add_sale_item(db, 1, 4, 1, "10.99");

    create_sale(db, 2);
    add_sale_item(db, 2, 3, 1, "14.99");
    add_sale_item(db, 2, 1, 1, "12.99");
    std::cout << "[cpp/mysql] inserted 2 sales with items\n";

    // ── Queries ─────────────────────────────────────────────────────────
    auto authors = list_authors(db);
    std::cout << "[cpp/mysql] list_authors: " << authors.size() << " row(s)\n";

    auto by_ids = get_books_by_ids(db, {1, 3});
    std::cout << "[cpp/mysql] get_books_by_ids([1,3]): " << by_ids.size() << " row(s)\n";
    for (const auto& b : by_ids)
        std::cout << "  \"" << b.title << "\"\n";

    auto books = list_books_by_genre(db, "sci-fi");
    std::cout << "[cpp/mysql] list_books_by_genre(sci-fi): " << books.size() << " row(s)\n";

    auto all_books = list_books_by_genre_or_all(db, "all");
    std::cout << "[cpp/mysql] list_books_by_genre_or_all(all): " << all_books.size()
              << " row(s) (repeated-param demo)\n";

    auto scifi2 = list_books_by_genre_or_all(db, "sci-fi");
    std::cout << "[cpp/mysql] list_books_by_genre_or_all(sci-fi): " << scifi2.size() << " row(s)\n";

    auto with_author = list_books_with_author(db);
    std::cout << "[cpp/mysql] list_books_with_author:\n";
    for (const auto& r : with_author)
        std::cout << "  \"" << r.title << "\" by " << r.author_name << "\n";

    auto never_ordered = get_books_never_ordered(db);
    std::cout << "[cpp/mysql] get_books_never_ordered: " << never_ordered.size() << " book(s)\n";
    for (const auto& b : never_ordered)
        std::cout << "  \"" << b.title << "\"\n";

    auto top = get_top_selling_books(db);
    std::cout << "[cpp/mysql] get_top_selling_books:\n";
    for (const auto& r : top)
        std::cout << "  \"" << r.title << "\" sold " << (r.units_sold ? *r.units_sold : "?") << "\n";

    auto best = get_best_customers(db);
    std::cout << "[cpp/mysql] get_best_customers:\n";
    for (const auto& r : best)
        std::cout << "  " << r.name << " spent " << (r.total_spent ? *r.total_spent : "?") << "\n";

    // ── UPDATE + DELETE (no RETURNING in MySQL) ─────────────────────────
    // Temp author gets id 4
    create_author(db, "Temp Author", std::nullopt, std::nullopt);

    update_author_bio(db, std::optional<std::string>{"Updated bio"}, 4);
    if (auto updated = get_author(db, 4))
        std::cout << "[cpp/mysql] update_author_bio: updated \"" << updated->name
                  << "\" — bio: " << (updated->bio ? *updated->bio : "(null)") << "\n";

    delete_author(db, 4);
    std::cout << "[cpp/mysql] delete_author: deleted temp author (id=4)\n";
}

int main() {
    try {
        const char* host     = std::getenv("MYSQL_HOST");
        const char* port_str = std::getenv("MYSQL_PORT");
        const char* user     = std::getenv("MYSQL_USER");
        const char* password = std::getenv("MYSQL_PASSWORD");
        const char* database = std::getenv("MYSQL_DATABASE");
        const char* migrations_env = std::getenv("MIGRATIONS_DIR");

        if (!host || !user || !password || !database || !migrations_env) {
            std::cerr << "MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, and MIGRATIONS_DIR must be set.\n";
            return 1;
        }

        unsigned int port = port_str ? static_cast<unsigned int>(std::stoi(port_str)) : 3306;
        std::string db_name = "sqltgen_cpp_" + random_hex();
        fs::path migrations_dir{migrations_env};

        // Create temp database.
        MYSQL* admin = mysql_init(nullptr);
        if (!admin) { std::cerr << "mysql_init failed\n"; return 1; }
        if (!mysql_real_connect(admin, host, user, password, database, port, nullptr, 0)) {
            std::cerr << "admin connect: " << mysql_error(admin) << "\n";
            mysql_close(admin);
            return 1;
        }
        if (mysql_query(admin, ("CREATE DATABASE `" + db_name + "`").c_str()) != 0)
            throw std::runtime_error(mysql_error(admin));
        mysql_close(admin);

        // Apply migrations using a connection with CLIENT_MULTI_STATEMENTS
        // (some migration files contain multiple statements).
        {
            MYSQL* mig = mysql_init(nullptr);
            if (!mig) { std::cerr << "mysql_init failed\n"; return 1; }
            if (!mysql_real_connect(mig, host, user, password, db_name.c_str(), port, nullptr, CLIENT_MULTI_STATEMENTS)) {
                std::cerr << "migration connect: " << mysql_error(mig) << "\n";
                mysql_close(mig);
                return 1;
            }

            std::vector<fs::path> migration_files;
            for (const auto& entry : fs::directory_iterator(migrations_dir))
                if (entry.path().extension() == ".sql")
                    migration_files.push_back(entry.path());
            std::sort(migration_files.begin(), migration_files.end());

            for (const auto& mf : migration_files) {
                std::string sql = read_file(mf);
                if (mysql_query(mig, sql.c_str()) != 0)
                    throw std::runtime_error(std::string("migration ") + mf.filename().string() + ": " + mysql_error(mig));
                // Drain all result sets from multi-statement execution.
                while (mysql_next_result(mig) == 0) {
                    MYSQL_RES* res = mysql_store_result(mig);
                    if (res) mysql_free_result(res);
                }
            }
            mysql_close(mig);
        }

        // Separate connection for the demo — without CLIENT_MULTI_STATEMENTS
        // so prepared statements work cleanly.
        MYSQL* db = mysql_init(nullptr);
        if (!db) { std::cerr << "mysql_init failed\n"; return 1; }
        if (!mysql_real_connect(db, host, user, password, db_name.c_str(), port, nullptr, 0)) {
            std::cerr << "demo connect: " << mysql_error(db) << "\n";
            mysql_close(db);
            return 1;
        }

        run_demo(db);

        mysql_close(db);

        // Drop temp database.
        MYSQL* cleanup = mysql_init(nullptr);
        if (cleanup && mysql_real_connect(cleanup, host, user, password, database, port, nullptr, 0)) {
            mysql_query(cleanup, ("DROP DATABASE IF EXISTS `" + db_name + "`").c_str());
            mysql_close(cleanup);
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
