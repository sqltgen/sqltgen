#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <algorithm>

#include <sqlite3.h>

#include "gen/queries/queries.hpp"

namespace fs = std::filesystem;

/// Read a file into a string.
static std::string read_file(const fs::path& path) {
    std::ifstream f(path);
    return {std::istreambuf_iterator<char>(f), std::istreambuf_iterator<char>()};
}

/// Execute a multi-statement SQL string.
static void exec_sql(sqlite3* db, const std::string& sql) {
    char* err = nullptr;
    if (sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
        std::string msg = err ? err : "unknown error";
        sqlite3_free(err);
        throw std::runtime_error(msg);
    }
}

/// Helper: get the last inserted rowid as int32_t.
static std::int32_t last_id(sqlite3* db) {
    return static_cast<std::int32_t>(sqlite3_last_insert_rowid(db));
}

static void run_demo(sqlite3* db) {
    // ── Insert authors ──────────────────────────────────────────────────
    create_author(db, "Ursula K. Le Guin",
                  std::optional<std::string>{"Science fiction and fantasy author"},
                  std::optional<std::int32_t>{1929});
    auto le_guin_id = last_id(db);

    create_author(db, "Frank Herbert",
                  std::optional<std::string>{"Author of the Dune series"},
                  std::optional<std::int32_t>{1920});
    auto herbert_id = last_id(db);

    create_author(db, "Isaac Asimov",
                  std::nullopt,
                  std::optional<std::int32_t>{1920});
    auto asimov_id = last_id(db);

    std::cout << "[cpp/sqlite] inserted 3 authors (ids: "
              << le_guin_id << ", " << herbert_id << ", " << asimov_id << ")\n";

    // ── Insert books ────────────────────────────────────────────────────
    create_book(db, le_guin_id, "The Left Hand of Darkness", "sci-fi", 12.99, std::optional<std::string>{"1969-03-01"});
    auto lhod_id = last_id(db);

    create_book(db, le_guin_id, "The Dispossessed",          "sci-fi", 11.50, std::optional<std::string>{"1974-05-01"});

    create_book(db, herbert_id, "Dune",                      "sci-fi", 14.99, std::optional<std::string>{"1965-08-01"});
    auto dune_id = last_id(db);

    create_book(db, asimov_id,  "Foundation",                "sci-fi", 10.99, std::optional<std::string>{"1951-06-01"});
    auto found_id = last_id(db);

    create_book(db, asimov_id,  "The Caves of Steel",        "sci-fi", 9.99,  std::optional<std::string>{"1954-02-01"});

    std::cout << "[cpp/sqlite] inserted 5 books\n";

    // ── Insert customers ────────────────────────────────────────────────
    create_customer(db, "Alice", "alice@example.com");
    auto alice_id = last_id(db);

    create_customer(db, "Bob",   "bob@example.com");
    auto bob_id = last_id(db);

    std::cout << "[cpp/sqlite] inserted 2 customers\n";

    // ── Insert sales + items ────────────────────────────────────────────
    create_sale(db, alice_id);
    auto sale1_id = last_id(db);
    add_sale_item(db, sale1_id, dune_id,  2, 14.99);
    add_sale_item(db, sale1_id, found_id, 1, 10.99);

    create_sale(db, bob_id);
    auto sale2_id = last_id(db);
    add_sale_item(db, sale2_id, dune_id, 1, 14.99);
    add_sale_item(db, sale2_id, lhod_id, 1, 12.99);

    std::cout << "[cpp/sqlite] inserted 2 sales with items\n";

    // ── Queries ─────────────────────────────────────────────────────────
    auto authors = list_authors(db);
    std::cout << "[cpp/sqlite] list_authors: " << authors.size() << " row(s)\n";

    auto by_ids = get_books_by_ids(db, {1, 3});
    std::cout << "[cpp/sqlite] get_books_by_ids([1,3]): " << by_ids.size() << " row(s)\n";
    for (const auto& b : by_ids)
        std::cout << "  \"" << b.title << "\"\n";

    auto books = list_books_by_genre(db, "sci-fi");
    std::cout << "[cpp/sqlite] list_books_by_genre(sci-fi): " << books.size() << " row(s)\n";

    auto all_books = list_books_by_genre_or_all(db, "all");
    std::cout << "[cpp/sqlite] list_books_by_genre_or_all(all): " << all_books.size()
              << " row(s) (repeated-param demo)\n";

    auto scifi2 = list_books_by_genre_or_all(db, "sci-fi");
    std::cout << "[cpp/sqlite] list_books_by_genre_or_all(sci-fi): " << scifi2.size() << " row(s)\n";

    auto with_author = list_books_with_author(db);
    std::cout << "[cpp/sqlite] list_books_with_author:\n";
    for (const auto& r : with_author)
        std::cout << "  \"" << r.title << "\" by " << r.author_name << "\n";

    auto never_ordered = get_books_never_ordered(db);
    std::cout << "[cpp/sqlite] get_books_never_ordered: " << never_ordered.size() << " book(s)\n";
    for (const auto& b : never_ordered)
        std::cout << "  \"" << b.title << "\"\n";

    auto top = get_top_selling_books(db);
    std::cout << "[cpp/sqlite] get_top_selling_books:\n";
    for (const auto& r : top)
        std::cout << "  \"" << r.title << "\" sold " << (r.units_sold ? std::to_string(*r.units_sold) : "?") << "\n";

    auto best = get_best_customers(db);
    std::cout << "[cpp/sqlite] get_best_customers:\n";
    for (const auto& r : best)
        std::cout << "  " << r.name << " spent " << (r.total_spent ? std::to_string(*r.total_spent) : "?") << "\n";
}

int main() {
    try {
        const char* migrations_env = std::getenv("MIGRATIONS_DIR");
        if (!migrations_env) {
            std::cerr << "MIGRATIONS_DIR must be set.\n";
            return 1;
        }
        fs::path migrations_dir{migrations_env};

        sqlite3* db = nullptr;
        if (sqlite3_open(":memory:", &db) != SQLITE_OK) {
            std::cerr << "Failed to open in-memory database.\n";
            return 1;
        }

        exec_sql(db, "PRAGMA foreign_keys = ON");

        // Apply migrations.
        std::vector<fs::path> migration_files;
        for (const auto& entry : fs::directory_iterator(migrations_dir))
            if (entry.path().extension() == ".sql")
                migration_files.push_back(entry.path());
        std::sort(migration_files.begin(), migration_files.end());

        for (const auto& mf : migration_files)
            exec_sql(db, read_file(mf));

        run_demo(db);

        sqlite3_close(db);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
