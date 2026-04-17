#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include <algorithm>

#include <pqxx/pqxx>

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

/// Split a multi-statement SQL string on ';' and execute each statement.
static void exec_sql(pqxx::connection& conn, const std::string& sql) {
    pqxx::work txn(conn);
    txn.exec(sql);
    txn.commit();
}

static void run_demo(pqxx::connection& conn) {
    // ── Insert authors ──────────────────────────────────────────────────
    auto le_guin = create_author(conn, "Ursula K. Le Guin",
                                 std::optional<std::string>{"Science fiction and fantasy author"},
                                 std::optional<std::int32_t>{1929});
    auto herbert = create_author(conn, "Frank Herbert",
                                 std::optional<std::string>{"Author of the Dune series"},
                                 std::optional<std::int32_t>{1920});
    auto asimov  = create_author(conn, "Isaac Asimov",
                                 std::nullopt,
                                 std::optional<std::int32_t>{1920});

    std::cout << "[cpp/pg] inserted 3 authors (ids: "
              << le_guin->id << ", " << herbert->id << ", " << asimov->id << ")\n";

    // ── Insert books ────────────────────────────────────────────────────
    auto lhod  = create_book(conn, le_guin->id, "The Left Hand of Darkness", "fiction", "12.99", std::optional<std::string>{"1969-03-01"});
    auto disp  = create_book(conn, le_guin->id, "The Dispossessed",          "fiction", "11.50", std::optional<std::string>{"1974-05-01"});
    auto dune  = create_book(conn, herbert->id, "Dune",                      "science", "14.99", std::optional<std::string>{"1965-08-01"});
    auto found = create_book(conn, asimov->id,  "Foundation",                "science", "10.99", std::optional<std::string>{"1951-06-01"});
    auto caves = create_book(conn, asimov->id,  "The Caves of Steel",        "fiction", "9.99",  std::optional<std::string>{"1954-02-01"});

    std::cout << "[cpp/pg] inserted 5 books\n";

    // ── Insert customers ────────────────────────────────────────────────
    auto alice = create_customer(conn, "Alice", "alice@example.com");
    auto bob   = create_customer(conn, "Bob",   "bob@example.com");

    std::cout << "[cpp/pg] inserted 2 customers\n";

    // ── Insert sales + items ────────────────────────────────────────────
    auto sale1 = create_sale(conn, alice->id);
    add_sale_item(conn, sale1->id, dune->id,  2, "14.99");
    add_sale_item(conn, sale1->id, found->id, 1, "10.99");

    auto sale2 = create_sale(conn, bob->id);
    add_sale_item(conn, sale2->id, dune->id, 1, "14.99");
    add_sale_item(conn, sale2->id, lhod->id, 1, "12.99");

    std::cout << "[cpp/pg] inserted 2 sales with items\n";

    // ── Queries ─────────────────────────────────────────────────────────
    auto authors = list_authors(conn);
    std::cout << "[cpp/pg] list_authors: " << authors.size() << " row(s)\n";

    auto by_ids = get_books_by_ids(conn, {1, 3});
    std::cout << "[cpp/pg] get_books_by_ids([1,3]): " << by_ids.size() << " row(s)\n";
    for (const auto& b : by_ids)
        std::cout << "  \"" << b.title << "\"\n";

    auto books = list_books_by_genre(conn, "science");
    std::cout << "[cpp/pg] list_books_by_genre(science): " << books.size() << " row(s)\n";

    auto all_books = list_books_by_genre_or_all(conn, std::nullopt);
    std::cout << "[cpp/pg] list_books_by_genre_or_all(null): " << all_books.size()
              << " row(s) (nullable-param demo)\n";

    auto scifi2 = list_books_by_genre_or_all(conn, std::optional<std::string>{"science"});
    std::cout << "[cpp/pg] list_books_by_genre_or_all(science): " << scifi2.size() << " row(s)\n";

    auto with_author = list_books_with_author(conn);
    std::cout << "[cpp/pg] list_books_with_author:\n";
    for (const auto& r : with_author)
        std::cout << "  \"" << r.title << "\" by " << r.author_name << "\n";

    auto never_ordered = get_books_never_ordered(conn);
    std::cout << "[cpp/pg] get_books_never_ordered: " << never_ordered.size() << " book(s)\n";
    for (const auto& b : never_ordered)
        std::cout << "  \"" << b.title << "\"\n";

    auto top = get_top_selling_books(conn);
    std::cout << "[cpp/pg] get_top_selling_books:\n";
    for (const auto& r : top)
        std::cout << "  \"" << r.title << "\" sold " << (r.units_sold ? std::to_string(*r.units_sold) : "?") << "\n";

    auto best = get_best_customers(conn);
    std::cout << "[cpp/pg] get_best_customers:\n";
    for (const auto& r : best)
        std::cout << "  " << r.name << " spent " << (r.total_spent ? *r.total_spent : "?") << "\n";

    // ── UPDATE RETURNING / DELETE RETURNING ──────────────────────────────
    auto temp = create_author(conn, "Temp Author", std::nullopt, std::nullopt);
    if (auto updated = update_author_bio(conn, std::optional<std::string>{"Updated via UPDATE RETURNING"}, temp->id))
        std::cout << "[cpp/pg] update_author_bio: updated \"" << updated->name
                  << "\" — bio: " << (updated->bio ? *updated->bio : "(null)") << "\n";

    if (auto deleted = delete_author(conn, temp->id))
        std::cout << "[cpp/pg] delete_author: deleted \"" << deleted->name
                  << "\" (id=" << deleted->id << ")\n";
}

int main() {
    try {
        const char* url_env = std::getenv("DATABASE_URL");
        const char* migrations_env = std::getenv("MIGRATIONS_DIR");

        if (!url_env || !migrations_env) {
            std::cerr << "DATABASE_URL and MIGRATIONS_DIR must be set.\n";
            return 1;
        }

        std::string admin_url{url_env};
        std::string db_name = "sqltgen_cpp_" + random_hex();
        std::string db_url = admin_url.substr(0, admin_url.rfind('/') + 1) + db_name;
        fs::path migrations_dir{migrations_env};

        // Create temp database.
        {
            pqxx::connection admin(admin_url);
            pqxx::nontransaction ntxn(admin);
            ntxn.exec("CREATE DATABASE " + ntxn.quote_name(db_name));
        }

        // Apply migrations and run demo.
        {
            pqxx::connection conn(db_url);

            std::vector<fs::path> migration_files;
            for (const auto& entry : fs::directory_iterator(migrations_dir))
                if (entry.path().extension() == ".sql")
                    migration_files.push_back(entry.path());
            std::sort(migration_files.begin(), migration_files.end());

            for (const auto& mf : migration_files)
                exec_sql(conn, read_file(mf));

            run_demo(conn);
        }

        // Drop temp database.
        {
            pqxx::connection admin(admin_url);
            pqxx::nontransaction ntxn(admin);
            ntxn.exec("DROP DATABASE IF EXISTS " + ntxn.quote_name(db_name));
        }
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
