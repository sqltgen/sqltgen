mod db;

use db::pg::queries as pg;
use db::sqlite::queries as sq;

const SQLITE_M1: &str = include_str!("../../common/sqlite/migrations/001_authors.sql");
const SQLITE_M2: &str = include_str!("../../common/sqlite/migrations/002_books.sql");
const SQLITE_M3: &str = include_str!("../../common/sqlite/migrations/003_customers.sql");
const SQLITE_M4: &str = include_str!("../../common/sqlite/migrations/004_orders.sql");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    // ── PostgreSQL ────────────────────────────────────────────────────────────
    let pg_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pg_pool = sqlx::PgPool::connect(&pg_url).await?;

    let le_guin  = pg::create_author(&pg_pool, "Ursula K. Le Guin".into(), Some("Science fiction and fantasy author".into()), Some(1929)).await?.unwrap();
    let herbert  = pg::create_author(&pg_pool, "Frank Herbert".into(), Some("Author of the Dune series".into()), Some(1920)).await?.unwrap();
    let asimov   = pg::create_author(&pg_pool, "Isaac Asimov".into(), None, Some(1920)).await?.unwrap();
    println!("[pg] inserted 3 authors (ids: {}, {}, {})", le_guin.id, herbert.id, asimov.id);

    let lhod  = pg::create_book(&pg_pool, le_guin.id,  "The Left Hand of Darkness".into(), "sci-fi".into(), 12.99, None).await?.unwrap();
    let _disp = pg::create_book(&pg_pool, le_guin.id,  "The Dispossessed".into(),           "sci-fi".into(), 11.50, None).await?.unwrap();
    let dune  = pg::create_book(&pg_pool, herbert.id,  "Dune".into(),                       "sci-fi".into(), 14.99, None).await?.unwrap();
    let found = pg::create_book(&pg_pool, asimov.id,   "Foundation".into(),                 "sci-fi".into(), 10.99, None).await?.unwrap();
    let _caves = pg::create_book(&pg_pool, asimov.id,  "The Caves of Steel".into(),         "sci-fi".into(),  9.99, None).await?.unwrap();
    println!("[pg] inserted 5 books");

    let alice = pg::create_customer(&pg_pool, "Alice".into(), "alice@example.com".into()).await?.unwrap();
    let bob   = pg::create_customer(&pg_pool, "Bob".into(),   "bob@example.com".into()).await?.unwrap();
    println!("[pg] inserted 2 customers");

    let sale1 = pg::create_sale(&pg_pool, alice.id).await?.unwrap();
    pg::add_sale_item(&pg_pool, sale1.id, dune.id,  2, 14.99).await?;  // Alice buys 2x Dune
    pg::add_sale_item(&pg_pool, sale1.id, found.id, 1, 10.99).await?;  // Alice buys 1x Foundation
    let sale2 = pg::create_sale(&pg_pool, bob.id).await?.unwrap();
    pg::add_sale_item(&pg_pool, sale2.id, dune.id, 1, 14.99).await?;   // Bob buys 1x Dune
    pg::add_sale_item(&pg_pool, sale2.id, lhod.id, 1, 12.99).await?;   // Bob buys 1x Left Hand
    println!("[pg] inserted 2 sales with items");

    let authors = pg::list_authors(&pg_pool).await?;
    println!("[pg] list_authors: {} row(s)", authors.len());

    let books = pg::list_books_by_genre(&pg_pool, "sci-fi".into()).await?;
    println!("[pg] list_books_by_genre(sci-fi): {} row(s)", books.len());

    let with_author = pg::list_books_with_author(&pg_pool).await?;
    println!("[pg] list_books_with_author: {} row(s)", with_author.len());
    for r in &with_author {
        println!("  \"{}\" by {}", r.title, r.author_name);
    }

    let never_ordered = pg::get_books_never_ordered(&pg_pool).await?;
    println!("[pg] get_books_never_ordered: {} book(s)", never_ordered.len());
    for b in &never_ordered {
        println!("  \"{}\"", b.title);
    }

    let top = pg::get_top_selling_books(&pg_pool).await?;
    println!("[pg] get_top_selling_books: {} row(s)", top.len());
    for r in &top {
        println!("  \"{}\" — sold {:?}", r.title, r.units_sold);
    }

    let best = pg::get_best_customers(&pg_pool).await?;
    println!("[pg] get_best_customers: {} row(s)", best.len());
    for r in &best {
        println!("  {} — spent {:?}", r.name, r.total_spent);
    }

    // Demonstrate UPDATE RETURNING and DELETE RETURNING with a transient author
    let temp = pg::create_author(&pg_pool, "Temp Author".into(), None, None).await?.unwrap();
    if let Some(updated) = pg::update_author_bio(&pg_pool, Some("Updated via UPDATE RETURNING".into()), temp.id).await? {
        println!("[pg] update_author_bio: updated \"{}\" — bio: {:?}", updated.name, updated.bio);
    }
    if let Some(deleted) = pg::delete_author(&pg_pool, temp.id).await? {
        println!("[pg] delete_author: deleted \"{}\" (id={})", deleted.name, deleted.id);
    }

    // ── SQLite (in-memory) ────────────────────────────────────────────────────
    let sq_pool = sqlx::SqlitePool::connect("sqlite::memory:").await?;

    for migration in [SQLITE_M1, SQLITE_M2, SQLITE_M3, SQLITE_M4] {
        for stmt in migration.split(';') {
            let s = stmt.trim();
            if !s.is_empty() {
                sqlx::query(s).execute(&sq_pool).await?;
            }
        }
    }

    sq::create_author(&sq_pool, "Ursula K. Le Guin".into(), Some("Science fiction and fantasy author".into()), Some(1929)).await?;
    sq::create_author(&sq_pool, "Frank Herbert".into(), Some("Author of the Dune series".into()), Some(1920)).await?;
    sq::create_author(&sq_pool, "Isaac Asimov".into(), None, Some(1920)).await?;
    println!("[sqlite] inserted 3 authors");

    sq::create_book(&sq_pool, 1, "The Left Hand of Darkness".into(), "sci-fi".into(), 12.99, None).await?;
    sq::create_book(&sq_pool, 1, "The Dispossessed".into(), "sci-fi".into(), 11.50, None).await?;
    sq::create_book(&sq_pool, 2, "Dune".into(), "sci-fi".into(), 14.99, None).await?;
    sq::create_book(&sq_pool, 3, "Foundation".into(), "sci-fi".into(), 10.99, None).await?;
    sq::create_book(&sq_pool, 3, "The Caves of Steel".into(), "sci-fi".into(), 9.99, None).await?;
    println!("[sqlite] inserted 5 books");

    sq::create_customer(&sq_pool, "Carol".into(), "carol@example.com".into()).await?;
    sq::create_customer(&sq_pool, "Dave".into(), "dave@example.com".into()).await?;
    println!("[sqlite] inserted 2 customers");

    sq::create_sale(&sq_pool, 1).await?;
    sq::add_sale_item(&sq_pool, 1, 3, 2, 14.99).await?;
    sq::add_sale_item(&sq_pool, 1, 4, 1, 10.99).await?;
    sq::create_sale(&sq_pool, 2).await?;
    sq::add_sale_item(&sq_pool, 2, 3, 1, 14.99).await?;
    sq::add_sale_item(&sq_pool, 2, 1, 1, 12.99).await?;
    println!("[sqlite] inserted 2 sales with items");

    let authors = sq::list_authors(&sq_pool).await?;
    println!("[sqlite] list_authors: {} row(s)", authors.len());

    let books = sq::list_books_by_genre(&sq_pool, "sci-fi".into()).await?;
    println!("[sqlite] list_books_by_genre(sci-fi): {} row(s)", books.len());

    let with_author = sq::list_books_with_author(&sq_pool).await?;
    println!("[sqlite] list_books_with_author: {} row(s)", with_author.len());
    for r in &with_author {
        println!("  \"{}\" by {}", r.title, r.author_name);
    }

    let never_ordered = sq::get_books_never_ordered(&sq_pool).await?;
    println!("[sqlite] get_books_never_ordered: {} book(s)", never_ordered.len());
    for b in &never_ordered {
        println!("  \"{}\"", b.title);
    }

    let top = sq::get_top_selling_books(&sq_pool).await?;
    println!("[sqlite] get_top_selling_books: {} row(s)", top.len());
    for r in &top {
        println!("  \"{}\" — sold {:?}", r.title, r.units_sold);
    }

    let best = sq::get_best_customers(&sq_pool).await?;
    println!("[sqlite] get_best_customers: {} row(s)", best.len());
    for r in &best {
        println!("  {} — spent {:?}", r.name, r.total_spent);
    }

    Ok(())
}
