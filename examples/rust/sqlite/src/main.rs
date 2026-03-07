mod db;

use db::queries as q;

const M1: &str = include_str!("../../../common/sqlite/migrations/001_authors.sql");
const M2: &str = include_str!("../../../common/sqlite/migrations/002_books.sql");
const M3: &str = include_str!("../../../common/sqlite/migrations/003_customers.sql");
const M4: &str = include_str!("../../../common/sqlite/migrations/004_orders.sql");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await?;

    for migration in [M1, M2, M3, M4] {
        for stmt in migration.split(';') {
            let s = stmt.trim();
            if !s.is_empty() {
                sqlx::query(s).execute(&pool).await?;
            }
        }
    }

    q::create_author(&pool, "Ursula K. Le Guin".into(), Some("Science fiction and fantasy author".into()), Some(1929)).await?;
    q::create_author(&pool, "Frank Herbert".into(), Some("Author of the Dune series".into()), Some(1920)).await?;
    q::create_author(&pool, "Isaac Asimov".into(), None, Some(1920)).await?;
    println!("[sqlite] inserted 3 authors");

    q::create_book(&pool, 1, "The Left Hand of Darkness".into(), "sci-fi".into(), 12.99, None).await?;
    q::create_book(&pool, 1, "The Dispossessed".into(), "sci-fi".into(), 11.50, None).await?;
    q::create_book(&pool, 2, "Dune".into(), "sci-fi".into(), 14.99, None).await?;
    q::create_book(&pool, 3, "Foundation".into(), "sci-fi".into(), 10.99, None).await?;
    q::create_book(&pool, 3, "The Caves of Steel".into(), "sci-fi".into(), 9.99, None).await?;
    println!("[sqlite] inserted 5 books");

    q::create_customer(&pool, "Carol".into(), "carol@example.com".into()).await?;
    q::create_customer(&pool, "Dave".into(), "dave@example.com".into()).await?;
    println!("[sqlite] inserted 2 customers");

    q::create_sale(&pool, 1).await?;
    q::add_sale_item(&pool, 1, 3, 2, 14.99).await?;
    q::add_sale_item(&pool, 1, 4, 1, 10.99).await?;
    q::create_sale(&pool, 2).await?;
    q::add_sale_item(&pool, 2, 3, 1, 14.99).await?;
    q::add_sale_item(&pool, 2, 1, 1, 12.99).await?;
    println!("[sqlite] inserted 2 sales with items");

    let authors = q::list_authors(&pool).await?;
    println!("[sqlite] list_authors: {} row(s)", authors.len());

    // Books inserted above have IDs 1–5; 1=Left Hand, 3=Dune.
    let by_ids = q::get_books_by_ids(&pool, &[1, 3]).await?;
    println!("[sqlite] get_books_by_ids([1,3]): {} row(s)", by_ids.len());
    for b in &by_ids {
        println!("  \"{}\"", b.title);
    }

    let books = q::list_books_by_genre(&pool, "sci-fi".into()).await?;
    println!("[sqlite] list_books_by_genre(sci-fi): {} row(s)", books.len());

    let all_books = q::list_books_by_genre_or_all(&pool, "all".into()).await?;
    println!("[sqlite] list_books_by_genre_or_all(all): {} row(s) (repeated-param demo)", all_books.len());
    let scifi2 = q::list_books_by_genre_or_all(&pool, "sci-fi".into()).await?;
    println!("[sqlite] list_books_by_genre_or_all(sci-fi): {} row(s)", scifi2.len());

    let with_author = q::list_books_with_author(&pool).await?;
    println!("[sqlite] list_books_with_author:");
    for r in &with_author {
        println!("  \"{}\" by {}", r.title, r.author_name);
    }

    let never_ordered = q::get_books_never_ordered(&pool).await?;
    println!("[sqlite] get_books_never_ordered: {} book(s)", never_ordered.len());
    for b in &never_ordered {
        println!("  \"{}\"", b.title);
    }

    let top = q::get_top_selling_books(&pool).await?;
    println!("[sqlite] get_top_selling_books:");
    for r in &top {
        println!("  \"{}\" sold {:?}", r.title, r.units_sold);
    }

    let best = q::get_best_customers(&pool).await?;
    println!("[sqlite] get_best_customers:");
    for r in &best {
        println!("  {} spent {:?}", r.name, r.total_spent);
    }

    Ok(())
}
