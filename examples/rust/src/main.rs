mod db;

use db::pg::queries as pg;
use db::sqlite::queries as sq;

const SQLITE_SCHEMA: &str = include_str!("../../sqlite/schema.sql");

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    // ── PostgreSQL ────────────────────────────────────────────────────────────
    let pg_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pg_pool = sqlx::PgPool::connect(&pg_url).await?;

    pg::create_user(&pg_pool, "Alice".into(), "alice@example.com".into(), Some("loves Rust".into())).await?;
    pg::create_user(&pg_pool, "Bob".into(), "bob@example.com".into(), None).await?;
    pg::create_post(&pg_pool, 1, "Hello World".into(), Some("My first post".into())).await?;
    pg::create_post(&pg_pool, 1, "Second Post".into(), None).await?;

    if let Some(u) = pg::get_user(&pg_pool, 1).await? {
        println!("[pg] get_user(1): {} <{}>  bio={:?}", u.name, u.email, u.bio);
    }

    let users = pg::list_users(&pg_pool).await?;
    println!("[pg] list_users: {} row(s)", users.len());

    let posts = pg::list_posts_by_user(&pg_pool, 1).await?;
    println!("[pg] list_posts_by_user(1): {} row(s)", posts.len());

    let joined = pg::list_posts_with_author(&pg_pool).await?;
    println!("[pg] list_posts_with_author: {} row(s)", joined.len());

    let authors = pg::get_active_authors(&pg_pool).await?;
    println!("[pg] get_active_authors: {} row(s)", authors.len());

    // ── SQLite (in-memory) ────────────────────────────────────────────────────
    let sq_pool = sqlx::SqlitePool::connect("sqlite::memory:").await?;

    for stmt in SQLITE_SCHEMA.split(';') {
        let s = stmt.trim();
        if !s.is_empty() {
            sqlx::query(s).execute(&sq_pool).await?;
        }
    }

    sq::create_user(&sq_pool, "Carol".into(), "carol@example.com".into(), Some("loves SQLite".into())).await?;
    sq::create_user(&sq_pool, "Dave".into(), "dave@example.com".into(), None).await?;
    sq::create_post(&sq_pool, 1, "SQLite Post".into(), Some("Written in SQLite".into())).await?;

    if let Some(u) = sq::get_user(&sq_pool, 1).await? {
        println!("[sqlite] get_user(1): {} <{}>  bio={:?}", u.name, u.email, u.bio);
    }

    let users = sq::list_users(&sq_pool).await?;
    println!("[sqlite] list_users: {} row(s)", users.len());

    let posts = sq::list_posts_by_user(&sq_pool, 1).await?;
    println!("[sqlite] list_posts_by_user(1): {} row(s)", posts.len());

    let joined = sq::list_posts_with_author(&sq_pool).await?;
    println!("[sqlite] list_posts_with_author: {} row(s)", joined.len());

    let authors = sq::get_active_authors(&sq_pool).await?;
    println!("[sqlite] get_active_authors: {} row(s)", authors.len());

    Ok(())
}
