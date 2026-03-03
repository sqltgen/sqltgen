mod db;

use db::queries;

#[tokio::main]
async fn main() -> Result<(), sqlx::Error> {
    dotenvy::dotenv().ok();
    let url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = sqlx::PgPool::connect(&url).await?;

    // Seed some data
    queries::create_user(&pool, "Alice".into(), "alice@example.com".into(), Some("loves Rust".into())).await?;
    queries::create_user(&pool, "Bob".into(), "bob@example.com".into(), None).await?;
    queries::create_post(&pool, 1, "Hello World".into(), Some("My first post".into())).await?;
    queries::create_post(&pool, 1, "Second Post".into(), None).await?;

    // Fetch one user
    if let Some(u) = queries::get_user(&pool, 1).await? {
        println!("get_user(1): {} <{}>  bio={:?}", u.name, u.email, u.bio);
    }

    // List all users
    let users = queries::list_users(&pool).await?;
    println!("\nlist_users: {} row(s)", users.len());
    for u in &users {
        println!("  {} | {} | {:?}", u.id, u.name, u.bio);
    }

    // Posts by user 1
    let posts = queries::list_posts_by_user(&pool, 1).await?;
    println!("\nlist_posts_by_user(1): {} row(s)", posts.len());
    for p in &posts {
        println!("  {} | {}", p.id, p.title);
    }

    // JOIN query
    let joined = queries::list_posts_with_author(&pool).await?;
    println!("\nlist_posts_with_author: {} row(s)", joined.len());
    for r in &joined {
        println!("  \"{}\" by {}", r.title, r.name);
    }

    // CTE query
    let authors = queries::get_active_authors(&pool).await?;
    println!("\nget_active_authors: {} row(s)", authors.len());
    for a in &authors {
        println!("  {} <{}>", a.name, a.email);
    }

    Ok(())
}
