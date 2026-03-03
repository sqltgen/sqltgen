use sqlx::PgPool;

use super::users::Users;

#[derive(Debug, sqlx::FromRow)]
pub struct ListPostsByUserRow {
    pub id: i64,
    pub title: String,
    pub body: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ListPostsWithAuthorRow {
    pub id: i64,
    pub title: String,
    pub name: String,
    pub email: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ListUsersWithPostCountRow {
    pub name: String,
    pub email: String,
    pub post_count: Option<serde_json::Value>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetActiveAuthorsRow {
    pub id: i64,
    pub name: String,
    pub email: String,
}

pub async fn get_user(pool: &PgPool, id: i64) -> Result<Option<Users>, sqlx::Error> {
    sqlx::query_as::<_, Users>("SELECT id, name, email, bio FROM users WHERE id = $1")
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn list_users(pool: &PgPool) -> Result<Vec<Users>, sqlx::Error> {
    sqlx::query_as::<_, Users>("SELECT id, name, email, bio FROM users")
        .fetch_all(pool)
        .await
}

pub async fn create_user(pool: &PgPool, name: String, email: String, bio: Option<String>) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO users (name, email, bio) VALUES ($1, $2, $3)")
        .bind(name)
        .bind(email)
        .bind(bio)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn delete_user(pool: &PgPool, id: i64) -> Result<(), sqlx::Error> {
    sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn create_post(pool: &PgPool, user_id: i64, title: String, body: Option<String>) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO posts (user_id, title, body) VALUES ($1, $2, $3)")
        .bind(user_id)
        .bind(title)
        .bind(body)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn list_posts_by_user(pool: &PgPool, user_id: i64) -> Result<Vec<ListPostsByUserRow>, sqlx::Error> {
    sqlx::query_as::<_, ListPostsByUserRow>("SELECT p.id, p.title, p.body FROM posts p WHERE p.user_id = $1")
        .bind(user_id)
        .fetch_all(pool)
        .await
}

pub async fn list_posts_with_author(pool: &PgPool) -> Result<Vec<ListPostsWithAuthorRow>, sqlx::Error> {
    sqlx::query_as::<_, ListPostsWithAuthorRow>("SELECT p.id, p.title, u.name, u.email FROM posts p INNER JOIN users u ON u.id = p.user_id")
        .fetch_all(pool)
        .await
}

pub async fn list_users_with_post_count(pool: &PgPool) -> Result<Vec<ListUsersWithPostCountRow>, sqlx::Error> {
    sqlx::query_as::<_, ListUsersWithPostCountRow>("SELECT u.name, u.email, pc.post_count FROM users u INNER JOIN (SELECT user_id, COUNT(*) AS post_count FROM posts GROUP BY user_id) pc ON u.id = pc.user_id")
        .fetch_all(pool)
        .await
}

pub async fn get_active_authors(pool: &PgPool) -> Result<Vec<GetActiveAuthorsRow>, sqlx::Error> {
    sqlx::query_as::<_, GetActiveAuthorsRow>("WITH post_authors AS (     SELECT DISTINCT user_id FROM posts ) SELECT u.id, u.name, u.email FROM users u JOIN post_authors pa ON pa.user_id = u.id")
        .fetch_all(pool)
        .await
}
