use super::super::sqltgen::DbPool;

use super::super::models::author::Author;
use super::super::models::book::Book;

#[derive(Debug, sqlx::FromRow)]
pub struct DeleteAuthorRow {
    pub id: i64,
    pub name: String,
}

pub async fn create_author(pool: &DbPool, name: String, bio: Option<String>, birth_year: Option<i32>) -> Result<Option<Author>, sqlx::Error> {
    let sql = r##"
        INSERT INTO author (name, bio, birth_year)
        VALUES ($1, $2, $3)
        RETURNING *
    "##;
    sqlx::query_as::<_, Author>(sql)
        .bind(name)
        .bind(bio)
        .bind(birth_year)
        .fetch_optional(pool)
        .await
}

pub async fn get_author(pool: &DbPool, id: i64) -> Result<Option<Author>, sqlx::Error> {
    let sql = r##"
        SELECT id, name, bio, birth_year
        FROM author
        WHERE id = $1
    "##;
    sqlx::query_as::<_, Author>(sql)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn update_author_bio(pool: &DbPool, bio: Option<String>, id: i64) -> Result<Option<Author>, sqlx::Error> {
    let sql = r##"
        UPDATE author SET bio = $1 WHERE id = $2
        RETURNING *
    "##;
    sqlx::query_as::<_, Author>(sql)
        .bind(bio)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn delete_author(pool: &DbPool, id: i64) -> Result<Option<DeleteAuthorRow>, sqlx::Error> {
    let sql = r##"
        DELETE FROM author WHERE id = $1
        RETURNING id, name
    "##;
    sqlx::query_as::<_, DeleteAuthorRow>(sql)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn create_book(pool: &DbPool, author_id: i64, title: String, genre: String, price: rust_decimal::Decimal, published_at: Option<time::Date>) -> Result<Option<Book>, sqlx::Error> {
    let sql = r##"
        INSERT INTO book (author_id, title, genre, price, published_at)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING *
    "##;
    sqlx::query_as::<_, Book>(sql)
        .bind(author_id)
        .bind(title)
        .bind(genre)
        .bind(price)
        .bind(published_at)
        .fetch_optional(pool)
        .await
}

pub struct Querier<'a> {
    pool: &'a DbPool,
}

impl<'a> Querier<'a> {
    pub fn new(pool: &'a DbPool) -> Self {
        Self { pool }
    }

    pub async fn create_author(&self, name: String, bio: Option<String>, birth_year: Option<i32>) -> Result<Option<Author>, sqlx::Error> {
        create_author(self.pool, name, bio, birth_year).await
    }

    pub async fn get_author(&self, id: i64) -> Result<Option<Author>, sqlx::Error> {
        get_author(self.pool, id).await
    }

    pub async fn update_author_bio(&self, bio: Option<String>, id: i64) -> Result<Option<Author>, sqlx::Error> {
        update_author_bio(self.pool, bio, id).await
    }

    pub async fn delete_author(&self, id: i64) -> Result<Option<DeleteAuthorRow>, sqlx::Error> {
        delete_author(self.pool, id).await
    }

    pub async fn create_book(&self, author_id: i64, title: String, genre: String, price: rust_decimal::Decimal, published_at: Option<time::Date>) -> Result<Option<Book>, sqlx::Error> {
        create_book(self.pool, author_id, title, genre, price, published_at).await
    }
}
