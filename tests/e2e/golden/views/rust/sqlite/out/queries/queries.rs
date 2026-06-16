use super::super::sqltgen::DbPool;

use super::super::models::book_summaries::BookSummaries;
use super::super::models::sci_fi_books::SciFiBooks;

pub async fn list_book_summaries<'e, E>(executor: E) -> Result<Vec<BookSummaries>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let sql = r##"
        SELECT id, title, genre, author_name
        FROM book_summaries
        ORDER BY title
    "##;
    sqlx::query_as::<_, BookSummaries>(sql)
        .fetch_all(executor)
        .await
}

pub async fn list_book_summaries_by_genre<'e, E>(executor: E, genre: String) -> Result<Vec<BookSummaries>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let sql = r##"
        SELECT id, title, genre, author_name
        FROM book_summaries
        WHERE genre = ?
        ORDER BY title
    "##;
    sqlx::query_as::<_, BookSummaries>(sql)
        .bind(genre)
        .fetch_all(executor)
        .await
}

pub async fn list_sci_fi_books<'e, E>(executor: E) -> Result<Vec<SciFiBooks>, sqlx::Error>
where
    E: sqlx::Executor<'e, Database = sqlx::Sqlite>,
{
    let sql = r##"
        SELECT id, title, author_name
        FROM sci_fi_books
        ORDER BY title
    "##;
    sqlx::query_as::<_, SciFiBooks>(sql)
        .fetch_all(executor)
        .await
}

pub struct Querier<'a> {
    pool: &'a DbPool,
}

impl<'a> Querier<'a> {
    pub fn new(pool: &'a DbPool) -> Self {
        Self { pool }
    }

    pub async fn list_book_summaries(&self) -> Result<Vec<BookSummaries>, sqlx::Error> {
        list_book_summaries(self.pool).await
    }

    pub async fn list_book_summaries_by_genre(&self, genre: String) -> Result<Vec<BookSummaries>, sqlx::Error> {
        list_book_summaries_by_genre(self.pool, genre).await
    }

    pub async fn list_sci_fi_books(&self) -> Result<Vec<SciFiBooks>, sqlx::Error> {
        list_sci_fi_books(self.pool).await
    }
}
