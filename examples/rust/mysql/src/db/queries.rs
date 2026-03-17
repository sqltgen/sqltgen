use super::_sqltgen::DbPool;

use super::author::Author;
use super::book::Book;

#[derive(Debug, sqlx::FromRow)]
pub struct ListBooksWithAuthorRow {
    pub id: i64,
    pub title: String,
    pub genre: String,
    pub price: rust_decimal::Decimal,
    pub published_at: Option<time::Date>,
    pub author_name: String,
    pub author_bio: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetTopSellingBooksRow {
    pub id: i64,
    pub title: String,
    pub genre: String,
    pub price: rust_decimal::Decimal,
    pub units_sold: Option<rust_decimal::Decimal>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct GetBestCustomersRow {
    pub id: i64,
    pub name: String,
    pub email: String,
    pub total_spent: Option<rust_decimal::Decimal>,
}

pub async fn create_author(pool: &DbPool, name: String, bio: Option<String>, birth_year: Option<i32>) -> Result<(), sqlx::Error> {
    let sql = r##"
        INSERT INTO author (name, bio, birth_year)
        VALUES (?, ?, ?)
    "##;
    sqlx::query(sql)
        .bind(name)
        .bind(bio)
        .bind(birth_year)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn get_author(pool: &DbPool, id: i64) -> Result<Option<Author>, sqlx::Error> {
    let sql = r##"
        SELECT id, name, bio, birth_year
        FROM author
        WHERE id = ?
    "##;
    sqlx::query_as::<_, Author>(sql)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn list_authors(pool: &DbPool) -> Result<Vec<Author>, sqlx::Error> {
    let sql = r##"
        SELECT id, name, bio, birth_year
        FROM author
        ORDER BY name
    "##;
    sqlx::query_as::<_, Author>(sql)
        .fetch_all(pool)
        .await
}

pub async fn update_author_bio(pool: &DbPool, bio: Option<String>, id: i64) -> Result<(), sqlx::Error> {
    let sql = r##"
        UPDATE author SET bio = ? WHERE id = ?
    "##;
    sqlx::query(sql)
        .bind(bio)
        .bind(id)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn delete_author(pool: &DbPool, id: i64) -> Result<(), sqlx::Error> {
    let sql = r##"
        DELETE FROM author WHERE id = ?
    "##;
    sqlx::query(sql)
        .bind(id)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn create_book(pool: &DbPool, author_id: i64, title: String, genre: String, price: rust_decimal::Decimal, published_at: Option<time::Date>) -> Result<(), sqlx::Error> {
    let sql = r##"
        INSERT INTO book (author_id, title, genre, price, published_at)
        VALUES (?, ?, ?, ?, ?)
    "##;
    sqlx::query(sql)
        .bind(author_id)
        .bind(title)
        .bind(genre)
        .bind(price)
        .bind(published_at)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn get_book(pool: &DbPool, id: i64) -> Result<Option<Book>, sqlx::Error> {
    let sql = r##"
        SELECT id, author_id, title, genre, price, published_at
        FROM book
        WHERE id = ?
    "##;
    sqlx::query_as::<_, Book>(sql)
        .bind(id)
        .fetch_optional(pool)
        .await
}

pub async fn get_books_by_ids(pool: &DbPool, ids: &[i64]) -> Result<Vec<Book>, sqlx::Error> {
    let sql = r##"
        SELECT id, author_id, title, genre, price, published_at
        FROM book
        WHERE id IN (SELECT value FROM JSON_TABLE(?,'$[*]' COLUMNS(value BIGINT PATH '$')) t)
        ORDER BY title
    "##;
    let ids_json = format!("[{}]", ids.iter().map(|x| x.to_string()).collect::<Vec<_>>().join(","));
    sqlx::query_as::<_, Book>(sql)
        .bind(ids_json)
        .fetch_all(pool)
        .await
}

pub async fn list_books_by_genre(pool: &DbPool, genre: String) -> Result<Vec<Book>, sqlx::Error> {
    let sql = r##"
        SELECT id, author_id, title, genre, price, published_at
        FROM book
        WHERE genre = ?
        ORDER BY title
    "##;
    sqlx::query_as::<_, Book>(sql)
        .bind(genre)
        .fetch_all(pool)
        .await
}

pub async fn list_books_by_genre_or_all(pool: &DbPool, genre: String) -> Result<Vec<Book>, sqlx::Error> {
    let sql = r##"
        SELECT id, author_id, title, genre, price, published_at
        FROM book
        WHERE ? = 'all' OR genre = ?
        ORDER BY title
    "##;
    sqlx::query_as::<_, Book>(sql)
        .bind(genre.clone())
        .bind(genre)
        .fetch_all(pool)
        .await
}

pub async fn create_customer(pool: &DbPool, name: String, email: String) -> Result<(), sqlx::Error> {
    let sql = r##"
        INSERT INTO customer (name, email)
        VALUES (?, ?)
    "##;
    sqlx::query(sql)
        .bind(name)
        .bind(email)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn create_sale(pool: &DbPool, customer_id: i64) -> Result<(), sqlx::Error> {
    let sql = r##"
        INSERT INTO sale (customer_id)
        VALUES (?)
    "##;
    sqlx::query(sql)
        .bind(customer_id)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn add_sale_item(pool: &DbPool, sale_id: i64, book_id: i64, quantity: i32, unit_price: rust_decimal::Decimal) -> Result<(), sqlx::Error> {
    let sql = r##"
        INSERT INTO sale_item (sale_id, book_id, quantity, unit_price)
        VALUES (?, ?, ?, ?)
    "##;
    sqlx::query(sql)
        .bind(sale_id)
        .bind(book_id)
        .bind(quantity)
        .bind(unit_price)
        .execute(pool)
        .await
        .map(|_| ())
}

pub async fn list_books_with_author(pool: &DbPool) -> Result<Vec<ListBooksWithAuthorRow>, sqlx::Error> {
    let sql = r##"
        SELECT b.id, b.title, b.genre, b.price, b.published_at,
               a.name AS author_name, a.bio AS author_bio
        FROM book b
        JOIN author a ON a.id = b.author_id
        ORDER BY b.title
    "##;
    sqlx::query_as::<_, ListBooksWithAuthorRow>(sql)
        .fetch_all(pool)
        .await
}

pub async fn get_books_never_ordered(pool: &DbPool) -> Result<Vec<Book>, sqlx::Error> {
    let sql = r##"
        SELECT b.id, b.author_id, b.title, b.genre, b.price, b.published_at
        FROM book b
        LEFT JOIN sale_item si ON si.book_id = b.id
        WHERE si.id IS NULL
        ORDER BY b.title
    "##;
    sqlx::query_as::<_, Book>(sql)
        .fetch_all(pool)
        .await
}

pub async fn get_top_selling_books(pool: &DbPool) -> Result<Vec<GetTopSellingBooksRow>, sqlx::Error> {
    let sql = r##"
        WITH book_sales AS (
            SELECT book_id,
                   SUM(quantity) AS units_sold
            FROM sale_item
            GROUP BY book_id
        )
        SELECT b.id, b.title, b.genre, b.price,
               bs.units_sold
        FROM book b
        JOIN book_sales bs ON bs.book_id = b.id
        ORDER BY bs.units_sold DESC
    "##;
    sqlx::query_as::<_, GetTopSellingBooksRow>(sql)
        .fetch_all(pool)
        .await
}

pub async fn get_best_customers(pool: &DbPool) -> Result<Vec<GetBestCustomersRow>, sqlx::Error> {
    let sql = r##"
        WITH customer_spend AS (
            SELECT s.customer_id,
                   SUM(si.quantity * si.unit_price) AS total_spent
            FROM sale s
            JOIN sale_item si ON si.sale_id = s.id
            GROUP BY s.customer_id
        )
        SELECT c.id, c.name, c.email,
               cs.total_spent
        FROM customer c
        JOIN customer_spend cs ON cs.customer_id = c.id
        ORDER BY cs.total_spent DESC
    "##;
    sqlx::query_as::<_, GetBestCustomersRow>(sql)
        .fetch_all(pool)
        .await
}

pub struct Querier<'a> {
    pool: &'a DbPool,
}

impl<'a> Querier<'a> {
    pub fn new(pool: &'a DbPool) -> Self {
        Self { pool }
    }

    pub async fn create_author(&self, name: String, bio: Option<String>, birth_year: Option<i32>) -> Result<(), sqlx::Error> {
        create_author(self.pool, name, bio, birth_year).await
    }

    pub async fn get_author(&self, id: i64) -> Result<Option<Author>, sqlx::Error> {
        get_author(self.pool, id).await
    }

    pub async fn list_authors(&self) -> Result<Vec<Author>, sqlx::Error> {
        list_authors(self.pool).await
    }

    pub async fn update_author_bio(&self, bio: Option<String>, id: i64) -> Result<(), sqlx::Error> {
        update_author_bio(self.pool, bio, id).await
    }

    pub async fn delete_author(&self, id: i64) -> Result<(), sqlx::Error> {
        delete_author(self.pool, id).await
    }

    pub async fn create_book(&self, author_id: i64, title: String, genre: String, price: rust_decimal::Decimal, published_at: Option<time::Date>) -> Result<(), sqlx::Error> {
        create_book(self.pool, author_id, title, genre, price, published_at).await
    }

    pub async fn get_book(&self, id: i64) -> Result<Option<Book>, sqlx::Error> {
        get_book(self.pool, id).await
    }

    pub async fn get_books_by_ids(&self, ids: &[i64]) -> Result<Vec<Book>, sqlx::Error> {
        get_books_by_ids(self.pool, ids).await
    }

    pub async fn list_books_by_genre(&self, genre: String) -> Result<Vec<Book>, sqlx::Error> {
        list_books_by_genre(self.pool, genre).await
    }

    pub async fn list_books_by_genre_or_all(&self, genre: String) -> Result<Vec<Book>, sqlx::Error> {
        list_books_by_genre_or_all(self.pool, genre).await
    }

    pub async fn create_customer(&self, name: String, email: String) -> Result<(), sqlx::Error> {
        create_customer(self.pool, name, email).await
    }

    pub async fn create_sale(&self, customer_id: i64) -> Result<(), sqlx::Error> {
        create_sale(self.pool, customer_id).await
    }

    pub async fn add_sale_item(&self, sale_id: i64, book_id: i64, quantity: i32, unit_price: rust_decimal::Decimal) -> Result<(), sqlx::Error> {
        add_sale_item(self.pool, sale_id, book_id, quantity, unit_price).await
    }

    pub async fn list_books_with_author(&self) -> Result<Vec<ListBooksWithAuthorRow>, sqlx::Error> {
        list_books_with_author(self.pool).await
    }

    pub async fn get_books_never_ordered(&self) -> Result<Vec<Book>, sqlx::Error> {
        get_books_never_ordered(self.pool).await
    }

    pub async fn get_top_selling_books(&self) -> Result<Vec<GetTopSellingBooksRow>, sqlx::Error> {
        get_top_selling_books(self.pool).await
    }

    pub async fn get_best_customers(&self) -> Result<Vec<GetBestCustomersRow>, sqlx::Error> {
        get_best_customers(self.pool).await
    }
}
