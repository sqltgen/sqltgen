-- name: CreateAuthor :exec
INSERT INTO author (name, bio, birth_year)
VALUES (@name, @bio, @birth_year);

-- name: GetAuthor :one
SELECT id, name, bio, birth_year
FROM author
WHERE id = @id;

-- name: ListAuthors :many
SELECT id, name, bio, birth_year
FROM author
ORDER BY name;

-- name: UpdateAuthorBio :exec
-- @bio null
UPDATE author SET bio = @bio WHERE id = @id;

-- name: DeleteAuthor :exec
DELETE FROM author WHERE id = @id;

-- name: CreateBook :exec
INSERT INTO book (author_id, title, genre, price, published_at)
VALUES (@author_id, @title, @genre, @price, @published_at);

-- name: GetBook :one
SELECT id, author_id, title, genre, price, published_at
FROM book
WHERE id = @id;

-- name: GetBooksByIds :many
-- @ids bigint[] not null
SELECT id, author_id, title, genre, price, published_at
FROM book
WHERE id IN (@ids)
ORDER BY title;

-- name: ListBooksByGenre :many
SELECT id, author_id, title, genre, price, published_at
FROM book
WHERE genre = @genre
ORDER BY title;

-- name: ListBooksByGenreOrAll :many
SELECT id, author_id, title, genre, price, published_at
FROM book
WHERE @genre = 'all' OR genre = @genre
ORDER BY title;

-- name: CreateCustomer :exec
INSERT INTO customer (name, email)
VALUES (@name, @email);

-- name: CreateSale :exec
INSERT INTO sale (customer_id)
VALUES (@customer_id);

-- name: AddSaleItem :exec
INSERT INTO sale_item (sale_id, book_id, quantity, unit_price)
VALUES (@sale_id, @book_id, @quantity, @unit_price);

-- name: ListBooksWithAuthor :many
SELECT b.id, b.title, b.genre, b.price, b.published_at,
       a.name AS author_name, a.bio AS author_bio
FROM book b
JOIN author a ON a.id = b.author_id
ORDER BY b.title;

-- name: GetBooksNeverOrdered :many
SELECT b.id, b.author_id, b.title, b.genre, b.price, b.published_at
FROM book b
LEFT JOIN sale_item si ON si.book_id = b.id
WHERE si.id IS NULL
ORDER BY b.title;

-- name: GetTopSellingBooks :many
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
ORDER BY bs.units_sold DESC;

-- name: GetBestCustomers :many
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
ORDER BY cs.total_spent DESC;

-- name: CountBooksByGenre :many
SELECT genre, COUNT(*) AS book_count
FROM book
GROUP BY genre
ORDER BY genre;

-- name: ListBooksWithLimit :many
SELECT id, title, genre, price
FROM book
ORDER BY title
LIMIT $1 OFFSET $2;

-- name: SearchBooksByTitle :many
SELECT id, title, genre, price
FROM book
WHERE title LIKE $1
ORDER BY title;

-- name: GetBooksByPriceRange :many
SELECT id, title, genre, price
FROM book
WHERE price BETWEEN $1 AND $2
ORDER BY price;

-- name: GetBooksInGenres :many
SELECT id, title, genre, price
FROM book
WHERE genre IN ($1, $2, $3)
ORDER BY title;

-- name: GetBookPriceLabel :many
SELECT id, title, price,
       CASE WHEN price > $1 THEN 'expensive' ELSE 'affordable' END AS price_label
FROM book
ORDER BY title;

-- name: GetBookPriceOrDefault :many
SELECT id, title, COALESCE(price, $1) AS effective_price
FROM book
ORDER BY title;

-- name: DeleteBookById :execrows
DELETE FROM book WHERE id = $1;

-- name: GetGenresWithManyBooks :many
SELECT genre, COUNT(*) AS book_count
FROM book
GROUP BY genre
HAVING COUNT(*) > $1
ORDER BY genre;

-- name: GetBooksByAuthorParam :many
SELECT b.id, b.title, b.price
FROM book b
JOIN author a ON a.id = b.author_id AND a.birth_year > $1
ORDER BY b.title;

-- name: GetAllBookFields :many
SELECT b.*
FROM book b
ORDER BY b.id;

-- name: GetBooksNotByAuthor :many
SELECT id, title, genre
FROM book
WHERE author_id NOT IN (SELECT id FROM author WHERE name = $1)
ORDER BY title;

-- name: GetBooksWithRecentSales :many
SELECT id, title, genre
FROM book
WHERE EXISTS (
    SELECT 1 FROM sale_item si
    JOIN sale s ON s.id = si.sale_id
    WHERE si.book_id = book.id AND s.ordered_at > $1
)
ORDER BY title;

-- name: GetBookWithAuthorName :many
SELECT b.id, b.title,
       (SELECT a.name FROM author a WHERE a.id = b.author_id) AS author_name
FROM book b
ORDER BY b.title;

-- name: GetAuthorStats :many
WITH book_counts AS (
    SELECT author_id, COUNT(*) AS num_books
    FROM book
    GROUP BY author_id
),
sale_counts AS (
    SELECT b.author_id, SUM(si.quantity) AS total_sold
    FROM sale_item si
    JOIN book b ON b.id = si.book_id
    GROUP BY b.author_id
)
SELECT a.id, a.name,
       COALESCE(bc.num_books, 0) AS num_books,
       COALESCE(sc.total_sold, 0) AS total_sold
FROM author a
LEFT JOIN book_counts bc ON bc.author_id = a.id
LEFT JOIN sale_counts sc ON sc.author_id = a.id
ORDER BY a.name;

-- name: GetProduct :one
SELECT id, sku, name, active, weight_kg, rating, metadata,
       thumbnail, created_at, stock_count
FROM product
WHERE id = $1;

-- name: ListActiveProducts :many
SELECT id, sku, name, active, weight_kg, rating, metadata,
       created_at, stock_count
FROM product
WHERE active = $1
ORDER BY name;

-- name: GetAuthorsWithNullBio :many
SELECT id, name, birth_year
FROM author
WHERE bio IS NULL
ORDER BY name;

-- name: GetAuthorsWithBio :many
SELECT id, name, bio, birth_year
FROM author
WHERE bio IS NOT NULL
ORDER BY name;

-- name: GetBooksPublishedBetween :many
SELECT id, title, genre, price, published_at
FROM book
WHERE published_at IS NOT NULL
  AND published_at BETWEEN $1 AND $2
ORDER BY published_at;

-- name: GetDistinctGenres :many
SELECT DISTINCT genre
FROM book
ORDER BY genre;

-- name: GetBooksWithSalesCount :many
SELECT b.id, b.title, b.genre,
       COALESCE(SUM(si.quantity), 0) AS total_quantity
FROM book b
LEFT JOIN sale_item si ON si.book_id = b.id
GROUP BY b.id, b.title, b.genre
ORDER BY total_quantity DESC, b.title;

-- name: CountSaleItems :one
SELECT COUNT(*) AS item_count
FROM sale_item
WHERE sale_id = $1;

-- name: GetSaleItemQuantityAggregates :one
SELECT MIN(quantity)  AS min_qty,
       MAX(quantity)  AS max_qty,
       SUM(quantity)  AS sum_qty,
       AVG(quantity)  AS avg_qty
FROM sale_item;

-- name: GetBookPriceAggregates :one
SELECT MIN(price)  AS min_price,
       MAX(price)  AS max_price,
       SUM(price)  AS sum_price,
       AVG(price)  AS avg_price
FROM book;
