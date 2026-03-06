-- name: CreateAuthor :one
INSERT INTO author (name, bio, birth_year)
VALUES (@name, @bio, @birth_year)
RETURNING *;

-- name: GetAuthor :one
SELECT id, name, bio, birth_year
FROM author
WHERE id = @id;

-- name: ListAuthors :many
SELECT id, name, bio, birth_year
FROM author
ORDER BY name;

-- name: UpdateAuthorBio :one
-- @bio null
UPDATE author SET bio = @bio WHERE id = @id
RETURNING *;

-- name: DeleteAuthor :one
DELETE FROM author WHERE id = @id
RETURNING id, name;

-- name: CreateBook :one
INSERT INTO book (author_id, title, genre, price, published_at)
VALUES (@author_id, @title, @genre, @price, @published_at)
RETURNING *;

-- name: GetBook :one
SELECT id, author_id, title, genre, price, published_at
FROM book
WHERE id = @id;

-- name: ListBooksByGenre :many
SELECT id, author_id, title, genre, price, published_at
FROM book
WHERE genre = @genre
ORDER BY title;

-- name: CreateCustomer :one
INSERT INTO customer (name, email)
VALUES (@name, @email)
RETURNING id;

-- name: CreateSale :one
INSERT INTO sale (customer_id)
VALUES (@customer_id)
RETURNING id;

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
