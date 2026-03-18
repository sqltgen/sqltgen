-- name: ListBookSummaries :many
SELECT id, title, genre, author_name
FROM book_summaries
ORDER BY title;

-- name: ListBookSummariesByGenre :many
SELECT id, title, genre, author_name
FROM book_summaries
WHERE genre = @genre
ORDER BY title;

-- name: ListSciFiBooks :many
SELECT id, title, author_name
FROM sci_fi_books
ORDER BY title;
