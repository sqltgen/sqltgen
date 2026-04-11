-- name: CreateAuthor :one
INSERT INTO author (name, bio, birth_year)
VALUES (@name, @bio, @birth_year)
RETURNING *;

-- name: GetAuthor :one
SELECT id, name, bio, birth_year
FROM author
WHERE id = @id;

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
