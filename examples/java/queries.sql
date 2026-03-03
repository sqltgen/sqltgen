-- name: GetUser :one
SELECT id, name, email, bio FROM users WHERE id = $1;

-- name: ListUsers :many
SELECT id, name, email, bio FROM users;

-- name: CreateUser :exec
INSERT INTO users (name, email, bio) VALUES ($1, $2, $3);

-- name: DeleteUser :exec
DELETE FROM users WHERE id = $1;

-- name: CreatePost :exec
INSERT INTO posts (user_id, title, body) VALUES ($1, $2, $3);

-- name: ListPostsByUser :many
SELECT p.id, p.title, p.body FROM posts p WHERE p.user_id = $1;

-- name: ListPostsWithAuthor :many
SELECT p.id, p.title, u.name, u.email
FROM posts p
INNER JOIN users u ON u.id = p.user_id;
