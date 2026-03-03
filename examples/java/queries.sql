-- name: GetUser :one
SELECT id, name, email, bio FROM users WHERE id = $1;

-- name: ListUsers :many
SELECT id, name, email, bio FROM users;

-- name: CreateUser :exec
INSERT INTO users (name, email, bio) VALUES ($1, $2, $3);

-- name: DeleteUser :exec
DELETE FROM users WHERE id = $1;
