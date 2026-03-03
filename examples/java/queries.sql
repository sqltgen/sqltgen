-- name: GetUser :one
SELECT id, name, email FROM users WHERE id = $1;

-- name: ListUsers :many
SELECT id, name, email FROM users;

-- name: CreateUser :exec
INSERT INTO users (name, email) VALUES ($1, $2);

-- name: DeleteUser :exec
DELETE FROM users WHERE id = $1;
