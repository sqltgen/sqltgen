-- name: GetUserViaDerived :one
SELECT * FROM (SELECT * FROM users) AS sub
WHERE sub.id = $1;

-- name: GetUsersViaCte :many
WITH recent AS (SELECT * FROM users WHERE id > $1)
SELECT * FROM recent;

-- name: GetUsersViaChainedCtes :many
WITH a AS (SELECT * FROM users),
     b AS (SELECT * FROM a)
SELECT * FROM b
WHERE id = $1;
