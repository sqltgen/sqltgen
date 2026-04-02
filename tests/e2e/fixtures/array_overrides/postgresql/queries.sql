-- name: InsertRecord :exec
INSERT INTO record (label, timestamps, uuids)
VALUES ($1, $2, $3);

-- name: GetRecord :one
SELECT id, label, timestamps, uuids
FROM record
WHERE id = $1;
