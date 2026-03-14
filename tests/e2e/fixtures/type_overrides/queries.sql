-- name: GetEvent :one
SELECT id, name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time
FROM event
WHERE id = $1;

-- name: ListEvents :many
SELECT id, name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time
FROM event
ORDER BY id;

-- name: InsertEvent :exec
INSERT INTO event (name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);

-- name: UpdatePayload :exec
UPDATE event SET payload = $1, meta = $2 WHERE id = $3;

-- name: FindByDate :one
SELECT id, name FROM event WHERE event_date = $1;

-- name: FindByUuid :one
SELECT id, name FROM event WHERE doc_id = $1;
