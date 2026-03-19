-- name: GetEvent :one
SELECT id, name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time
FROM event
WHERE id = ?1;

-- name: ListEvents :many
SELECT id, name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time
FROM event
ORDER BY id;

-- name: InsertEvent :exec
INSERT INTO event (name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);

-- name: UpdatePayload :exec
UPDATE event SET payload = ?1, meta = ?2 WHERE id = ?3;

-- name: FindByDate :one
SELECT id, name FROM event WHERE event_date = ?1;

-- name: FindByDocId :one
SELECT id, name FROM event WHERE doc_id = ?1;

-- name: InsertEventRows :execrows
INSERT INTO event (name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time)
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8);

-- name: GetEventsByDateRange :many
SELECT id, name, payload, meta, doc_id, created_at, scheduled_at, event_date, event_time
FROM event
WHERE created_at BETWEEN ?1 AND ?2
ORDER BY created_at;

-- name: CountEvents :one
SELECT COUNT(*) AS total FROM event WHERE created_at > ?1;

-- name: UpdateEventDate :exec
UPDATE event SET event_date = ?1 WHERE id = ?2;
