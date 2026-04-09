-- name: CreateTask :one
INSERT INTO task (title, priority, status, description)
VALUES (@title, @priority, @status, @description)
RETURNING *;

-- name: GetTask :one
SELECT id, title, priority, status, description
FROM task
WHERE id = @id;

-- name: ListTasksByPriority :many
SELECT id, title, priority, status
FROM task
WHERE priority = @priority
ORDER BY id;

-- name: ListTasksByStatus :many
SELECT id, title, priority, status
FROM task
WHERE status = @status
ORDER BY id;

-- name: UpdateTaskStatus :one
UPDATE task SET status = @status WHERE id = @id
RETURNING *;

-- name: ListTasksByPriorityOrAll :many
-- @priority null
SELECT id, title, priority, status
FROM task
WHERE (@priority::priority IS NULL OR priority = @priority::priority)
ORDER BY id;

-- name: CountByStatus :many
SELECT status, COUNT(*) AS task_count
FROM task
GROUP BY status
ORDER BY status;

-- name: CreateTaskWithTags :one
INSERT INTO task (title, priority, status, description, tags)
VALUES (@title, @priority, @status, @description, @tags)
RETURNING *;

-- name: GetTaskTags :one
SELECT id, title, tags
FROM task
WHERE id = @id;

-- name: UpdateTaskTags :one
UPDATE task SET tags = @tags WHERE id = @id
RETURNING *;

-- name: DeleteTask :exec
DELETE FROM task WHERE id = @id;
