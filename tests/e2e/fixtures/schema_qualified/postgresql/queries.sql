-- name: GetUser :one
SELECT * FROM public.users WHERE id = $1;

-- name: ListAuditLogs :many
SELECT * FROM internal.audit_log ORDER BY created_at DESC;

-- name: CreateAuditLog :exec
INSERT INTO internal.audit_log (user_id, action) VALUES ($1, $2);
