CREATE SCHEMA IF NOT EXISTS internal;

CREATE TABLE public.users (
    id    BIGSERIAL PRIMARY KEY,
    name  TEXT      NOT NULL,
    email TEXT      NOT NULL
);

CREATE TABLE internal.audit_log (
    id         BIGSERIAL  PRIMARY KEY,
    user_id    BIGINT     NOT NULL,
    action     TEXT       NOT NULL,
    created_at TIMESTAMP  NOT NULL DEFAULT NOW()
);
