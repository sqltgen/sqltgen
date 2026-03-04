CREATE TABLE customer (
    id    BIGSERIAL PRIMARY KEY,
    name  TEXT      NOT NULL,
    email TEXT      NOT NULL UNIQUE
);
