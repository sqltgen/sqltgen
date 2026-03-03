CREATE TABLE users (
    id    BIGSERIAL PRIMARY KEY,
    name  TEXT      NOT NULL,
    email TEXT      NOT NULL
);

ALTER TABLE users ADD COLUMN bio TEXT;
