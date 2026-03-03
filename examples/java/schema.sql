CREATE TABLE users (
    id    BIGSERIAL PRIMARY KEY,
    name  TEXT      NOT NULL,
    email TEXT      NOT NULL
);

ALTER TABLE users ADD COLUMN bio TEXT;

CREATE TABLE posts (
    id      BIGSERIAL PRIMARY KEY,
    user_id BIGINT    NOT NULL REFERENCES users(id),
    title   TEXT      NOT NULL,
    body    TEXT
);
