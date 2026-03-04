CREATE TABLE book (
    id           BIGSERIAL      PRIMARY KEY,
    author_id    BIGINT         NOT NULL REFERENCES author(id),
    title        TEXT           NOT NULL,
    genre        TEXT           NOT NULL,
    price        NUMERIC(10, 2) NOT NULL,
    published_at DATE
);
