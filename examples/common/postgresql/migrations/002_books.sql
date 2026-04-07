CREATE TYPE genre AS ENUM ('fiction', 'non_fiction', 'science', 'history', 'biography');

CREATE TABLE book (
    id           BIGSERIAL      PRIMARY KEY,
    author_id    BIGINT         NOT NULL REFERENCES author(id),
    title        TEXT           NOT NULL,
    genre        genre          NOT NULL,
    price        NUMERIC(10, 2) NOT NULL,
    published_at DATE
);
