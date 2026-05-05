CREATE TABLE author (
    id         BIGSERIAL    PRIMARY KEY,
    name       TEXT         NOT NULL,
    bio        TEXT,
    birth_year INTEGER
);

CREATE TABLE book (
    id           BIGSERIAL      PRIMARY KEY,
    author_id    BIGINT         NOT NULL REFERENCES author(id),
    title        TEXT           NOT NULL,
    genre        TEXT           NOT NULL,
    price        NUMERIC(10, 2) NOT NULL,
    published_at DATE
);
