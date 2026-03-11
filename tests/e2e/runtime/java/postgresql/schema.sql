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

CREATE TABLE customer (
    id    BIGSERIAL PRIMARY KEY,
    name  TEXT      NOT NULL,
    email TEXT      NOT NULL UNIQUE
);

CREATE TABLE sale (
    id          BIGSERIAL  PRIMARY KEY,
    customer_id BIGINT     NOT NULL REFERENCES customer(id),
    ordered_at  TIMESTAMP  NOT NULL DEFAULT NOW()
);

CREATE TABLE sale_item (
    id         BIGSERIAL      PRIMARY KEY,
    sale_id    BIGINT         NOT NULL REFERENCES sale(id),
    book_id    BIGINT         NOT NULL REFERENCES book(id),
    quantity   INTEGER        NOT NULL,
    unit_price NUMERIC(10, 2) NOT NULL
);
