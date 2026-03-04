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
