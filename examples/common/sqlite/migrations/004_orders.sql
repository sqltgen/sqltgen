CREATE TABLE sale (
    id          INTEGER  PRIMARY KEY AUTOINCREMENT,
    customer_id INTEGER  NOT NULL REFERENCES customer(id),
    ordered_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE sale_item (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    sale_id    INTEGER NOT NULL REFERENCES sale(id),
    book_id    INTEGER NOT NULL REFERENCES book(id),
    quantity   INTEGER NOT NULL,
    unit_price DECIMAL NOT NULL
);
