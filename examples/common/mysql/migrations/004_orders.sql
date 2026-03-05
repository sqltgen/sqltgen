CREATE TABLE sale (
    id          BIGINT   NOT NULL AUTO_INCREMENT PRIMARY KEY,
    customer_id BIGINT   NOT NULL,
    ordered_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customer(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE sale_item (
    id         BIGINT        NOT NULL AUTO_INCREMENT PRIMARY KEY,
    sale_id    BIGINT        NOT NULL,
    book_id    BIGINT        NOT NULL,
    quantity   INTEGER       NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (sale_id) REFERENCES sale(id),
    FOREIGN KEY (book_id) REFERENCES book(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
