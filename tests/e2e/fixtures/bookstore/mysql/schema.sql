CREATE TABLE author (
    id         BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name       TEXT   NOT NULL,
    bio        TEXT,
    birth_year INTEGER
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE book (
    id           BIGINT        NOT NULL AUTO_INCREMENT PRIMARY KEY,
    author_id    BIGINT        NOT NULL,
    title        TEXT          NOT NULL,
    genre        TEXT          NOT NULL,
    price        DECIMAL(10,2) NOT NULL,
    published_at DATE,
    FOREIGN KEY (author_id) REFERENCES author(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE customer (
    id    BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name  VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    UNIQUE KEY uq_customer_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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

CREATE TABLE product (
    id          VARCHAR(36)  NOT NULL PRIMARY KEY,
    sku         VARCHAR(50)  NOT NULL,
    name        TEXT         NOT NULL,
    active      BOOLEAN      NOT NULL DEFAULT TRUE,
    weight_kg   FLOAT,
    rating      DOUBLE,
    metadata    JSON,
    thumbnail   BLOB,
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    stock_count SMALLINT     NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
