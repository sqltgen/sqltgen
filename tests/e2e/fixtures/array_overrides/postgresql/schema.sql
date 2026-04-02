CREATE TABLE record (
    id           BIGSERIAL    PRIMARY KEY,
    label        TEXT         NOT NULL,
    timestamps   TIMESTAMP[]  NOT NULL,
    uuids        UUID[]       NOT NULL
);
