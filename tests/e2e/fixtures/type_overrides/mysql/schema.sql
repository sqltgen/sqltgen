CREATE TABLE event (
    id           BIGINT    NOT NULL AUTO_INCREMENT PRIMARY KEY,
    name         TEXT      NOT NULL,
    payload      JSON      NOT NULL,
    meta         JSON,
    doc_id       CHAR(36)  NOT NULL,
    created_at   DATETIME  NOT NULL,
    scheduled_at DATETIME,
    event_date   DATE,
    event_time   TIME
);
