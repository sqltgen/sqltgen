CREATE TABLE event (
    id           INTEGER   PRIMARY KEY AUTOINCREMENT,
    name         TEXT      NOT NULL,
    payload      TEXT      NOT NULL,
    meta         TEXT,
    doc_id       TEXT      NOT NULL,
    created_at   DATETIME  NOT NULL,
    scheduled_at DATETIME,
    event_date   DATE,
    event_time   TIME
);
