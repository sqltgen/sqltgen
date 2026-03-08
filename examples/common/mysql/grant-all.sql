-- Grant sqltgen user global privileges so it can CREATE and DROP
-- per-run randomly-named databases without needing root credentials.
GRANT ALL PRIVILEGES ON *.* TO 'sqltgen'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
