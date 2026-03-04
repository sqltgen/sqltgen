SQLTGEN := ../../target/debug/sqltgen

.PHONY: generate build run

generate:
	$(SQLTGEN) generate --config sqltgen-postgresql.json
	$(SQLTGEN) generate --config sqltgen-sqlite.json

build:
	mvn -q compile

run: build
	mvn -q exec:java
