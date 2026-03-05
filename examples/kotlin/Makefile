SQLTGEN := ../../target/debug/sqltgen
PG      := ../common/postgresql

.PHONY: generate build db-up db-down run

generate:
	$(SQLTGEN) generate --config sqltgen-postgresql.json
	$(SQLTGEN) generate --config sqltgen-sqlite.json

build: generate
	mvn -q compile

db-up:
	$(MAKE) -C $(PG) db-up

db-down:
	$(MAKE) -C $(PG) db-down

run: build
	$(MAKE) db-up
	mvn -q exec:java; ret=$$?; $(MAKE) db-down; exit $$ret
