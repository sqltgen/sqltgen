SQLTGEN := ../../../target/debug/sqltgen
MYSQL   := ../../common/mysql

.PHONY: generate build db-up db-down run

generate:
	$(SQLTGEN) generate --config sqltgen.json

build: generate
	mvn -q compile

db-up:
	$(MAKE) -C $(MYSQL) db-up

db-down:
	$(MAKE) -C $(MYSQL) db-down

run: build
	$(MAKE) db-up
	mvn -q exec:java; ret=$$?; $(MAKE) db-down; exit $$ret
