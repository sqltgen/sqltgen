SQLTGEN    := ../../../target/debug/sqltgen
MYSQL      := ../../common/mysql
MIGRATIONS := ../../common/mysql/migrations
MYSQLHOST  := 127.0.0.1
MYSQLPORT  := 3307
MYSQLUSER  := sqltgen
MYSQLPASS  := sqltgen

.PHONY: clean generate build db-up db-down run run-shared

node_modules:
	npm install --silent

clean:
	rm -rf gen node_modules

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

build: generate node_modules

db-up:
	$(MAKE) -C $(MYSQL) db-up

db-down:
	$(MAKE) -C $(MYSQL) db-down

# run-shared: container already running (used by run-all).
run-shared: build
	MIGRATIONS_DIR=$(MIGRATIONS) node main.js

# run: standalone — starts its own container, runs, tears down.
run: build
	$(MAKE) -C $(MYSQL) db-up
	@MIGRATIONS_DIR=$(MIGRATIONS) node main.js; ret=$$?; $(MAKE) -C $(MYSQL) db-down || true; exit $$ret
