SQLTGEN    := ../../../target/debug/sqltgen
MYSQL      := ../../common/mysql
MIGRATIONS := ../../common/mysql/migrations
MYSQLHOST  := 127.0.0.1
MYSQLPORT  := 3307
MYSQLUSER  := sqltgen
MYSQLPASS  := sqltgen

.PHONY: clean generate build db-up db-down run run-shared

clean:
	cargo clean
	rm -rf src/db

generate:
	rm -rf src/db
	$(SQLTGEN) generate --config sqltgen.json

build:
	cargo build

db-up:
	$(MAKE) -C $(MYSQL) db-up

db-down:
	$(MAKE) -C $(MYSQL) db-down

# run-shared: container already running (used by run-all).
run-shared: generate
	MIGRATIONS_DIR=$(MIGRATIONS) cargo run

# run: standalone — starts its own container, runs, tears down.
run: generate
	$(MAKE) -C $(MYSQL) db-up
	@MIGRATIONS_DIR=$(MIGRATIONS) cargo run; ret=$$?; $(MAKE) -C $(MYSQL) db-down || true; exit $$ret
