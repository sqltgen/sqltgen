SQLTGEN := ../../../target/debug/sqltgen
PG      := ../../common/postgresql

.PHONY: clean generate build db-up db-down run

clean:
	cargo clean
	rm -rf src/db

generate:
	rm -rf src/db
	$(SQLTGEN) generate --config sqltgen.json

build:
	cargo build

db-up:
	$(MAKE) -C $(PG) db-up

db-down:
	$(MAKE) -C $(PG) db-down

run: generate db-up
	cargo run; ret=$$?; $(MAKE) db-down; exit $$ret
