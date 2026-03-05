SQLTGEN := ../../../target/debug/sqltgen
PG      := ../../common/postgresql

.PHONY: generate build db-up db-down run

generate:
	$(SQLTGEN) generate --config sqltgen.json

build:
	cargo build

db-up:
	$(MAKE) -C $(PG) db-up

db-down:
	$(MAKE) -C $(PG) db-down

run: generate db-up
	cargo run; ret=$$?; $(MAKE) db-down; exit $$ret
