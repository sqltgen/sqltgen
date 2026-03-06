SQLTGEN := ../../../target/debug/sqltgen
DB      := ../../common/mysql

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
	$(MAKE) -C $(DB) db-up

db-down:
	$(MAKE) -C $(DB) db-down

run: generate db-up
	cargo run; ret=$$?; $(MAKE) db-down; exit $$ret
