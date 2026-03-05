SQLTGEN := ../../../target/debug/sqltgen

.PHONY: clean generate build run

clean:
	cargo clean
	rm -rf src/db

generate:
	$(SQLTGEN) generate --config sqltgen.json

build:
	cargo build

run: generate
	cargo run
