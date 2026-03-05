SQLTGEN := ../../../target/debug/sqltgen

.PHONY: generate build run

generate:
	$(SQLTGEN) generate --config sqltgen.json

build:
	cargo build

run: generate
	cargo run
