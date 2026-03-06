SQLTGEN := ../../../target/debug/sqltgen
PYTHON  := python3

.PHONY: generate build run

generate:
	$(SQLTGEN) generate --config sqltgen.json

build: generate

run: build
	$(PYTHON) main.py
