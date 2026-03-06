SQLTGEN := ../../../target/debug/sqltgen
PYTHON  := python3

.PHONY: clean generate build run

clean:
	rm -rf gen

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

build: generate

run: build
	$(PYTHON) main.py
