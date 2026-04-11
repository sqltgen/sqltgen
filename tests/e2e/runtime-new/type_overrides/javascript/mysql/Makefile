REPO_ROOT := $(shell git rev-parse --show-toplevel)
SQLTGEN   := $(REPO_ROOT)/target/debug/sqltgen

.PHONY: install generate test clean

install:
	npm install

generate:
	rm -rf src/gen
	$(SQLTGEN) generate --config sqltgen.json

test: install generate
	npm test

clean:
	rm -rf src/gen node_modules
