REPO_ROOT := $(shell git rev-parse --show-toplevel)
SQLTGEN   := $(REPO_ROOT)/target/debug/sqltgen

.PHONY: generate test clean

generate:
	rm -rf src/main/java/com/example/db
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	mvn -q test

clean:
	mvn -q clean
	rm -rf src/main/java/com/example/db
