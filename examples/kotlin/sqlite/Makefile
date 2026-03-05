SQLTGEN := ../../../target/debug/sqltgen

.PHONY: generate build run

generate:
	$(SQLTGEN) generate --config sqltgen.json

build: generate
	mvn -q compile

run: build
	mvn -q exec:java
