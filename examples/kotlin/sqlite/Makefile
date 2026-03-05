SQLTGEN := ../../../target/debug/sqltgen

.PHONY: clean generate build run

clean:
	mvn -q clean
	rm -rf gen

generate:
	$(SQLTGEN) generate --config sqltgen.json

build: generate
	mvn -q compile

run: build
	mvn -q exec:java
