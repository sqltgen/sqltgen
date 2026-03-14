SQLTGEN := ../../../../../../target/debug/sqltgen

.PHONY: generate test clean

generate:
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	mvn -q test

clean:
	mvn -q clean
	rm -rf src/main/java/com/example/db
