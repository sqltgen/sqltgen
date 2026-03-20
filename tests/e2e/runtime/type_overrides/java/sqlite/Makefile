include ../../../common.mk

.PHONY: generate test clean

generate:
	rm -rf src/main/java/com/example/db
	$(SQLTGEN) generate --config sqltgen.json

test: generate
	mvn test

clean:
	mvn -q clean
	rm -rf src/main/java/com/example/db
