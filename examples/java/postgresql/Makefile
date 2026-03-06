SQLTGEN := ../../../target/debug/sqltgen
PG      := ../../common/postgresql

.PHONY: clean generate build db-up db-down run

clean:
	mvn -q clean
	rm -rf gen

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

build: generate
	mvn -q compile

db-up:
	$(MAKE) -C $(PG) db-up

db-down:
	$(MAKE) -C $(PG) db-down

run: build
	$(MAKE) db-up
	mvn -q exec:java; ret=$$?; $(MAKE) db-down; exit $$ret
