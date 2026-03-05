SQLTGEN := ../../../target/debug/sqltgen
MYSQL   := ../../common/mysql

.PHONY: clean generate build db-up db-down run

clean:
	mvn -q clean
	rm -rf gen

generate:
	$(SQLTGEN) generate --config sqltgen.json

build: generate
	mvn -q compile

db-up:
	$(MAKE) -C $(MYSQL) db-up

db-down:
	$(MAKE) -C $(MYSQL) db-down

run: build
	$(MAKE) db-up
	mvn -q exec:java; ret=$$?; $(MAKE) db-down; exit $$ret
