include ../../../common.mk

.PHONY: generate build test clean

generate:
	rm -rf gen
	$(SQLTGEN) generate --config sqltgen.json

build: generate
	cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug
	cmake --build build

test: build
	cd build && ctest --output-on-failure

clean:
	rm -rf gen build
