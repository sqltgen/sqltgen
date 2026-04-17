# CMake generated Testfile for 
# Source directory: /Users/joao/Desktop/sqltgen/tests/e2e/runtime/bookstore/cpp/postgresql
# Build directory: /Users/joao/Desktop/sqltgen/tests/e2e/runtime/bookstore/cpp/postgresql/build
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(runtime_test "/Users/joao/Desktop/sqltgen/tests/e2e/runtime/bookstore/cpp/postgresql/build/runtime_test")
set_tests_properties(runtime_test PROPERTIES  _BACKTRACE_TRIPLES "/Users/joao/Desktop/sqltgen/tests/e2e/runtime/bookstore/cpp/postgresql/CMakeLists.txt;32;add_test;/Users/joao/Desktop/sqltgen/tests/e2e/runtime/bookstore/cpp/postgresql/CMakeLists.txt;0;")
subdirs("_deps/googletest-build")
