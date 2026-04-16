cmake_minimum_required(VERSION 3.14)
project(bookstore_cpp_sqlite_runtime CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz
  URL_HASH SHA256=8ad598c73ad796e0d8280b082cebd82a630d73e73cd3c70057938a6501bba5d7
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(runtime_test
  runtime_test.cpp
  gen/queries/queries.cpp
)

target_include_directories(runtime_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(runtime_test PRIVATE GTest::gtest_main sqlite3)
target_compile_definitions(runtime_test PRIVATE
  "SCHEMA_PATH=\"${CMAKE_CURRENT_SOURCE_DIR}/../../../../fixtures/bookstore/sqlite/schema.sql\""
)

enable_testing()
add_test(NAME runtime_test COMMAND runtime_test)
