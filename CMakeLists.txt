cmake_minimum_required(VERSION 3.10)
project(DataFrame C)

set(CMAKE_C_STANDARD 11)

# 1. Build DataFrame library as STATIC
add_library(DataFrame STATIC
    src/aggregate.c
    src/combine.c
    src/core.c
    src/date.c
    src/indexing.c
    src/io.c
    src/plot.c
    src/print.c
    src/query.c
    src/reshape.c
    src/series.c
    src/dftime.c
)

# 2. Compiler flags
target_compile_options(DataFrame PRIVATE
    -Wall
    -Wextra
    -pedantic
)

# 3. Macros for date/time usage on macOS
target_compile_definitions(DataFrame PRIVATE
    _XOPEN_SOURCE=700
    _DARWIN_C_SOURCE
)

# 4. Bring in MyDataStructures from a subdirectory
add_subdirectory(
    "/Users/hassanamad/Projects/DataStructures"
    "${CMAKE_CURRENT_BINARY_DIR}/DataStructures_build"
)

# 5. Link DataFrame with the MyDataStructures library
target_link_libraries(DataFrame PUBLIC MyDataStructures)

# 6. Include directories for DataFrame
target_include_directories(DataFrame PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# 7. Install DataFrame
install(TARGETS DataFrame
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
)
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/
        DESTINATION include)

# 8. Print which compiler is in use
message(STATUS "C Compiler: ${CMAKE_C_COMPILER}")

# 9. Add tests subdirectory (assuming it has a CMakeLists that defines 'test_runner')
add_subdirectory(tests)

# 10. Also link test_runner with MyDataStructures if needed
# (This requires that your 'tests/CMakeLists.txt' actually defines a target named 'test_runner'.)
