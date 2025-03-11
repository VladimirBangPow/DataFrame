# tests/CMakeLists.txt

include(CTest)

set(TEST_FLAGS "-Wall -Wextra -pedantic")

add_executable(df_test_runner
    df_test_runner.c
    aggregate_test.c
    combine_test.c
    core_test.c
    date_test.c
    indexing_test.c
    io_test.c
    plot_test.c
    print_test.c
    query_test.c
    reshape_test.c
    test_series.c
    # add more test files here
)

target_compile_options(df_test_runner PRIVATE ${TEST_FLAGS})

target_include_directories(df_test_runner
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(df_test_runner PRIVATE DataFrame)

add_test(NAME AllTests COMMAND df_test_runner)
