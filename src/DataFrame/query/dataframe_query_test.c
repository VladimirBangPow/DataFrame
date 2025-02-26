#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include "dataframe_query_test.h"
#include "../dataframe.h"   // or wherever your DataFrame struct is declared
#include "../../Series/series.h"           // for creating Series

/**
 * Helper: build a small integer Series with the given name and values.
 */
static Series buildIntSeries(const char* name, const int* values, size_t count)
{
    Series s;
    seriesInit(&s, name, DF_INT);
    for (size_t i = 0; i < count; i++) {
        seriesAddInt(&s, values[i]);
    }
    return s;
}

/**
 * Helper: build a string Series for testing
 */
static Series buildStringSeries(const char* name, const char* const* strings, size_t count)
{
    Series s;
    seriesInit(&s, name, DF_STRING);
    for (size_t i = 0; i < count; i++) {
        seriesAddString(&s, strings[i]);
    }
    return s;
}

/**
 * Test scenario:
 *  - Create a DataFrame with 2 integer columns (10 rows) and 1 string column (10 rows).
 *  - Call dfHead, dfTail, dfDescribe, ensuring no crash & basic correctness.
 */
static void testHeadTailDescribeBasic(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // Column 1
    int col1[] = { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 };
    Series s1 = buildIntSeries("Numbers1", col1, 10);
    bool ok = df.addSeries(&df, &s1);
    assert(ok == true);
    seriesFree(&s1);

    // Column 2
    int col2[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    Series s2 = buildIntSeries("Numbers2", col2, 10);
    ok = df.addSeries(&df, &s2);
    assert(ok == true);
    seriesFree(&s2);

    // Column 3 (strings)
    const char* col3[] = { 
        "Alpha", "Beta", "Gamma", "Delta", "Epsilon", 
        "Zeta", "Eta", "Theta", "Iota", "Kappa" 
    };
    Series s3 = buildStringSeries("Words", col3, 10);
    ok = df.addSeries(&df, &s3);
    assert(ok == true);
    seriesFree(&s3);

    // Check row/column count
    assert(df.numColumns(&df) == 3);
    assert(df.numRows(&df) == 10);

    // dfHead
    // This should print the first n rows. We'll just ensure it doesn't crash.
    df.head(&df, 5);

    // dfTail
    df.tail(&df, 3);

    // dfDescribe
    df.describe(&df);

    // We can't easily assert the console output without redirecting stdout,
    // but we can at least ensure no segfault, no crash.

    DataFrame_Destroy(&df);
}

/**
 * Test scenario:
 *  - Create an empty DataFrame (0 columns), call dfHead, dfTail, dfDescribe
 *  - Create a DataFrame with columns but 0 rows, call them
 *  - Ensure no crash
 */
static void testHeadTailDescribeEdgeCases(void)
{
    // Case 1: truly empty DF
    {
        DataFrame df;
        DataFrame_Create(&df);
        assert(df.numColumns(&df) == 0);
        assert(df.numRows(&df) == 0);

        df.head(&df, 5);
        df.tail(&df, 5);
        df.describe(&df);

        DataFrame_Destroy(&df);
    }

    // Case 2: columns but 0 rows
    {
        DataFrame df;
        DataFrame_Create(&df);

        // We'll add columns, but each has 0 elements
        Series s1;
        seriesInit(&s1, "ColEmpty1", DF_INT);
        bool ok = df.addSeries(&df, &s1);
        assert(ok == true);
        seriesFree(&s1);

        Series s2;
        seriesInit(&s2, "ColEmpty2", DF_STRING);
        ok = df.addSeries(&df, &s2);
        assert(ok == true);
        seriesFree(&s2);

        assert(df.numColumns(&df) == 2);
        assert(df.numRows(&df) == 0);

        // Now call dfHead, dfTail, dfDescribe
        df.head(&df, 3);
        df.tail(&df, 3);
        df.describe(&df);

        DataFrame_Destroy(&df);
    }
}

/**
 * Possibly a stress test: For dfHead & dfTail, though, 
 * the cost is mostly in printing. We'll do a moderate test.
 */
static void testHeadTailStress(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // Let's add 2 numeric columns with many rows
    const size_t N = 100000; // 100k rows

    Series s1;
    seriesInit(&s1, "LargeCol1", DF_INT);
    bool ok = df.addSeries(&df, &s1);
    assert(ok == true);
    seriesFree(&s1);

    Series s2;
    seriesInit(&s2, "LargeCol2", DF_INT);
    ok = df.addSeries(&df, &s2);
    assert(ok == true);
    seriesFree(&s2);

    // Now add N rows
    for (size_t i = 0; i < N; i++) {
        int val1 = (int)i;
        int val2 = (int)(i*2);
        const void* rowData[] = { &val1, &val2 };
        bool rowOk = df.addRow(&df, rowData);
        assert(rowOk == true);
    }
    assert(df.numRows(&df) == N);

    // dfHead should print the first 5 (or so).
    // We won't read the output, just ensure no crash.
    df.head(&df, 5);

    // dfTail should print the last 5.
    df.tail(&df, 5);

    // describe might do min, max, sum => big. Should not crash.
    df.describe(&df);

    DataFrame_Destroy(&df);
}

/**
 * @brief testQuery
 * Main test driver for dataframe_query (dfHead, dfTail, dfDescribe).
 * Calls various sub-tests.
 */
void testQuery(void)
{
    printf("Running DataFrame query tests (head/tail/describe)...\n");

    testHeadTailDescribeBasic();
    printf(" - Basic tests passed.\n");

    testHeadTailDescribeEdgeCases();
    printf(" - Edge case tests (empty DF, 0-row DF) passed.\n");

    testHeadTailStress();
    printf(" - Stress test for head/tail/describe passed.\n");

    printf("All dataframe_query tests passed successfully!\n");
}
