#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include "print_test.h"
#include "dataframe.h"
#include "series.h"
/**
 * Helper: create an integer Series with 'count' elements.
 */
static Series buildIntSeries(const char* name, const int* data, size_t count)
{
    Series s;
    seriesInit(&s, name, DF_INT);
    for (size_t i = 0; i < count; i++) {
        seriesAddInt(&s, data[i]);
    }
    return s;
}

/**
 * Helper: create a string Series with 'count' elements.
 */
static Series buildStringSeries(const char* name, const char* const* data, size_t count)
{
    Series s;
    seriesInit(&s, name, DF_STRING);
    for (size_t i = 0; i < count; i++) {
        seriesAddString(&s, data[i]);
    }
    return s;
}

/**
 * Test printing a small DataFrame with ~5-10 rows, multiple columns.
 * We'll check for no crash and do a quick row/col assert for sanity.
 */
static void testPrintBasic(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // First column (int)
    int colA[] = { 10, 20, 30, 40, 50 };
    Series sA = buildIntSeries("IntCol", colA, 5);
    bool ok = df.addSeries(&df, &sA);
    assert(ok == true);
    seriesFree(&sA);

    // Second column (strings)
    const char* colB[] = { "Alice", "Bob", "Charlie", "Diana", "Ethan" };
    Series sB = buildStringSeries("NameCol", colB, 5);
    ok = df.addSeries(&df, &sB);
    assert(ok == true);
    seriesFree(&sB);

    // Check df shape
    assert(df.numColumns(&df) == 2);
    assert(df.numRows(&df) == 5);

    // Now call df.print
    df.print(&df); 
    // We won't parse output automatically, 
    // but at least we confirm no crash/segfault.

    DataFrame_Destroy(&df);
}

/**
 * Test printing an empty DataFrame (0 rows, 0 columns) 
 * and a DataFrame with columns but 0 rows.
 */
static void testPrintEdgeCases(void)
{
    // 1) Completely empty
    {
        DataFrame df;
        DataFrame_Create(&df);
        assert(df.numColumns(&df) == 0);
        assert(df.numRows(&df) == 0);
        df.print(&df);  // Should print "Empty DataFrame"
        DataFrame_Destroy(&df);
    }

    // 2) Columns but 0 rows
    {
        DataFrame df;
        DataFrame_Create(&df);

        // Create an int column with 0 entries
        Series s1;
        seriesInit(&s1, "EmptyInt", DF_INT);
        bool ok = df.addSeries(&df, &s1);
        assert(ok == true);
        seriesFree(&s1);

        // Create a string column with 0 entries
        Series s2;
        seriesInit(&s2, "EmptyString", DF_STRING);
        ok = df.addSeries(&df, &s2);
        assert(ok == true);
        seriesFree(&s2);

        assert(df.numColumns(&df) == 2);
        assert(df.numRows(&df) == 0);

        df.print(&df); // should show "Empty DataFrame"

        DataFrame_Destroy(&df);
    }
}

/**
 * Stress test: create 2 int columns with ~20k rows each. 
 * Then call df.print => it should display a "pandas-like" 
 * truncated output (5 rows at top, 5 at bottom).
 */
static void testPrintStress(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    const size_t N = 20000; // 20k rows
    // Column 1:
    Series s1;
    seriesInit(&s1, "LargeInt1", DF_INT);
    bool ok = df.addSeries(&df, &s1);
    assert(ok == true);
    seriesFree(&s1);

    // Column 2:
    Series s2;
    seriesInit(&s2, "LargeInt2", DF_INT);
    ok = df.addSeries(&df, &s2);
    assert(ok == true);
    seriesFree(&s2);

    // Now add N rows
    for (size_t i = 0; i < N; i++) {
        int val1 = (int)i;
        int val2 = (int)(i * 2);
        const void* rowData[] = { &val1, &val2 };
        bool rowOk = df.addRow(&df, rowData);
        assert(rowOk == true);
    }

    assert(df.numRows(&df) == N);
    assert(df.numColumns(&df) == 2);

    // Now call df.print => should print top 5, "..." row, bottom 5
    df.print(&df);

    DataFrame_Destroy(&df);
}

/**
 * @brief testPrint
 * Main test driver for dataframe_print (dfPrint).
 * Calls sub-tests.
 */
void testPrint(void)
{
    printf("Running DataFrame print tests...\n");

    testPrintBasic();
    printf(" - Basic test passed.\n");

    testPrintEdgeCases();
    printf(" - Edge cases (empty DF, 0-row DF) passed.\n");

    testPrintStress();
    printf(" - Stress test (20k rows) passed.\n");

    printf("All dataframe_print tests passed successfully!\n");
}
