#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>   // for random seeds in stress test
#include "dataframe_core_test.h"
#include "../dataframe.h"        // or wherever your DataFrame headers are
#include "../../Series/series.h"           // for creating Series

/**
 * Helper: build a small integer Series with the given name and some values.
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
 * Helper: test adding a few series, check row/col counts, etc.
 */
static void testBasicAddSeriesAndRows(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // Initially, no columns, no rows:
    assert(df.numColumns(&df) == 0);
    assert(df.numRows(&df) == 0);

    // Build a small Series of 3 integers
    int v1[] = { 10, 20, 30 };
    Series s1 = buildIntSeries("ColumnA", v1, 3);

    bool ok = df.addSeries(&df, &s1);
    assert(ok == true);
    assert(df.numColumns(&df) == 1);
    assert(df.numRows(&df) == 3);  // matches series length

    // seriesFree(&s1);  // we can free local copy, df has its own

    // Build a second column of 3 integers
    int v2[] = { 100, 200, 300 };
    Series s2 = buildIntSeries("ColumnB", v2, 3);

    ok = df.addSeries(&df, &s2);
    assert(ok == true);
    assert(df.numColumns(&df) == 2);
    assert(df.numRows(&df) == 3);

    seriesFree(&s2);

    // Now add a row. We have 2 columns, each of type int.
    // rowData must be an array of pointers, each pointing to the correct type.
    int newValA = 40;
    int newValB = 400;
    const void* rowData[] = { &newValA, &newValB };

    ok = df.addRow(&df, rowData);
    assert(ok == true);
    assert(df.numRows(&df) == 4);

    // Let's verify the data in column 1, row 3 (zero-based)
    const Series* colA = df.getSeries(&df, 0);
    assert(colA != NULL);
    int checkVal = 0;
    bool gotIt = seriesGetInt(colA, 3, &checkVal);
    assert(gotIt == true);
    assert(checkVal == 40);

    // Similarly, verify column 2, row 3
    const Series* colB = df.getSeries(&df, 1);
    assert(colB != NULL);
    gotIt = seriesGetInt(colB, 3, &checkVal);
    assert(gotIt == true);
    assert(checkVal == 400);

    DataFrame_Destroy(&df);
}

/**
 * Helper: stress test with many rows/columns.
 * We'll create 3 columns of integers, each with N rows, 
 * fill them randomly, then do some checks.
 */
static void testStress(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // Let's define N moderately large to test performance.
    // Increase if you want a bigger stress test.
    const size_t N = 100000; // 100k rows

    // We'll add 3 columns, each empty at first
    Series s1, s2, s3;
    seriesInit(&s1, "Col1", DF_INT);
    seriesInit(&s2, "Col2", DF_INT);
    seriesInit(&s3, "Col3", DF_INT);

    // Add them to df. Now df->nrows is 0.
    bool ok = df.addSeries(&df, &s1); assert(ok == true);
    ok = df.addSeries(&df, &s2);     assert(ok == true);
    ok = df.addSeries(&df, &s3);     assert(ok == true);
    // seriesFree(&s1); // local copy
    // seriesFree(&s2);
    // seriesFree(&s3);
    assert(df.numColumns(&df) == 3);
    assert(df.numRows(&df) == 0);

    // Now let's add N rows with random int values
    srand((unsigned)time(NULL));
    for (size_t i = 0; i < N; i++) {
        int vA = rand() % 100000;
        int vB = rand() % 100000;
        int vC = rand() % 100000;
        const void* rowData[] = { &vA, &vB, &vC };
        ok = df.addRow(&df, rowData);
        assert(ok == true);
    }
    assert(df.numRows(&df) == N);

    // Spot check a few random rows
    const Series* col1 = df.getSeries(&df, 0);
    const Series* col2 = df.getSeries(&df, 1);
    const Series* col3 = df.getSeries(&df, 2);
    assert(col1 != NULL && col2 != NULL && col3 != NULL);

    for (int trial = 0; trial < 10; trial++) {
        size_t idx = rand() % N;
        int val1, val2, val3;
        bool got1 = seriesGetInt(col1, idx, &val1);
        bool got2 = seriesGetInt(col2, idx, &val2);
        bool got3 = seriesGetInt(col3, idx, &val3);
        assert(got1 && got2 && got3);
        // We can't assert exact values since they're random, 
        // but we at least confirm they exist.
    }

    DataFrame_Destroy(&df);
}

/**
 * @brief testCore
 * Main test driver for dataframe_core. 
 * Calls various sub-tests, including a stress test.
 */
void testCore(void)
{
    printf("Running DataFrame core tests...\n");

    testBasicAddSeriesAndRows();
    printf(" - Basic addSeries/addRow tests passed.\n");

    testStress();
    printf(" - Stress test passed.\n");

    printf("All dataframe_core tests passed successfully!\n");
}
