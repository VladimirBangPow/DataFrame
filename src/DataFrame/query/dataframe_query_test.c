#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include "dataframe_query_test.h"
#include "../dataframe.h"
#include "../../Series/series.h"

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
 *  - Call dfHead (returns new DF), dfTail (returns new DF), and dfDescribe (returns new DF).
 *  - Ensure shapes and subset row data are correct (for head/tail).
 */
static void testHeadTailDescribeBasic(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // Column 1 (int)
    int col1[] = { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100 };
    Series s1 = buildIntSeries("Numbers1", col1, 10);
    bool ok = df.addSeries(&df, &s1);
    assert(ok == true);
    seriesFree(&s1);

    // Column 2 (int)
    int col2[] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    Series s2 = buildIntSeries("Numbers2", col2, 10);
    ok = df.addSeries(&df, &s2);
    assert(ok == true);
    seriesFree(&s2);

    // Column 3 (string)
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

    // 1) HEAD => first 5 rows
    DataFrame headDF = df.head(&df, 5);
    // headDF should have 3 columns, 5 rows
    assert(headDF.numColumns(&headDF) == 3);
    assert(headDF.numRows(&headDF) == 5);

    // Spot check data in "Numbers1" (first col) for row 0 and row 4
    {
        const Series* s = headDF.getSeries(&headDF, 0);
        assert(s != NULL);
        int val = 0;
        bool got = seriesGetInt(s, 0, &val);
        assert(got && val == 10);
        got = seriesGetInt(s, 4, &val);
        assert(got && val == 50);
    }

    // Clean up
    DataFrame_Destroy(&headDF);

    // 2) TAIL => last 3 rows
    DataFrame tailDF = df.tail(&df, 3);
    // tailDF should have 3 columns, 3 rows
    assert(tailDF.numColumns(&tailDF) == 3);
    assert(tailDF.numRows(&tailDF) == 3);

    // Spot check data in "Numbers2" (second col) for row 0 of tail => that corresponds
    // to df's row 7 if we had 10 rows and we wanted the last 3. So original row 7 => 8
    // but let's see: the tail DF is a brand new 0-based DF. 
    // If the original DF's last 3 rows are (row 7,8,9) => 8,9,10 in col2
    // Then in tailDF, row 0 => (original) row 7 => value=8
    //                 row 1 => (original) row 8 => value=9
    //                 row 2 => (original) row 9 => value=10

    {
        const Series* s = tailDF.getSeries(&tailDF, 1); // second col
        assert(s != NULL);
        int val = 0;
        bool got = seriesGetInt(s, 0, &val);
        assert(got && val == 8); // original row 7
        got = seriesGetInt(s, 2, &val);
        assert(got && val == 10); // original row 9
    }

    DataFrame_Destroy(&tailDF);

    // 3) DESCRIBE => new DF with aggregated stats, or however you implemented it
    DataFrame descDF = df.describe(&df);
    // Let's assume your describe returns 1 row per original column (3 rows),
    // with columns named "colName", "count", "min", "max", "mean".
    // Then descDF.numRows(&descDF) == 3
    // and descDF.numColumns(&descDF) == 5
    // This can vary based on your chosen describe design. We'll do a minimal check.

    assert(descDF.numRows(&descDF) == 3);   // 3 original columns => 3 rows
    assert(descDF.numColumns(&descDF) == 5); // "colName","count","min","max","mean"

    DataFrame_Destroy(&descDF);

    // Finally, destroy the main DF
    DataFrame_Destroy(&df);
}

/**
 * Test scenario:
 *  - Create an empty DataFrame (0 columns), call dfHead, dfTail, dfDescribe
 *  - Create a DataFrame with columns but 0 rows, call them
 *  - Ensure new DFs are also empty or appropriately shaped, and no crash
 */
static void testHeadTailDescribeEdgeCases(void)
{
    // Case 1: truly empty DF (0 columns, 0 rows)
    {
        DataFrame df;
        DataFrame_Create(&df);
        assert(df.numColumns(&df) == 0);
        assert(df.numRows(&df) == 0);

        // head
        DataFrame headDF = df.head(&df, 5);
        assert(headDF.numColumns(&headDF) == 0);
        assert(headDF.numRows(&headDF) == 0);
        DataFrame_Destroy(&headDF);

        // tail
        DataFrame tailDF = df.tail(&df, 5);
        assert(tailDF.numColumns(&tailDF) == 0);
        assert(tailDF.numRows(&tailDF) == 0);
        DataFrame_Destroy(&tailDF);

        // describe
        DataFrame descDF = df.describe(&df);
        // Possibly 0 rows, 0 columns or a minimal shape, depending on your design.
        // We'll assume 0x0 for an empty DF
        assert(descDF.numColumns(&descDF) == 5);
        assert(descDF.numRows(&descDF) == 0);
        DataFrame_Destroy(&descDF);

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

        // head
        DataFrame headDF = df.head(&df, 3);
        assert(headDF.numColumns(&headDF) == 2);
        assert(headDF.numRows(&headDF) == 0);
        DataFrame_Destroy(&headDF);

        // tail
        DataFrame tailDF = df.tail(&df, 3);
        assert(tailDF.numColumns(&tailDF) == 2);
        assert(tailDF.numRows(&tailDF) == 0);
        DataFrame_Destroy(&tailDF);

        // describe
        DataFrame descDF = df.describe(&df);
        // Possibly 2 rows? or 2 columns? Depends on your design. 
        // If you create 1 row per column, but there's no data, you might do count=0, min=0, etc. 
        // We'll guess it returns 2 rows (one for each column).
        // We'll do a simpler check: 
        assert(descDF.numRows(&descDF) == 2);
        // Suppose it still has 5 columns: colName,count,min,max,mean
        assert(descDF.numColumns(&descDF) == 5);

        DataFrame_Destroy(&descDF);

        DataFrame_Destroy(&df);
    }
}

/**
 * Possibly a stress test: For dfHead & dfTail returning subsets, 
 * we just check that we can handle many rows without crashing.
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
        int val2 = (int)(i * 2);
        const void* rowData[] = { &val1, &val2 };
        bool rowOk = df.addRow(&df, rowData);
        assert(rowOk == true);
    }
    assert(df.numRows(&df) == N);

    // HEAD => 5 row subset
    DataFrame headDF = df.head(&df, 5);
    assert(headDF.numColumns(&headDF) == 2);
    assert(headDF.numRows(&headDF) == 5);
    DataFrame_Destroy(&headDF);

    // TAIL => 5 row subset
    DataFrame tailDF = df.tail(&df, 5);
    assert(tailDF.numColumns(&tailDF) == 2);
    assert(tailDF.numRows(&tailDF) == 5);
    DataFrame_Destroy(&tailDF);

    // describe => large stats, just ensure no crash
    DataFrame descDF = df.describe(&df);
    // We'll do minimal shape checks
    assert(descDF.numRows(&descDF) == 2);   // presumably 2 original columns => 2 rows
    assert(descDF.numColumns(&descDF) == 5);
    DataFrame_Destroy(&descDF);

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


