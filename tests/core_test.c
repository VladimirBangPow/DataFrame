#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include <time.h>
#include <math.h>
#include <assert.h>
#include "core_test.h"
#include "dataframe.h"
#include "series.h"

// Existing testCore prototypes:
extern void testCore(void);


/**
 * Builds a small DF_DOUBLE Series for testing.
 */
static Series buildDoubleSeries(const char* name, const double* values, size_t count)
{
    Series s;
    seriesInit(&s, name, DF_DOUBLE);
    for (size_t i = 0; i < count; i++) {
        seriesAddDouble(&s, values[i]);
    }
    return s;
}

/**
 * Builds a small DF_STRING Series for testing.
 */
static Series buildStringSeries(const char* name, const char* const* values, size_t count)
{
    Series s;
    seriesInit(&s, name, DF_STRING);
    for (size_t i = 0; i < count; i++) {
        seriesAddString(&s, values[i]);
    }
    return s;
}

/**
 * Builds a DF_DATETIME Series with given epoch values (seconds or millis).
 */
static Series buildDatetimeSeries(const char* name, const long long* epochValues, size_t count)
{
    Series s;
    seriesInit(&s, name, DF_DATETIME);
    for (size_t i = 0; i < count; i++) {
        seriesAddDateTime(&s, epochValues[i]);
    }
    return s;
}

/**
 * Compare two double values with some small epsilon
 */
static bool nearlyEqual(double a, double b, double eps)
{
    return (fabs(a - b) < eps);
}

static Series buildIntSeries(const char* name, const int* values, size_t count)
{
    Series s;
    seriesInit(&s, name, DF_INT);
    for (size_t i = 0; i < count; i++) {
        seriesAddInt(&s, values[i]);
    }
    return s;
}


static void testDifferentColumnTypes(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // 1) Build some sample data
    int intVals[] = { 10, 20, 30, 40 };
    double doubleVals[] = { 1.5, 2.5, 3.25, 4.75 };
    const char* stringVals[] = { "apple", "banana", "cherry", "date" };
    long long datetimeVals[] = { 1677612345LL, 1677612400LL, 1677612500LL, 1677613000LL }; 
    // E.g., some arbitrary epoch seconds

    Series sInt = buildIntSeries("IntCol", intVals, 4);
    Series sDouble = buildDoubleSeries("DoubleCol", doubleVals, 4);
    Series sString = buildStringSeries("StrCol", stringVals, 4);
    Series sDatetime = buildDatetimeSeries("TimeCol", datetimeVals, 4);

    bool ok = df.addSeries(&df, &sInt);
    assert(ok);
    ok = df.addSeries(&df, &sDouble);
    assert(ok);
    ok = df.addSeries(&df, &sString);
    assert(ok);
    ok = df.addSeries(&df, &sDatetime);
    assert(ok);

    // We can free the local series copies now
    seriesFree(&sInt);
    seriesFree(&sDouble);
    seriesFree(&sString);
    seriesFree(&sDatetime);

    // 2) Check row/col counts
    assert(df.numColumns(&df) == 4);
    assert(df.numRows(&df) == 4);

    // 3) Retrieve some values
    const Series* colInt = df.getSeries(&df, 0);
    const Series* colDouble = df.getSeries(&df, 1);
    const Series* colString = df.getSeries(&df, 2);
    const Series* colTime = df.getSeries(&df, 3);

    assert(colInt && colDouble && colString && colTime);

    // Check row 2 (zero-based => 3rd row):
    int ival;
    bool gotI = seriesGetInt(colInt, 2, &ival);
    assert(gotI && ival == 30);

    double dval;
    bool gotD = seriesGetDouble(colDouble, 2, &dval);
    assert(gotD && nearlyEqual(dval, 3.25, 1e-9));

    char* strVal = NULL;
    bool gotS = seriesGetString(colString, 2, &strVal);
    assert(gotS && strVal && strcmp(strVal, "cherry") == 0);
    free(strVal);

    long long dtVal;
    bool gotDT = seriesGetDateTime(colTime, 2, &dtVal);
    assert(gotDT && dtVal == 1677612500LL);

    // 4) Test aggregations on numeric columns
    double sumInt = df.sum(&df, 0);   // sum of IntCol => 10+20+30+40=100
    double sumDbl = df.sum(&df, 1);   // sum of DoubleCol => 1.5+2.5+3.25+4.75=12.0
    double meanInt = df.mean(&df, 0); // => 100 / 4 = 25
    double meanDbl = df.mean(&df, 1); // => 12.0 / 4 = 3.0

    assert(nearlyEqual(sumInt, 100.0, 1e-9));
    assert(nearlyEqual(sumDbl, 12.0, 1e-9));
    assert(nearlyEqual(meanInt, 25.0, 1e-9));
    assert(nearlyEqual(meanDbl, 3.0, 1e-9));

    // 5) Test slicing: head(2), tail(2)
    DataFrame head2 = df.head(&df, 2);
    assert(head2.numRows(&head2) == 2);
    assert(head2.numColumns(&head2) == 4);
    // check row 1 col 0 => 20
    const Series* h0 = head2.getSeries(&head2, 0);
    seriesGetInt(h0, 1, &ival);
    assert(ival == 20);
    DataFrame_Destroy(&head2);

    DataFrame tail2 = df.tail(&df, 2);
    assert(tail2.numRows(&tail2) == 2);
    const Series* t0 = tail2.getSeries(&tail2, 0);
    seriesGetInt(t0, 0, &ival);
    assert(ival == 30);
    seriesGetInt(t0, 1, &ival);
    assert(ival == 40);
    DataFrame_Destroy(&tail2);

    DataFrame_Destroy(&df);

    printf(" - testDifferentColumnTypes passed.\n");
}

static void testAddRow(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // We'll define 2 columns: "EventTime" (DF_DATETIME), "Description" (DF_STRING)
    // Initially empty
    Series timeSeries;
    seriesInit(&timeSeries, "EventTime", DF_DATETIME);
    bool ok = df.addSeries(&df, &timeSeries);
    seriesFree(&timeSeries);
    assert(ok);

    Series descSeries;
    seriesInit(&descSeries, "Description", DF_STRING);
    ok = df.addSeries(&df, &descSeries);
    seriesFree(&descSeries);
    assert(ok);

    // Now let's add 3 rows
    // rowData => array of 2 pointers: [ptr to long long, ptr to const char*]
    long long e1 = 1678000000LL;
    const char* d1 = "Start process";
    const void* row1[] = { &e1, d1 };
    ok = df.addRow(&df, row1);
    assert(ok);

    long long e2 = 1678000100LL;
    const char* d2 = "Middle process";
    const void* row2[] = { &e2, d2 };
    ok = df.addRow(&df, row2);
    assert(ok);

    long long e3 = 1678000200LL;
    const char* d3 = "End process";
    const void* row3[] = { &e3, d3 };
    ok = df.addRow(&df, row3);
    assert(ok);

    // Check final counts
    assert(df.numColumns(&df) == 2);
    assert(df.numRows(&df) == 3);

    // Verify the data
    const Series* tsCol = df.getSeries(&df, 0);
    const Series* descCol = df.getSeries(&df, 1);
    assert(tsCol && descCol);


    DataFrame_Destroy(&df);

    printf(" - testAddRow passed.\n");
}

static void testGetRow(void)
{
    // 1) Create a small DataFrame
    DataFrame df;
    DataFrame_Create(&df);

    // Let's have 3 columns: DF_INT, DF_STRING, DF_DATETIME
    int intValues[] = { 100, 200, 300, 400 };
    const char* strValues[] = { "alpha", "beta", "gamma", "delta" };
    long long dtValues[] = { 1678000000LL, 1678000100LL, 1678000200LL, 1678000300LL };

    Series colInt, colStr, colDT;
    seriesInit(&colInt, "IntCol", DF_INT);
    for (size_t i = 0; i < 4; i++) {
        seriesAddInt(&colInt, intValues[i]);
    }
    seriesInit(&colStr, "StrCol", DF_STRING);
    for (size_t i = 0; i < 4; i++) {
        seriesAddString(&colStr, strValues[i]);
    }
    seriesInit(&colDT, "TimeCol", DF_DATETIME);
    for (size_t i = 0; i < 4; i++) {
        seriesAddDateTime(&colDT, dtValues[i]);
    }

    // 2) Add columns to the DataFrame
    bool ok = df.addSeries(&df, &colInt);  assert(ok);
    ok = df.addSeries(&df, &colStr);      assert(ok);
    ok = df.addSeries(&df, &colDT);       assert(ok);

    // (We can free local Series if we like, DataFrame holds its own copy)
    seriesFree(&colInt);
    seriesFree(&colStr);
    seriesFree(&colDT);

    // Check row/col count
    assert(df.numColumns(&df) == 3);
    assert(df.numRows(&df) == 4);

    // 3) Test dfGetRow_impl on row 2 (zero-based => 3rd item)
    void** rowData = NULL;
    ok = df.getRow(&df, 2, &rowData);
    assert(ok);
    // rowData is now an array of 3 pointers, one per column

    // col0 => DF_INT => rowData[0] is (int*)
    int* pInt = (int*)rowData[0];
    assert(pInt && *pInt == 300);

    // col1 => DF_STRING => rowData[1] is (char*)
    char* pStr = (char*)rowData[1];
    assert(pStr && strcmp(pStr, "gamma") == 0);

    // col2 => DF_DATETIME => rowData[2] is (long long*)
    long long* pDT = (long long*)rowData[2];
    assert(pDT && *pDT == 1678000200LL);

    // Now free each pointer
    for (size_t c = 0; c < 3; c++) {
        free(rowData[c]);
    }
    free(rowData);

    // 4) Check out-of-range row
    // e.g. rowIndex=99 => should fail
    void** badRowData = NULL;
    ok = df.getRow(&df, 99, &badRowData);
    assert(!ok); // should fail

    // 5) Destroy the DataFrame
    DataFrame_Destroy(&df);

    printf(" - testGetRow passed.\n");
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

    seriesFree(&s1);  // we can free local copy, df has its own

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
    seriesFree(&s1); // local copy
    seriesFree(&s2);
    seriesFree(&s3);
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
    testDifferentColumnTypes();
    testAddRow();
    testGetRow();
    testBasicAddSeriesAndRows();
    testStress();
    printf("All DataFrame core tests passed successfully!\n");
}
