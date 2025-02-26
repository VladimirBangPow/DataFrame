#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>

#include "test_dataframe.h"
#include "dataframe.h"

/*
 * Helper: Compare two strings safely. 
 * Returns 0 if equal, non-zero if different.
 */
static int safeStrcmp(const char* a, const char* b) {
    if (!a && !b) return 0;
    if (!a || !b) return 1; // one is NULL
    return strcmp(a, b);
}



/*
 * Test DataFrame operations on smaller scale:
 *   1) Create a DataFrame
 *   2) Add multiple columns (Series)
 *   3) Validate row/column counts
 *   4) Add rows individually, check correctness
 */
static void testDataFrameOperations(void) {
    DataFrame df;
    dfInit(&df);

    // Prepare 3 Series with the same number of rows
    Series sInt, sDbl, sStr;
    seriesInit(&sInt, "Integers", DF_INT);
    seriesInit(&sDbl, "Doubles", DF_DOUBLE);
    seriesInit(&sStr, "Strings", DF_STRING);

    // Fill them with 5 elements each
    for (int i = 0; i < 5; i++) {
        seriesAddInt(&sInt, i);
        seriesAddDouble(&sDbl, i * 2.5);
        
        char buffer[32];
        snprintf(buffer, sizeof(buffer), "Row_%d", i);
        seriesAddString(&sStr, buffer);
    }

    // Add Series to DataFrame
    bool ok;
    ok = dfAddSeries(&df, &sInt);  assert(ok);
    ok = dfAddSeries(&df, &sDbl);  assert(ok);
    ok = dfAddSeries(&df, &sStr);  assert(ok);

    // After adding columns, we can free local copies
    seriesFree(&sInt);
    seriesFree(&sDbl);
    seriesFree(&sStr);

    // Check shape
    assert(dfNumColumns(&df) == 3);
    assert(dfNumRows(&df) == 5);

    // Add one more row with dfAddRow
    int    newInt    = 100;
    double newDouble = 99.99;
    const char* newString = "ExtraRow";

    const void* rowData[3];
    rowData[0] = &newInt;
    rowData[1] = &newDouble;
    rowData[2] = newString;

    ok = dfAddRow(&df, rowData);
    assert(ok);

    // DataFrame should now have 6 rows
    assert(dfNumRows(&df) == 6);

    // Check the last row
    {
        const Series* s0 = dfGetSeries(&df, 0);
        const Series* s1 = dfGetSeries(&df, 1);
        const Series* s2 = dfGetSeries(&df, 2);
        assert(s0 && s1 && s2);

        int    gotInt = 0;
        double gotDbl = 0.0;
        char*  gotStr = NULL;

        bool successInt = seriesGetInt(s0, 5, &gotInt);
        bool successDbl = seriesGetDouble(s1, 5, &gotDbl);
        bool successStr = seriesGetString(s2, 5, &gotStr);

        assert(successInt);
        assert(successDbl);
        assert(successStr);

        assert(gotInt == 100);
        assert(gotDbl == 99.99);
        assert(safeStrcmp(gotStr, "ExtraRow") == 0);

        free(gotStr);
    }

    // Clean up
    dfFree(&df);
    printf("testDataFrameOperations() passed.\n");
}

/*
 * Test dfHead, dfTail, dfDescribe with basic coverage.
 * Just ensure they run without crashing and produce the expected row counts.
 * We won't do a detailed parse of stdout, but we can do rough checks.
 */
static void testHeadTailDescribe(void) {
    DataFrame df;
    dfInit(&df);

    // Create a single numeric column
    Series sInt;
    seriesInit(&sInt, "TestInt", DF_INT);
    for (int i = 0; i < 10; i++) {
        seriesAddInt(&sInt, i);
    }
    bool ok = dfAddSeries(&df, &sInt);
    assert(ok);

    seriesFree(&sInt);

    // Basic checks
    assert(dfNumColumns(&df) == 1);
    assert(dfNumRows(&df) == 10);

    // Call dfHead, dfTail, dfDescribe
    // (We won't parse their output, but we can confirm that they run.)
    dfHead(&df, 5);      // Should show rows 0..4
    dfTail(&df, 3);      // Should show rows 7..9
    dfDescribe(&df);     // Should show min=0, max=9, mean=4.5 for a single int column

    dfFree(&df);

    printf("testHeadTailDescribe() passed.\n");
}

/*
 * Stress test: create a large DataFrame with multiple columns,
 * add thousands of rows, and check some invariants.
 */
static void stressTestDataFrame(void) {
    // For demonstration, let's choose a fairly large number, but not too big.
    // You can adjust 'N' to a bigger number (like 1e5) if performance allows.
    const size_t N = 10000;

    // Initialize the DataFrame
    DataFrame df;
    dfInit(&df);

    // Prepare 3 Series: int, double, string
    Series sInt, sDbl, sStr;
    seriesInit(&sInt, "IntCol", DF_INT);
    seriesInit(&sDbl, "DblCol", DF_DOUBLE);
    seriesInit(&sStr, "StrCol", DF_STRING);

    // For the stress test, let's add them empty first (0 rows).
    // Then we will add rows one by one via dfAddRow.
    bool ok;
    ok = dfAddSeries(&df, &sInt); assert(ok);
    ok = dfAddSeries(&df, &sDbl); assert(ok);
    ok = dfAddSeries(&df, &sStr); assert(ok);

    // Free the local Series copies
    seriesFree(&sInt);
    seriesFree(&sDbl);
    seriesFree(&sStr);

    // Now add N rows
    srand((unsigned int)time(NULL));
    for (size_t i = 0; i < N; i++) {
        int    rndInt    = rand() % 1000000;  // some random int
        double rndDouble = (double)(rand() % 10000) / 100.0; 
        char   strBuf[32];

        snprintf(strBuf, sizeof(strBuf), "Row_%zu", i);

        const void* rowData[3];
        rowData[0] = &rndInt;
        rowData[1] = &rndDouble;
        rowData[2] = strBuf;

        ok = dfAddRow(&df, rowData);
        assert(ok);
    }

    // Check row/column counts
    assert(dfNumColumns(&df) == 3);
    assert(dfNumRows(&df) == N);

    // Optionally, check a few random rows
    for (int test_i = 0; test_i < 5; test_i++) {
        size_t randomRow = rand() % N;
        // Check that we can retrieve the data (not verifying correctness
        // of random values, but at least ensuring there's no crash).
        const Series* s0 = dfGetSeries(&df, 0);
        const Series* s1 = dfGetSeries(&df, 1);
        const Series* s2 = dfGetSeries(&df, 2);

        assert(s0 && s1 && s2);

        // Int
        int valInt = -1;
        bool success = seriesGetInt(s0, randomRow, &valInt);
        assert(success);

        // Double
        double valDbl = 0.0;
        success = seriesGetDouble(s1, randomRow, &valDbl);
        assert(success);

        // String
        char* valStr = NULL;
        success = seriesGetString(s2, randomRow, &valStr);
        assert(success);
        free(valStr);
    }
    dfPrint(&df);
    // Cleanup
    dfFree(&df);

    printf("stressTestDataFrame() with %zu rows passed.\n", N);
}

static void testDfPlot(void){
    DataFrame df;
    dfInit(&df);

    // Suppose we have 3 numeric columns
    Series sIndex, sColA, sColB;
    seriesInit(&sIndex, "IndexCol", DF_INT);
    seriesInit(&sColA,  "A", DF_DOUBLE);
    seriesInit(&sColB,  "B", DF_DOUBLE);

    // Add data to each
    for (int i = 0; i < 10; i++) {
        seriesAddInt(&sIndex, i);
        seriesAddDouble(&sColA, (double)(i*i));
        seriesAddDouble(&sColB, (double)i * 1.5);
    }

    // Add them to the DataFrame
    dfAddSeries(&df, &sIndex);
    dfAddSeries(&df, &sColA);
    dfAddSeries(&df, &sColB);

    // Freed local series (the DataFrame has copies)
    seriesFree(&sIndex);
    seriesFree(&sColA);
    seriesFree(&sColB);

    // Now let's plot columns 1 and 2 as Y vs. column 0 as X
    // That is: xColIndex=0 => "IndexCol", yColIndices = {1,2} => columns "A" and "B"
    size_t yCols[2] = {1, 2};

    // We'll do a line plot, saving to "myplot.png"
    dfPlot(&df, 0, yCols, 2, "line", "./DataFrame/myplot.png");

    // Or do a scatter plot (no file, just pop up a window)
    // dfPlot(&df, 0, yCols, 2, "scatter", NULL);

    dfFree(&df);

}

static void testReadCsv(void) {
    // 1) Create a small CSV file with known contents
    const char* testFilename = "./DataFrame/test_dummy.csv";
    FILE* fp = fopen(testFilename, "w");
    assert(fp);

    // CSV with 3 columns: ID, Value, Label
    fprintf(fp, "ID,Value,Label\n");
    fprintf(fp, "1,3.14,Foo\n");
    fprintf(fp, "2,100,Bar\n");
    fprintf(fp, "X,Hello,Baz\n"); // row with non-numeric in ID => fallback to string
    fclose(fp);

    // 2) Read into a DataFrame
    DataFrame df;
    dfInit(&df);  // or we can pass an uninitialized df
    bool ok = readCsv(&df, testFilename);
    assert(ok);

    // 3) Check shape
    assert(dfNumColumns(&df) == 3);
    assert(dfNumRows(&df) == 3); // "X,Hello,Baz" is the 3rd data row

    // 4) Validate each column’s type
    //    - ID: first row had int, second row had int, third row had "X" => fallback to string
    //    So final type is DF_STRING for ID
    const Series* sID = dfGetSeries(&df, 0);
    assert(sID);
    assert(sID->type == DF_STRING);

    //    - Value: first row was 3.14 => double, second row was 100 => still parseable as double, third row was "Hello" => fallback to string
    //    So final type is DF_STRING for Value as well
    const Series* sValue = dfGetSeries(&df, 1);
    assert(sValue);
    assert(sValue->type == DF_STRING);

    //    - Label: always strings => DF_STRING
    const Series* sLabel = dfGetSeries(&df, 2);
    assert(sLabel);
    assert(sLabel->type == DF_STRING);

    // 5) Validate data
    // Row 0: ID="1", Value="3.14", Label="Foo"
    {
        char* tmp = NULL;
        bool ok = seriesGetString(sID, 0, &tmp); 
        assert(ok && strcmp(tmp,"1")==0);
        free(tmp);

        ok = seriesGetString(sValue, 0, &tmp);
        assert(ok && strcmp(tmp,"3.14")==0);
        free(tmp);

        ok = seriesGetString(sLabel, 0, &tmp);
        assert(ok && strcmp(tmp,"Foo")==0);
        free(tmp);
    }

    // Row 1: ID="2", Value="100", Label="Bar"
    {
        char* tmp = NULL;
        bool ok = seriesGetString(sID, 1, &tmp); 
        assert(ok && strcmp(tmp,"2")==0);
        free(tmp);

        ok = seriesGetString(sValue, 1, &tmp);
        assert(ok && strcmp(tmp,"100")==0);
        free(tmp);

        ok = seriesGetString(sLabel, 1, &tmp);
        assert(ok && strcmp(tmp,"Bar")==0);
        free(tmp);
    }

    // Row 2: ID="X", Value="Hello", Label="Baz"
    {
        char* tmp = NULL;
        bool ok = seriesGetString(sID, 2, &tmp); 
        assert(ok && strcmp(tmp,"X")==0);
        free(tmp);

        ok = seriesGetString(sValue, 2, &tmp);
        assert(ok && strcmp(tmp,"Hello")==0);
        free(tmp);

        ok = seriesGetString(sLabel, 2, &tmp);
        assert(ok && strcmp(tmp,"Baz")==0);
        free(tmp);
    }

    dfFree(&df);

    printf("testReadCsv() passed.\n");
}

void testHLOC(void) {
    DataFrame df;
    dfInit(&df);

    // 1) Read the CSV
    const char* filename = "./DataFrame/btcusd.csv";
    bool ok = readCsv(&df, filename);
    if (!ok) {
        printf("testHLOC: Could not read %s\n", filename);
        return;
    }

    // 2) Check how many columns we got
    size_t nCols = dfNumColumns(&df);
    printf("testHLOC: Loaded %zu columns, %zu rows from %s\n", nCols, dfNumRows(&df), filename);


    if (nCols < 5) {
        printf("testHLOC: Not enough columns to do O,H,L,C.\n");
        dfFree(&df);
        return;
    }

    // yColIndices {open, high, low, close} in the CSV order:
    size_t yCols[4] = {1, 3, 4, 2}; 
    // (Open=1, High=3, Low=4, Close=2)

    size_t dateColIndex = 0;

    bool success = dfConvertDatesToEpoch(&df,
                                         dateColIndex,
                                         "YYYYMMDD",  // or whichever format
                                         true);       // toMillis = true
    if (!success) {
        fprintf(stderr, "Conversion failed.\n");
    }

    // 3) Plot
    // We'll save to a PNG file called "btcusd_candle.png"
    dfPlot(&df, 
           0 /* xColIndex => time */, 
           yCols, 
           4 /* yCount */, 
           "hloc", 
           "./DataFrame/btcusd_candle.png");

    // 4) Clean up
    dfFree(&df);

    printf("testHLOC: Completed. Check btcusd_candle.png for the candlestick chart.\n");
}

/*
 * testDataFrame: driver function that calls all sub-tests.
 * This is the function you would call from a main() or from another test harness.
 */
void testDataFrame(void) {
    printf("Running DataFrame tests...\n");
    testDataFrameOperations();
    testHeadTailDescribe();
    stressTestDataFrame();
    testDfPlot();
    testReadCsv();
    testHLOC();
    printf("All DataFrame tests passed successfully!\n");
}
