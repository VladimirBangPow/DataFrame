#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dataframe_io_test.h"
#include "../dataframe.h"    // for DataFrame struct, df->readCsv, etc.
#include "../../Series/series.h"           // for creating Series

/**
 * Helper: create a temporary file with given content, return the filename.
 * We store content into a file under a known name or a random name. 
 * On *nix you might use mkstemp, but here we do something simpler for example.
 */
static const char* createTempCsvFile(const char* filename, const char* content)
{
    FILE* fp = fopen(filename, "w");
    if (!fp) {
        fprintf(stderr, "Error creating temp file '%s'\n", filename);
        return NULL;
    }
    fputs(content, fp);
    fclose(fp);
    return filename;
}

/**
 * This test checks reading a small CSV with a header and a few rows of numeric and string data.
 */
static void testReadingSmallCsv(void)
{
    // CSV content with 3 columns: "ID", "Value", "Name"
    // Mixed data: integer, double, string
    const char* csvContent =
        "ID,Value,Name\n"
        "1,10.5,Alice\n"
        "2,20.75,Bob\n"
        "3,30.0,Charlie\n";

    // Create a temp file
    const char* tmpFile = "test_small.csv";
    createTempCsvFile(tmpFile, csvContent);

    // Now read into DataFrame
    DataFrame df;
    DataFrame_Create(&df);

    bool ok = df.readCsv(&df, tmpFile);
    assert(ok == true);

    // Verify shape
    assert(df.numColumns(&df) == 3);
    assert(df.numRows(&df) == 3);

    // Check each column name
    const Series* col0 = df.getSeries(&df, 0);
    const Series* col1 = df.getSeries(&df, 1);
    const Series* col2 = df.getSeries(&df, 2);

    assert(col0 && strcmp(col0->name, "ID") == 0);
    assert(col1 && strcmp(col1->name, "Value") == 0);
    assert(col2 && strcmp(col2->name, "Name") == 0);

    // Check types: ID => DF_INT (inferred), Value => DF_DOUBLE, Name => DF_STRING
    assert(col0->type == DF_INT);
    assert(col1->type == DF_DOUBLE);
    assert(col2->type == DF_STRING);

    // Spot check data
    int iVal;
    bool gotIt = seriesGetInt(col0, 0, &iVal);
    assert(gotIt && iVal == 1);
    gotIt = seriesGetInt(col0, 2, &iVal);
    assert(gotIt && iVal == 3);

    double dVal;
    gotIt = seriesGetDouble(col1, 1, &dVal);
    assert(gotIt && (dVal == 20.75));

    char* sVal = NULL;
    gotIt = seriesGetString(col2, 2, &sVal);
    assert(gotIt && strcmp(sVal, "Charlie") == 0);
    free(sVal);

    DataFrame_Destroy(&df);

    // Cleanup temp file
    remove(tmpFile);
}

/**
 * Test reading a CSV that is empty (no rows except header).
 */
static void testReadingEmptyCsv(void)
{
    const char* csvContent =
        "A,B,C\n"; // Only header, no data rows

    const char* tmpFile = "test_empty.csv";
    createTempCsvFile(tmpFile, csvContent);

    DataFrame df;
    DataFrame_Create(&df);

    bool ok = df.readCsv(&df, tmpFile);
    assert(ok == true);

    // We expect 3 columns, 0 rows
    assert(df.numColumns(&df) == 3);
    assert(df.numRows(&df) == 0);

    // The columns should be DF_STRING type by default (since no data to infer).
    for (size_t c = 0; c < df.numColumns(&df); c++) {
        const Series* s = df.getSeries(&df, c);
        assert(s);
        assert(s->type == DF_STRING);
    }

    DataFrame_Destroy(&df);
    remove(tmpFile);
}

/**
 * Test reading a CSV that has only one column with purely int data.
 */
static void testReadingSingleColumnCsv(void)
{
    const char* csvContent =
        "Numbers\n"
        "10\n"
        "20\n"
        "30\n"
        "40\n";

    const char* tmpFile = "test_single_col.csv";
    createTempCsvFile(tmpFile, csvContent);

    DataFrame df;
    DataFrame_Create(&df);

    bool ok = df.readCsv(&df, tmpFile);
    assert(ok == true);

    // Expect 1 column, 4 rows
    assert(df.numColumns(&df) == 1);
    assert(df.numRows(&df) == 4);

    const Series* col = df.getSeries(&df, 0);
    assert(col && strcmp(col->name, "Numbers") == 0);
    assert(col->type == DF_INT);

    // check data
    int val = 0;
    seriesGetInt(col, 0, &val);
    assert(val == 10);
    seriesGetInt(col, 3, &val);
    assert(val == 40);

    DataFrame_Destroy(&df);
    remove(tmpFile);
}

/**
 * Stress test reading a large CSV (~100k rows).
 * We'll create the file on the fly with 2 columns: an integer and a double.
 */
static void testReadingLargeCsv(void)
{
    const char* tmpFile = "test_large.csv";
    FILE* fp = fopen(tmpFile, "w");
    assert(fp != NULL);

    // Write header
    fputs("Index,Value\n", fp);

    // We'll do 100k rows
    const size_t N = 100000;
    for (size_t i = 0; i < N; i++) {
        // Let's do i, i + 0.5 as data
        // e.g. "0,0.5\n", "1,1.5\n", etc.
        fprintf(fp, "%zu,%.2f\n", i, (double)i + 0.5);
    }
    fclose(fp);

    DataFrame df;
    DataFrame_Create(&df);

    bool ok = df.readCsv(&df, tmpFile);
    assert(ok == true);

    // Expect 2 columns, 100k rows
    assert(df.numColumns(&df) == 2);
    assert(df.numRows(&df) == N);

    const Series* c0 = df.getSeries(&df, 0);
    const Series* c1 = df.getSeries(&df, 1);
    assert(c0 && c1);

    // c0 => DF_INT, c1 => DF_DOUBLE
    assert(c0->type == DF_INT);
    assert(c1->type == DF_DOUBLE);

    // Spot check first row
    int iVal = 0;
    seriesGetInt(c0, 0, &iVal);
    assert(iVal == 0);

    double dVal;
    seriesGetDouble(c1, 0, &dVal);
    assert(dVal == 0.5);

    // Spot check last row
    seriesGetInt(c0, N - 1, &iVal);
    assert((size_t)iVal == N - 1);

    seriesGetDouble(c1, N - 1, &dVal);
    // Should be (N-1 + 0.5)
    // floating arithmetic: let's just check within a tiny epsilon
    double expected = (double)(N - 1) + 0.5;
    assert((dVal - expected) > -1e-9 && (dVal - expected) < 1e-9);

    DataFrame_Destroy(&df);
    remove(tmpFile);
}

/**
 * @brief testIO
 * Main test driver for dataframe_io (df.readCsv).
 * Calls all sub-tests.
 */
void testIO(void)
{
    printf("Running DataFrame IO tests...\n");

    testReadingSmallCsv();
    printf(" - Small CSV test passed.\n");

    testReadingEmptyCsv();
    printf(" - Empty CSV test passed.\n");

    testReadingSingleColumnCsv();
    printf(" - Single-column CSV test passed.\n");

    testReadingLargeCsv();
    printf(" - Large CSV stress test passed.\n");

    printf("All dataframe_io tests passed successfully!\n");
}
