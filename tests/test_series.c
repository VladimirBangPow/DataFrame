#include "series.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <time.h>

#include "test_series.h"

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
 * Test basic Series operations:
 *   1) Initialize a Series
 *   2) Add some elements
 *   3) Retrieve and validate them
 *   4) Free the Series
 */
static void testSeriesFunctions(void) {
    {
        // Test an integer Series
        Series intSeries;
        seriesInit(&intSeries, "IntSeries", DF_INT);

        // Add ints
        for (int i = 0; i < 5; i++) {
            seriesAddInt(&intSeries, i * 10);
        }

        // Check size
        assert(seriesSize(&intSeries) == 5);

        // Validate data
        for (size_t i = 0; i < 5; i++) {
            int val;
            bool ok = seriesGetInt(&intSeries, i, &val);
            assert(ok);
            assert(val == (int)i * 10);
        }
        // seriesPrint(&intSeries);
        // Clean up
        seriesFree(&intSeries);
    }
    {
        // Test a double Series
        Series dblSeries;
        seriesInit(&dblSeries, "DblSeries", DF_DOUBLE);

        // Add doubles
        for (int i = 0; i < 5; i++) {
            seriesAddDouble(&dblSeries, i * 1.5);
        }

        // Check size
        assert(seriesSize(&dblSeries) == 5);

        // Validate data
        for (size_t i = 0; i < 5; i++) {
            double val;
            bool ok = seriesGetDouble(&dblSeries, i, &val);
            assert(ok);
            assert(val == (double)i * 1.5);
        }
        // seriesPrint(&dblSeries);

        // Clean up
        seriesFree(&dblSeries);
    }
    {
        // Test a string Series
        Series strSeries;
        seriesInit(&strSeries, "StrSeries", DF_STRING);

        // Add strings
        seriesAddString(&strSeries, "Alpha");
        seriesAddString(&strSeries, "Bravo");
        seriesAddString(&strSeries, "Charles");

        // Check size
        assert(seriesSize(&strSeries) == 3);

        // Validate data
        const char* expected[] = {"Alpha", "Bravo", "Charles"};
        for (size_t i = 0; i < 3; i++) {
            char* got = NULL;
            bool ok = seriesGetString(&strSeries, i, &got);
            assert(ok);
            assert(safeStrcmp(got, expected[i]) == 0);
            free(got);
        }
        // seriesPrint(&strSeries);

        // Clean up
        seriesFree(&strSeries);
    }
    printf("testSeriesFunctions() passed.\n");
}


void testSeries(void) {
    printf("Running Series tests...\n");
    testSeriesFunctions();
    printf("All Series tests passed successfully!\n");
}
