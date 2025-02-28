#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../dataframe.h"

// Helper to convert an integer to string safely
static size_t intToStrLen(int value, char* buffer, size_t bufSize)
{
    if (!buffer || bufSize == 0) return 0;
    int len = snprintf(buffer, bufSize, "%d", value);
    return (len > 0) ? (size_t)len : 0;
}

// Helper to convert a double to string safely (3 decimals)
static size_t doubleToStrLen(double value, char* buffer, size_t bufSize)
{
    if (!buffer || bufSize == 0) return 0;
    int len = snprintf(buffer, bufSize, "%.3f", value);
    return (len > 0) ? (size_t)len : 0;
}

// Helper to convert a 64-bit timestamp to string
static size_t datetimeToStrLen(long long value, char* buffer, size_t bufSize)
{
    // Here we just print the raw long long. 
    // For a real date/time, you'd parse to struct tm and format with strftime.
    if (!buffer || bufSize == 0) return 0;
    int len = snprintf(buffer, bufSize, "%lld", value);
    return (len > 0) ? (size_t)len : 0;
}

// Compute the width needed to print an index up to nRows-1
static size_t computeIndexWidth(size_t nRows)
{
    if (nRows <= 1) return 1;
    size_t maxIndex = nRows - 1;
    size_t width = 0;
    while (maxIndex > 0) {
        maxIndex /= 10;
        width++;
    }
    return width;
}

// Print a single row. If ellipsis is set, print "..." for each column.
static void printRow(
    const DataFrame* df,
    size_t rowIndex,
    size_t idxWidth,
    const size_t* colWidths,
    int nCols,
    int ellipsis
) {
    char tempBuf[256];
    // Print row index (left side)
    printf("%*zu  ", (int)idxWidth, rowIndex);

    // Print each column value in rowIndex
    for (int c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) {
            printf("???  ");
            continue;
        }

        if (ellipsis) {
            // If we are just showing the "..." row
            printf("%-*s  ", (int)colWidths[c], "...");
            continue;
        }

        switch (s->type) {
            case DF_INT: {
                int val;
                if (seriesGetInt(s, rowIndex, &val)) {
                    snprintf(tempBuf, sizeof(tempBuf), "%d", val);
                    printf("%*s  ", (int)colWidths[c], tempBuf);
                } else {
                    printf("%*s  ", (int)colWidths[c], "?");
                }
            } break;

            case DF_DOUBLE: {
                double val;
                if (seriesGetDouble(s, rowIndex, &val)) {
                    snprintf(tempBuf, sizeof(tempBuf), "%.3f", val);
                    printf("%*s  ", (int)colWidths[c], tempBuf);
                } else {
                    printf("%*s  ", (int)colWidths[c], "?");
                }
            } break;

            case DF_STRING: {
                char* str = NULL;
                if (seriesGetString(s, rowIndex, &str)) {
                    // Print string left-aligned
                    printf("%-*s  ", (int)colWidths[c], str);
                    free(str);
                } else {
                    printf("%-*s  ", (int)colWidths[c], "?");
                }
            } break;

            /* NEW: Handle DF_DATETIME */
            case DF_DATETIME: {
                long long dtVal;
                if (seriesGetDateTime(s, rowIndex, &dtVal)) {
                    snprintf(tempBuf, sizeof(tempBuf), "%lld", dtVal);
                    // or a nicer date/time format if you prefer
                    printf("%*s  ", (int)colWidths[c], tempBuf);
                } else {
                    printf("%*s  ", (int)colWidths[c], "?");
                }
            } break;
        }
    }
    printf("\n");
}

/* -------------------------------------------------------------------------
 * Public function: dfPrint_impl
 * ------------------------------------------------------------------------- */
void dfPrint_impl(const DataFrame* df)
{
    if (!df) {
        printf("NULL DataFrame pointer.\n");
        return;
    }

    size_t nCols = df->numColumns(df);
    size_t nRows = df->numRows(df);

    if (nCols == 0 || nRows == 0) {
        printf("Empty DataFrame\n");
        printf("Columns: %zu\n", nCols);
        printf("Index: %zu entries\n", nRows);
        return;
    }

    // Allocate array for each column's width
    size_t* colWidths = (size_t*)calloc(nCols, sizeof(size_t));
    if (!colWidths) {
        fprintf(stderr, "Memory allocation failed for colWidths.\n");
        return;
    }

    // Figure out max width needed for each column
    char tempBuf[256];
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) {
            colWidths[c] = 3; // fallback
            continue;
        }
        // Start with at least the column name length
        size_t colNameLen = strlen(s->name);
        if (colNameLen > colWidths[c]) {
            colWidths[c] = colNameLen;
        }

        // For each row, see if the printed value is larger
        for (size_t r = 0; r < nRows; r++) {
            switch (s->type) {
                case DF_INT: {
                    int val;
                    if (seriesGetInt(s, r, &val)) {
                        size_t length = intToStrLen(val, tempBuf, sizeof(tempBuf));
                        if (length > colWidths[c]) {
                            colWidths[c] = length;
                        }
                    }
                } break;
                case DF_DOUBLE: {
                    double val;
                    if (seriesGetDouble(s, r, &val)) {
                        size_t length = doubleToStrLen(val, tempBuf, sizeof(tempBuf));
                        if (length > colWidths[c]) {
                            colWidths[c] = length;
                        }
                    }
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(s, r, &str)) {
                        size_t length = strlen(str);
                        if (length > colWidths[c]) {
                            colWidths[c] = length;
                        }
                        free(str);
                    }
                } break;

                /* NEW: DF_DATETIME => check length of the integer string */
                case DF_DATETIME: {
                    long long dtVal;
                    if (seriesGetDateTime(s, r, &dtVal)) {
                        // Or convert to human-readable to see its length
                        int len = snprintf(tempBuf, sizeof(tempBuf), "%lld", dtVal);
                        if (len > (int)colWidths[c]) {
                            colWidths[c] = len;
                        }
                    }
                } break;
            }
        }
    }

    // Compute width for the row index
    size_t idxWidth = computeIndexWidth(nRows);

    // Print header row
    printf("%*s  ", (int)idxWidth, "");
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (s) {
            // column name left-aligned
            printf("%-*s  ", (int)colWidths[c], s->name);
        } else {
            printf("%-*s  ", (int)colWidths[c], "???");
        }
    }
    printf("\n");

    // Decide how many rows to print (similar to pandas)
    if (nRows <= 10) {
        // Print all rows
        for (size_t r = 0; r < nRows; r++) {
            printRow(df, r, idxWidth, colWidths, (int)nCols, 0);
        }
    } else {
        // First 5
        for (size_t r = 0; r < 5; r++) {
            printRow(df, r, idxWidth, colWidths, (int)nCols, 0);
        }
        // Ellipsis row
        printf("%*s  ", (int)idxWidth, "");
        for (size_t c = 0; c < nCols; c++) {
            printf("%-*s  ", (int)colWidths[c], "...");
        }
        printf("\n");
        // Last 5
        for (size_t r = nRows - 5; r < nRows; r++) {
            printRow(df, r, idxWidth, colWidths, (int)nCols, 0);
        }
    }

    free(colWidths);
    printf("\n[%zu rows x %zu columns]\n", nRows, nCols);
}
