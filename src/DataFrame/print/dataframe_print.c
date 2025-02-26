#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dataframe.h"

static size_t intToStrLen(int value, char* buffer, size_t bufSize)
{
    if (!buffer || bufSize == 0) return 0;
    int len = snprintf(buffer, bufSize, "%d", value);
    return (len > 0) ? (size_t)len : 0;
}

static size_t doubleToStrLen(double value, char* buffer, size_t bufSize)
{
    if (!buffer || bufSize == 0) return 0;
    int len = snprintf(buffer, bufSize, "%.3f", value);
    return (len > 0) ? (size_t)len : 0;
}

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

static void printRow(
    const DataFrame* df,
    size_t rowIndex,
    size_t idxWidth,
    const size_t* colWidths,
    int nCols,
    int ellipsis
) {
    char tempBuf[256];
    printf("%*zu  ", (int)idxWidth, rowIndex);

    for (int c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) {
            printf("???  ");
            continue;
        }

        if (ellipsis) {
            printf("%-*s  ", (int)colWidths[c], "...");
            continue;
        }

        switch (s->type) {
            case DF_INT: {
                int val;
                if (seriesGetInt(s, rowIndex, &val)) {
                    int len = snprintf(tempBuf, sizeof(tempBuf), "%d", val);
                    if (len < 0) len = 0;
                    printf("%*s  ", (int)colWidths[c], tempBuf);
                } else {
                    printf("%*s  ", (int)colWidths[c], "?");
                }
            } break;
            case DF_DOUBLE: {
                double val;
                if (seriesGetDouble(s, rowIndex, &val)) {
                    int len = snprintf(tempBuf, sizeof(tempBuf), "%.3f", val);
                    if (len < 0) len = 0;
                    printf("%*s  ", (int)colWidths[c], tempBuf);
                } else {
                    printf("%*s  ", (int)colWidths[c], "?");
                }
            } break;
            case DF_STRING: {
                char* str = NULL;
                if (seriesGetString(s, rowIndex, &str)) {
                    printf("%-*s  ", (int)colWidths[c], str);
                    free(str);
                } else {
                    printf("%-*s  ", (int)colWidths[c], "?");
                }
            } break;
        }
    }
    printf("\n");
}

/* remove `static` so it can be assigned in dataframe_core.c */
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

    size_t* colWidths = (size_t*)calloc(nCols, sizeof(size_t));
    if (!colWidths) {
        fprintf(stderr, "Memory allocation failed for colWidths.\n");
        return;
    }

    char tempBuf[256];
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) {
            colWidths[c] = 3;
            continue;
        }
        size_t colNameLen = strlen(s->name);
        if (colNameLen > colWidths[c]) {
            colWidths[c] = colNameLen;
        }
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
            }
        }
    }

    size_t idxWidth = computeIndexWidth(nRows);

    /* Print header */
    printf("%*s  ", (int)idxWidth, "");
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (s) {
            printf("%-*s  ", (int)colWidths[c], s->name);
        } else {
            printf("%-*s  ", (int)colWidths[c], "???");
        }
    }
    printf("\n");

    /* Decide how many rows to print */
    if (nRows <= 10) {
        for (size_t r = 0; r < nRows; r++) {
            printRow(df, r, idxWidth, colWidths, (int)nCols, 0);
        }
    } else {
        for (size_t r = 0; r < 5; r++) {
            printRow(df, r, idxWidth, colWidths, (int)nCols, 0);
        }
        // ellipsis row
        printf("%*s  ", (int)idxWidth, "");
        for (size_t c = 0; c < nCols; c++) {
            printf("%-*s  ", (int)colWidths[c], "...");
        }
        printf("\n");
        for (size_t r = nRows - 5; r < nRows; r++) {
            printRow(df, r, idxWidth, colWidths, (int)nCols, 0);
        }
    }

    free(colWidths);
    printf("\n[%zu rows x %zu columns]\n", nRows, nCols);
}
