#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <time.h>
#include "dataframe.h"
#include <ctype.h>
#include "../DynamicArray/dynamic_array.h"
#define MAX_LINE_LEN  4096
#define MAX_COLS      1024



/* -------------------------------------------------------------------------
 * DataFrame Functions
 * ------------------------------------------------------------------------- */

void dfInit(DataFrame* df) {
    if (!df) return;
    daInit(&df->columns, 4); // Start with capacity 4 columns
    df->nrows = 0;
}

void dfFree(DataFrame* df) {
    if (!df) return;
    // Free all Series inside the DataFrame
    for (size_t i = 0; i < daSize(&df->columns); i++) {
        Series* s = (Series*)daGetMutable(&df->columns, i);
        seriesFree(s);
    }
    // Free the array of columns
    daFree(&df->columns);
    df->nrows = 0;
}

bool dfAddSeries(DataFrame* df, const Series* s) {
    if (!df || !s) return false;

    // If no existing columns, set nrows = seriesSize(s) and just add.
    if (daSize(&df->columns) == 0) {
        df->nrows = seriesSize(s);
        // We need to copy the Series struct as well as re-copy internal data (since it's a separate Series).
        // Easiest approach: create a local copy by first initializing a new Series, then pushing all elements.
        Series newSeries;
        seriesInit(&newSeries, s->name, s->type);
        // Copy over all elements from s:
        for (size_t i = 0; i < seriesSize(s); i++) {
            switch (s->type) {
                case DF_INT: {
                    int val;
                    seriesGetInt(s, i, &val);
                    seriesAddInt(&newSeries, val);
                } break;
                case DF_DOUBLE: {
                    double val;
                    seriesGetDouble(s, i, &val);
                    seriesAddDouble(&newSeries, val);
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(s, i, &str)) {
                        seriesAddString(&newSeries, str);
                        free(str);
                    }
                } break;
            }
        }
        daPushBack(&df->columns, &newSeries, sizeof(Series));
        return true;
    } else {
        // We have existing columns. Check that the new series has the same size.
        if (seriesSize(s) != df->nrows) {
            fprintf(stderr, 
                "Error: new Series '%s' has %zu rows; existing DataFrame has %zu rows.\n", 
                s->name, seriesSize(s), df->nrows);
            return false;
        }
        // Copy series into a new local instance
        Series newSeries;
        seriesInit(&newSeries, s->name, s->type);
        for (size_t i = 0; i < seriesSize(s); i++) {
            switch (s->type) {
                case DF_INT: {
                    int val;
                    seriesGetInt(s, i, &val);
                    seriesAddInt(&newSeries, val);
                } break;
                case DF_DOUBLE: {
                    double val;
                    seriesGetDouble(s, i, &val);
                    seriesAddDouble(&newSeries, val);
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(s, i, &str)) {
                        seriesAddString(&newSeries, str);
                        free(str);
                    }
                } break;
            }
        }
        daPushBack(&df->columns, &newSeries, sizeof(Series));
        return true;
    }
}

size_t dfNumColumns(const DataFrame* df) {
    if (!df) return 0;
    return daSize(&df->columns);
}

size_t dfNumRows(const DataFrame* df) {
    if (!df) return 0;
    return df->nrows;
}

const Series* dfGetSeries(const DataFrame* df, size_t colIndex) {
    if (!df) return NULL;
    if (colIndex >= daSize(&df->columns)) return NULL;
    return (const Series*)daGet(&df->columns, colIndex);
}

bool dfAddRow(DataFrame* df, const void** rowData) {
    if (!df || !rowData) return false;

    size_t nCols = dfNumColumns(df);
    if (nCols == 0) {
        // No columns, can't really add a row. Or you could interpret that we do nothing.
        fprintf(stderr, "Error: DataFrame has no columns; can't add row.\n");
        return false;
    }
    // For each column, add the data item of the correct type.
    for (size_t c = 0; c < nCols; c++) {
        Series* s = (Series*)daGetMutable(&df->columns, c);
        if (!s) return false;
        switch (s->type) {
            case DF_INT: {
                const int* valPtr = (const int*)rowData[c];
                if (!valPtr) return false;
                seriesAddInt(s, *valPtr);
            } break;
            case DF_DOUBLE: {
                const double* valPtr = (const double*)rowData[c];
                if (!valPtr) return false;
                seriesAddDouble(s, *valPtr);
            } break;
            case DF_STRING: {
                const char* strPtr = (const char*)rowData[c];
                if (!strPtr) return false;
                seriesAddString(s, strPtr);
            } break;
        }
    }
    df->nrows += 1;
    return true;
}


/* -------------------------------------------------------------------------
 * DataFrame Query Functions
 * ------------------------------------------------------------------------- */
void dfHead(const DataFrame* df, size_t n) {
    if (!df) return;
    printf("==== dfHead(%zu) ====\n", n);
    size_t numRows = dfNumRows(df);
    size_t limit = (n < numRows) ? n : numRows;

    printf("(Showing first %zu of %zu rows)\n", limit, numRows);
    for (size_t r = 0; r < limit; r++) {
        printf("Row %zu: ", r);
        size_t nCols = dfNumColumns(df);
        for (size_t c = 0; c < nCols; c++) {
            const Series* s = dfGetSeries(df, c);
            if (!s) continue;
            switch (s->type) {
                case DF_INT: {
                    int val;
                    seriesGetInt(s, r, &val);
                    printf("%s=%d ", s->name, val);
                } break;
                case DF_DOUBLE: {
                    double val;
                    seriesGetDouble(s, r, &val);
                    printf("%s=%.3f ", s->name, val);
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(s, r, &str)) {
                        printf("%s=\"%s\" ", s->name, str);
                        free(str);
                    }
                } break;
            }
        }
        printf("\n");
    }
}

void dfTail(const DataFrame* df, size_t n) {
    if (!df) return;
    printf("==== dfTail(%zu) ====\n", n);
    size_t numRows = dfNumRows(df);
    if (n > numRows) n = numRows;
    size_t start = (numRows > n) ? (numRows - n) : 0;

    printf("(Showing last %zu of %zu rows)\n", n, numRows);
    for (size_t r = start; r < numRows; r++) {
        printf("Row %zu: ", r);
        size_t nCols = dfNumColumns(df);
        for (size_t c = 0; c < nCols; c++) {
            const Series* s = dfGetSeries(df, c);
            if (!s) continue;
            switch (s->type) {
                case DF_INT: {
                    int val;
                    seriesGetInt(s, r, &val);
                    printf("%s=%d ", s->name, val);
                } break;
                case DF_DOUBLE: {
                    double val;
                    seriesGetDouble(s, r, &val);
                    printf("%s=%.3f ", s->name, val);
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(s, r, &str)) {
                        printf("%s=\"%s\" ", s->name, str);
                        free(str);
                    }
                } break;
            }
        }
        printf("\n");
    }
}

void dfDescribe(const DataFrame* df) {
    if (!df) return;
    printf("==== dfDescribe() ====\n");

    size_t nCols = dfNumColumns(df);
    size_t nRows = dfNumRows(df);

    // For each numeric column, compute min, max, sum, etc.
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = dfGetSeries(df, c);
        if (!s) continue;

        if (s->type == DF_INT) {
            // Gather stats for integer column
            if (nRows == 0) {
                printf("Column '%s': no data.\n", s->name);
                continue;
            }
            int minVal, maxVal, tmp;
            double sumVal = 0.0;
            seriesGetInt(s, 0, &minVal);
            maxVal = minVal;
            sumVal = minVal;

            for (size_t r = 1; r < nRows; r++) {
                seriesGetInt(s, r, &tmp);
                if (tmp < minVal) minVal = tmp;
                if (tmp > maxVal) maxVal = tmp;
                sumVal += tmp;
            }
            double meanVal = sumVal / nRows;
            printf("Column '%s' (int): count=%zu, min=%d, max=%d, mean=%.3f\n",
                   s->name, nRows, minVal, maxVal, meanVal);
        }
        else if (s->type == DF_DOUBLE) {
            // Gather stats for double column
            if (nRows == 0) {
                printf("Column '%s': no data.\n", s->name);
                continue;
            }
            double minVal, maxVal, tmp;
            double sumVal = 0.0;
            seriesGetDouble(s, 0, &minVal);
            maxVal = minVal;
            sumVal = minVal;

            for (size_t r = 1; r < nRows; r++) {
                seriesGetDouble(s, r, &tmp);
                if (tmp < minVal) minVal = tmp;
                if (tmp > maxVal) maxVal = tmp;
                sumVal += tmp;
            }
            double meanVal = sumVal / nRows;
            printf("Column '%s' (double): count=%zu, min=%f, max=%f, mean=%.3f\n",
                   s->name, nRows, minVal, maxVal, meanVal);
        }
        else if (s->type == DF_STRING) {
            // For string columns, we'll just print count.
            printf("Column '%s' (string): count=%zu\n", s->name, nRows);
        }
    }
}

/* -------------------------------------------------------------------------
 * DataFrame Printing Functions
 * ------------------------------------------------------------------------- */
/*
 * Helper: convert an integer to a string, return the length.
 */
static size_t intToStrLen(int value, char* buffer, size_t bufSize) {
    if (!buffer || bufSize == 0) return 0;
    int len = snprintf(buffer, bufSize, "%d", value);
    return (len > 0) ? (size_t)len : 0;
}

/*
 * Helper: convert a double to string (e.g. "%.3f"), return the length.
 */
static size_t doubleToStrLen(double value, char* buffer, size_t bufSize) {
    if (!buffer || bufSize == 0) return 0;
    int len = snprintf(buffer, bufSize, "%.3f", value);
    return (len > 0) ? (size_t)len : 0;
}

/*
 * Compute the digits needed to display the largest row index (nRows-1).
 */
static size_t computeIndexWidth(size_t nRows) {
    if (nRows <= 1) return 1; // at least 1 char
    size_t maxIndex = nRows - 1;
    size_t width = 0;
    while (maxIndex > 0) {
        maxIndex /= 10;
        width++;
    }
    return width;
}

/*
 * Helper to print a single row (r) with precomputed column widths.
 * If 'ellipsis' is true, we print "..." instead of actual row data
 * (used for the middle row in large DataFrames).
 */
static void printRow(
    const DataFrame* df, 
    size_t rowIndex, 
    size_t idxWidth, 
    const size_t* colWidths, 
    int nCols, 
    int ellipsis
) {
    char tempBuf[256];

    // Print row index (right-aligned).
    printf("%*zu  ", (int)idxWidth, rowIndex);

    for (int c = 0; c < nCols; c++) {
        const Series* s = dfGetSeries(df, c);
        if (!s) {
            // Should not happen, but in case
            printf("???  ");
            continue;
        }

        if (ellipsis) {
            // Just print "..." for each column cell
            // Typically pandas leaves it blank or 
            // prints "..." in all columns. We'll do the latter.
            // Right-align or left-align doesn't matter too much here.
            // We'll just center it. For simplicity, let's do left-align.
            // But if the column is wide, let's do it left anyway.
            printf("%-*s  ", (int)colWidths[c], "...");
            continue;
        }

        // Actually print the cell value
        switch (s->type) {
            case DF_INT: {
                int val;
                if (seriesGetInt(s, rowIndex, &val)) {
                    int len = snprintf(tempBuf, sizeof(tempBuf), "%d", val);
                    if (len < 0) len = 0;
                    // Right-align numeric
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
                    // Right-align numeric
                    printf("%*s  ", (int)colWidths[c], tempBuf);
                } else {
                    printf("%*s  ", (int)colWidths[c], "?");
                }
            } break;

            case DF_STRING: {
                char* str = NULL;
                if (seriesGetString(s, rowIndex, &str)) {
                    // Left-align strings
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

/*
 * Print the DataFrame in a "pandas-like" style, *only* showing
 * the first 5 and last 5 rows if the dataset is large (over 10 rows).
 * Otherwise, print all rows. Align columns to emulate pandas look.
 */
void dfPrint(const DataFrame* df) {
    if (!df) {
        printf("NULL DataFrame pointer.\n");
        return;
    }

    size_t nCols = dfNumColumns(df);
    size_t nRows = dfNumRows(df);

    // If no columns or rows, just report an empty DataFrame.
    if (nCols == 0 || nRows == 0) {
        printf("Empty DataFrame\n");
        printf("Columns: %zu\n", nCols);
        printf("Index: %zu entries\n", nRows);
        return;
    }

    // 1) Precompute column widths.
    size_t* colWidths = (size_t*)calloc(nCols, sizeof(size_t));
    if (!colWidths) {
        fprintf(stderr, "Memory allocation failed for colWidths.\n");
        return;
    }

    char tempBuf[256];
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = dfGetSeries(df, c);
        if (!s) {
            colWidths[c] = 3; // "???" fallback
            continue;
        }
        // Start with the column name length
        size_t colNameLen = strlen(s->name);
        colWidths[c] = (colNameLen > colWidths[c]) ? colNameLen : colWidths[c];

        // Check data to find maximum width
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

    // 2) Compute index width
    size_t idxWidth = computeIndexWidth(nRows);

    // 3) Print the column header line.
    //    pandas typically prints the index column header as blank.
    //    We'll follow that (just right-align the blank for idxWidth).
    printf("%*s  ", (int)idxWidth, "");
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = dfGetSeries(df, c);
        if (s) {
            // Column names are typically left-aligned in pandas
            printf("%-*s  ", (int)colWidths[c], s->name);
        } else {
            printf("%-*s  ", (int)colWidths[c], "???");
        }
    }
    printf("\n");

    // 4) Decide how many rows to print:
    //    If nRows <= 10, print them all.
    //    If nRows > 10, print first 5, a "..." row, and last 5.
    if (nRows <= 10) {
        // Print all rows
        for (size_t r = 0; r < nRows; r++) {
            printRow(df, r, idxWidth, colWidths, (int)nCols, 0);
        }
    } else {
        // Print first 5
        for (size_t r = 0; r < 5; r++) {
            printRow(df, r, idxWidth, colWidths, (int)nCols, 0);
        }
        // Print ellipsis row (no real data, just placeholders)
        // Typically pandas prints empty index or some dotted line. 
        // We'll do the dotted line for the index, and "..." in each column.
        printf("%*s  ", (int)idxWidth, "");
        for (size_t c = 0; c < nCols; c++) {
            // Center or left doesn't matter much for an ellipsis marker.
            // We'll left-align. 
            printf("%-*s  ", (int)colWidths[c], "...");
        }
        printf("\n");
        // Print last 5
        for (size_t r = nRows - 5; r < nRows; r++) {
            printRow(df, r, idxWidth, colWidths, (int)nCols, 0);
        }
    }

    // 5) Clean up
    free(colWidths);
    printf("\n[%zu rows x %zu columns]\n", nRows, nCols);
}


/* -------------------------------------------------------------------------
 * DataFrame CSV Reading
 * ------------------------------------------------------------------------- */
/*
 * Helper to parse a line by comma. Modifies the line in place.
 * Returns the number of tokens found, storing pointers in 'tokens'.
 */
static size_t splitCsvLine(char* line, char** tokens, size_t maxTokens) {
    size_t count = 0;
    char* pch = strtok(line, ",");
    while (pch != NULL && count < maxTokens) {
        tokens[count++] = pch;
        pch = strtok(NULL, ",");
    }
    return count;
}

/*
 * Determine if a string can be parsed entirely as int or double.
 * Return 0 = int, 1 = double, -1 = neither.
 */
static int checkNumericType(const char* str) {
    if (!str || !*str) return -1;
    while (isspace((unsigned char)*str)) str++; // skip leading
    char* endptr;
    // Try int
    strtol(str, &endptr, 10);
    if (*endptr == '\0') {
        return 0;
    }
    // Try double
    strtod(str, &endptr);
    if (*endptr == '\0') {
        return 1;
    }
    return -1;
}

/*
 * We store all CSV data in memory as strings first.
 */
typedef struct {
    size_t nCols;
    size_t nRows;
    char** headers;   // array of string pointers for column names
    char*** cells;    // 2D array [r][c], each is a string
} CsvBuffer;

/*
 * Free function for CsvBuffer
 */
static void freeCsvBuffer(CsvBuffer* cb) {
    if (!cb) return;
    if (cb->headers) {
        for (size_t c = 0; c < cb->nCols; c++) {
            free(cb->headers[c]);
        }
        free(cb->headers);
    }
    if (cb->cells) {
        for (size_t r = 0; r < cb->nRows; r++) {
            for (size_t c = 0; c < cb->nCols; c++) {
                free(cb->cells[r][c]); // free each cell
            }
            free(cb->cells[r]);
        }
        free(cb->cells);
    }
    memset(cb, 0, sizeof(*cb));
}

/*
 * 1) Read the entire CSV into a CsvBuffer (two-pass: here is the first pass).
 *    - Read the header -> nCols
 *    - For each data line, if not blank, parse into tokens (filling short lines with "")
 *    - Store tokens in memory
 */
static bool loadCsvIntoBuffer(const char* filename, CsvBuffer* cb) {
    FILE* fp = fopen(filename, "r");
    if (!fp) {
        fprintf(stderr, "loadCsvIntoBuffer: cannot open file '%s'\n", filename);
        return false;
    }

    char lineBuf[MAX_LINE_LEN];
    // Read header
    if (!fgets(lineBuf, sizeof(lineBuf), fp)) {
        fprintf(stderr, "loadCsvIntoBuffer: file '%s' is empty.\n", filename);
        fclose(fp);
        return false;
    }
    // Strip newline
    lineBuf[strcspn(lineBuf, "\r\n")] = '\0';

    // Parse header
    char* headerTokens[MAX_COLS];
    size_t colCount = splitCsvLine(lineBuf, headerTokens, MAX_COLS);
    if (colCount == 0) {
        fprintf(stderr, "loadCsvIntoBuffer: file '%s' has an empty header.\n", filename);
        fclose(fp);
        return false;
    }

    cb->nCols = colCount;
    cb->headers = (char**)calloc(colCount, sizeof(char*));
    if (!cb->headers) {
        fclose(fp);
        fprintf(stderr, "loadCsvIntoBuffer: out of memory for headers.\n");
        return false;
    }
    for (size_t c = 0; c < colCount; c++) {
        cb->headers[c] = strdup(headerTokens[c]);
    }

    // We do not know nRows yet, so let's store lines in a temporary array
    // Then we'll allocate cb->cells once we know the final row count
    size_t capacityRows = 1000;  // arbitrary
    size_t rowCount = 0;
    char*** rowData = (char***)malloc(sizeof(char**) * capacityRows);
    if (!rowData) {
        fclose(fp);
        fprintf(stderr, "loadCsvIntoBuffer: out of memory for row pointers.\n");
        return false;
    }

    while (fgets(lineBuf, sizeof(lineBuf), fp)) {
        // Remove newline
        lineBuf[strcspn(lineBuf, "\r\n")] = '\0';
        // Check blank line
        char* checkp = lineBuf;
        while (*checkp && isspace((unsigned char)*checkp)) checkp++;
        if (*checkp == '\0') {
            // skip empty
            continue;
        }

        // Now parse
        char* tokens[MAX_COLS];
        size_t nTokens = splitCsvLine(lineBuf, tokens, MAX_COLS);
        // pad if short
        if (nTokens < colCount) {
            for (size_t cc = nTokens; cc < colCount; cc++) {
                tokens[cc] = "";
            }
            nTokens = colCount;
        }

        // store this row in memory
        if (rowCount >= capacityRows) {
            // expand
            capacityRows *= 2;
            rowData = (char***)realloc(rowData, sizeof(char**) * capacityRows);
            if (!rowData) {
                fclose(fp);
                fprintf(stderr, "loadCsvIntoBuffer: out of memory expanding rowData.\n");
                return false;
            }
        }
        rowData[rowCount] = (char**)malloc(sizeof(char*) * colCount);
        if (!rowData[rowCount]) {
            fclose(fp);
            fprintf(stderr, "loadCsvIntoBuffer: out of memory for row.\n");
            return false;
        }
        // copy tokens
        for (size_t cc = 0; cc < colCount; cc++) {
            rowData[rowCount][cc] = strdup(tokens[cc]);
        }
        rowCount++;
    }
    fclose(fp);

    // Now store them in cb
    cb->nRows = rowCount;
    cb->cells = (char***)malloc(sizeof(char**) * rowCount);
    if (!cb->cells) {
        fprintf(stderr, "loadCsvIntoBuffer: out of memory for cb->cells.\n");
        free(rowData);
        return false;
    }
    for (size_t r = 0; r < rowCount; r++) {
        cb->cells[r] = rowData[r];
    }
    free(rowData);

    return true;
}

/*
 * 2) For each column, decide if it's int, double, or string 
 *    by scanning *all* row values in that column.
 */
static ColumnType inferColumnType(CsvBuffer* cb, size_t colIndex) {
    // We'll do a quick check:
    // - if all parse as int => DF_INT
    // - else if all parse as double => DF_DOUBLE
    // - else => DF_STRING
    int colStage = 0; // 0 => still might be int, 1 => might be double, 2 => must be string
    for (size_t r = 0; r < cb->nRows; r++) {
        const char* val = cb->cells[r][colIndex];
        int t = checkNumericType(val);
        if (t < 0) {
            // not numeric => must be string
            colStage = 2;
            break;
        } else if (t == 1) {
            // double
            if (colStage == 0) {
                // we saw int so far, now we see double => entire column becomes double
                colStage = 1;
            }
            // if colStage is 1 or 2, stay there
        }
        // if t=0 => int, we remain colStage=0 if it was 0
        // if colStage was 1 => we must keep it at 1
    }
    // interpret colStage
    if (colStage == 0) {
        return DF_INT;
    } else if (colStage == 1) {
        return DF_DOUBLE;
    }
    return DF_STRING;
}

/*
 * 3) Build the final DataFrame: create Series columns of the chosen type,
 *    parse each cell accordingly, and add. 
 */
bool readCsv(DataFrame* df, const char* filename) {
    if (!df || !filename) {
        fprintf(stderr, "readCsv: invalid arguments.\n");
        return false;
    }

    CsvBuffer cb;
    memset(&cb, 0, sizeof(cb));

    // 1) Load entire CSV into memory
    if (!loadCsvIntoBuffer(filename, &cb)) {
        freeCsvBuffer(&cb);
        return false;
    }

    // Now we have cb.nCols, cb.nRows, cb.headers, and cb.cells[r][c] as strings
    // 2) If no rows, just create empty string columns
    if (cb.nRows == 0) {
        // Means we have columns from header but 0 data rows
        dfInit(df);
        for (size_t c = 0; c < cb.nCols; c++) {
            Series s;
            seriesInit(&s, cb.headers[c], DF_STRING);
            // no data, so 0 rows
            dfAddSeries(df, &s);
            seriesFree(&s);
        }
        freeCsvBuffer(&cb);
        return true;
    }

    // 3) Infer each column’s type
    ColumnType* finalTypes = (ColumnType*)malloc(sizeof(ColumnType) * cb.nCols);
    if (!finalTypes) {
        fprintf(stderr, "readCsv: out of memory for finalTypes.\n");
        freeCsvBuffer(&cb);
        return false;
    }

    for (size_t c = 0; c < cb.nCols; c++) {
        finalTypes[c] = inferColumnType(&cb, c);
    }

    // 4) Build the DataFrame
    dfInit(df);
    for (size_t c = 0; c < cb.nCols; c++) {
        Series s;
        seriesInit(&s, cb.headers[c], finalTypes[c]);
        // parse each row's string and add
        for (size_t r = 0; r < cb.nRows; r++) {
            const char* valStr = cb.cells[r][c];
            switch (finalTypes[c]) {
                case DF_INT: {
                    int parsed = (int)strtol(valStr, NULL, 10);
                    seriesAddInt(&s, parsed);
                } break;
                case DF_DOUBLE: {
                    double d = strtod(valStr, NULL);
                    seriesAddDouble(&s, d);
                } break;
                case DF_STRING: {
                    seriesAddString(&s, valStr);
                } break;
            }
        }
        // Now add it to the DataFrame
        bool ok = dfAddSeries(df, &s);
        if (!ok) {
            fprintf(stderr, "readCsv: dfAddSeries failed for column %s.\n", s.name);
        }
        seriesFree(&s);
    }

    free(finalTypes);
    freeCsvBuffer(&cb);
    return true;
}




/* -------------------------------------------------------------------------
 * DataFrame Plotting Functions
 * ------------------------------------------------------------------------- */

 // Forward declarations of internal helper to read numeric data
static bool getNumericValue(const Series* s, size_t index, double* outVal);

/**
 * A small utility: attempt to retrieve numeric data (int or double)
 * from a Series at a given row. Return as a double.
 */
/**
 * Helper: getNumericValue
 * Attempt to retrieve a numeric value (int or double) from a Series row, 
 * store it in *outVal as a double. Returns false if out of range or not numeric.
 */
static bool getNumericValue(const Series* s, size_t index, double* outVal) {
    if (!s || !outVal) return false;
    if (index >= seriesSize(s)) return false;

    if (s->type == DF_INT) {
        int temp;
        if (!seriesGetInt(s, index, &temp)) return false;
        *outVal = (double)temp;
        return true;
    }
    else if (s->type == DF_DOUBLE) {
        double temp;
        if (!seriesGetDouble(s, index, &temp)) return false;
        *outVal = temp;
        return true;
    }
    // Not a numeric column
    return false;
}

/**
 * dfPlot:
 * Generates a temporary Python script that uses matplotlib (and optionally mplfinance)
 * to plot DataFrame columns. Supports:
 *  - "line" (default)
 *  - "scatter"
 *  - "hloc" (candlestick), requires exactly 4 y columns: open, high, low, close
 *
 * xColIndex = -1 uses the row index as X.
 * Otherwise we expect xColIndex to be numeric (int/double).
 * All yColumns must be numeric (int/double).
 *
 * If outputFile is non-empty, we call plt.savefig(...), else plt.show() for an interactive window.
 */
void dfPlot(const DataFrame* df,
            size_t xColIndex,
            const size_t* yColIndices,
            size_t yCount,
            const char* plotType,
            const char* outputFile)
{
    if (!df) {
        fprintf(stderr, "dfPlot Error: DataFrame is NULL.\n");
        return;
    }
    size_t nRows = dfNumRows(df);
    size_t nCols = dfNumColumns(df);
    if (nRows == 0 || nCols == 0) {
        fprintf(stderr, "dfPlot Error: DataFrame is empty.\n");
        return;
    }
    if (!yColIndices || yCount == 0) {
        fprintf(stderr, "dfPlot Error: Must provide at least one y column.\n");
        return;
    }
    if (!plotType) {
        plotType = "line"; // default
    }

    // Check if xColIndex == (size_t)-1 => use row index as X
    bool useIndexAsX = (xColIndex == (size_t)-1);

    // Validate Y columns must be numeric
    for (size_t i = 0; i < yCount; i++) {
        const Series* sy = dfGetSeries(df, yColIndices[i]);
        if (!sy) {
            fprintf(stderr, "dfPlot Error: Invalid yCol index %zu.\n", yColIndices[i]);
            return;
        }
        if (sy->type != DF_INT && sy->type != DF_DOUBLE) {
            fprintf(stderr,
                    "dfPlot Error: Column '%s' is not numeric (type=%d). Cannot plot.\n",
                    sy->name, (int)sy->type);
            return;
        }
    }

    // If using a real column as X, check that it's numeric
    const Series* sx = NULL;
    if (!useIndexAsX) {
        sx = dfGetSeries(df, xColIndex);
        if (!sx) {
            fprintf(stderr, "dfPlot Error: Invalid xCol index %zu.\n", xColIndex);
            return;
        }
        if (sx->type != DF_INT && sx->type != DF_DOUBLE) {
            fprintf(stderr,
                    "dfPlot Error: X column '%s' is not numeric. Cannot plot.\n",
                    sx->name);
            return;
        }
    }

    // Create the temporary Python script
    const char* pyFilename = "temp_plot.py";
    FILE* pyFile = fopen(pyFilename, "w");
    if (!pyFile) {
        fprintf(stderr, "dfPlot Error: Unable to open temp file '%s' for writing.\n", pyFilename);
        return;
    }

    // Basic imports
    fprintf(pyFile, "import matplotlib.pyplot as plt\n");
    fprintf(pyFile, "import sys\n\n");

    // 1) Create X array (either row indices or numeric column)
    if (useIndexAsX) {
        // Use row indices 0..(nRows-1)
        fprintf(pyFile, "x = [");
        for (size_t r = 0; r < nRows; r++) {
            fprintf(pyFile, "%zu", r);
            if (r < nRows - 1) fprintf(pyFile, ", ");
        }
        fprintf(pyFile, "]\n");
    } else {
        fprintf(pyFile, "x = [");
        for (size_t r = 0; r < nRows; r++) {
            double val = 0.0;
            getNumericValue(sx, r, &val);
            fprintf(pyFile, "%g", val);
            if (r < nRows - 1) fprintf(pyFile, ", ");
        }
        fprintf(pyFile, "]\n");
    }

    // 2) Create each Y array: y0, y1, etc.
    for (size_t i = 0; i < yCount; i++) {
        const Series* s = dfGetSeries(df, yColIndices[i]);
        fprintf(pyFile, "y%zu = [", i);
        for (size_t r = 0; r < nRows; r++) {
            double val = 0.0;
            getNumericValue(s, r, &val);
            fprintf(pyFile, "%g", val);
            if (r < nRows - 1) fprintf(pyFile, ", ");
        }
        fprintf(pyFile, "]\n");
    }

    // 3) Plot logic
    if (strcmp(plotType, "scatter") == 0) {
        // scatter plot
        for (size_t i = 0; i < yCount; i++) {
            const Series* s = dfGetSeries(df, yColIndices[i]);
            fprintf(pyFile, "plt.scatter(x, y%zu, label=\"%s\")\n", i, s->name);
        }
        fprintf(pyFile, "plt.xlabel(\"%s\")\n",
                useIndexAsX ? "Index" : sx->name);
        fprintf(pyFile, "plt.ylabel(\"Value\")\n");
        fprintf(pyFile, "plt.title(\"DataFrame Scatter Plot\")\n");
        fprintf(pyFile, "plt.legend()\n");

    } else if (strcmp(plotType, "hloc") == 0) {
        // HLOC candlestick: we need yCount == 4 => (Open, High, Low, Close)
        // We'll use "mplfinance" to plot.  (pip install mplfinance)
        if (yCount != 4) {
            fprintf(stderr, "dfPlot Error: 'hloc' plotType requires exactly 4 y columns (O,H,L,C)\n");
            fclose(pyFile);
            remove(pyFilename);
            return;
        }
        fprintf(pyFile, "import mplfinance as mpf\n");
        fprintf(pyFile, "import pandas as pd\n\n");

        // Build candleData = [(x[i], open[i], high[i], low[i], close[i]) ...]
        fprintf(pyFile, "candleData = []\n");
        fprintf(pyFile, "for i in range(len(x)):\n");
        fprintf(pyFile, "    candleData.append((x[i], y0[i], y1[i], y2[i], y3[i]))\n\n");

        // Convert that into a pandas DataFrame with columns = ['time','Open','High','Low','Close']
        fprintf(pyFile, "df_data = pd.DataFrame(candleData, columns=['time','Open','High','Low','Close'])\n");

        // If x is time in milliseconds, for example, you can convert:
        //   df_data['time'] = pd.to_datetime(df_data['time'], unit='ms')
        // If it's just a numeric index, we can skip that. 

        // We'll assume a user might have real time in 'x'. Let's try to parse as ms if you want:
        fprintf(pyFile, "# If you want to interpret x as timestamps in ms, uncomment:\n");
        fprintf(pyFile, "df_data['time'] = pd.to_datetime(df_data['time'], unit='ms')\n");

        fprintf(pyFile, "df_data.set_index('time', inplace=True)\n\n");

        // Now plot
        fprintf(pyFile, "mpf.plot(df_data, type='candle', style='charles', title='HLOC Candlestick')\n");

    } else {
        // Default: line plot
        for (size_t i = 0; i < yCount; i++) {
            const Series* s = dfGetSeries(df, yColIndices[i]);
            fprintf(pyFile, "plt.plot(x, y%zu, label=\"%s\")\n", i, s->name);
        }
        fprintf(pyFile, "plt.xlabel(\"%s\")\n",
                useIndexAsX ? "Index" : sx->name);
        fprintf(pyFile, "plt.ylabel(\"Value\")\n");
        fprintf(pyFile, "plt.title(\"DataFrame Line Plot\")\n");
        fprintf(pyFile, "plt.legend()\n");
    }

    // 4) Save or Show
    if (outputFile && strlen(outputFile) > 0) {
        fprintf(pyFile, "import matplotlib.pyplot as plt\n");
        fprintf(pyFile, "plt.savefig(\"%s\")\n", outputFile);
        fprintf(pyFile, "print(\"Plot saved to %s\")\n", outputFile);
    } else {
        fprintf(pyFile, "import matplotlib.pyplot as plt\n");
        fprintf(pyFile, "plt.show()\n");
    }

    fclose(pyFile);

    // 5) Run the Python script
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "python3 \"%s\"", pyFilename);
    int ret = system(cmd);
    if (ret != 0) {
        fprintf(stderr, "dfPlot Warning: system(\"%s\") returned %d.\n", cmd, ret);
    }

    // 6) Remove the temp script
    remove(pyFilename);
}




/* -------------------------------------------------------------------------
 * Date Parsing and Conversion
 * ------------------------------------------------------------------------- */ 
/**
 * parseYYYYMMDD:
 *   Expects an integer like 20230131 => Jan 31, 2023
 *   or a double that equals an integer.
 * Returns time_t (seconds since epoch, UTC 00:00).
 * On error, returns -1.
 */
static time_t parseYYYYMMDD(double num) {
    // For example, 20230131. 
    // We'll parse it as an int: YYYYMMDD
    int dateVal = (int)num;  // cast from double -> int
    if (dateVal <= 10000101) {
        // no real checks, but let's do a naive boundary
        return -1;
    }

    int year  = dateVal / 10000;        // e.g. 2023
    int month = (dateVal / 100) % 100;  // e.g. 01 => January
    int day   = dateVal % 100;         // e.g. 31

    struct tm tinfo;
    tinfo.tm_year = year - 1900;       // tm_year is years since 1900
    tinfo.tm_mon  = month - 1;         // tm_mon is 0-based
    tinfo.tm_mday = day;
    tinfo.tm_hour = 0;
    tinfo.tm_min  = 0;
    tinfo.tm_sec  = 0;
    tinfo.tm_isdst= -1;  // let mktime guess DST

    time_t seconds = mktime(&tinfo);
    return (seconds == -1) ? -1 : seconds;
}

/**
 * dfConvertDatesToEpoch:
 *  Convert a numeric date column (e.g. YYYYMMDD) to Unix epoch time.
 *  If toMillis=true, store milliseconds; else store seconds.
 *
 *  formatType can be "YYYYMMDD" or something else if you want to parse differently.
 */
bool dfConvertDatesToEpoch(DataFrame* df, size_t dateColIndex, const char* formatType, bool toMillis) {
    if (!df) return false;

    Series* s = (Series*)daGetMutable(&df->columns, dateColIndex);
    if (!s) return false;

    if (s->type != DF_INT && s->type != DF_DOUBLE) {
        fprintf(stderr, "dfConvertDatesToEpoch: column %zu is not numeric.\n", dateColIndex);
        return false;
    }

    size_t nRows = seriesSize(s);
    // We'll read each row's numeric date, parse it, and rewrite it as an epoch
    for (size_t r = 0; r < nRows; r++) {
        double numericVal = 0.0;
        if (s->type == DF_INT) {
            int v;
            seriesGetInt(s, r, &v);
            numericVal = (double)v;
        } else {
            seriesGetDouble(s, r, &numericVal);
        }

        time_t epochSec = 0;
        if (strcmp(formatType, "YYYYMMDD") == 0) {
            epochSec = parseYYYYMMDD(numericVal);
            if (epochSec == (time_t)-1) {
                // fallback or keep it as is? We'll just store 0 if parse fails
                epochSec = 0;
            }
        } else if (strcmp(formatType, "unix_seconds") == 0) {
            // then numericVal is already seconds since epoch
            epochSec = (time_t)numericVal;
        } else if (strcmp(formatType, "unix_millis") == 0) {
            // numericVal is milliseconds since epoch
            // convert to seconds
            epochSec = (time_t)(numericVal / 1000.0);
        } else {
            // default or unknown
            // If you have your own format, parse accordingly
            epochSec = 0;
        }

        // If toMillis, multiply
        double finalVal = (toMillis) ? (epochSec * 1000.0) : (double)epochSec;

        // Now rewrite the value in the Series
        if (s->type == DF_INT) {
            // We can either convert the entire column to DF_DOUBLE or store as int if it fits
            // If storing as int might overflow for large epoch?
            int intVal = (int)finalVal;
            // If it doesn't fit, you might want to fallback to double. We'll just do int here.
            // Overflows beyond 2038 for 32-bit time_t, so be aware.
            *(int*)daGetMutable(&s->data, r) = intVal;
        } else {
            // DF_DOUBLE
            *(double*)daGetMutable(&s->data, r) = finalVal;
        }
    }

    // The column now holds epoch times in either seconds or milliseconds
    // That is enough for you to do "pd.to_datetime(..., unit='s' or 'ms')" in the python script.

    return true;
}
