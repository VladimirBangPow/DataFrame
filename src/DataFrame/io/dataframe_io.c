#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdbool.h>
#include <time.h>
#include "../dataframe.h"

/* -------------------------------------------------------------------------
 *  Forward-declared static helpers
 * ------------------------------------------------------------------------- */
static size_t splitCsvLine(char* line, char** tokens, size_t maxTokens);
static int checkNumericType(const char* str);

/* NEW: Check if a string is likely a date/time (very naive approach). */
static bool isLikelyDateTime(const char* str);

/* NEW: Convert a date/time string to epoch as a long long. */
static long long parseDateTimeToEpoch(const char* dateStr);

/**
 * loadCsvIntoBuffer:
 *  1) Reads the CSV file fully.
 *  2) Stores the column headers in 'outHeaders'.
 *  3) Stores row cells in 'outCells' (2D).
 *  4) Returns true/false on success/fail.
 */
static bool loadCsvIntoBuffer(
    const char* filename,
    size_t* outNCols,
    size_t* outNRows,
    char*** outHeaders,
    char**** outCells
);

/**
 * Frees the headers and cells allocated by loadCsvIntoBuffer.
 */
static void freeCsvBuffer(
    size_t nCols,
    size_t nRows,
    char** headers,
    char*** cells
);

/**
 * Decide whether a column is DF_INT, DF_DOUBLE, DF_STRING, or DF_DATETIME
 * by scanning all rows in that column.
 */
static ColumnType inferColumnType(
    size_t nRows,
    char*** cells,
    size_t nCols,
    size_t colIndex
);

bool readCsv_impl(DataFrame* df, const char* filename)
{
    if (!df || !filename) {
        fprintf(stderr, "readCsv_impl: invalid arguments.\n");
        return false;
    }

    size_t nCols = 0, nRows = 0;
    char** headers = NULL;
    char*** cells = NULL;

    if (!loadCsvIntoBuffer(filename, &nCols, &nRows, &headers, &cells)) {
        return false; // already logged error
    }

    // If df was used before, you might want to df->free(df) here
    df->init(df); // ensure it's in a clean state

    // If no data rows, create empty columns (DF_STRING by default)
    if (nRows == 0) {
        for (size_t c = 0; c < nCols; c++) {
            Series s;
            seriesInit(&s, headers[c], DF_STRING);
            df->addSeries(df, &s);
            seriesFree(&s);
        }
        freeCsvBuffer(nCols, nRows, headers, cells);
        return true;
    }

    // Infer column types
    ColumnType* finalTypes = (ColumnType*)malloc(sizeof(ColumnType) * nCols);
    if (!finalTypes) {
        fprintf(stderr, "readCsv_impl: out of memory for finalTypes.\n");
        freeCsvBuffer(nCols, nRows, headers, cells);
        return false;
    }
    for (size_t c = 0; c < nCols; c++) {
        finalTypes[c] = inferColumnType(nRows, cells, nCols, c);
    }

    // Build the DataFrame
    for (size_t c = 0; c < nCols; c++) {
        Series s;
        seriesInit(&s, headers[c], finalTypes[c]);
        for (size_t r = 0; r < nRows; r++) {
            const char* valStr = cells[r][c];
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
                /* ------------------------------------------------------
                 * NEW: DF_DATETIME => parse date/time to a 64-bit epoch
                 * ------------------------------------------------------*/
                case DF_DATETIME: {
                    long long dtEpoch = parseDateTimeToEpoch(valStr);
                    seriesAddDateTime(&s, dtEpoch);
                } break;
            }
        }
        df->addSeries(df, &s);
        seriesFree(&s);
    }

    free(finalTypes);
    freeCsvBuffer(nCols, nRows, headers, cells);
    return true;
}

/* -------------------------------------------------------------------------
 * Implementation of static helpers
 * ------------------------------------------------------------------------- */

static size_t splitCsvLine(char* line, char** tokens, size_t maxTokens)
{
    size_t count = 0;
    char* pch = strtok(line, ",");
    while (pch != NULL && count < maxTokens) {
        tokens[count++] = pch;
        pch = strtok(NULL, ",");
    }
    return count;
}

static int checkNumericType(const char* str)
{
    if (!str || !*str) return -1;
    while (isspace((unsigned char)*str)) str++;
    char* endptr;

    // Try int
    strtol(str, &endptr, 10);
    if (*endptr == '\0') {
        return 0; // int
    }
    // Try double
    strtod(str, &endptr);
    if (*endptr == '\0') {
        return 1; // double
    }
    return -1; // neither
}

/* 
 * isLikelyDateTime:
 *   - Very naive check: attempts to parse with strptime 
 *     using a known format (e.g. "%Y-%m-%d %H:%M:%S").
 *   - If parse fails, returns false.
 *   - In real usage, you might test multiple formats or check for partial date.
 */
static bool isLikelyDateTime(const char* str)
{
    if (!str || !*str) return false;

    // Try a common datetime format
    struct tm tmVal;
    memset(&tmVal, 0, sizeof(tmVal));

    // Example format: "YYYY-MM-DD HH:MM:SS"
    // Adjust to your CSV’s typical date/time format as needed.
    char* ret = strptime(str, "%Y-%m-%d %H:%M:%S", &tmVal);
    if (ret == NULL) {
        // If that fails, try just date "YYYY-MM-DD"
        memset(&tmVal, 0, sizeof(tmVal));
        ret = strptime(str, "%Y-%m-%d", &tmVal);
    }
    return (ret != NULL);
}

/*
 * parseDateTimeToEpoch:
 *   - Convert the string (assumed date/time) to a 64-bit epoch time in seconds.
 *     If you want microseconds, multiply by 1,000,000.
 *   - Uses timegm (GNU extension) or mktime (local time).
 *     Adjust to your preference/timezone.
 */
static long long parseDateTimeToEpoch(const char* dateStr)
{
    if (!dateStr) return 0;

    // Attempt to parse with the same formats as isLikelyDateTime
    struct tm tmVal;
    memset(&tmVal, 0, sizeof(tmVal));
    char* ret = strptime(dateStr, "%Y-%m-%d %H:%M:%S", &tmVal);
    if (!ret) {
        // fallback: try just date
        memset(&tmVal, 0, sizeof(tmVal));
        ret = strptime(dateStr, "%Y-%m-%d", &tmVal);
    }

    if (!ret) {
        // fallback if parse fails
        return 0;
    }

    // Use timegm if you want UTC-based, or mktime for local time
    time_t seconds = timegm(&tmVal);
    // Return as seconds (for DF_DATETIME you might prefer microseconds)
    // e.g.: return (long long)seconds * 1000000LL;
    return (long long)seconds;
}

static bool loadCsvIntoBuffer(
    const char* filename,
    size_t* outNCols,
    size_t* outNRows,
    char*** outHeaders,
    char**** outCells
)
{
    #define MAX_LINE_LEN 4096
    #define MAX_COLS     1024
    FILE* fp = fopen(filename, "r");
    if (!fp) {
        fprintf(stderr, "loadCsvIntoBuffer: cannot open file '%s'\n", filename);
        return false;
    }

    char lineBuf[MAX_LINE_LEN];

    // 1) Read header
    if (!fgets(lineBuf, sizeof(lineBuf), fp)) {
        fprintf(stderr, "loadCsvIntoBuffer: file '%s' is empty.\n", filename);
        fclose(fp);
        return false;
    }
    // Strip newline
    lineBuf[strcspn(lineBuf, "\r\n")] = '\0';

    char* headerTokens[MAX_COLS];
    size_t colCount = splitCsvLine(lineBuf, headerTokens, MAX_COLS);
    if (colCount == 0) {
        fprintf(stderr, "loadCsvIntoBuffer: file '%s' has an empty header.\n", filename);
        fclose(fp);
        return false;
    }

    char** headers = (char**)calloc(colCount, sizeof(char*));
    if (!headers) {
        fclose(fp);
        fprintf(stderr, "out of memory for headers.\n");
        return false;
    }
    for (size_t c = 0; c < colCount; c++) {
        headers[c] = strdup(headerTokens[c]);
    }

    // 2) Read rows
    size_t capacityRows = 1000;
    size_t rowCount = 0;
    char*** rowData = (char***)malloc(sizeof(char**) * capacityRows);
    if (!rowData) {
        fclose(fp);
        fprintf(stderr, "out of memory for row pointers.\n");
        return false;
    }

    while (fgets(lineBuf, sizeof(lineBuf), fp)) {
        // Strip newline
        lineBuf[strcspn(lineBuf, "\r\n")] = '\0';
        // Check if blank
        char* checkp = lineBuf;
        while (*checkp && isspace((unsigned char)*checkp)) checkp++;
        if (*checkp == '\0') {
            continue; // skip empty lines
        }

        char* tokens[MAX_COLS];
        size_t nTokens = splitCsvLine(lineBuf, tokens, MAX_COLS);
        if (nTokens < colCount) {
            // pad with ""
            for (size_t cc = nTokens; cc < colCount; cc++) {
                tokens[cc] = "";
            }
            nTokens = colCount;
        }

        if (rowCount >= capacityRows) {
            capacityRows *= 2;
            rowData = (char***)realloc(rowData, sizeof(char**) * capacityRows);
            if (!rowData) {
                fclose(fp);
                fprintf(stderr, "out of memory expanding rowData.\n");
                return false;
            }
        }
        rowData[rowCount] = (char**)malloc(sizeof(char*) * colCount);
        if (!rowData[rowCount]) {
            fclose(fp);
            fprintf(stderr, "out of memory for row.\n");
            return false;
        }
        for (size_t cc = 0; cc < colCount; cc++) {
            rowData[rowCount][cc] = strdup(tokens[cc]);
        }
        rowCount++;
    }
    fclose(fp);

    // Assign out-params
    *outNCols = colCount;
    *outNRows = rowCount;
    *outHeaders = headers;
    *outCells = rowData;
    return true;
}

static void freeCsvBuffer(
    size_t nCols,
    size_t nRows,
    char** headers,
    char*** cells
)
{
    if (headers) {
        for (size_t c = 0; c < nCols; c++) {
            free(headers[c]);
        }
        free(headers);
    }

    if (cells) {
        for (size_t r = 0; r < nRows; r++) {
            for (size_t c = 0; c < nCols; c++) {
                free(cells[r][c]);
            }
            free(cells[r]);
        }
        free(cells);
    }
}

/*
 * inferColumnType: 
 *   1) If all rows in col parse as date/time => DF_DATETIME
 *   2) else if all rows numeric => DF_INT or DF_DOUBLE
 *   3) else => DF_STRING
 */
static ColumnType inferColumnType(
    size_t nRows,
    char*** cells,
    size_t nCols,
    size_t colIndex
)
{
    bool allDatetime = true;
    for (size_t r = 0; r < nRows; r++) {
        if (!isLikelyDateTime(cells[r][colIndex])) {
            allDatetime = false;
            break;
        }
    }
    if (allDatetime) {
        return DF_DATETIME;
    }

    // Next fallback: numeric or string
    // 0 => can be int, 1 => can be double, 2 => must be string
    int colStage = 0;
    for (size_t r = 0; r < nRows; r++) {
        const char* val = cells[r][colIndex];
        int t = checkNumericType(val);
        if (t < 0) {
            colStage = 2; // string
            break;
        } else if (t == 1) {
            // once we see a double, entire column is double
            colStage = 1;
        }
    }
    if (colStage == 0) return DF_INT;
    if (colStage == 1) return DF_DOUBLE;
    return DF_STRING;
}
