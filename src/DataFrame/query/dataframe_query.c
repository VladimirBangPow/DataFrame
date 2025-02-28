#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdbool.h>
#include "../dataframe.h"  // Make sure this has RowPredicate, RowFunction, etc.

/* 
   -------------
   HEAD + TAIL
   -------------
*/

/**
 * @brief Return a new DataFrame containing the first `n` rows of `df`.
 *        If `df` has fewer than `n` rows, it returns all of them.
 */
DataFrame dfHead_impl(const DataFrame* df, size_t n)
{
    DataFrame result;
    DataFrame_Create(&result);  // initialize a blank DataFrame

    if (!df) {
        // Return an empty DataFrame if df is NULL
        return result;
    }

    size_t numRows = df->numRows(df);
    size_t limit = (n < numRows) ? n : numRows;

    size_t nCols = df->numColumns(df);

    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) continue;

        Series newSeries;
        seriesInit(&newSeries, s->name, s->type);

        for (size_t r = 0; r < limit; r++) {
            switch (s->type) {
                case DF_INT: {
                    int val;
                    if (seriesGetInt(s, r, &val)) {
                        seriesAddInt(&newSeries, val);
                    }
                } break;
                case DF_DOUBLE: {
                    double val;
                    if (seriesGetDouble(s, r, &val)) {
                        seriesAddDouble(&newSeries, val);
                    }
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(s, r, &str)) {
                        seriesAddString(&newSeries, str);
                        free(str); 
                    }
                } break;
                /* NEW: DF_DATETIME */
                case DF_DATETIME: {
                    long long dtVal;
                    if (seriesGetDateTime(s, r, &dtVal)) {
                        seriesAddDateTime(&newSeries, dtVal);
                    }
                } break;
            }
        }

        result.addSeries(&result, &newSeries);
        seriesFree(&newSeries);
    }

    return result;
}

/**
 * @brief Return a new DataFrame containing the last `n` rows of `df`.
 *        If `df` has fewer than `n` rows, it returns all of them.
 */
DataFrame dfTail_impl(const DataFrame* df, size_t n)
{
    DataFrame result;
    DataFrame_Create(&result);

    if (!df) {
        return result;
    }

    size_t numRows = df->numRows(df);
    if (n > numRows) {
        n = numRows;
    }
    size_t start = (numRows > n) ? (numRows - n) : 0;

    size_t nCols = df->numColumns(df);

    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) continue;

        Series newSeries;
        seriesInit(&newSeries, s->name, s->type);

        for (size_t r = start; r < numRows; r++) {
            switch (s->type) {
                case DF_INT: {
                    int val;
                    if (seriesGetInt(s, r, &val)) {
                        seriesAddInt(&newSeries, val);
                    }
                } break;
                case DF_DOUBLE: {
                    double val;
                    if (seriesGetDouble(s, r, &val)) {
                        seriesAddDouble(&newSeries, val);
                    }
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(s, r, &str)) {
                        seriesAddString(&newSeries, str);
                        free(str);
                    }
                } break;
                /* NEW: DF_DATETIME */
                case DF_DATETIME: {
                    long long dtVal;
                    if (seriesGetDateTime(s, r, &dtVal)) {
                        seriesAddDateTime(&newSeries, dtVal);
                    }
                } break;
            }
        }
        result.addSeries(&result, &newSeries);
        seriesFree(&newSeries);
    }

    return result;
}

/* 
   -------------
   DESCRIBE
   -------------
*/

DataFrame dfDescribe_impl(const DataFrame* df)
{
    DataFrame result;
    DataFrame_Create(&result);

    if (!df) return result;

    size_t nCols = df->numColumns(df);
    size_t nRows = df->numRows(df);

    Series colNameS, countS, minS, maxS, meanS;
    seriesInit(&colNameS, "colName", DF_STRING);
    seriesInit(&countS,   "count",   DF_INT);
    seriesInit(&minS,     "min",     DF_DOUBLE);
    seriesInit(&maxS,     "max",     DF_DOUBLE);
    seriesInit(&meanS,    "mean",    DF_DOUBLE);

    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) continue;

        seriesAddString(&colNameS, s->name);  // add col name

        // For numeric columns, compute stats
        if ((s->type == DF_INT || s->type == DF_DOUBLE) && nRows > 0) {
            double minVal, maxVal, sumVal = 0.0;

            // initialize from row 0
            if (s->type == DF_INT) {
                int temp;
                seriesGetInt(s, 0, &temp);
                minVal = maxVal = (double)temp;
                sumVal = (double)temp;
            }
            else {
                double temp;
                seriesGetDouble(s, 0, &temp);
                minVal = maxVal = temp;
                sumVal = temp;
            }
            // gather stats
            for (size_t r = 1; r < nRows; r++) {
                double d = 0.0;
                if (s->type == DF_INT) {
                    int ti;
                    seriesGetInt(s, r, &ti);
                    d = (double)ti;
                } else {
                    double td;
                    seriesGetDouble(s, r, &td);
                    d = td;
                }
                if (d < minVal) minVal = d;
                if (d > maxVal) maxVal = d;
                sumVal += d;
            }

            double meanVal = sumVal / (double)nRows;

            seriesAddInt(&countS,  (int)nRows);
            seriesAddDouble(&minS,  minVal);
            seriesAddDouble(&maxS,  maxVal);
            seriesAddDouble(&meanS, meanVal);
        }
        else if (s->type == DF_STRING) {
            // For string columns, store count + 0 for min/max/mean
            seriesAddInt(&countS, (int)nRows);
            seriesAddDouble(&minS,  0.0);
            seriesAddDouble(&maxS,  0.0);
            seriesAddDouble(&meanS, 0.0);
        }
        /* NEW: DF_DATETIME => treat like a "non-numeric" for now */
        else if (s->type == DF_DATETIME) {
            seriesAddInt(&countS, (int)nRows);
            // Could compute actual min, max from epoch. For now, set 0
            seriesAddDouble(&minS,  0.0);
            seriesAddDouble(&maxS,  0.0);
            seriesAddDouble(&meanS, 0.0);
        }
        else {
            // no rows => everything is 0 or empty
            seriesAddInt(&countS, 0);
            seriesAddDouble(&minS,  0.0);
            seriesAddDouble(&maxS,  0.0);
            seriesAddDouble(&meanS, 0.0);
        }
    }

    // Add columns to the result
    result.addSeries(&result, &colNameS);
    result.addSeries(&result, &countS);
    result.addSeries(&result, &minS);
    result.addSeries(&result, &maxS);
    result.addSeries(&result, &meanS);

    seriesFree(&colNameS);
    seriesFree(&countS);
    seriesFree(&minS);
    seriesFree(&maxS);
    seriesFree(&meanS);

    return result;
}

/* -------------------------------------------------------------------------
 * 1) Row Slicing / Sampling
 * ------------------------------------------------------------------------- */

DataFrame dfSlice_impl(const DataFrame* df, size_t start, size_t end)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    size_t nRows = df->numRows(df);
    if (start >= nRows) {
        return result; // empty
    }
    if (end > nRows) {
        end = nRows;
    }

    size_t nCols = df->numColumns(df);
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) continue;

        Series newSeries;
        seriesInit(&newSeries, s->name, s->type);

        for (size_t r = start; r < end; r++) {
            switch (s->type) {
                case DF_INT: {
                    int val;
                    if (seriesGetInt(s, r, &val)) {
                        seriesAddInt(&newSeries, val);
                    }
                } break;
                case DF_DOUBLE: {
                    double val;
                    if (seriesGetDouble(s, r, &val)) {
                        seriesAddDouble(&newSeries, val);
                    }
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(s, r, &str)) {
                        seriesAddString(&newSeries, str);
                        free(str);
                    }
                } break;
                /* NEW: DF_DATETIME */
                case DF_DATETIME: {
                    long long dtVal;
                    if (seriesGetDateTime(s, r, &dtVal)) {
                        seriesAddDateTime(&newSeries, dtVal);
                    }
                } break;
            }
        }

        result.addSeries(&result, &newSeries);
        seriesFree(&newSeries);
    }
    return result;
}

DataFrame dfSample_impl(const DataFrame* df, size_t count)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    size_t nRows = df->numRows(df);
    if (count >= nRows) {
        return dfSlice_impl(df, 0, nRows);
    }

    // Shuffle row indices
    size_t* indices = (size_t*)malloc(nRows * sizeof(size_t));
    for (size_t i = 0; i < nRows; i++) {
        indices[i] = i;
    }
    srand((unsigned)time(NULL));
    for (size_t i = 0; i < nRows; i++) {
        size_t j = i + rand() / (RAND_MAX / (nRows - i) + 1);
        size_t tmp = indices[j];
        indices[j] = indices[i];
        indices[i] = tmp;
    }

    size_t nCols = df->numColumns(df);
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) continue;

        Series newSeries;
        seriesInit(&newSeries, s->name, s->type);

        for (size_t i = 0; i < count; i++) {
            size_t rowIdx = indices[i];
            switch (s->type) {
                case DF_INT: {
                    int val;
                    if (seriesGetInt(s, rowIdx, &val)) {
                        seriesAddInt(&newSeries, val);
                    }
                } break;
                case DF_DOUBLE: {
                    double val;
                    if (seriesGetDouble(s, rowIdx, &val)) {
                        seriesAddDouble(&newSeries, val);
                    }
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(s, rowIdx, &str)) {
                        seriesAddString(&newSeries, str);
                        free(str);
                    }
                } break;
                /* NEW: DF_DATETIME */
                case DF_DATETIME: {
                    long long dtVal;
                    if (seriesGetDateTime(s, rowIdx, &dtVal)) {
                        seriesAddDateTime(&newSeries, dtVal);
                    }
                } break;
            }
        }
        result.addSeries(&result, &newSeries);
        seriesFree(&newSeries);
    }

    free(indices);
    return result;
}

/* -------------------------------------------------------------------------
 * 2) Column Subsets / Manipulation
 * ------------------------------------------------------------------------- */

DataFrame dfSelectColumns_impl(const DataFrame* df, const size_t* colIndices, size_t count)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df || !colIndices) return result;

    size_t nCols = df->numColumns(df);
    for (size_t i = 0; i < count; i++) {
        size_t cIndex = colIndices[i];
        if (cIndex >= nCols) continue;
        const Series* s = df->getSeries(df, cIndex);
        if (!s) continue;

        Series newSeries;
        seriesInit(&newSeries, s->name, s->type);

        size_t nRows = seriesSize(s);
        for (size_t r = 0; r < nRows; r++) {
            switch (s->type) {
                case DF_INT: {
                    int val;
                    if (seriesGetInt(s, r, &val)) {
                        seriesAddInt(&newSeries, val);
                    }
                } break;
                case DF_DOUBLE: {
                    double val;
                    if (seriesGetDouble(s, r, &val)) {
                        seriesAddDouble(&newSeries, val);
                    }
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(s, r, &str)) {
                        seriesAddString(&newSeries, str);
                        free(str);
                    }
                } break;
                /* NEW: DF_DATETIME */
                case DF_DATETIME: {
                    long long dtVal;
                    if (seriesGetDateTime(s, r, &dtVal)) {
                        seriesAddDateTime(&newSeries, dtVal);
                    }
                } break;
            }
        }
        result.addSeries(&result, &newSeries);
        seriesFree(&newSeries);
    }

    return result;
}

DataFrame dfDropColumns_impl(const DataFrame* df, const size_t* dropIndices, size_t dropCount)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df || !dropIndices) return result;

    size_t nCols = df->numColumns(df);
    bool* toDrop = (bool*)calloc(nCols, sizeof(bool));
    for (size_t i = 0; i < dropCount; i++) {
        if (dropIndices[i] < nCols) {
            toDrop[dropIndices[i]] = true;
        }
    }

    for (size_t c = 0; c < nCols; c++) {
        if (toDrop[c]) {
            continue;
        }
        const Series* s = df->getSeries(df, c);
        if (!s) continue;

        Series newSeries;
        seriesInit(&newSeries, s->name, s->type);

        size_t nRows = seriesSize(s);
        for (size_t r = 0; r < nRows; r++) {
            switch (s->type) {
                case DF_INT: {
                    int val;
                    if (seriesGetInt(s, r, &val)) {
                        seriesAddInt(&newSeries, val);
                    }
                } break;
                case DF_DOUBLE: {
                    double val;
                    if (seriesGetDouble(s, r, &val)) {
                        seriesAddDouble(&newSeries, val);
                    }
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(s, r, &str)) {
                        seriesAddString(&newSeries, str);
                        free(str);
                    }
                } break;
                /* NEW: DF_DATETIME */
                case DF_DATETIME: {
                    long long dtVal;
                    if (seriesGetDateTime(s, r, &dtVal)) {
                        seriesAddDateTime(&newSeries, dtVal);
                    }
                } break;
            }
        }
        result.addSeries(&result, &newSeries);
        seriesFree(&newSeries);
    }

    free(toDrop);
    return result;
}

DataFrame dfRenameColumns_impl(const DataFrame* df,
                               const char** oldNames,
                               const char** newNames,
                               size_t count)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    size_t nCols = df->numColumns(df);

    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) continue;

        Series newSeries;
        seriesInit(&newSeries, s->name, s->type);

        size_t nRows = seriesSize(s);
        for (size_t r = 0; r < nRows; r++) {
            switch (s->type) {
                case DF_INT: {
                    int val;
                    if (seriesGetInt(s, r, &val)) {
                        seriesAddInt(&newSeries, val);
                    }
                } break;
                case DF_DOUBLE: {
                    double val;
                    if (seriesGetDouble(s, r, &val)) {
                        seriesAddDouble(&newSeries, val);
                    }
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(s, r, &str)) {
                        seriesAddString(&newSeries, str);
                        free(str);
                    }
                } break;
                /* NEW: DF_DATETIME */
                case DF_DATETIME: {
                    long long dtVal;
                    if (seriesGetDateTime(s, r, &dtVal)) {
                        seriesAddDateTime(&newSeries, dtVal);
                    }
                } break;
            }
        }

        // check if s->name is in oldNames => rename
        for (size_t i = 0; i < count; i++) {
            if (strcmp(s->name, oldNames[i]) == 0) {
                free(newSeries.name); 
                newSeries.name = (char*)malloc(strlen(newNames[i]) + 1);
                strcpy(newSeries.name, newNames[i]);
                break;
            }
        }

        result.addSeries(&result, &newSeries);
        seriesFree(&newSeries);
    }

    return result;
}

/* -------------------------------------------------------------------------
 * 3) Filtering Rows
 * ------------------------------------------------------------------------- */

static bool isRowNA(const DataFrame* df, size_t rowIndex);

DataFrame dfFilter_impl(const DataFrame* df, RowPredicate predicate)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df || !predicate) return result;

    size_t nRows = df->numRows(df);
    size_t nCols = df->numColumns(df);

    bool* keepRow = (bool*)calloc(nRows, sizeof(bool));

    for (size_t r = 0; r < nRows; r++) {
        if (predicate(df, r)) {
            keepRow[r] = true;
        }
    }

    // Copy those rows
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) continue;

        Series newSeries;
        seriesInit(&newSeries, s->name, s->type);

        for (size_t r = 0; r < nRows; r++) {
            if (!keepRow[r]) continue;

            switch (s->type) {
                case DF_INT: {
                    int val;
                    if (seriesGetInt(s, r, &val)) {
                        seriesAddInt(&newSeries, val);
                    }
                } break;
                case DF_DOUBLE: {
                    double val;
                    if (seriesGetDouble(s, r, &val)) {
                        seriesAddDouble(&newSeries, val);
                    }
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(s, r, &str)) {
                        seriesAddString(&newSeries, str);
                        free(str);
                    }
                } break;
                /* NEW: DF_DATETIME */
                case DF_DATETIME: {
                    long long dtVal;
                    if (seriesGetDateTime(s, r, &dtVal)) {
                        seriesAddDateTime(&newSeries, dtVal);
                    }
                } break;
            }
        }
        result.addSeries(&result, &newSeries);
        seriesFree(&newSeries);
    }

    free(keepRow);
    return result;
}

static bool dropNA_predicate(const DataFrame* d, size_t rowIdx)
{
    return !isRowNA(d, rowIdx);
}

DataFrame dfDropNA_impl(const DataFrame* df)
{
    if (!df) {
        DataFrame empty;
        DataFrame_Create(&empty);
        return empty;
    }
    // Filter out rows that are "NA"
    return dfFilter_impl(df, dropNA_predicate);
}

/* 
   helper to detect if row has "NA" in numeric=0 or string="" 
   (You can adapt for DF_DATETIME as needed.)
*/
static bool isRowNA(const DataFrame* df, size_t rowIndex)
{
    size_t nCols = df->numColumns(df);
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) continue;
        switch (s->type) {
            case DF_INT: {
                int val;
                if (seriesGetInt(s, rowIndex, &val)) {
                    if (val == 0) {
                        return true;
                    }
                }
            } break;
            case DF_DOUBLE: {
                double val;
                if (seriesGetDouble(s, rowIndex, &val)) {
                    if (val == 0.0) {
                        return true;
                    }
                }
            } break;
            case DF_STRING: {
                char* str = NULL;
                if (seriesGetString(s, rowIndex, &str)) {
                    bool empty = (str[0] == '\0');
                    free(str);
                    if (empty) {
                        return true;
                    }
                }
            } break;
            /* If you want DF_DATETIME = 0 to be "NA", do so here, e.g. */
            case DF_DATETIME: {
                long long dtVal;
                if (seriesGetDateTime(s, rowIndex, &dtVal)) {
                    if (dtVal == 0) {
                        return true;
                    }
                }
            } break;
        }
    }
    return false;
}

/* -------------------------------------------------------------------------
 * 4) Sorting
 * ------------------------------------------------------------------------- */

typedef struct {
    const DataFrame* df;
    size_t columnIndex;
    bool ascending;
} SortContext;

static int compareRowIndices(size_t ra, size_t rb, const SortContext* ctx)
{
    if (!ctx || !ctx->df) return 0;
    const Series* s = ctx->df->getSeries(ctx->df, ctx->columnIndex);
    if (!s) return 0;

    if (s->type == DF_INT) {
        int va, vb;
        bool gotA = seriesGetInt(s, ra, &va);
        bool gotB = seriesGetInt(s, rb, &vb);
        if (!gotA || !gotB) return 0;
        if (va < vb) return ctx->ascending ? -1 : 1;
        if (va > vb) return ctx->ascending ? 1 : -1;
        return 0;
    }
    else if (s->type == DF_DOUBLE) {
        double va, vb;
        bool gotA = seriesGetDouble(s, ra, &va);
        bool gotB = seriesGetDouble(s, rb, &vb);
        if (!gotA || !gotB) return 0;
        if (va < vb) return ctx->ascending ? -1 : 1;
        if (va > vb) return ctx->ascending ? 1 : -1;
        return 0;
    }
    else if (s->type == DF_STRING) {
        char* strA = NULL;
        char* strB = NULL;
        bool gotA = seriesGetString(s, ra, &strA);
        bool gotB = seriesGetString(s, rb, &strB);
        if (!gotA || !gotB) {
            if (strA) free(strA);
            if (strB) free(strB);
            return 0;
        }
        int cmp = strcmp(strA, strB);
        free(strA);
        free(strB);
        return ctx->ascending ? cmp : -cmp;
    }
    /* NEW: DF_DATETIME => compare as long long */
    else if (s->type == DF_DATETIME) {
        long long va, vb;
        bool gotA = seriesGetDateTime(s, ra, &va);
        bool gotB = seriesGetDateTime(s, rb, &vb);
        if (!gotA || !gotB) return 0;
        if (va < vb) return ctx->ascending ? -1 : 1;
        if (va > vb) return ctx->ascending ? 1 : -1;
        return 0;
    }

    return 0;
}

static void insertionSortRows(size_t* rowIdx, size_t count, const SortContext* ctx)
{
    for (size_t i = 1; i < count; i++) {
        size_t key = rowIdx[i];
        size_t j = i;
        while (j > 0) {
            int cmp = compareRowIndices(rowIdx[j - 1], key, ctx);
            if (cmp <= 0) {
                break;
            }
            rowIdx[j] = rowIdx[j - 1];
            j--;
        }
        rowIdx[j] = key;
    }
}

DataFrame dfSort_impl(const DataFrame* df, size_t columnIndex, bool ascending)
{
    DataFrame result;
    DataFrame_Create(&result);

    if (!df) return result;

    size_t nRows = df->numRows(df);
    size_t nCols = df->numColumns(df);
    if (columnIndex >= nCols) {
        return result;
    }

    size_t* rowIdx = (size_t*)malloc(nRows * sizeof(size_t));
    if (!rowIdx) {
        return result;
    }
    for (size_t i = 0; i < nRows; i++) {
        rowIdx[i] = i;
    }

    SortContext ctx;
    ctx.df = df;
    ctx.columnIndex = columnIndex;
    ctx.ascending = ascending;

    insertionSortRows(rowIdx, nRows, &ctx);

    // Now build sorted DF
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) continue;

        Series newSeries;
        seriesInit(&newSeries, s->name, s->type);

        for (size_t i = 0; i < nRows; i++) {
            size_t oldRow = rowIdx[i];
            switch (s->type) {
                case DF_INT: {
                    int val;
                    if (seriesGetInt(s, oldRow, &val)) {
                        seriesAddInt(&newSeries, val);
                    }
                } break;
                case DF_DOUBLE: {
                    double dval;
                    if (seriesGetDouble(s, oldRow, &dval)) {
                        seriesAddDouble(&newSeries, dval);
                    }
                } break;
                case DF_STRING: {
                    char* strVal = NULL;
                    if (seriesGetString(s, oldRow, &strVal)) {
                        seriesAddString(&newSeries, strVal);
                        free(strVal);
                    }
                } break;
                /* NEW: DF_DATETIME */
                case DF_DATETIME: {
                    long long dtVal;
                    if (seriesGetDateTime(s, oldRow, &dtVal)) {
                        seriesAddDateTime(&newSeries, dtVal);
                    }
                } break;
            }
        }
        result.addSeries(&result, &newSeries);
        seriesFree(&newSeries);
    }

    free(rowIdx);
    return result;
}

/* -------------------------------------------------------------------------
 * 7) Deduplication / Uniqueness
 * ------------------------------------------------------------------------- */

/* 
   buildRowKey now must also handle DF_DATETIME, 
   converting the long long epoch to a string
*/
static void buildRowKey(const DataFrame* df,
                        size_t row,
                        const size_t* subsetCols,
                        size_t subsetCount,
                        char* outBuf,
                        size_t bufSize)
{
    outBuf[0] = '\0';
    if (!df) return;

    for (size_t i = 0; i < subsetCount; i++) {
        size_t colIdx = subsetCols[i];
        const Series* s = df->getSeries(df, colIdx);
        if (!s) continue;

        char valBuf[128] = "";
        switch (s->type) {
            case DF_INT: {
                int ival;
                if (seriesGetInt(s, row, &ival)) {
                    snprintf(valBuf, sizeof(valBuf), "%d", ival);
                }
            } break;
            case DF_DOUBLE: {
                double dval;
                if (seriesGetDouble(s, row, &dval)) {
                    snprintf(valBuf, sizeof(valBuf), "%g", dval);
                }
            } break;
            case DF_STRING: {
                char* str = NULL;
                if (seriesGetString(s, row, &str)) {
                    strncpy(valBuf, str, sizeof(valBuf) - 1);
                    valBuf[sizeof(valBuf) - 1] = '\0';
                    free(str);
                }
            } break;
            /* NEW: DF_DATETIME => convert epoch to string */
            case DF_DATETIME: {
                long long dtVal;
                if (seriesGetDateTime(s, row, &dtVal)) {
                    snprintf(valBuf, sizeof(valBuf), "%lld", dtVal);
                }
            } break;
        }

        if (i > 0) {
            strncat(outBuf, "|", bufSize - strlen(outBuf) - 1);
        }
        strncat(outBuf, valBuf, bufSize - strlen(outBuf) - 1);
    }
}

/* ... The rest of dfDropDuplicates_impl remains the same, just ensure 
 * the copying loop includes DF_DATETIME:
 */
DataFrame dfDropDuplicates_impl(const DataFrame* df, const size_t* subsetCols, size_t subsetCount)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    size_t nCols = df->numColumns(df);
    size_t nRows = df->numRows(df);

    // If subsetCount == 0 => use all columns
    int usedAll = 0;
    size_t* allCols = NULL;
    if (subsetCount == 0) {
        usedAll = 1;
        allCols = (size_t*)malloc(nCols * sizeof(size_t));
        for (size_t i = 0; i < nCols; i++) {
            allCols[i] = i;
        }
        subsetCols = allCols;
        subsetCount = nCols;
    }

    typedef struct {
        char* key;
    } DeduKey;

    DeduKey* usedKeys = NULL;
    size_t usedSize = 0, usedCap = 0;

    size_t* keepRows = (size_t*)malloc(nRows * sizeof(size_t));
    size_t keepCount = 0;

    char rowBuf[1024];
    for (size_t r = 0; r < nRows; r++) {
        rowBuf[0] = '\0';
        buildRowKey(df, r, subsetCols, subsetCount, rowBuf, sizeof(rowBuf));

        // hasKey check
        bool found = false;
        for (size_t i = 0; i < usedSize; i++) {
            if (strcmp(usedKeys[i].key, rowBuf) == 0) {
                found = true;
                break;
            }
        }
        if (!found) {
            // add key
            if (usedSize >= usedCap) {
                usedCap = (usedCap == 0) ? 64 : usedCap * 2;
                usedKeys = (DeduKey*)realloc(usedKeys, usedCap * sizeof(DeduKey));
            }
            usedKeys[usedSize].key = strdup(rowBuf);
            usedSize++;

            keepRows[keepCount++] = r;
        }
    }

    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) continue;

        Series newS;
        seriesInit(&newS, s->name, s->type);

        for (size_t i = 0; i < keepCount; i++) {
            size_t oldRow = keepRows[i];
            switch (s->type) {
                case DF_INT: {
                    int ival;
                    if (seriesGetInt(s, oldRow, &ival)) {
                        seriesAddInt(&newS, ival);
                    }
                } break;
                case DF_DOUBLE: {
                    double dval;
                    if (seriesGetDouble(s, oldRow, &dval)) {
                        seriesAddDouble(&newS, dval);
                    }
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(s, oldRow, &str)) {
                        seriesAddString(&newS, str);
                        free(str);
                    }
                } break;
                /* NEW: DF_DATETIME */
                case DF_DATETIME: {
                    long long dtVal;
                    if (seriesGetDateTime(s, oldRow, &dtVal)) {
                        seriesAddDateTime(&newS, dtVal);
                    }
                } break;
            }
        }
        result.addSeries(&result, &newS);
        seriesFree(&newS);
    }

    for (size_t i = 0; i < usedSize; i++) {
        free(usedKeys[i].key);
    }
    free(usedKeys);
    free(keepRows);

    if (usedAll) {
        free((void*)subsetCols);
    }

    return result;
}

/*-------------------------------------------------------------------------
 * 8) Uniqueness
 * ------------------------------------------------------------------------ */

/* Similarly, dfUnique_impl can handle DF_DATETIME by converting the epoch to string. */

DataFrame dfUnique_impl(const DataFrame* df, size_t colIndex)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    size_t nCols = df->numColumns(df);
    if (colIndex >= nCols) {
        return result;
    }

    const Series* s = df->getSeries(df, colIndex);
    if (!s) {
        return result;
    }

    typedef struct {
        char* val;
    } DistItem;
    DistItem* arr = NULL;
    size_t used = 0, cap = 0;

    size_t nRows = df->numRows(df);

    for (size_t r = 0; r < nRows; r++) {
        char buf[128] = "";
        switch (s->type) {
            case DF_INT: {
                int ival;
                if (seriesGetInt(s, r, &ival)) {
                    snprintf(buf, sizeof(buf), "%d", ival);
                }
            } break;
            case DF_DOUBLE: {
                double dval;
                if (seriesGetDouble(s, r, &dval)) {
                    snprintf(buf, sizeof(buf), "%g", dval);
                }
            } break;
            case DF_STRING: {
                char* str = NULL;
                if (seriesGetString(s, r, &str)) {
                    strncpy(buf, str, sizeof(buf) - 1);
                    buf[sizeof(buf) - 1] = '\0';
                    free(str);
                }
            } break;
            /* NEW: DF_DATETIME => convert epoch to string */
            case DF_DATETIME: {
                long long dtVal;
                if (seriesGetDateTime(s, r, &dtVal)) {
                    snprintf(buf, sizeof(buf), "%lld", dtVal);
                }
            } break;
        }

        if (buf[0] == '\0') {
            // skip empty
            continue;
        }

        // check if we have it
        bool found = false;
        for (size_t i = 0; i < used; i++) {
            if (strcmp(arr[i].val, buf) == 0) {
                found = true;
                break;
            }
        }
        if (!found) {
            if (used >= cap) {
                cap = (cap == 0) ? 16 : (cap * 2);
                arr = (DistItem*)realloc(arr, cap * sizeof(DistItem));
            }
            arr[used].val = strdup(buf);
            used++;
        }
    }

    // Build a single-col DF
    Series newSeries;
    seriesInit(&newSeries, s->name, DF_STRING);

    for (size_t i = 0; i < used; i++) {
        seriesAddString(&newSeries, arr[i].val);
    }
    result.addSeries(&result, &newSeries);
    seriesFree(&newSeries);

    // Cleanup
    for (size_t i = 0; i < used; i++) {
        free(arr[i].val);
    }
    free(arr);

    return result;
}

/* -------------------------------------------------------------------------
 * 9) Transpose
 * ------------------------------------------------------------------------- */

DataFrame dfTranspose_impl(const DataFrame* df)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    size_t nCols = df->numColumns(df);
    size_t nRows = df->numRows(df);

    // We'll produce nRows columns in result
    for (size_t r = 0; r < nRows; r++) {
        char tempName[64];
        snprintf(tempName, sizeof(tempName), "Row#%zu", r);

        Series col;
        seriesInit(&col, "", DF_STRING);  
        free(col.name);
        col.name = strdup(tempName);

        // fill col with nCols entries (all strings)
        for (size_t c = 0; c < nCols; c++) {
            const Series* orig = df->getSeries(df, c);
            if (!orig) {
                seriesAddString(&col, "???");
                continue;
            }
            switch(orig->type) {
                case DF_INT: {
                    int v;
                    if (seriesGetInt(orig, r, &v)) {
                        char buf[32];
                        snprintf(buf, sizeof(buf), "%d", v);
                        seriesAddString(&col, buf);
                    } else {
                        seriesAddString(&col, "NA");
                    }
                } break;
                case DF_DOUBLE: {
                    double d;
                    if (seriesGetDouble(orig, r, &d)) {
                        char buf[64];
                        snprintf(buf, sizeof(buf), "%g", d);
                        seriesAddString(&col, buf);
                    } else {
                        seriesAddString(&col, "NA");
                    }
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(orig, r, &str)) {
                        seriesAddString(&col, str);
                        free(str);
                    } else {
                        seriesAddString(&col, "NA");
                    }
                } break;
                /* NEW: DF_DATETIME => convert to string, e.g. epoch */
                case DF_DATETIME: {
                    long long dtVal;
                    if (seriesGetDateTime(orig, r, &dtVal)) {
                        char buf[64];
                        snprintf(buf, sizeof(buf), "%lld", dtVal);
                        // or convert to human-readable date/time
                        seriesAddString(&col, buf);
                    } else {
                        seriesAddString(&col, "NA");
                    }
                } break;
            }
        }
        result.addSeries(&result, &col);
        seriesFree(&col);
    }
    return result;
}

/* -------------------------------------------------------------------------
 * 10) Searching / IndexOf
 * ------------------------------------------------------------------------- */

size_t dfIndexOf_impl(const DataFrame* df, size_t colIndex, double value)
{
    if (!df) return (size_t)-1;
    const Series* s = df->getSeries(df, colIndex);
    if (!s) return (size_t)-1;

    size_t n = seriesSize(s);
    if (s->type == DF_INT) {
        for (size_t r = 0; r < n; r++) {
            int val;
            if (seriesGetInt(s, r, &val)) {
                if ((double)val == value) {
                    return r;
                }
            }
        }
    }
    else if (s->type == DF_DOUBLE) {
        for (size_t r = 0; r < n; r++) {
            double d;
            if (seriesGetDouble(s, r, &d)) {
                if (d == value) {
                    return r;
                }
            }
        }
    }
    /* 
       NEW: DF_DATETIME => you need a separate method 
       or interpret "value" as epoch. For now, we skip 
       or you can cast double->long long
    */
    else if (s->type == DF_DATETIME) {
        long long target = (long long)value; // naive cast
        for (size_t r = 0; r < n; r++) {
            long long dtVal;
            if (seriesGetDateTime(s, r, &dtVal)) {
                if (dtVal == target) {
                    return r;
                }
            }
        }
    }
    return (size_t)-1;
}

/* -------------------------------------------------------------------------
 * 11) dfApply / dfWhere / dfExplode
 * ------------------------------------------------------------------------- */

DataFrame dfApply_impl(const DataFrame* df, RowFunction func)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df || !func) return result;

    size_t nRows = df->numRows(df);
    for (size_t r = 0; r < nRows; r++) {
        func(&result, df, r);
    }
    return result;
}

DataFrame dfWhere_impl(const DataFrame* df, RowPredicate predicate, double defaultVal)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df || !predicate) return result;

    size_t nRows = df->numRows(df);
    size_t nCols = df->numColumns(df);

    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) continue;

        Series newSeries;
        seriesInit(&newSeries, s->name, s->type);

        for (size_t r = 0; r < nRows; r++) {
            bool cond = predicate(df, r);
            if (cond) {
                switch (s->type) {
                    case DF_INT: {
                        int val;
                        if (seriesGetInt(s, r, &val)) {
                            seriesAddInt(&newSeries, val);
                        } else {
                            seriesAddInt(&newSeries, 0);
                        }
                    } break;
                    case DF_DOUBLE: {
                        double dval;
                        if (seriesGetDouble(s, r, &dval)) {
                            seriesAddDouble(&newSeries, dval);
                        } else {
                            seriesAddDouble(&newSeries, 0.0);
                        }
                    } break;
                    case DF_STRING: {
                        char* str = NULL;
                        if (seriesGetString(s, r, &str)) {
                            seriesAddString(&newSeries, str);
                            free(str);
                        } else {
                            seriesAddString(&newSeries, "");
                        }
                    } break;
                    /* NEW: DF_DATETIME => if cond is true, copy actual */
                    case DF_DATETIME: {
                        long long dtVal;
                        if (seriesGetDateTime(s, r, &dtVal)) {
                            seriesAddDateTime(&newSeries, dtVal);
                        } else {
                            // fallback => 0
                            seriesAddDateTime(&newSeries, 0LL);
                        }
                    } break;
                }
            } else {
                // set default
                switch (s->type) {
                    case DF_INT:
                        seriesAddInt(&newSeries, (int)defaultVal);
                        break;
                    case DF_DOUBLE:
                        seriesAddDouble(&newSeries, defaultVal);
                        break;
                    case DF_STRING:
                        seriesAddString(&newSeries, "NA");
                        break;
                    /* NEW: DF_DATETIME => default to 0? or some sentinel? */
                    case DF_DATETIME:
                        seriesAddDateTime(&newSeries, (long long)defaultVal); 
                        break;
                }
            }
        }
        result.addSeries(&result, &newSeries);
        seriesFree(&newSeries);
    }

    return result;
}

/**
 * dfExplode_impl:
 *   If colIndex is DF_STRING, we treat each cell as a comma-separated list => multiple rows.
 *   If colIndex is DF_DATETIME (or other), we skip exploding or just copy.
 *   For demonstration, if it's not DF_STRING, we simply return a copy of original DF. 
 *   (You could define special logic for DF_DATETIME, but it's less common.)
 */

/* Keep the rest as is, just handle DF_DATETIME in your copy code. */

typedef struct {
    char** otherCols; 
    size_t subCount;  
    char** subItems;  
} ExplodeRow;

static char* _cellToString(const Series* s, size_t rowIndex)
{
    char buffer[128];
    buffer[0] = '\0';

    switch (s->type) {
        case DF_INT: {
            int iv;
            if (seriesGetInt(s, rowIndex, &iv)) {
                snprintf(buffer, sizeof(buffer), "%d", iv);
            }
        } break;
        case DF_DOUBLE: {
            double dv;
            if (seriesGetDouble(s, rowIndex, &dv)) {
                snprintf(buffer, sizeof(buffer), "%g", dv);
            }
        } break;
        case DF_STRING: {
            char* st = NULL;
            if (seriesGetString(s, rowIndex, &st)) {
                strncpy(buffer, st, sizeof(buffer) - 1);
                buffer[sizeof(buffer) - 1] = '\0';
                free(st);
            }
        } break;
        /* NEW: DF_DATETIME => convert to string as epoch */
        case DF_DATETIME: {
            long long dtVal;
            if (seriesGetDateTime(s, rowIndex, &dtVal)) {
                snprintf(buffer, sizeof(buffer), "%lld", dtVal);
            }
        } break;
    }
    return strdup(buffer);
}

static DataFrame copyEntireDF(const DataFrame* df)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    size_t nCols = df->numColumns(df);
    size_t nRows = df->numRows(df);

    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) continue;

        Series newS;
        seriesInit(&newS, s->name, s->type);

        for (size_t r = 0; r < nRows; r++) {
            switch (s->type) {
                case DF_INT: {
                    int val;
                    if (seriesGetInt(s, r, &val)) {
                        seriesAddInt(&newS, val);
                    }
                } break;
                case DF_DOUBLE: {
                    double dv;
                    if (seriesGetDouble(s, r, &dv)) {
                        seriesAddDouble(&newS, dv);
                    }
                } break;
                case DF_STRING: {
                    char* st = NULL;
                    if (seriesGetString(s, r, &st)) {
                        seriesAddString(&newS, st);
                        free(st);
                    }
                } break;
                /* NEW: DF_DATETIME */
                case DF_DATETIME: {
                    long long dtVal;
                    if (seriesGetDateTime(s, r, &dtVal)) {
                        seriesAddDateTime(&newS, dtVal);
                    }
                } break;
            }
        }
        result.addSeries(&result, &newS);
        seriesFree(&newS);
    }
    return result;
}


/**
 * dfExplode_impl
 * 
 * If the column at `colIndex` is DF_STRING, we treat each cell as a comma-separated list.
 * We create multiple rows, one per list item. Other columns replicate the same row data.
 * If colIndex is invalid or not DF_STRING, we return a copy of the original DF (no explosion).
 */
DataFrame dfExplode_impl(const DataFrame* df, size_t colIndex)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    size_t nCols = df->numColumns(df);
    if (colIndex >= nCols) {
        // invalid => just copy entire DF
        return copyEntireDF(df);
    }

    const Series* explodeSer = df->getSeries(df, colIndex);
    if (!explodeSer) {
        // no data => copy
        return copyEntireDF(df);
    }
    // If explode column isn't string, no explosion
    if (explodeSer->type != DF_STRING) {
        return copyEntireDF(df);
    }

    // We'll store an array of ExplodeRow for each row in df.
    size_t nRows = df->numRows(df);
    ExplodeRow* rows = (ExplodeRow*)calloc(nRows, sizeof(ExplodeRow));

    // First pass: read each original row => gather "otherCols" and parse the explode col
    for (size_t r = 0; r < nRows; r++) {
        rows[r].otherCols = (char**)calloc(nCols, sizeof(char*));
        // Fill otherCols
        for (size_t c = 0; c < nCols; c++) {
            if (c == colIndex) {
                rows[r].otherCols[c] = NULL; // skip, as we parse separately
                continue;
            }
            // Convert cell to string
            const Series* s = df->getSeries(df, c);
            rows[r].otherCols[c] = _cellToString(s, r);
        }

        // parse colIndex for “explode” values
        char* mainStr = NULL;
        if (seriesGetString(explodeSer, r, &mainStr)) {
            if (!mainStr || !mainStr[0]) {
                // empty => subCount=1 => [""]
                rows[r].subCount = 1;
                rows[r].subItems = (char**)calloc(1, sizeof(char*));
                rows[r].subItems[0] = strdup("");
                if (mainStr) free(mainStr);
            } else {
                // naive split on commas
                size_t sc = 1;
                for (char* p = mainStr; *p; p++) {
                    if (*p == ',') sc++;
                }
                rows[r].subCount = sc;
                rows[r].subItems = (char**)calloc(sc, sizeof(char*));

                // in-place split on mainStr
                size_t idx = 0;
                char* tok = strtok(mainStr, ",");
                while (tok) {
                    rows[r].subItems[idx] = strdup(tok);
                    idx++;
                    tok = strtok(NULL, ",");
                }
                free(mainStr);
            }
        } else {
            // seriesGetString => false => treat as empty
            rows[r].subCount = 1;
            rows[r].subItems = (char**)calloc(1, sizeof(char*));
            rows[r].subItems[0] = strdup("");
        }
    }

    // Build an array of Series for the final DF
    // The exploded column => DF_STRING, others => same as original
    Series* outCols = (Series*)calloc(nCols, sizeof(Series));
    for (size_t c = 0; c < nCols; c++) {
        const Series* orig = df->getSeries(df, c);
        if (!orig) {
            seriesInit(&outCols[c], "unknown", DF_STRING);
            continue;
        }
        if (c == colIndex) {
            // forced DF_STRING for exploded column
            seriesInit(&outCols[c], orig->name, DF_STRING);
        } else {
            // same type
            seriesInit(&outCols[c], orig->name, orig->type);
        }
    }

    // Fill the new rows
    for (size_t r = 0; r < nRows; r++) {
        // for each sub-item in the “explode” column
        for (size_t s = 0; s < rows[r].subCount; s++) {
            // build 1 new row
            for (size_t c = 0; c < nCols; c++) {
                if (c == colIndex) {
                    // sub item => DF_STRING
                    seriesAddString(&outCols[c], rows[r].subItems[s]);
                } else {
                    // parse rows[r].otherCols[c] back to the original type
                    const Series* orig = df->getSeries(df, c);
                    if (!orig) {
                        // fallback
                        if (outCols[c].type == DF_STRING) {
                            seriesAddString(&outCols[c], "");
                        } else if (outCols[c].type == DF_INT) {
                            seriesAddInt(&outCols[c], 0);
                        } else if (outCols[c].type == DF_DATETIME) {
                            seriesAddDateTime(&outCols[c], 0LL);
                        } else {
                            seriesAddDouble(&outCols[c], 0.0);
                        }
                        continue;
                    }

                    char* cellStr = rows[r].otherCols[c];
                    if (!cellStr) cellStr = (char*)""; // fallback if somehow null

                    switch (orig->type) {
                        case DF_INT: {
                            int vi = atoi(cellStr);
                            seriesAddInt(&outCols[c], vi);
                        } break;
                        case DF_DOUBLE: {
                            double vd = atof(cellStr);
                            seriesAddDouble(&outCols[c], vd);
                        } break;
                        case DF_STRING: {
                            seriesAddString(&outCols[c], cellStr);
                        } break;
                        /* NEW: DF_DATETIME => parse the string as a 64-bit epoch */
                        case DF_DATETIME: {
                            long long dtVal = atoll(cellStr);
                            seriesAddDateTime(&outCols[c], dtVal);
                        } break;
                    }
                }
            }
        }
    }

    // Attach outCols to result
    for (size_t c = 0; c < nCols; c++) {
        result.addSeries(&result, &outCols[c]);
        seriesFree(&outCols[c]);
    }
    free(outCols);

    // free the rows data
    for (size_t r = 0; r < nRows; r++) {
        // free otherCols
        for (size_t c = 0; c < nCols; c++) {
            if (c == colIndex) continue; 
            free(rows[r].otherCols[c]);
        }
        free(rows[r].otherCols);

        // free subItems
        for (size_t s = 0; s < rows[r].subCount; s++) {
            free(rows[r].subItems[s]);
        }
        free(rows[r].subItems);
    }
    free(rows);

    return result;
}

