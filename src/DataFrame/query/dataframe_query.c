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

            seriesAddInt(&countS, (int)nRows);
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
            }
        }

        // check if s->name is in oldNames
        for (size_t i = 0; i < count; i++) {
            if (strcmp(s->name, oldNames[i]) == 0) {
                strncpy(newSeries.name, newNames[i], sizeof(newSeries.name) - 1);
                newSeries.name[sizeof(newSeries.name) - 1] = '\0';
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

/* Instead of re-typedef RowPredicate, we just use what's in dataframe.h. */

/* We'll define a helper for dfDropNA: */
static bool isRowNA(const DataFrame* df, size_t rowIndex);

/* 
   dfFilter_impl is the general filter that uses a RowPredicate 
*/
DataFrame dfFilter_impl(const DataFrame* df, RowPredicate predicate)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df || !predicate) return result;

    size_t nRows = df->numRows(df);
    size_t nCols = df->numColumns(df);

    bool* keepRow = (bool*)calloc(nRows, sizeof(bool));
    // keepCount is not used, so omit it or keep for debugging
    // size_t keepCount = 0;

    for (size_t r = 0; r < nRows; r++) {
        if (predicate(df, r)) {
            keepRow[r] = true;
            // keepCount++;
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
            }
        }
        result.addSeries(&result, &newSeries);
        seriesFree(&newSeries);
    }

    free(keepRow);
    return result;
}

/* 
   dfDropNA_impl uses dfFilter_impl, but we define a small static predicate 
   that returns !isRowNA(...) 
*/

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
        }
    }
    return false;
}

/* -------------------------------------------------------------------------
 * 4) Sorting
 * ------------------------------------------------------------------------- */

/* ------------------------------------------------------------------
 * Internal: We define a struct to hold sort context
 * and a helper function to compare row indices using that context.
 * ------------------------------------------------------------------ */
typedef struct {
    const DataFrame* df;
    size_t columnIndex;
    bool ascending;
} SortContext;

/**
 * @brief compareRowIndices
 *  Compares two row indices (ra, rb) by looking up the cell values in
 *  df->columns[columnIndex]. Respects ascending/descending order.
 *  Returns negative if ra<rb, 0 if equal, positive if ra>rb
 */
static int compareRowIndices(size_t ra, size_t rb, const SortContext* ctx)
{
    if (!ctx || !ctx->df) return 0;
    const Series* s = ctx->df->getSeries(ctx->df, ctx->columnIndex);
    if (!s) return 0;

    // Compare cells (ra, rb) in column 's'
    if (s->type == DF_INT) {
        int va, vb;
        bool gotA = seriesGetInt(s, ra, &va);
        bool gotB = seriesGetInt(s, rb, &vb);
        if (!gotA || !gotB) return 0; // fallback
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
    else {
        // DF_STRING
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
}

/**
 * @brief insertionSortRows
 *  Sorts the array of row indices in-place via insertion sort, using compareRowIndices.
 */
static void insertionSortRows(size_t* rowIdx, size_t count, const SortContext* ctx)
{
    for (size_t i = 1; i < count; i++) {
        size_t key = rowIdx[i];
        size_t j = i;
        while (j > 0) {
            int cmp = compareRowIndices(rowIdx[j - 1], key, ctx);
            if (cmp <= 0) {
                // correct spot found
                break;
            }
            rowIdx[j] = rowIdx[j - 1];
            j--;
        }
        rowIdx[j] = key;
    }
}

/* ------------------------------------------------------------------
 * The dfSort_impl function
 * ------------------------------------------------------------------ */

DataFrame dfSort_impl(const DataFrame* df, size_t columnIndex, bool ascending)
{
    // Initialize an empty result
    DataFrame result;
    DataFrame_Create(&result);

    // If df is NULL, just return the empty result
    if (!df) return result;

    // Validate columnIndex
    size_t nRows = df->numRows(df);
    size_t nCols = df->numColumns(df);
    if (columnIndex >= nCols) {
        // Invalid column index => return empty DF
        return result;
    }

    // Build a list of row indices [0..(nRows-1)]
    size_t* rowIdx = (size_t*)malloc(nRows * sizeof(size_t));
    if (!rowIdx) {
        // allocation failed => return empty
        return result;
    }
    for (size_t i = 0; i < nRows; i++) {
        rowIdx[i] = i;
    }

    // Prepare a sort context
    SortContext ctx;
    ctx.df = df;
    ctx.columnIndex = columnIndex;
    ctx.ascending = ascending;

    // Sort the row indices in-place
    insertionSortRows(rowIdx, nRows, &ctx);

    // Now build a new DataFrame by copying the rows in sorted order
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) continue;

        // Make a new Series with the same name & type
        Series newSeries;
        seriesInit(&newSeries, s->name, s->type);

        // Copy row data according to rowIdx
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
            }
        }

        // Add this new Series to the result DF
        result.addSeries(&result, &newSeries);
        seriesFree(&newSeries);
    }

    free(rowIdx);
    return result;
}

/* -------------------------------------------------------------------------
 * 5) Grouping / Aggregation
 * ------------------------------------------------------------------------- */

DataFrame dfGroupBy_impl(const DataFrame* df, size_t groupColIndex)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;
    // STUB
    return result;
}

/* -------------------------------------------------------------------------
 * 6) Pivot / Melt
 * ------------------------------------------------------------------------- */

DataFrame dfPivot_impl(const DataFrame* df, size_t indexCol, size_t columnsCol, size_t valuesCol)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;
    // STUB
    return result;
}

DataFrame dfMelt_impl(const DataFrame* df, const size_t* idCols, size_t idCount)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;
    // STUB
    return result;
}

/* -------------------------------------------------------------------------
 * 7) Deduplication / Uniqueness
 * ------------------------------------------------------------------------- */

DataFrame dfDropDuplicates_impl(const DataFrame* df, const size_t* subsetCols, size_t subsetCount)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;
    // STUB
    return result;
}

DataFrame dfUnique_impl(const DataFrame* df, size_t colIndex)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;
    // STUB
    return result;
}

/* -------------------------------------------------------------------------
 * 8) Simple Aggregations (dfSum, dfMean, dfMin, dfMax)
 * ------------------------------------------------------------------------- */

double dfSum_impl(const DataFrame* df, size_t colIndex)
{
    // Implementation same as your snippet
    if (!df) return 0.0;
    const Series* s = df->getSeries(df, colIndex);
    if (!s) return 0.0;
    double sumVal = 0.0;
    size_t nRows = seriesSize(s);
    if (s->type == DF_INT) {
        for (size_t r = 0; r < nRows; r++) {
            int v;
            if (seriesGetInt(s, r, &v)) {
                sumVal += v;
            }
        }
    }
    else if (s->type == DF_DOUBLE) {
        for (size_t r = 0; r < nRows; r++) {
            double d;
            if (seriesGetDouble(s, r, &d)) {
                sumVal += d;
            }
        }
    }
    return sumVal;
}

double dfMean_impl(const DataFrame* df, size_t colIndex)
{
    if (!df) return 0.0;
    const Series* s = df->getSeries(df, colIndex);
    if (!s) return 0.0;
    size_t n = seriesSize(s);
    if (n == 0) return 0.0;
    double total = dfSum_impl(df, colIndex);
    return total / n;
}

double dfMin_impl(const DataFrame* df, size_t colIndex)
{
    if (!df) return 0.0;
    const Series* s = df->getSeries(df, colIndex);
    if (!s) return 0.0;
    size_t n = seriesSize(s);
    if (n == 0) return 0.0;

    double minVal = 0.0;
    if (s->type == DF_INT) {
        int tmp;
        seriesGetInt(s, 0, &tmp);
        minVal = (double)tmp;
        for (size_t r = 1; r < n; r++) {
            if (seriesGetInt(s, r, &tmp)) {
                if (tmp < minVal) minVal = tmp;
            }
        }
    }
    else if (s->type == DF_DOUBLE) {
        double d;
        seriesGetDouble(s, 0, &d);
        minVal = d;
        for (size_t r = 1; r < n; r++) {
            if (seriesGetDouble(s, r, &d)) {
                if (d < minVal) minVal = d;
            }
        }
    }
    return minVal;
}

double dfMax_impl(const DataFrame* df, size_t colIndex)
{
    if (!df) return 0.0;
    const Series* s = df->getSeries(df, colIndex);
    if (!s) return 0.0;
    size_t n = seriesSize(s);
    if (n == 0) return 0.0;

    double maxVal = 0.0;
    if (s->type == DF_INT) {
        int tmp;
        seriesGetInt(s, 0, &tmp);
        maxVal = (double)tmp;
        for (size_t r = 1; r < n; r++) {
            if (seriesGetInt(s, r, &tmp)) {
                if (tmp > maxVal) maxVal = tmp;
            }
        }
    }
    else if (s->type == DF_DOUBLE) {
        double d;
        seriesGetDouble(s, 0, &d);
        maxVal = d;
        for (size_t r = 1; r < n; r++) {
            if (seriesGetDouble(s, r, &d)) {
                if (d > maxVal) maxVal = d;
            }
        }
    }
    return maxVal;
}

/* -------------------------------------------------------------------------
 * 9) Transpose
 * ------------------------------------------------------------------------- */

DataFrame dfTranspose_impl(const DataFrame* df)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    // Implementation as per your snippet
    // ...
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
    return (size_t)-1;
}

/* -------------------------------------------------------------------------
 * 11) dfApply / dfWhere / dfExplode
 * ------------------------------------------------------------------------- */

/* 
   We do NOT re-typedef RowFunction. 
   We just implement dfApply_impl. 
*/

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
                }
            }
        }
        result.addSeries(&result, &newSeries);
        seriesFree(&newSeries);
    }

    return result;
}

DataFrame dfExplode_impl(const DataFrame* df, size_t colIndex)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    // STUB
    return result;
}
