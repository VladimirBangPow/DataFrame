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

/**
 * We'll store an array of (key, count).
 */
typedef struct {
    char* key;
    size_t count;
} GroupItem;

/**
 * We'll keep 'items', 'size', 'capacity' as file-static or 
 * pass them around as parameters. 
 * Typically you'd store them in a struct. 
 * We'll do the latter here: a GroupContext.
 */
typedef struct {
    GroupItem* items;
    size_t size;
    size_t capacity;
} GroupContext;

/**
 * @brief ensureCapacity
 * Helper to expand the dynamic array if needed.
 */
static void ensureCapacity(GroupContext* ctx)
{
    if (!ctx) return;
    if (ctx->size >= ctx->capacity) {
        ctx->capacity = (ctx->capacity == 0) ? 8 : ctx->capacity * 2;
        ctx->items = (GroupItem*)realloc(ctx->items, ctx->capacity * sizeof(GroupItem));
    }
}

/**
 * @brief findOrCreateItem
 *  Linear search for 'key', or create a new item with count=0.
 *  Returns pointer to the found or newly-created GroupItem.
 */
static GroupItem* findOrCreateItem(GroupContext* ctx, const char* key)
{
    if (!ctx) return NULL;
    // linear search
    for (size_t i = 0; i < ctx->size; i++) {
        if (strcmp(ctx->items[i].key, key) == 0) {
            return &ctx->items[i];
        }
    }
    // not found -> create new
    ensureCapacity(ctx);
    ctx->items[ctx->size].key   = strdup(key);
    ctx->items[ctx->size].count = 0;
    ctx->size++;
    return &ctx->items[ctx->size - 1];
}

/**
 * @brief dfGroupBy_impl
 * Group the DataFrame by a single column (`groupColIndex`), returning a new
 * DataFrame with columns ["group", "count"] for each unique value.
 */
DataFrame dfGroupBy_impl(const DataFrame* df, size_t groupColIndex)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    size_t nCols = df->numColumns(df);
    if (groupColIndex >= nCols) {
        // invalid column
        return result;
    }

    const Series* groupSeries = df->getSeries(df, groupColIndex);
    if (!groupSeries) {
        // no data
        return result;
    }

    // We'll store our items in a local GroupContext
    GroupContext ctx;
    ctx.items    = NULL;
    ctx.size     = 0;
    ctx.capacity = 0;

    size_t nRows = df->numRows(df);

    for (size_t r = 0; r < nRows; r++) {
        char buffer[128];
        buffer[0] = '\0';

        // Convert the cell to string
        switch (groupSeries->type) {
            case DF_INT: {
                int val;
                if (seriesGetInt(groupSeries, r, &val)) {
                    snprintf(buffer, sizeof(buffer), "%d", val);
                }
            } break;
            case DF_DOUBLE: {
                double d;
                if (seriesGetDouble(groupSeries, r, &d)) {
                    snprintf(buffer, sizeof(buffer), "%g", d);
                }
            } break;
            case DF_STRING: {
                char* str = NULL;
                if (seriesGetString(groupSeries, r, &str)) {
                    strncpy(buffer, str, sizeof(buffer) - 1);
                    buffer[sizeof(buffer) - 1] = '\0';
                    free(str);
                }
            } break;
        }

        // find or create item
        GroupItem* gi = findOrCreateItem(&ctx, buffer);
        if (gi) {
            gi->count++;
        }
    }

    // Build a result DataFrame with columns: "group" (string) and "count" (int)
    Series groupCol, countCol;
    seriesInit(&groupCol, "group", DF_STRING);
    seriesInit(&countCol, "count", DF_INT);

    // Add each item
    for (size_t i = 0; i < ctx.size; i++) {
        seriesAddString(&groupCol, ctx.items[i].key);
        seriesAddInt(&countCol, (int)ctx.items[i].count);
    }

    // Add them to the result
    result.addSeries(&result, &groupCol);
    result.addSeries(&result, &countCol);

    seriesFree(&groupCol);
    seriesFree(&countCol);

    // free memory
    for (size_t i = 0; i < ctx.size; i++) {
        free(ctx.items[i].key);
    }
    free(ctx.items);

    return result;
}

/* -------------------------------------------------------------------------
 * 6) Pivot / Melt
 * ------------------------------------------------------------------------- */

/**
 * @brief pivotAddIndexKey
 *  Manages a dynamic array of indexKeys. If 'key' not present, add it.
 */
static void pivotAddIndexKey(char*** indexKeys, size_t* iCount, size_t* iCap, const char* key)
{
    // linear search
    for (size_t i = 0; i < *iCount; i++) {
        if (strcmp((*indexKeys)[i], key) == 0) {
            return; // already present
        }
    }
    // not found -> append
    if (*iCount >= *iCap) {
        *iCap = (*iCap == 0) ? 8 : (*iCap * 2);
        *indexKeys = (char**)realloc(*indexKeys, (*iCap) * sizeof(char*));
    }
    (*indexKeys)[*iCount] = strdup(key);
    (*iCount)++;
}

/**
 * @brief pivotAddColKey
 *  Similar for the "columns" keys array.
 */
static void pivotAddColKey(char*** colKeys, size_t* cCount, size_t* cCap, const char* key)
{
    // linear search
    for (size_t i = 0; i < *cCount; i++) {
        if (strcmp((*colKeys)[i], key) == 0) {
            return;
        }
    }
    if (*cCount >= *cCap) {
        *cCap = (*cCap == 0) ? 8 : (*cCap * 2);
        *colKeys = (char**)realloc(*colKeys, (*cCap) * sizeof(char*));
    }
    (*colKeys)[*cCount] = strdup(key);
    (*cCount)++;
}

/**
 * We'll store each row's (indexValue, columnsValue, valuesValue) in pivotData.
 */
typedef struct {
    char* idx; 
    char* col;
    char* val;
} PivotItem;

/**
 * @brief pivotPush
 *  Append a new (idx,col,val) triple to pivotData array
 */
static void pivotPush(PivotItem** pivotData, size_t* pUsed, size_t* pCap, 
                      const char* iK, const char* cK, const char* vK)
{
    if (*pUsed >= *pCap) {
        *pCap = (*pCap == 0) ? 64 : (*pCap * 2);
        *pivotData = (PivotItem*)realloc(*pivotData, (*pCap) * sizeof(PivotItem));
    }
    (*pivotData)[*pUsed].idx = strdup(iK);
    (*pivotData)[*pUsed].col = strdup(cK);
    (*pivotData)[*pUsed].val = strdup(vK);
    (*pUsed)++;
}

/**
 * @brief pivotCellToString
 *  Convert the cell at rowIndex in 's' to a string in buf[]. If none found, buf[0] = '\0'.
 */
static void pivotCellToString(const Series* s, size_t rowIndex, char* buf, size_t bufSize)
{
    buf[0] = '\0';
    if (!s) return;
    switch (s->type) {
        case DF_INT: {
            int ival;
            if (seriesGetInt(s, rowIndex, &ival)) {
                snprintf(buf, bufSize, "%d", ival);
            }
        } break;
        case DF_DOUBLE: {
            double dval;
            if (seriesGetDouble(s, rowIndex, &dval)) {
                snprintf(buf, bufSize, "%g", dval);
            }
        } break;
        case DF_STRING: {
            char* str = NULL;
            if (seriesGetString(s, rowIndex, &str)) {
                strncpy(buf, str, bufSize - 1);
                buf[bufSize - 1] = '\0';
                free(str);
            }
        } break;
    }
}

/* 
   ========================
   Actual dfPivot_impl
   ========================
*/
DataFrame dfPivot_impl(const DataFrame* df, size_t indexCol, size_t columnsCol, size_t valuesCol)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    size_t nC = df->numColumns(df);
    if (indexCol >= nC || columnsCol >= nC || valuesCol >= nC) {
        // invalid indexes => return empty
        return result;
    }

    const Series* idxS = df->getSeries(df, indexCol);
    const Series* colS = df->getSeries(df, columnsCol);
    const Series* valS = df->getSeries(df, valuesCol);
    if (!idxS || !colS || !valS) {
        // any missing => return empty
        return result;
    }

    size_t nRows = df->numRows(df);

    // 1) gather distinct index keys, distinct column keys
    char** indexKeys = NULL;
    size_t iCount = 0, iCap = 0;

    char** colKeys = NULL;
    size_t cCount = 0, cCap = 0;

    // 2) pivotData = array of (idx,col,val)
    PivotItem* pData = NULL;
    size_t pUsed = 0, pCap2 = 0;

    char bufIdx[128], bufCol[128], bufVal[128];

    // read each row once for index & col keys
    for (size_t r = 0; r < nRows; r++) {
        pivotCellToString(idxS, r, bufIdx, sizeof(bufIdx));
        pivotAddIndexKey(&indexKeys, &iCount, &iCap, bufIdx);

        pivotCellToString(colS, r, bufCol, sizeof(bufCol));
        pivotAddColKey(&colKeys, &cCount, &cCap, bufCol);
    }

    // read again, building pivotData
    // Actually we can do it in the same pass, but let's keep it simple
    for (size_t r = 0; r < nRows; r++) {
        pivotCellToString(idxS, r, bufIdx, sizeof(bufIdx));
        pivotCellToString(colS, r, bufCol, sizeof(bufCol));
        pivotCellToString(valS, r, bufVal, sizeof(bufVal));
        pivotPush(&pData, &pUsed, &pCap2, bufIdx, bufCol, bufVal);
    }

    // 3) Build the result with 1 + cCount columns => "index", then each colKey as DF_STRING
    // The row count = iCount
    // For each indexKeys[i], we fill row i.
    // For each colKeys[j], we find if pivotData has that (idx, col) => fill the cell.

    // First, create "index" column
    Series indexSeries;
    seriesInit(&indexSeries, "index", DF_STRING);

    // Then create cCount columns
    Series* pivotCols = (Series*)calloc(cCount, sizeof(Series));
    for (size_t j = 0; j < cCount; j++) {
        seriesInit(&pivotCols[j], colKeys[j], DF_STRING);
    }

    // For each indexKey => row i
    for (size_t i = 0; i < iCount; i++) {
        // add indexKeys[i] to indexSeries
        seriesAddString(&indexSeries, indexKeys[i]);

        // for each pivot column => try to find pivotData
        for (size_t j = 0; j < cCount; j++) {
            // default is ""
            const char* fillVal = "";
            // search pData
            for (size_t p = 0; p < pUsed; p++) {
                if (strcmp(pData[p].idx, indexKeys[i]) == 0 &&
                    strcmp(pData[p].col, colKeys[j]) == 0) {
                    fillVal = pData[p].val;
                    break;
                }
            }
            seriesAddString(&pivotCols[j], fillVal);
        }
    }

    // attach them to result
    result.addSeries(&result, &indexSeries);
    seriesFree(&indexSeries);

    for (size_t j = 0; j < cCount; j++) {
        result.addSeries(&result, &pivotCols[j]);
        seriesFree(&pivotCols[j]);
    }
    free(pivotCols);

    // free arrays
    for (size_t i = 0; i < iCount; i++) {
        free(indexKeys[i]);
    }
    free(indexKeys);

    for (size_t j = 0; j < cCount; j++) {
        free(colKeys[j]);
    }
    free(colKeys);

    // free pData
    for (size_t p = 0; p < pUsed; p++) {
        free(pData[p].idx);
        free(pData[p].col);
        free(pData[p].val);
    }
    free(pData);

    return result;
}



/* 
   ========================
   MELT FUNCTIONS
   ========================
*/

/**
 * @brief isIdColumn
 *  Returns true if 'colIndex' is in the idCols[] array, false otherwise.
 */
static bool isIdColumn(size_t colIndex, const size_t* idCols, size_t idCount)
{
    for (size_t i = 0; i < idCount; i++) {
        if (idCols[i] == colIndex) {
            return true;
        }
    }
    return false;
}

/**
 * @brief cellToString
 *  Convert the cell at rowIndex in Series s -> string stored in buf[].
 */
static void cellToString(const Series* s, size_t rowIndex, char* buf, size_t bufSize)
{
    buf[0] = '\0';
    if (!s) return;
    switch (s->type) {
        case DF_INT: {
            int v;
            if (seriesGetInt(s, rowIndex, &v)) {
                snprintf(buf, bufSize, "%d", v);
            }
        } break;
        case DF_DOUBLE: {
            double d;
            if (seriesGetDouble(s, rowIndex, &d)) {
                snprintf(buf, bufSize, "%g", d);
            }
        } break;
        case DF_STRING: {
            char* st = NULL;
            if (seriesGetString(s, rowIndex, &st)) {
                strncpy(buf, st, bufSize - 1);
                buf[bufSize - 1] = '\0';
                free(st);
            }
        } break;
    }
}

/**
 * @brief dfMelt_impl
 *  Convert the wide data into a long format with columns: 
 *    [ idCol1, idCol2, ..., variable (string), value (string) ]
 */
DataFrame dfMelt_impl(const DataFrame* df, const size_t* idCols, size_t idCount)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    size_t nCols = df->numColumns(df);
    size_t nRows = df->numRows(df);
    if (nCols == 0 || nRows == 0) {
        return result;  // empty or no data => result also empty
    }

    // We'll gather the "non-id" columns => these become "melted" into variable/value pairs.
    // We'll build an array of all "value" col indices
    size_t* valCols = (size_t*)malloc(sizeof(size_t) * nCols);
    size_t valCount = 0;

    for (size_t c = 0; c < nCols; c++) {
        if (!isIdColumn(c, idCols, idCount)) {
            valCols[valCount] = c;
            valCount++;
        }
    }

    // We'll produce a new DataFrame. 
    // The new DF has idCount columns with the same type as original, 
    // plus 1 column "variable" (DF_STRING), plus 1 column "value" (DF_STRING).

    // Let's create an array of Series for the ID columns so we can fill them as we build the melted rows.
    // We'll do that after we figure out how many total rows we get: totalRows = nRows * valCount 
    // Because each of the valCount columns becomes "stacked" for each row.

    // We want to store all the "melted" data in memory, then push to the Series. 
    // Alternatively, we can do it row by row.

    // We'll do row by row approach:
    // For each row r in [0..nRows-1]:
    //   - read the ID col values 
    //   - for each valCol => create 1 new row in result => copy ID col values, "variable"= colName, "value"= cellValue

    // Prepare to build Series in 'result'
    // 1) For each ID col, we'll create a Series with same name & type as original
    Series* outIdSeries = (Series*)calloc(idCount, sizeof(Series));
    for (size_t i = 0; i < idCount; i++) {
        const Series* s = df->getSeries(df, idCols[i]);
        if (s) {
            seriesInit(&outIdSeries[i], s->name, s->type);
        } else {
            // fallback
            seriesInit(&outIdSeries[i], "UNKNOWN_ID", DF_STRING);
        }
    }

    // 2) variable & value columns => both DF_STRING
    Series varSeries, valSeries;
    seriesInit(&varSeries, "variable", DF_STRING);
    seriesInit(&valSeries, "value", DF_STRING);

    // We'll iterate all rows r. For each row, each valCol => produce 1 new row in the melted DF.
    // So total new rows = nRows * valCount
    // We'll just do appends in the same order.

    for (size_t r = 0; r < nRows; r++) {
        // read ID col values
        // We'll store them as local variables for numeric, or as string for string. 
        // But we need to do it for each "sub-row" we create. 
        // Easiest approach is to fill them each time. 
        // We'll store them as "pending" additions in an array, so we can do repeated appends.

        // We'll store them as strings or numeric. If the ID column is numeric, we do numeric appends; if string, we do string appends.
        // We'll do an array for the numeric or string approach. 
        // Actually, let's do direct appends on each valCol iteration, though that means repeating the copying.

        // Actually simpler is: for each valCol, we do:
        //   - for each ID col => read & add
        //   - add var, add val
        // That means we do the "row" building in the series themselves. 
        // Because each ID column might have a different type.

        for (size_t vc = 0; vc < valCount; vc++) {
            size_t realCol = valCols[vc];
            const Series* meltS = df->getSeries(df, realCol);
            if (!meltS) continue;

            // Step A: push ID col values for this new row
            for (size_t i = 0; i < idCount; i++) {
                const Series* idSer = df->getSeries(df, idCols[i]);
                if (!idSer) {
                    // fallback
                    if (outIdSeries[i].type == DF_STRING) {
                        seriesAddString(&outIdSeries[i], "");
                    } else if (outIdSeries[i].type == DF_INT) {
                        seriesAddInt(&outIdSeries[i], 0);
                    } else { // DF_DOUBLE
                        seriesAddDouble(&outIdSeries[i], 0.0);
                    }
                    continue;
                }

                // We match the type
                switch (idSer->type) {
                    case DF_INT: {
                        int v;
                        if (seriesGetInt(idSer, r, &v)) {
                            seriesAddInt(&outIdSeries[i], v);
                        } else {
                            seriesAddInt(&outIdSeries[i], 0);
                        }
                    } break;
                    case DF_DOUBLE: {
                        double d;
                        if (seriesGetDouble(idSer, r, &d)) {
                            seriesAddDouble(&outIdSeries[i], d);
                        } else {
                            seriesAddDouble(&outIdSeries[i], 0.0);
                        }
                    } break;
                    case DF_STRING: {
                        char* st = NULL;
                        if (seriesGetString(idSer, r, &st)) {
                            seriesAddString(&outIdSeries[i], st);
                            free(st);
                        } else {
                            seriesAddString(&outIdSeries[i], "");
                        }
                    } break;
                }
            }

            // Step B: push "variable" => meltS->name
            seriesAddString(&varSeries, meltS->name);

            // Step C: push "value" => read the cell from meltS at row r, convert to string
            // We'll do a local buffer approach
            char cellBuf[128] = "";
            cellToString(meltS, r, cellBuf, sizeof(cellBuf));
            seriesAddString(&valSeries, cellBuf);
        }
    }

    // Now we attach the ID columns to result, then var & val
    for (size_t i = 0; i < idCount; i++) {
        result.addSeries(&result, &outIdSeries[i]);
        seriesFree(&outIdSeries[i]);
    }
    free(outIdSeries);

    result.addSeries(&result, &varSeries);
    result.addSeries(&result, &valSeries);
    seriesFree(&varSeries);
    seriesFree(&valSeries);

    free(valCols);

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
