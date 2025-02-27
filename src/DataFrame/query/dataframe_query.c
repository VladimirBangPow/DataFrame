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
                free(newSeries.name); // free the old name (allocated to size of the old column's name)
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
 * 7) Deduplication / Uniqueness
 * ------------------------------------------------------------------------- */

/**
 * Helper: buildRowKey
 * Given a row r, create a string key from the columns in subsetCols.
 * For example: "colVal1|colVal2|..."
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

        // Convert cell to string
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
        }

        if (i > 0) {
            strncat(outBuf, "|", bufSize - strlen(outBuf) - 1);
        }
        strncat(outBuf, valBuf, bufSize - strlen(outBuf) - 1);
    }
}

/**
 * We'll store a dynamic array of keys we've seen, and skip row r if the key is already present.
 */
typedef struct {
    char* key;
} DeduKey;

/**
 * hasKey / addKey: to check if a rowKey is in the array or not.
 */
static bool hasKey(DeduKey* arr, size_t used, const char* key)
{
    for (size_t i = 0; i < used; i++) {
        if (strcmp(arr[i].key, key) == 0) {
            return true;
        }
    }
    return false;
}

static void addKey(DeduKey** arr, size_t* used, size_t* cap, const char* key)
{
    if (*used >= *cap) {
        *cap = (*cap == 0) ? 64 : (*cap * 2);
        *arr = (DeduKey*)realloc(*arr, (*cap) * sizeof(DeduKey));
    }
    (*arr)[*used].key = strdup(key);
    (*used)++;
}

/**
 * dfDropDuplicates_impl
 * Keep the first occurrence of each unique row key (formed from subsetCols).
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

    // We'll store an array of (key) to track which row keys we've seen
    DeduKey* usedKeys = NULL;
    size_t usedSize = 0, usedCap = 0;

    // We'll store the row indices that pass the filter in a local array
    size_t* keepRows = (size_t*)malloc(nRows * sizeof(size_t));
    size_t keepCount = 0;

    // For each row, build a key from subsetCols => check if we have it
    char rowBuf[1024];
    for (size_t r = 0; r < nRows; r++) {
        rowBuf[0] = '\0';
        buildRowKey(df, r, subsetCols, subsetCount, rowBuf, sizeof(rowBuf));

        if (!hasKey(usedKeys, usedSize, rowBuf)) {
            // first time => keep row
            addKey(&usedKeys, &usedSize, &usedCap, rowBuf);
            keepRows[keepCount] = r;
            keepCount++;
        }
    }

    // Build the new DF from the kept rows
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) continue;

        // create a new Series with same name/type
        Series newS;
        seriesInit(&newS, s->name, s->type);

        // copy the kept rows
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
            }
        }

        // add to result
        result.addSeries(&result, &newS);
        seriesFree(&newS);
    }

    // cleanup
    for (size_t i = 0; i < usedSize; i++) {
        free(usedKeys[i].key);
    }
    free(usedKeys);
    free(keepRows);

    if (usedAll) {
        free((void*)subsetCols); // the allCols we allocated
    }

    return result;
}

/*-------------------------------------------------------------------------
 * 8) Uniqueness
 * ------------------------------------------------------------------------ */


/**
 * Internal helper: track distinct string values in a dynamic array
 */
typedef struct {
    char* val;
} DistItem;

/**
 * Check if val is in the array, linear search
 */
static bool hasVal(DistItem* arr, size_t used, const char* val)
{
    for (size_t i = 0; i < used; i++) {
        if (strcmp(arr[i].val, val) == 0) {
            return true;
        }
    }
    return false;
}

static void addVal(DistItem** arr, size_t* used, size_t* cap, const char* val)
{
    if (*used >= *cap) {
        *cap = (*cap == 0) ? 16 : (*cap * 2);
        *arr = (DistItem*)realloc(*arr, (*cap) * sizeof(DistItem));
    }
    (*arr)[*used].val = strdup(val);
    (*used)++;
}

/**
 * dfUnique_impl:
 *  Returns a new DataFrame with exactly one column (DF_STRING) containing
 *  the distinct values of the specified colIndex, in order of first appearance.
 */
DataFrame dfUnique_impl(const DataFrame* df, size_t colIndex)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result; // return empty

    size_t nCols = df->numColumns(df);
    if (colIndex >= nCols) {
        // invalid col => return empty
        return result;
    }

    const Series* s = df->getSeries(df, colIndex);
    if (!s) {
        // no such column => empty
        return result;
    }

    // We'll gather distinct values in DistItem array
    DistItem* arr = NULL;
    size_t used = 0, cap = 0;

    size_t nRows = df->numRows(df);
    char buf[128];

    // For each row, read cell -> convert to string -> if not present, add
    for (size_t r = 0; r < nRows; r++) {
        buf[0] = '\0';

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
        }

        if (buf[0] != '\0' && !hasVal(arr, used, buf)) {
            addVal(&arr, &used, &cap, buf);
        }
    }

    // Now build a single-col DataFrame with the same column name as s->name, but DF_STRING
    Series newSeries;
    seriesInit(&newSeries, s->name, DF_STRING);

    for (size_t i = 0; i < used; i++) {
        seriesAddString(&newSeries, arr[i].val);
    }

    result.addSeries(&result, &newSeries);
    seriesFree(&newSeries);

    // free memory
    for (size_t i = 0; i < used; i++) {
        free(arr[i].val);
    }
    free(arr);

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
 
     size_t nCols = df->numColumns(df);
     size_t nRows = df->numRows(df);
 
     // We'll produce nRows columns in result
     for (size_t r = 0; r < nRows; r++) {
         // 1) Build a name on stack
         char tempName[64];
         snprintf(tempName, sizeof(tempName), "Row#%zu", r);
 
         // 2) Instead of passing the stack array to seriesInit, do:
         Series col;
         seriesInit(&col, "", DF_STRING);  // start with an empty name
         // Free the col.name if allocated, then replace with a heap copy of tempName
         free(col.name);
         col.name = strdup(tempName);  // dynamic copy
 
         // Now fill col with nCols entries (all strings)
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
             }
         }
         // Add to result
         result.addSeries(&result, &col);
         seriesFree(&col);  // result keeps its own copy
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



/**
 * dfExplode_impl
 * 
 * If the column at `colIndex` is DF_STRING, we treat each cell as a comma-separated list.
 * We create multiple rows, one per list item. Other columns replicate the same row data.
 * If colIndex is invalid or not DF_STRING, we return a copy of the original DF (no explosion).
 */

/**
 * We'll create a small struct to hold, for each original row:
 *  - all other columns' data as strings
 *  - the "explode" column's data split into items subItems[]
 */
typedef struct {
    char** otherCols;     // array of string data for nCols columns, but for colIndex we store NULL
    size_t subCount;      // how many splitted items from the explode column
    char** subItems;      // subCount items
} ExplodeRow;

/**
 * We'll define a helper to read a cell in `Series s` at row r, convert it to string (for numeric), or copy it if DF_STRING.
 */
static char* _cellToString(const Series* s, size_t rowIndex)
{
    if (!s) return strdup("");
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
                // copy into buffer if it fits
                strncpy(buffer, st, sizeof(buffer) - 1);
                buffer[sizeof(buffer) - 1] = '\0';
                free(st);
            }
        } break;
    }
    // Return a heap-allocated copy
    return strdup(buffer);
}

/**
 * We'll define a function to copy the entire DF if colIndex is invalid or not string.
 */
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
    if (explodeSer->type != DF_STRING) {
        // not a string => no explosion
        return copyEntireDF(df);
    }

    // We'll store an array of ExplodeRow for each row in df. 
    // Then build the final DF from that.
    size_t nRows = df->numRows(df);
    ExplodeRow* rows = (ExplodeRow*)calloc(nRows, sizeof(ExplodeRow));

    // First pass: read each original row => gather "otherCols" and parse the explode col
    for (size_t r = 0; r < nRows; r++) {
        rows[r].otherCols = (char**)calloc(nCols, sizeof(char*));
        // fill otherCols
        for (size_t c = 0; c < nCols; c++) {
            if (c == colIndex) {
                rows[r].otherCols[c] = NULL; // we'll parse separately
                continue;
            }
            // convert cell to string
            const Series* s = df->getSeries(df, c);
            rows[r].otherCols[c] = _cellToString(s, r);
        }

        // parse colIndex
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

                // do an in-place split on mainStr
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
            // seriesGetString => false => no data => treat as empty
            rows[r].subCount = 1;
            rows[r].subItems = (char**)calloc(1, sizeof(char*));
            rows[r].subItems[0] = strdup("");
        }
    }

    // Next, we know how many new rows we'll produce: sum of all subCounts
    // We'll build an array of Series for the final DF, one for each column. 
    // For colIndex => DF_STRING. Others => same type as original.

    Series* outCols = (Series*)calloc(nCols, sizeof(Series));
    // initialize them
    for (size_t c = 0; c < nCols; c++) {
        const Series* orig = df->getSeries(df, c);
        if (!orig) {
            // fallback
            seriesInit(&outCols[c], "unknown", DF_STRING);
            continue;
        }
        if (c == colIndex) {
            // the "exploded" col is forced DF_STRING
            seriesInit(&outCols[c], orig->name, DF_STRING);
        } else {
            // same type
            seriesInit(&outCols[c], orig->name, orig->type);
        }
    }

    // Fill the new rows
    for (size_t r = 0; r < nRows; r++) {
        // for each sub item
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
                        } else {
                            seriesAddDouble(&outCols[c], 0.0);
                        }
                        continue;
                    }

                    switch (orig->type) {
                        case DF_INT: {
                            int vi = atoi(rows[r].otherCols[c]);
                            seriesAddInt(&outCols[c], vi);
                        } break;
                        case DF_DOUBLE: {
                            double vd = atof(rows[r].otherCols[c]);
                            seriesAddDouble(&outCols[c], vd);
                        } break;
                        case DF_STRING: {
                            seriesAddString(&outCols[c], rows[r].otherCols[c]);
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
