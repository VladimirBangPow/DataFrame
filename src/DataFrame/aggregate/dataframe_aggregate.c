#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdbool.h>
#include "../dataframe.h"  // Make sure this has RowPredicate, RowFunction, etc.


//GroupBy
//Sum
//Min
//Max
//Mean
//Count
//Median
//Stddev
//Variance
//quantiles


/* -------------------------------------------------------------------------
 * Grouping / Aggregation
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
 * Simple Aggregations (dfSum, dfMean, dfMin, dfMax)
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
 