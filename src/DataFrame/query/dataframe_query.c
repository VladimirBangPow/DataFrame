#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdbool.h>
#include "../dataframe.h"  // must already define RowPredicate, RowFunction, etc.

/* 
   ============
   HEAD + TAIL
   ============
*/

/**
 * @brief Return a new DataFrame containing the first `n` rows of `df`.
 */
DataFrame dfHead_impl(const DataFrame* df, size_t n)
{
    DataFrame result;
    DataFrame_Create(&result);

    if (!df) {
        return result; // empty
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
   ============
   DESCRIBE
   ============
*/
DataFrame dfDescribe_impl(const DataFrame* df)
{
    DataFrame result;
    DataFrame_Create(&result);

    if (!df) return result;

    size_t nCols = df->numColumns(df);
    size_t nRows = df->numRows(df);

    // We'll create 5 columns: colName, count, min, max, mean
    Series colNameS, countS, minS, maxS, meanS;
    seriesInit(&colNameS, "colName", DF_STRING);
    seriesInit(&countS,   "count",   DF_INT);
    seriesInit(&minS,     "min",     DF_DOUBLE);
    seriesInit(&maxS,     "max",     DF_DOUBLE);
    seriesInit(&meanS,    "mean",    DF_DOUBLE);

    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) continue;

        seriesAddString(&colNameS, s->name);

        if ((s->type == DF_INT || s->type == DF_DOUBLE) && nRows > 0) {
            // gather stats
            double minVal, maxVal, sumVal;
            if (s->type == DF_INT) {
                int temp;
                seriesGetInt(s, 0, &temp);
                minVal = maxVal = sumVal = (double)temp;
            } else {
                double temp;
                seriesGetDouble(s, 0, &temp);
                minVal = maxVal = sumVal = temp;
            }

            for (size_t r = 1; r < nRows; r++) {
                double d = 0.0;
                if (s->type == DF_INT) {
                    int ti;
                    if (seriesGetInt(s, r, &ti)) {
                        d = (double)ti;
                    }
                } else {
                    double td;
                    if (seriesGetDouble(s, r, &td)) {
                        d = td;
                    }
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
            seriesAddInt(&countS, (int)nRows);
            seriesAddDouble(&minS,  0.0);
            seriesAddDouble(&maxS,  0.0);
            seriesAddDouble(&meanS, 0.0);
        }
        else {
            // no rows => everything is 0
            seriesAddInt(&countS, 0);
            seriesAddDouble(&minS,  0.0);
            seriesAddDouble(&maxS,  0.0);
            seriesAddDouble(&meanS, 0.0);
        }
    }

    // Add all columns to result
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

/* 
   =====================================
   1) Row Slicing / Sampling
   =====================================
*/
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
        // just copy entire DF
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

/* 
   =====================================
   2) Column Subsets / Manipulation
   =====================================
*/
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
                    double d;
                    if (seriesGetDouble(s, r, &d)) {
                        seriesAddDouble(&newSeries, d);
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

        // rename if s->name is in oldNames
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

/* 
   =====================================
   3) Filtering Rows
   =====================================
*/

/* A static helper for dfDropNA_impl */
static bool isRowNA(const DataFrame* df, size_t rowIndex)
{
    // Example: numeric=0 or string=""
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
                double d;
                if (seriesGetDouble(s, rowIndex, &d)) {
                    if (d == 0.0) {
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

/* General filter with a RowPredicate */
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

    // copy rows
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
                    double d;
                    if (seriesGetDouble(s, r, &d)) {
                        seriesAddDouble(&newSeries, d);
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

/* A static predicate for "dropNA": keep row if !isRowNA(...) */
static bool dropNA_predicate(const DataFrame* df, size_t rowIdx)
{
    return !isRowNA(df, rowIdx);
}

DataFrame dfDropNA_impl(const DataFrame* df)
{
    if (!df) {
        DataFrame empty;
        DataFrame_Create(&empty);
        return empty;
    }
    // Filter out rows that have "NA"
    return dfFilter_impl(df, dropNA_predicate);
}

/* 
   =====================================
   4) Sorting
   =====================================
*/
DataFrame dfSort_impl(const DataFrame* df, size_t columnIndex, bool ascending)
{
    (void)ascending; // to silence "unused param"
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;
    // STUB - do sorting logic
    return result;
}

/* 
   =====================================
   5) Grouping
   =====================================
*/
DataFrame dfGroupBy_impl(const DataFrame* df, size_t groupColIndex)
{
    (void)groupColIndex;
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;
    // STUB
    return result;
}

/* 
   =====================================
   6) Pivot / Melt
   =====================================
*/
DataFrame dfPivot_impl(const DataFrame* df, size_t indexCol, size_t columnsCol, size_t valuesCol)
{
    (void)indexCol; (void)columnsCol; (void)valuesCol;
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;
    // STUB
    return result;
}
DataFrame dfMelt_impl(const DataFrame* df, const size_t* idCols, size_t idCount)
{
    (void)idCols; (void)idCount;
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;
    // STUB
    return result;
}

/* 
   =====================================
   7) Deduplication / Uniqueness
   =====================================
*/
DataFrame dfDropDuplicates_impl(const DataFrame* df, const size_t* subsetCols, size_t subsetCount)
{
    (void)subsetCols; (void)subsetCount;
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;
    // STUB
    return result;
}

DataFrame dfUnique_impl(const DataFrame* df, size_t colIndex)
{
    (void)colIndex;
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;
    // STUB
    return result;
}

/* 
   =====================================
   8) Simple Aggregations (sum,mean,min,max)
   =====================================
*/

double dfSum_impl(const DataFrame* df, size_t colIndex)
{
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
    } else if (s->type == DF_DOUBLE) {
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
    return total / (double)n;
}

double dfMin_impl(const DataFrame* df, size_t colIndex)
{
    if (!df) return 0.0;
    const Series* s = df->getSeries(df, colIndex);
    if (!s) return 0.0;

    size_t n = seriesSize(s);
    if (n == 0) return 0.0;

    double minVal;
    if (s->type == DF_INT) {
        int tmp;
        seriesGetInt(s, 0, &tmp);
        minVal = tmp;
        for (size_t r = 1; r < n; r++) {
            if (seriesGetInt(s, r, &tmp)) {
                if (tmp < minVal) minVal = tmp;
            }
        }
    } else {
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

    double maxVal;
    if (s->type == DF_INT) {
        int tmp;
        seriesGetInt(s, 0, &tmp);
        maxVal = tmp;
        for (size_t r = 1; r < n; r++) {
            if (seriesGetInt(s, r, &tmp)) {
                if (tmp > maxVal) maxVal = tmp;
            }
        }
    } else {
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

/* 
   =====================================
   9) Transpose
   =====================================
*/
DataFrame dfTranspose_impl(const DataFrame* df)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    // STUB or implement your logic
    return result;
}

/* 
   =====================================
   10) Searching / IndexOf
   =====================================
*/
size_t dfIndexOf_impl(const DataFrame* df, size_t colIndex, double value)
{
    if (!df) return (size_t)-1;
    const Series* s = df->getSeries(df, colIndex);
    if (!s) return (size_t)-1;

    size_t n = seriesSize(s);
    if (s->type == DF_INT) {
        for (size_t r = 0; r < n; r++) {
            int v;
            if (seriesGetInt(s, r, &v)) {
                if ((double)v == value) {
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

/* 
   =====================================
   11) dfApply / dfWhere / dfExplode
   =====================================
*/
DataFrame dfApply_impl(const DataFrame* df, RowFunction func)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df || !func) return result;

    size_t nRows = df->numRows(df);
    for (size_t r = 0; r < nRows; r++) {
        // The function signature is: RowFunction(DataFrame* outDF, const DataFrame* inDF, size_t rowIndex)
        // So we pass &result as outDF, df as inDF, r as rowIndex
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
                // Copy original cell
                switch (s->type) {
                    case DF_INT: {
                        int iv;
                        if (seriesGetInt(s, r, &iv)) {
                            seriesAddInt(&newSeries, iv);
                        } else {
                            seriesAddInt(&newSeries, 0);
                        }
                    } break;
                    case DF_DOUBLE: {
                        double dv;
                        if (seriesGetDouble(s, r, &dv)) {
                            seriesAddDouble(&newSeries, dv);
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
    (void)colIndex; // unused for now
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    // STUB
    return result;
}
