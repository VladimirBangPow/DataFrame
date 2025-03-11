#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdbool.h>
#include "dataframe.h"
/* -------------------------------------------------------------------------
 * Existing: at, iat, loc, iloc
 * ------------------------------------------------------------------------- */

/**
 * @brief dfAt_impl
 */
DataFrame dfAt_impl(const DataFrame* df, size_t rowIndex, const char* colName)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df || !colName) return result;

    size_t nCols = df->numColumns(df);
    size_t foundCol = (size_t)-1;
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (s && strcmp(s->name, colName) == 0) {
            foundCol = c;
            break;
        }
    }
    if (foundCol == (size_t)-1) {
        return result; // not found
    }
    size_t nRows = df->numRows(df);
    if (rowIndex >= nRows) {
        return result; // out-of-range
    }
    const Series* origCol = df->getSeries(df, foundCol);
    if (!origCol) return result;

    Series newSeries;
    seriesInit(&newSeries, origCol->name, origCol->type);

    switch (origCol->type) {
        case DF_INT: {
            int val;
            if (seriesGetInt(origCol, rowIndex, &val)) {
                seriesAddInt(&newSeries, val);
            }
        } break;
        case DF_DOUBLE: {
            double dval;
            if (seriesGetDouble(origCol, rowIndex, &dval)) {
                seriesAddDouble(&newSeries, dval);
            }
        } break;
        case DF_STRING: {
            char* str = NULL;
            if (seriesGetString(origCol, rowIndex, &str)) {
                seriesAddString(&newSeries, str);
                free(str);
            }
        } break;
        /* --------------------------------------------------------
         *  DF_DATETIME
         * -------------------------------------------------------- */
        case DF_DATETIME: {
            long long dtVal;
            if (seriesGetDateTime(origCol, rowIndex, &dtVal)) {
                seriesAddDateTime(&newSeries, dtVal);
            }
        } break;
    }

    result.addSeries(&result, &newSeries);
    seriesFree(&newSeries);

    return result;
}

/**
 * @brief dfIat_impl
 */
DataFrame dfIat_impl(const DataFrame* df, size_t rowIndex, size_t colIndex)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    size_t nCols = df->numColumns(df);
    size_t nRows = df->numRows(df);
    if (colIndex >= nCols || rowIndex >= nRows) {
        return result;
    }
    const Series* origCol = df->getSeries(df, colIndex);
    if (!origCol) return result;

    Series newSeries;
    seriesInit(&newSeries, origCol->name, origCol->type);

    switch (origCol->type) {
        case DF_INT: {
            int val;
            if (seriesGetInt(origCol, rowIndex, &val)) {
                seriesAddInt(&newSeries, val);
            }
        } break;
        case DF_DOUBLE: {
            double dval;
            if (seriesGetDouble(origCol, rowIndex, &dval)) {
                seriesAddDouble(&newSeries, dval);
            }
        } break;
        case DF_STRING: {
            char* str = NULL;
            if (seriesGetString(origCol, rowIndex, &str)) {
                seriesAddString(&newSeries, str);
                free(str);
            }
        } break;
        /* --------------------------------------------------------
         *  DF_DATETIME
         * -------------------------------------------------------- */
        case DF_DATETIME: {
            long long dtVal;
            if (seriesGetDateTime(origCol, rowIndex, &dtVal)) {
                seriesAddDateTime(&newSeries, dtVal);
            }
        } break;
    }

    result.addSeries(&result, &newSeries);
    seriesFree(&newSeries);
    return result;
}

/**
 * @brief dfLoc_impl
 */
DataFrame dfLoc_impl(const DataFrame* df,
                     const size_t* rowIndices, 
                     size_t rowCount,
                     const char* const* colNames,
                     size_t colCount)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    size_t nRows = df->numRows(df);
    size_t nCols = df->numColumns(df);

    for (size_t cn = 0; cn < colCount; cn++) {
        size_t foundCol = (size_t)-1;
        for (size_t c = 0; c < nCols; c++) {
            const Series* s = df->getSeries(df, c);
            if (s && strcmp(s->name, colNames[cn]) == 0) {
                foundCol = c;
                break;
            }
        }
        if (foundCol == (size_t)-1) {
            // skip unknown column name
            continue;
        }
        const Series* orig = df->getSeries(df, foundCol);
        if (!orig) continue;

        Series newSeries;
        seriesInit(&newSeries, orig->name, orig->type);

        for (size_t r = 0; r < rowCount; r++) {
            size_t realRow = rowIndices[r];
            if (realRow >= nRows) {
                // skip
                continue;
            }
            switch (orig->type) {
                case DF_INT: {
                    int val;
                    if (seriesGetInt(orig, realRow, &val)) {
                        seriesAddInt(&newSeries, val);
                    }
                } break;
                case DF_DOUBLE: {
                    double dv;
                    if (seriesGetDouble(orig, realRow, &dv)) {
                        seriesAddDouble(&newSeries, dv);
                    }
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(orig, realRow, &str)) {
                        seriesAddString(&newSeries, str);
                        free(str);
                    }
                } break;
                /* --------------------------------------------
                 *  DF_DATETIME
                 * -------------------------------------------- */
                case DF_DATETIME: {
                    long long dtVal;
                    if (seriesGetDateTime(orig, realRow, &dtVal)) {
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
 * @brief dfIloc_impl
 */
DataFrame dfIloc_impl(const DataFrame* df,
                      size_t rowStart,
                      size_t rowEnd,
                      const size_t* colIndices,
                      size_t colCount)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    size_t nRows = df->numRows(df);
    size_t nCols = df->numColumns(df);
    if (rowStart >= nRows) {
        return result;
    }
    if (rowEnd > nRows) {
        rowEnd = nRows;
    }

    for (size_t i = 0; i < colCount; i++) {
        size_t cIndex = colIndices[i];
        if (cIndex >= nCols) {
            // skip
            continue;
        }
        const Series* orig = df->getSeries(df, cIndex);
        if (!orig) continue;

        Series newSeries;
        seriesInit(&newSeries, orig->name, orig->type);

        for (size_t r = rowStart; r < rowEnd; r++) {
            switch (orig->type) {
                case DF_INT: {
                    int val;
                    if (seriesGetInt(orig, r, &val)) {
                        seriesAddInt(&newSeries, val);
                    }
                } break;
                case DF_DOUBLE: {
                    double dv;
                    if (seriesGetDouble(orig, r, &dv)) {
                        seriesAddDouble(&newSeries, dv);
                    }
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(orig, r, &str)) {
                        seriesAddString(&newSeries, str);
                        free(str);
                    }
                } break;
                /* --------------------------------------------
                 *  DF_DATETIME
                 * -------------------------------------------- */
                case DF_DATETIME: {
                    long long dtVal;
                    if (seriesGetDateTime(orig, r, &dtVal)) {
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

/* -------------------------------------------------------------------------
 * Additional "pandas-like" methods: drop, pop, insert, index, columns
 * ------------------------------------------------------------------------- */

/**
 * @brief dfDrop_impl
 */
DataFrame dfDrop_impl(const DataFrame* df, const char* const* colNames, size_t nameCount)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df || !colNames) return result;

    size_t nCols = df->numColumns(df);

    // We'll build a "toDrop" boolean array for each column
    bool* dropMask = (bool*)calloc(nCols, sizeof(bool));
    
    // Mark columns that match any name in colNames
    for (size_t i = 0; i < nameCount; i++) {
        const char* dropName = colNames[i];
        if (!dropName) continue;
        // find a column with that name
        for (size_t c = 0; c < nCols; c++) {
            const Series* s = df->getSeries(df, c);
            if (!s) continue;
            if (strcmp(s->name, dropName) == 0) {
                dropMask[c] = true;
            }
        }
    }

    // Now copy columns that are not dropped
    for (size_t c = 0; c < nCols; c++) {
        if (dropMask[c]) {
            continue; // skip
        }
        const Series* s = df->getSeries(df, c);
        if (!s) continue;

        // copy entire column
        Series newS;
        seriesInit(&newS, s->name, s->type);

        size_t nRows = seriesSize(s);
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
                    char* str = NULL;
                    if (seriesGetString(s, r, &str)) {
                        seriesAddString(&newS, str);
                        free(str);
                    }
                } break;
                /* --------------------------------------------
                 *  DF_DATETIME
                 * -------------------------------------------- */
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

    free(dropMask);
    return result;
}

/**
 * @brief dfPop_impl
 */
DataFrame dfPop_impl(const DataFrame* df, const char* colName, DataFrame* poppedColDF)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    // Initialize *poppedColDF as empty
    if (poppedColDF) {
        DataFrame_Create(poppedColDF);
    }

    size_t nCols = df->numColumns(df);
    size_t foundIdx = (size_t)-1;

    // 1) Find the column
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (s && strcmp(s->name, colName)==0) {
            foundIdx = c;
            break;
        }
    }

    // 2) Build the new DF w/o that column, also build popped DF
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) continue;

        if (c == foundIdx) {
            // Move this column to poppedColDF
            if (poppedColDF) {
                Series poppedS;
                seriesInit(&poppedS, s->name, s->type);

                size_t nRows = seriesSize(s);
                for (size_t r = 0; r < nRows; r++) {
                    switch (s->type) {
                        case DF_INT: {
                            int val;
                            if (seriesGetInt(s, r, &val)) {
                                seriesAddInt(&poppedS, val);
                            }
                        } break;
                        case DF_DOUBLE: {
                            double dv;
                            if (seriesGetDouble(s, r, &dv)) {
                                seriesAddDouble(&poppedS, dv);
                            }
                        } break;
                        case DF_STRING: {
                            char* str = NULL;
                            if (seriesGetString(s, r, &str)) {
                                seriesAddString(&poppedS, str);
                                free(str);
                            }
                        } break;
                        /* --------------------------------
                         *  DF_DATETIME
                         * -------------------------------- */
                        case DF_DATETIME: {
                            long long dtVal;
                            if (seriesGetDateTime(s, r, &dtVal)) {
                                seriesAddDateTime(&poppedS, dtVal);
                            }
                        } break;
                    }
                }
                poppedColDF->addSeries(poppedColDF, &poppedS);
                seriesFree(&poppedS);
            }
        } else {
            // Copy to the result
            Series newS;
            seriesInit(&newS, s->name, s->type);

            size_t nRows = seriesSize(s);
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
                        char* str = NULL;
                        if (seriesGetString(s, r, &str)) {
                            seriesAddString(&newS, str);
                            free(str);
                        }
                    } break;
                    /* --------------------------------
                     *  DF_DATETIME
                     * -------------------------------- */
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
    }

    return result;
}

/**
 * @brief dfInsert_impl
 */
DataFrame dfInsert_impl(const DataFrame* df, size_t insertPos, const Series* newCol)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df || !newCol) return result;

    size_t nCols = df->numColumns(df);
    size_t nRows = df->numRows(df);

    // If DF has existing columns, ensure row count matches
    if (df->numColumns(df) > 0) {
        if (seriesSize(newCol) != nRows) {
            fprintf(stderr, "dfInsert_impl: newCol row mismatch, ignoring.\n");
            // Return copy of original DF
            for (size_t c = 0; c < nCols; c++) {
                const Series* s = df->getSeries(df, c);
                if (!s) continue;

                Series copyS;
                seriesInit(&copyS, s->name, s->type);
                size_t cRows = seriesSize(s);
                for (size_t r = 0; r < cRows; r++) {
                    switch (s->type) {
                        case DF_INT: {
                            int val;
                            if (seriesGetInt(s, r, &val)) {
                                seriesAddInt(&copyS, val);
                            }
                        } break;
                        case DF_DOUBLE: {
                            double dv;
                            if (seriesGetDouble(s, r, &dv)) {
                                seriesAddDouble(&copyS, dv);
                            }
                        } break;
                        case DF_STRING: {
                            char* str = NULL;
                            if (seriesGetString(s, r, &str)) {
                                seriesAddString(&copyS, str);
                                free(str);
                            }
                        } break;
                        /* -------------------------
                         *  DF_DATETIME
                         * ------------------------- */
                        case DF_DATETIME: {
                            long long dtVal;
                            if (seriesGetDateTime(s, r, &dtVal)) {
                                seriesAddDateTime(&copyS, dtVal);
                            }
                        } break;
                    }
                }
                result.addSeries(&result, &copyS);
                seriesFree(&copyS);
            }
            return result;
        }
    }

    // clamp insertPos
    if (insertPos > nCols) {
        insertPos = nCols;
    }

    // We'll copy columns up to insertPos, then newCol, then the rest.
    for (size_t c = 0; c < nCols + 1; c++) {
        if (c == insertPos) {
            // Insert newCol here
            Series newCopy;
            seriesInit(&newCopy, newCol->name, newCol->type);

            size_t cRows = seriesSize(newCol);
            for (size_t r = 0; r < cRows; r++) {
                switch (newCol->type) {
                    case DF_INT: {
                        int val;
                        if (seriesGetInt(newCol, r, &val)) {
                            seriesAddInt(&newCopy, val);
                        }
                    } break;
                    case DF_DOUBLE: {
                        double dv;
                        if (seriesGetDouble(newCol, r, &dv)) {
                            seriesAddDouble(&newCopy, dv);
                        }
                    } break;
                    case DF_STRING: {
                        char* str = NULL;
                        if (seriesGetString(newCol, r, &str)) {
                            seriesAddString(&newCopy, str);
                            free(str);
                        }
                    } break;
                    /* ----------------------------
                     *  DF_DATETIME
                     * ---------------------------- */
                    case DF_DATETIME: {
                        long long dtVal;
                        if (seriesGetDateTime(newCol, r, &dtVal)) {
                            seriesAddDateTime(&newCopy, dtVal);
                        }
                    } break;
                }
            }
            result.addSeries(&result, &newCopy);
            seriesFree(&newCopy);
        }
        else {
            // For the original columns
            size_t origIndex = (c < insertPos) ? c : (c - 1);
            if (origIndex >= nCols) {
                // out of range => skip
                continue;
            }
            const Series* s = df->getSeries(df, origIndex);
            if (!s) continue;

            Series copyS;
            seriesInit(&copyS, s->name, s->type);
            size_t cRows = seriesSize(s);
            for (size_t r = 0; r < cRows; r++) {
                switch (s->type) {
                    case DF_INT: {
                        int val;
                        if (seriesGetInt(s, r, &val)) {
                            seriesAddInt(&copyS, val);
                        }
                    } break;
                    case DF_DOUBLE: {
                        double dv;
                        if (seriesGetDouble(s, r, &dv)) {
                            seriesAddDouble(&copyS, dv);
                        }
                    } break;
                    case DF_STRING: {
                        char* str = NULL;
                        if (seriesGetString(s, r, &str)) {
                            seriesAddString(&copyS, str);
                            free(str);
                        }
                    } break;
                    /* ----------------------------
                     *  DF_DATETIME
                     * ---------------------------- */
                    case DF_DATETIME: {
                        long long dtVal;
                        if (seriesGetDateTime(s, r, &dtVal)) {
                            seriesAddDateTime(&copyS, dtVal);
                        }
                    } break;
                }
            }
            result.addSeries(&result, &copyS);
            seriesFree(&copyS);
        }
    }

    return result;
}

/**
 * @brief dfIndex_impl
 *  Emulate "df.index" in pandas. Return a new DataFrame with 
 *  a single DF_INT column named "index" that has values [0..nRows-1].
 */
DataFrame dfIndex_impl(const DataFrame* df)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;
    size_t nRows = df->numRows(df);

    Series idxSeries;
    seriesInit(&idxSeries, "index", DF_INT);

    for (size_t i = 0; i < nRows; i++) {
        seriesAddInt(&idxSeries, (int)i);
    }

    result.addSeries(&result, &idxSeries);
    seriesFree(&idxSeries);

    return result;
}

/**
 * @brief dfColumns_impl
 *  Emulate "df.columns" in pandas. Return a new DataFrame with 
 *  a single DF_STRING column named "columns" that lists the column names in order.
 */
DataFrame dfColumns_impl(const DataFrame* df)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;
    size_t nCols = df->numColumns(df);

    Series colSeries;
    seriesInit(&colSeries, "columns", DF_STRING);

    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) {
            seriesAddString(&colSeries, "");
            continue;
        }
        seriesAddString(&colSeries, s->name);
    }

    result.addSeries(&result, &colSeries);
    seriesFree(&colSeries);

    return result;
}


static void copyDataFrame(const DataFrame* src, DataFrame* dst)
{
    // Initialize dst
    DataFrame_Create(dst);

    size_t nCols = src->numColumns(src);
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = src->getSeries(src, c);
        if (!s) continue;
        // create a copy
        Series newS;
        seriesInit(&newS, s->name, s->type);

        size_t nRows = seriesSize(s);
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
                    char* strVal=NULL;
                    if (seriesGetString(s, r, &strVal)) {
                        seriesAddString(&newS, strVal);
                        free(strVal);
                    }
                } break;
                case DF_DATETIME: {
                    long long dtVal;
                    if (seriesGetDateTime(s, r, &dtVal)) {
                        seriesAddDateTime(&newS, dtVal);
                    }
                } break;
            }
        }
        dst->addSeries(dst, &newS);
        seriesFree(&newS);
    }
}


DataFrame dfSetValue_impl(const DataFrame* df,
                          size_t rowIndex,
                          size_t colIndex,
                          const void* newValue)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df || !newValue) {
        return result; // empty
    }

    size_t nCols = df->numColumns(df);
    size_t nRows = df->numRows(df);

    // out-of-range checks
    if (colIndex >= nCols || rowIndex >= nRows) {
        // just copy original DF unchanged
        copyDataFrame(df, &result);
        return result;
    }

    // fetch the Series we want to set
    const Series* origCol = df->getSeries(df, colIndex);
    if (!origCol) {
        // copy original
        copyDataFrame(df, &result);
        return result;
    }

    // Build result by copying all columns first
    copyDataFrame(df, &result);

    // Now locate the column in the result => we will modify that cell
    Series* modCol = (Series*)daGetMutable(&result.columns, colIndex);
    if (!modCol) {
        // fallback => no change
        return result;
    }

    // Check the column's type => set the cell if matching
    switch (modCol->type) {
        case DF_INT: {
            // interpret newValue as (int*)
            const int* valPtr = (const int*) newValue;
            // Overwrite rowIndex in modCol->data
            int* cellPtr = (int*)daGetMutable(&modCol->data, rowIndex);
            if (cellPtr && valPtr) {
                *cellPtr = *valPtr;
            }
        } break;
        case DF_DOUBLE: {
            const double* dPtr = (const double*) newValue;
            double* cellPtr = (double*)daGetMutable(&modCol->data, rowIndex);
            if (cellPtr && dPtr) {
                *cellPtr = *dPtr;
            }
        } break;
        case DF_STRING: {
            // For DF_STRING, we stored each row as a char*. 
            // We can free the old string (if you store it separately), then store a copy.
            char** cellPtr = (char**)daGetMutable(&modCol->data, rowIndex);
            if (cellPtr) {
                // free old
                // (depending on how your library does string storage—some do copy on add)
                // We'll assume we can free(*cellPtr):
                free(*cellPtr);
                // now store a fresh copy of newValue
                const char* newStr = (const char*) newValue;
                if (newStr) {
                    char* copyStr = strdup(newStr);
                    *cellPtr = copyStr;
                } else {
                    *cellPtr = NULL;
                }
            }
        } break;
        case DF_DATETIME: {
            // interpret newValue as (long long*) if using ms as epoch
            const long long* dtPtr = (const long long*) newValue;
            long long* cellPtr = (long long*)daGetMutable(&modCol->data, rowIndex);
            if (cellPtr && dtPtr) {
                *cellPtr = *dtPtr;
            }
        } break;
    }

    return result;
}




DataFrame dfSetRow_impl(const DataFrame* df,
                        size_t rowIndex,
                        const void** rowValues, 
                        size_t valueCount)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df || !rowValues) {
        return result;
    }

    size_t nCols = df->numColumns(df);
    size_t nRows = df->numRows(df);

    if (rowIndex >= nRows || valueCount != nCols) {
        // mismatch => return copy
        copyDataFrame(df, &result);
        return result;
    }

    // build a copy of df
    copyDataFrame(df, &result);

    // For each column => set the cell to rowValues[c]
    for (size_t c = 0; c < nCols; c++) {
        // get the column in the result
        Series* modCol = (Series*)daGetMutable(&result.columns, c);
        if (!modCol) continue;

        const void* valPtr = rowValues[c];
        if (!valPtr) {
            // skip or set to NA
            continue;
        }

        switch (modCol->type) {
            case DF_INT: {
                int* cellPtr = (int*)daGetMutable(&modCol->data, rowIndex);
                if (cellPtr) {
                    *cellPtr = *(const int*)valPtr;
                }
            } break;
            case DF_DOUBLE: {
                double* cellPtr = (double*)daGetMutable(&modCol->data, rowIndex);
                if (cellPtr) {
                    *cellPtr = *(const double*)valPtr;
                }
            } break;
            case DF_STRING: {
                char** cellPtr = (char**)daGetMutable(&modCol->data, rowIndex);
                if (cellPtr) {
                    // free old
                    free(*cellPtr);
                    // store new
                    *cellPtr = strdup((const char*)valPtr);
                }
            } break;
            case DF_DATETIME: {
                long long* cellPtr = (long long*)daGetMutable(&modCol->data, rowIndex);
                if (cellPtr) {
                    *cellPtr = *(const long long*)valPtr;
                }
            } break;
        }
    }

    return result;
}

DataFrame dfSetColumn_impl(const DataFrame* df,
                           const char* colName,
                           const Series* newCol)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df || !colName || !newCol) {
        return result;
    }

    size_t nCols = df->numColumns(df);
    size_t nRows = df->numRows(df);

    // must match row count
    if (seriesSize(newCol) != nRows) {
        // mismatch => copy DF
        copyDataFrame(df, &result);
        return result;
    }

    // find colName
    size_t found = (size_t)-1;
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (s && strcmp(s->name, colName)==0) {
            found = c;
            break;
        }
    }
    if (found == (size_t)-1) {
        // not found => copy
        copyDataFrame(df, &result);
        return result;
    }

    // create copy
    copyDataFrame(df, &result);

    // now overwrite column "found" with newCol data (types must match)
    Series* modCol = (Series*)daGetMutable(&result.columns, found);
    if (!modCol) {
        return result; // fallback
    }
    if (modCol->type != newCol->type) {
        // skip if mismatch
        return result;
    }

    // free the old data in modCol->data
    // but careful if DF_STRING => free each pointer
    // We'll just re-init for simplicity
    seriesFree(modCol);
    seriesInit(modCol, newCol->name, newCol->type);

    // copy newCol's rows into modCol
    size_t cRows = seriesSize(newCol);
    for (size_t r = 0; r < cRows; r++) {
        switch (newCol->type) {
            case DF_INT: {
                int v; 
                if (seriesGetInt(newCol, r, &v)) {
                    seriesAddInt(modCol, v);
                }
            } break;
            case DF_DOUBLE: {
                double d;
                if (seriesGetDouble(newCol, r, &d)) {
                    seriesAddDouble(modCol, d);
                }
            } break;
            case DF_STRING: {
                char* st=NULL;
                if (seriesGetString(newCol, r, &st)) {
                    seriesAddString(modCol, st);
                    free(st);
                }
            } break;
            case DF_DATETIME: {
                long long dt;
                if (seriesGetDateTime(newCol, r, &dt)) {
                    seriesAddDateTime(modCol, dt);
                }
            } break;
        }
    }
    return result;
}


DataFrame dfRenameColumn_impl(const DataFrame* df,
                              const char* oldName,
                              const char* newName)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df || !oldName || !newName) {
        return result;
    }

    // copy DF
    copyDataFrame(df, &result);

    // find col with oldName
    size_t nCols = result.numColumns(&result);
    for (size_t c = 0; c < nCols; c++) {
        Series* s = (Series*)daGetMutable(&result.columns, c);
        if (!s) continue;
        if (strcmp(s->name, oldName)==0) {
            // rename
            free(s->name);  // old name
            s->name = strdup(newName);
            break; 
        }
    }
    return result;
}

// helper: addNAValue => just adds "0" or "NA" depending on type
static void addNAValue(Series* s)
{
    switch (s->type) {
        case DF_INT:      seriesAddInt(s, 0);  break;
        case DF_DOUBLE:   seriesAddDouble(s, 0.0); break;
        case DF_STRING:   seriesAddString(s, "NA"); break;
        case DF_DATETIME: seriesAddDateTime(s, 0LL); break;
    }
}

// helper: copyCell => read row=oldIdx from 'orig' and add to 'dest'
static void copyCell(const Series* orig, Series* dest, size_t oldIdx)
{
    switch (orig->type) {
        case DF_INT: {
            int v;
            if (seriesGetInt(orig, oldIdx, &v)) {
                seriesAddInt(dest, v);
            } else {
                addNAValue(dest);
            }
        } break;
        case DF_DOUBLE: {
            double d;
            if (seriesGetDouble(orig, oldIdx, &d)) {
                seriesAddDouble(dest, d);
            } else {
                addNAValue(dest);
            }
        } break;
        case DF_STRING: {
            char* st=NULL;
            if (seriesGetString(orig, oldIdx, &st)) {
                seriesAddString(dest, st);
                free(st);
            } else {
                addNAValue(dest);
            }
        } break;
        case DF_DATETIME: {
            long long dt;
            if (seriesGetDateTime(orig, oldIdx, &dt)) {
                seriesAddDateTime(dest, dt);
            } else {
                addNAValue(dest);
            }
        } break;
    }
}


DataFrame dfReindex_impl(const DataFrame* df,
                         const size_t* newIndices,
                         size_t newN)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df || !newIndices || newN==0) {
        return result; 
    }

    size_t nRows = df->numRows(df);
    size_t nCols = df->numColumns(df);

    // We'll build each column from scratch
    for (size_t c = 0; c < nCols; c++) {
        const Series* orig = df->getSeries(df, c);
        if (!orig) continue;

        Series newS;
        seriesInit(&newS, orig->name, orig->type);

        for (size_t i = 0; i < newN; i++) {
            size_t oldIdx = newIndices[i];
            if (oldIdx >= nRows) {
                // out-of-range => "NA"
                addNAValue(&newS); 
            } else {
                copyCell(orig, &newS, oldIdx);
            }
        }
        result.addSeries(&result, &newS);
        seriesFree(&newS);
    }
    return result;
}


DataFrame dfTake_impl(const DataFrame* df,
                      const size_t* rowIndices,
                      size_t count)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df || !rowIndices || count==0) {
        return result;
    }

    size_t nRows = df->numRows(df);
    size_t nCols = df->numColumns(df);

    // We'll create each column from scratch
    for (size_t c = 0; c < nCols; c++) {
        const Series* orig = df->getSeries(df, c);
        if (!orig) continue;

        Series newS;
        seriesInit(&newS, orig->name, orig->type);

        for (size_t i = 0; i < count; i++) {
            size_t r = rowIndices[i];
            if (r >= nRows) {
                addNAValue(&newS);
            } else {
                copyCell(orig, &newS, r);
            }
        }
        result.addSeries(&result, &newS);
        seriesFree(&newS);
    }
    return result;
}


DataFrame dfReorderColumns_impl(const DataFrame* df,
                                const size_t* newOrder,
                                size_t colCount)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df || !newOrder || colCount==0) {
        return result;
    }

    size_t nCols = df->numColumns(df);

    // for each newOrder[i], copy that column from df => result
    for (size_t i = 0; i < colCount; i++) {
        size_t oldPos = newOrder[i];
        if (oldPos >= nCols) {
            // skip or add blank
            continue;
        }
        const Series* s = df->getSeries(df, oldPos);
        if (!s) continue;

        // copy entire column
        Series copyS;
        seriesInit(&copyS, s->name, s->type);

        size_t nRows = seriesSize(s);
        for (size_t r = 0; r < nRows; r++) {
            copyCell(s, &copyS, r); // re-uses the helper from above
        }
        result.addSeries(&result, &copyS);
        seriesFree(&copyS);
    }
    return result;
}
