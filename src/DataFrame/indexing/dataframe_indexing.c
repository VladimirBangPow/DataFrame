#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdbool.h>
#include "../dataframe.h"  // Must have RowPredicate, RowFunction, etc.

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
