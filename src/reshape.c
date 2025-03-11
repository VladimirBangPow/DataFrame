#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdbool.h>
#include "dataframe.h"

/* -------------------------------------------------------------------------
 * Pivot / Melt
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
 *  Extended to handle DF_DATETIME by printing the 64-bit timestamp.
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
        /* NEW: DF_DATETIME => print as epoch or convert to string */
        case DF_DATETIME: {
            long long dtVal;
            if (seriesGetDateTime(s, rowIndex, &dtVal)) {
                snprintf(buf, bufSize, "%lld", dtVal);
                // Or convert to something like YYYY-MM-DD
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
 *  Extended to handle DF_DATETIME by printing epoch.
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
        /* NEW: DF_DATETIME => just print the epoch */
        case DF_DATETIME: {
            long long dtVal;
            if (seriesGetDateTime(s, rowIndex, &dtVal)) {
                snprintf(buf, bufSize, "%lld", dtVal);
            }
        } break;
    }
}

/**
 * @brief dfMelt_impl
 *  Convert the wide data into a long format with columns: 
 *    [ idCol1, idCol2, ..., variable (string), value (string) ]
 *  For DF_DATETIME columns in ID, we store them as DF_DATETIME in the result ID column.
 *  For the "value" column, everything is stored as a string.
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
    size_t* valCols = (size_t*)malloc(sizeof(size_t) * nCols);
    size_t valCount = 0;
    for (size_t c = 0; c < nCols; c++) {
        if (!isIdColumn(c, idCols, idCount)) {
            valCols[valCount++] = c;
        }
    }

    // Create an array of Series for the ID columns (same type as original)
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

    // Create "variable" (DF_STRING) and "value" (DF_STRING)
    Series varSeries, valSeries;
    seriesInit(&varSeries, "variable", DF_STRING);
    seriesInit(&valSeries, "value", DF_STRING);

    // For each row r => for each valCol => produce 1 new "long" row
    for (size_t r = 0; r < nRows; r++) {
        for (size_t vc = 0; vc < valCount; vc++) {
            size_t realCol = valCols[vc];
            const Series* meltS = df->getSeries(df, realCol);
            if (!meltS) continue;

            // A) For each ID column, copy the row's data to outIdSeries[i]
            for (size_t i = 0; i < idCount; i++) {
                const Series* idSer = df->getSeries(df, idCols[i]);
                if (!idSer) {
                    // fallback
                    switch (outIdSeries[i].type) {
                        case DF_INT:    seriesAddInt(&outIdSeries[i], 0);    break;
                        case DF_DOUBLE: seriesAddDouble(&outIdSeries[i], 0); break;
                        case DF_STRING: seriesAddString(&outIdSeries[i], ""); break;
                        case DF_DATETIME: seriesAddDateTime(&outIdSeries[i], 0LL); break;
                    }
                    continue;
                }

                // Match the original type
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
                    /* NEW: DF_DATETIME in ID column => keep as DF_DATETIME */
                    case DF_DATETIME: {
                        long long dt;
                        if (seriesGetDateTime(idSer, r, &dt)) {
                            seriesAddDateTime(&outIdSeries[i], dt);
                        } else {
                            seriesAddDateTime(&outIdSeries[i], 0LL);
                        }
                    } break;
                }
            }

            // B) "variable" => meltS->name
            seriesAddString(&varSeries, meltS->name);

            // C) "value" => string representation of the cell
            char cellBuf[128] = "";
            cellToString(meltS, r, cellBuf, sizeof(cellBuf));
            seriesAddString(&valSeries, cellBuf);
        }
    }

    // Attach ID columns to result
    for (size_t i = 0; i < idCount; i++) {
        result.addSeries(&result, &outIdSeries[i]);
        seriesFree(&outIdSeries[i]);
    }
    free(outIdSeries);

    // Attach variable/value
    result.addSeries(&result, &varSeries);
    result.addSeries(&result, &valSeries);
    seriesFree(&varSeries);
    seriesFree(&valSeries);

    free(valCols);
    return result;
}
