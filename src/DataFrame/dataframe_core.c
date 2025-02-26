#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dataframe.h"

/* Forward declarations of all 'static' implementations from other .c files: */
extern void   dfInit_impl(DataFrame* df);
extern void   dfFree_impl(DataFrame* df);
extern bool   dfAddSeries_impl(DataFrame* df, const Series* s);
extern size_t dfNumColumns_impl(const DataFrame* df);
extern size_t dfNumRows_impl(const DataFrame* df);
extern const Series* dfGetSeries_impl(const DataFrame* df, size_t colIndex);
extern bool   dfAddRow_impl(DataFrame* df, const void** rowData);

extern void dfHead_impl(const DataFrame* df, size_t n);
extern void dfTail_impl(const DataFrame* df, size_t n);
extern void dfDescribe_impl(const DataFrame* df);

extern void dfPrint_impl(const DataFrame* df);

extern bool readCsv_impl(DataFrame* df, const char* filename);

extern void dfPlot_impl(
    const DataFrame* df,
    size_t xColIndex,
    const size_t* yColIndices,
    size_t yCount,
    const char* plotType,
    const char* outputFile
);

extern bool dfConvertDatesToEpoch_impl(
    DataFrame* df, 
    size_t dateColIndex, 
    const char* formatType, 
    bool toMillis
);


/* dataframe_core.c */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dataframe.h"

/* -------------------------------------------------------------------------
 * Core Implementation Functions
 * ------------------------------------------------------------------------- */

/**
 * dfInit_impl
 * Initializes the DataFrame by setting up the dynamic array for columns
 * and zeroing out nrows.
 */
void dfInit_impl(DataFrame* df)
{
    if (!df) return;
    // Initialize the 'columns' dynamic array with initial capacity, e.g. 4
    daInit(&df->columns, 4);
    df->nrows = 0;
}

/**
 * dfFree_impl
 * Frees all Series within the DataFrame and then frees the dynamic array.
 */
void dfFree_impl(DataFrame* df)
{
    if (!df) return;
    // Free each Series
    for (size_t i = 0; i < daSize(&df->columns); i++) {
        Series* s = (Series*)daGetMutable(&df->columns, i);
        seriesFree(s);
    }
    // Free the columns array
    daFree(&df->columns);
    df->nrows = 0;
}

/**
 * dfAddSeries_impl
 * Adds a new Series to the DataFrame, ensuring row count matches existing columns.
 */
bool dfAddSeries_impl(DataFrame* df, const Series* s)
{
    if (!df || !s) return false;

    // If DataFrame has no columns yet, take the series size as df->nrows
    if (daSize(&df->columns) == 0) {
        df->nrows = seriesSize(s);
    } else {
        // Otherwise, must match existing nrows
        if (seriesSize(s) != df->nrows) {
            fprintf(stderr,
                "Error: new Series '%s' has %zu rows; DataFrame has %zu rows.\n",
                s->name, seriesSize(s), df->nrows);
            return false;
        }
    }

    // Create a local copy of the Series so the DataFrame owns its own data
    Series newSeries;
    seriesInit(&newSeries, s->name, s->type);

    for (size_t i = 0; i < seriesSize(s); i++) {
        switch (s->type) {
            case DF_INT: {
                int val;
                seriesGetInt(s, i, &val);
                seriesAddInt(&newSeries, val);
            } break;
            case DF_DOUBLE: {
                double val;
                seriesGetDouble(s, i, &val);
                seriesAddDouble(&newSeries, val);
            } break;
            case DF_STRING: {
                char* str = NULL;
                if (seriesGetString(s, i, &str)) {
                    seriesAddString(&newSeries, str);
                    free(str);
                }
            } break;
        }
    }

    daPushBack(&df->columns, &newSeries, sizeof(Series));
    return true;
}

/**
 * dfAddRow_impl
 * Adds a new row of data (void** rowData) to each column, updating nrows by 1.
 */
bool dfAddRow_impl(DataFrame* df, const void** rowData)
{
    if (!df || !rowData) return false;

    size_t nCols = daSize(&df->columns);
    if (nCols == 0) {
        fprintf(stderr, "Error: DataFrame has no columns; cannot add row.\n");
        return false;
    }

    // For each column, insert the corresponding element from rowData
    for (size_t c = 0; c < nCols; c++) {
        Series* s = (Series*)daGetMutable(&df->columns, c);
        if (!s) return false;

        switch (s->type) {
            case DF_INT: {
                const int* valPtr = (const int*)rowData[c];
                if (!valPtr) return false;
                seriesAddInt(s, *valPtr);
            } break;
            case DF_DOUBLE: {
                const double* valPtr = (const double*)rowData[c];
                if (!valPtr) return false;
                seriesAddDouble(s, *valPtr);
            } break;
            case DF_STRING: {
                const char* strPtr = (const char*)rowData[c];
                if (!strPtr) return false;
                seriesAddString(s, strPtr);
            } break;
        }
    }

    df->nrows += 1;
    return true;
}

/**
 * dfNumColumns_impl
 * Returns how many columns are in the DataFrame.
 */
size_t dfNumColumns_impl(const DataFrame* df)
{
    if (!df) return 0;
    return daSize(&df->columns);
}

/**
 * dfNumRows_impl
 * Returns how many rows are in the DataFrame.
 */
size_t dfNumRows_impl(const DataFrame* df)
{
    if (!df) return 0;
    return df->nrows;
}

/**
 * dfGetSeries_impl
 * Returns a pointer to the Series at colIndex, or NULL if out of range.
 */
const Series* dfGetSeries_impl(const DataFrame* df, size_t colIndex)
{
    if (!df) return NULL;
    if (colIndex >= daSize(&df->columns)) return NULL;
    return (const Series*)daGet(&df->columns, colIndex);
}

/* -------------------------------------------------------------
 * DataFrame_Create
 * ------------------------------------------------------------- */
void DataFrame_Create(DataFrame* df)
{
    if (!df) return;
    memset(df, 0, sizeof(*df));

    // Hook up all function pointers in one shot:
    df->init         = dfInit_impl;
    df->free         = dfFree_impl;
    df->addSeries    = dfAddSeries_impl;
    df->numColumns   = dfNumColumns_impl;
    df->numRows      = dfNumRows_impl;
    df->getSeries    = dfGetSeries_impl;
    df->addRow       = dfAddRow_impl;

    df->head         = dfHead_impl;
    df->tail         = dfTail_impl;
    df->describe     = dfDescribe_impl;
    df->print        = dfPrint_impl;

    df->readCsv      = readCsv_impl;
    df->plot         = dfPlot_impl;
    df->convertDatesToEpoch = dfConvertDatesToEpoch_impl;

    // Finally, call init
    df->init(df);
}

/* -------------------------------------------------------------
 * DataFrame_Destroy
 * ------------------------------------------------------------- */
void DataFrame_Destroy(DataFrame* df)
{
    if (!df) return;
    df->free(df);
}
