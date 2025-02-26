#ifndef DATAFRAME_H
#define DATAFRAME_H

#include <stddef.h>
#include <stdbool.h>
#include "../Tools/column_type.h"
#include "../../../DataStructures/src/DynamicArray/dynamic_array.h"
#include "../Series/series.h"


/* -------------------------------------------------------------------------
 * Forward Declaration for DataFrame
 * ------------------------------------------------------------------------- */
typedef struct DataFrame DataFrame;

/* -------------------------------------------------------------------------
 * Function pointer types for DataFrame "methods".
 * ------------------------------------------------------------------------- */
typedef void   (*DataFrameInitFunc)(DataFrame* df);
typedef void   (*DataFrameFreeFunc)(DataFrame* df);
typedef bool   (*DataFrameAddSeriesFunc)(DataFrame* df, const Series* s);
typedef size_t (*DataFrameNumColumnsFunc)(const DataFrame* df);
typedef size_t (*DataFrameNumRowsFunc)(const DataFrame* df);
typedef const Series* (*DataFrameGetSeriesFunc)(const DataFrame* df, size_t colIndex);
typedef bool   (*DataFrameAddRowFunc)(DataFrame* df, const void** rowData);

typedef void   (*DataFrameHeadFunc)(const DataFrame* df, size_t n);
typedef void   (*DataFrameTailFunc)(const DataFrame* df, size_t n);
typedef void   (*DataFrameDescribeFunc)(const DataFrame* df);
typedef void   (*DataFramePrintFunc)(const DataFrame* df);

typedef bool   (*DataFrameReadCsvFunc)(DataFrame* df, const char* filename);

typedef void   (*DataFramePlotFunc)(
    const DataFrame* df,
    size_t xColIndex,
    const size_t* yColIndices,
    size_t yCount,
    const char* plotType,
    const char* outputFile
);

typedef bool (*DataFrameConvertDatesToEpochFunc)(
    DataFrame* df,
    size_t dateColIndex,
    const char* formatType,
    bool toMillis
);

/* -------------------------------------------------------------------------
 * The DataFrame struct itself
 * ------------------------------------------------------------------------- */
struct DataFrame {
    /* Actual data members: */
    DynamicArray columns;  // Holds Series
    size_t       nrows;

    /* "Methods": */
    DataFrameInitFunc              init;
    DataFrameFreeFunc              free;
    DataFrameAddSeriesFunc         addSeries;
    DataFrameNumColumnsFunc        numColumns;
    DataFrameNumRowsFunc           numRows;
    DataFrameGetSeriesFunc         getSeries;
    DataFrameAddRowFunc            addRow;

    DataFrameHeadFunc              head;
    DataFrameTailFunc              tail;
    DataFrameDescribeFunc          describe;
    DataFramePrintFunc             print;

    DataFrameReadCsvFunc           readCsv;
    DataFramePlotFunc              plot;
    DataFrameConvertDatesToEpochFunc convertDatesToEpoch;
};

/* -------------------------------------------------------------------------
 * Public API
 * ------------------------------------------------------------------------- */

/**
 * @brief Create a new DataFrame object and set all its function pointers.
 *        You do not have to call anything else; it is ready to use.
 */
void DataFrame_Create(DataFrame* df);

/**
 * @brief Destroy (clean up) a DataFrame object.
 */
void DataFrame_Destroy(DataFrame* df);

#endif // DATAFRAME_H
