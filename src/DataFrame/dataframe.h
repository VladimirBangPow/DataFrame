#ifndef DATAFRAME_H
#define DATAFRAME_H

#include <stddef.h>   // for size_t
#include <stdbool.h>  // for bool
#include "../Tools/column_type.h"
#include "../../../DataStructures/src/DynamicArray/dynamic_array.h"
#include "../Series/series.h"

/* -------------------------------------------------------------------------
 * First, define RowPredicate and RowFunction so that 
 * we can use them in function-pointer typedefs below.
 * ------------------------------------------------------------------------- */

// Forward-declare:
typedef struct DataFrame DataFrame; 

// Now define RowPredicate in terms of `DataFrame*`:
typedef bool (*RowPredicate)(const DataFrame* df, size_t rowIndex);

// Same for RowFunction:
typedef void (*RowFunction)(DataFrame* outDF, const DataFrame* inDF, size_t rowIndex);


/* -------------------------------------------------------------------------
 * Forward declaration of DataFrame
 * ------------------------------------------------------------------------- */
typedef struct DataFrame DataFrame;

/* -------------------------------------------------------------------------
 * Function pointer types for DataFrame "methods".
 * ------------------------------------------------------------------------- */

/* Core methods */
typedef void   (*DataFrameInitFunc)(DataFrame* df);
typedef void   (*DataFrameFreeFunc)(DataFrame* df);
typedef bool   (*DataFrameAddSeriesFunc)(DataFrame* df, const Series* s);
typedef size_t (*DataFrameNumColumnsFunc)(const DataFrame* df);
typedef size_t (*DataFrameNumRowsFunc)(const DataFrame* df);
typedef const Series* (*DataFrameGetSeriesFunc)(const DataFrame* df, size_t colIndex);
typedef bool   (*DataFrameAddRowFunc)(DataFrame* df, const void** rowData);
typedef bool   (*DataFrameGetRowFunc)(const DataFrame* df, size_t rowIndex, void*** outRow);

/* Query-like methods returning DataFrame subsets or transformations */
typedef DataFrame (*DataFrameHeadFunc)(const DataFrame* df, size_t n);
typedef DataFrame (*DataFrameTailFunc)(const DataFrame* df, size_t n);
typedef DataFrame (*DataFrameDescribeFunc)(const DataFrame* df);
typedef DataFrame (*DataFrameSliceFunc)(const DataFrame* df, size_t start, size_t end);
typedef DataFrame (*DataFrameSampleFunc)(const DataFrame* df, size_t count);
typedef DataFrame (*DataFrameSelectColumnsFunc)(const DataFrame* df, const size_t* colIndices, size_t count);
typedef DataFrame (*DataFrameDropColumnsFunc)(const DataFrame* df, const size_t* dropIndices, size_t dropCount);
typedef DataFrame (*DataFrameRenameColumnsFunc)(const DataFrame* df, const char** oldNames, const char** newNames, size_t count);

/*Indexing-like methods*/
typedef DataFrame (*DataFrameAtFunc)(const DataFrame* df, size_t rowIndex, const char* colName);
typedef DataFrame (*DataFrameIatFunc)(const DataFrame* df, size_t rowIndex, size_t colIndex);
typedef DataFrame (*DataFrameLocFunc)(const DataFrame* df, const size_t* rowIndices, size_t rowCount, const char* const* colNames, size_t colCount);
typedef DataFrame (*DataFrameIlocFunc)(const DataFrame* df, size_t rowStart, size_t rowEnd, const size_t* colIndices, size_t colCount);
typedef DataFrame (*DataFrameDropFunc)(const DataFrame* df, const char* const* colNames, size_t nameCount);
typedef DataFrame (*DataFramePopFunc)(const DataFrame* df, const char* colName, DataFrame* poppedColDF);
typedef DataFrame (*DataFrameInsertFunc)(DataFrame* df, size_t colIndex, const Series* s);
typedef DataFrame (*DataFrameIndexFunc)(const DataFrame* df);
typedef DataFrame (*DataFrameColumnsFunc)(const DataFrame* df);


//Combining
typedef enum {
    JOIN_INNER,
    JOIN_LEFT,
    JOIN_RIGHT
} JoinType;

typedef DataFrame (*DataFrameConcatFunc)(const DataFrame*, const DataFrame*);
typedef DataFrame (*DataFrameMergeFunc)(const DataFrame*, const DataFrame*, const char*, const char*);
typedef DataFrame (*DataFrameJoinFunc)(const DataFrame*, const DataFrame*, const char*, const char*, JoinType);


/* 
   For filtering, we need the RowPredicate we defined above:
*/
typedef DataFrame (*DataFrameFilterFunc)(const DataFrame* df, RowPredicate);

typedef DataFrame (*DataFrameDropNAFunc)(const DataFrame* df);
typedef DataFrame (*DataFrameSortFunc)(const DataFrame* df, size_t colIndex, bool ascending);
typedef DataFrame (*DataFrameGroupByFunc)(const DataFrame* df, size_t groupColIndex);
typedef DataFrame (*DataFramePivotFunc)(const DataFrame* df, size_t indexCol, size_t columnsCol, size_t valuesCol);
typedef DataFrame (*DataFrameMeltFunc)(const DataFrame* df, const size_t* idCols, size_t idCount);
typedef DataFrame (*DataFrameDropDuplicatesFunc)(const DataFrame* df, const size_t* subsetCols, size_t subsetCount);
typedef DataFrame (*DataFrameUniqueFunc)(const DataFrame* df, size_t colIndex);

/* Aggregations returning double */
typedef double (*DataFrameSumFunc)(const DataFrame* df, size_t colIndex);
typedef double (*DataFrameMeanFunc)(const DataFrame* df, size_t colIndex);
typedef double (*DataFrameMinFunc)(const DataFrame* df, size_t colIndex);
typedef double (*DataFrameMaxFunc)(const DataFrame* df, size_t colIndex);

/* -------------------------------------------------------------------------
 * ADDITIONAL AGGREGATIONS/TRANSFORMS
 * ------------------------------------------------------------------------- */
// We'll declare the rest returning double or DataFrame as needed:

/* Additional stats returning double */
typedef double (*DataFrameCountFunc)(const DataFrame* df, size_t colIndex);
typedef double (*DataFrameMedianFunc)(const DataFrame* df, size_t colIndex);
typedef double (*DataFrameModeFunc)(const DataFrame* df, size_t colIndex);
typedef double (*DataFrameStdFunc)(const DataFrame* df, size_t colIndex);
typedef double (*DataFrameVarFunc)(const DataFrame* df, size_t colIndex);
typedef double (*DataFrameRangeFunc)(const DataFrame* df, size_t colIndex);
typedef double (*DataFrameQuantileFunc)(const DataFrame* df, size_t colIndex, double q);
typedef double (*DataFrameIQRFunc)(const DataFrame* df, size_t colIndex);
typedef double (*DataFrameNullCountFunc)(const DataFrame* df, size_t colIndex);
typedef double (*DataFrameUniqueCountFunc)(const DataFrame* df, size_t colIndex);
typedef double (*DataFrameProductFunc)(const DataFrame* df, size_t colIndex);
typedef double (*DataFrameNthLargestFunc)(const DataFrame* df, size_t colIndex, size_t n);
typedef double (*DataFrameNthSmallestFunc)(const DataFrame* df, size_t colIndex, size_t n);
typedef double (*DataFrameSkewnessFunc)(const DataFrame* df, size_t colIndex);
typedef double (*DataFrameKurtosisFunc)(const DataFrame* df, size_t colIndex);
typedef double (*DataFrameCovarianceFunc)(const DataFrame* df, size_t colIndex1, size_t colIndex2);
typedef double (*DataFrameCorrelationFunc)(const DataFrame* df, size_t colIndexX, size_t colIndexY);

/* Additional transforms returning DataFrame */
typedef DataFrame (*DataFrameUniqueValuesFunc)(const DataFrame* df, size_t colIndex);
typedef DataFrame (*DataFrameValueCountsFunc)(const DataFrame* df, size_t colIndex);
typedef DataFrame (*DataFrameCumulativeSumFunc)(const DataFrame* df, size_t colIndex);
typedef DataFrame (*DataFrameCumulativeProductFunc)(const DataFrame* df, size_t colIndex);
typedef DataFrame (*DataFrameCumulativeMaxFunc)(const DataFrame* df, size_t colIndex);
typedef DataFrame (*DataFrameCumulativeMinFunc)(const DataFrame* df, size_t colIndex);

/* Other transforms */
typedef DataFrame (*DataFrameTransposeFunc)(const DataFrame* df);
typedef size_t    (*DataFrameIndexOfFunc)(const DataFrame* df, size_t colIndex, double value);
typedef DataFrame (*DataFrameApplyFunc)(const DataFrame* df, RowFunction);
typedef DataFrame (*DataFrameWhereFunc)(const DataFrame* df, RowPredicate, double);
typedef DataFrame (*DataFrameExplodeFunc)(const DataFrame* df, size_t colIndex);

/* IO / Plotting / Conversion */
typedef void   (*DataFramePrintFunc)(const DataFrame* df);
typedef bool   (*DataFrameReadCsvFunc)(DataFrame* df, const char* filename);
typedef void   (*DataFramePlotFunc)(const DataFrame* df,
                                    size_t xColIndex,
                                    const size_t* yColIndices,
                                    size_t yCount,
                                    const char* plotType,
                                    const char* outputFile);
typedef bool   (*DataFrameConvertToDatetimeFunc)(DataFrame* df,
                                                   size_t dateColIndex,
                                                   const char* formatType);

typedef bool  (*DataFrameDatetimeToStringFunc)(DataFrame* df, 
                                                size_t dateColIndex, 
                                                const char* outFormat);

typedef DataFrame  (*DataFrameDatetimeFilterFunc)(const DataFrame* df,
                                            size_t dateColIndex,
                                            long long startEpoch,
                                            long long endEpoch);

typedef bool  (*DataFrameDatetimeTruncateFunc)(DataFrame* df,
                                                size_t colIndex,
                                                const char* unit);
typedef bool  (*DataFrameDatetimeAddFunc)(DataFrame* df,
                                           size_t dateColIndex,
                                           long long msToAdd);
typedef DataFrame  (*DataFrameDatetimeDiffFunc)(const DataFrame* df,
                                                size_t col1Index,
                                                size_t col2Index,
                                                const char* newColName);

typedef DataFrame (*DataFrameDatetimeExtractFunc)(const DataFrame* df,
                                                   size_t dateColIndex,
                                                   const char* const* fields,
                                                   size_t numFields);
typedef DataFrame (*DataFrameDatetimeGroupByFunc)(const DataFrame* df, 
                                                   size_t dateColIndex,
                                                   const char* truncateUnit);

typedef bool (*DataFrameDatetimeRoundFunc)(DataFrame* df, size_t colIndex, const char* unit);

typedef bool (*DataFrameDatetimeRebaseFunc)(DataFrame* df, size_t colIndex, long long anchorMs);

typedef DataFrame (*DataFrameDatetimeBetweenFunc)(const DataFrame* df,
                                                  size_t dateColIndex,
                                                  const char* startStr,
                                                  const char* endStr,
                                                  const char* formatType);

typedef bool (*DataFrameDatetimeClampFunc)(const DataFrame* df,
                                           size_t colIndex,
                                           long long minMs,
                                           long long maxMs);

/* -------------------------------------------------------------------------
 * The DataFrame struct itself
 * ------------------------------------------------------------------------- */
struct DataFrame {
    /* Actual data members: */
    DynamicArray columns;  // Holds Series
    size_t       nrows;

    /* "Methods": */
    /* Core */
    DataFrameInitFunc              init;
    DataFrameFreeFunc              free;
    DataFrameAddSeriesFunc         addSeries;
    DataFrameNumColumnsFunc        numColumns;
    DataFrameNumRowsFunc           numRows;
    DataFrameGetSeriesFunc         getSeries;
    DataFrameAddRowFunc            addRow;
    DataFrameGetRowFunc            getRow;

    /* Query methods returning new DataFrames */
    DataFrameHeadFunc              head;
    DataFrameTailFunc              tail;
    DataFrameDescribeFunc          describe;
    DataFrameSliceFunc             slice;
    DataFrameSampleFunc            sample;
    DataFrameSelectColumnsFunc     selectColumns;
    DataFrameDropColumnsFunc       dropColumns;
    DataFrameRenameColumnsFunc     renameColumns;
    DataFrameFilterFunc            filter;
    DataFrameDropNAFunc            dropNA;
    DataFrameSortFunc              sort;
    DataFrameGroupByFunc           groupBy;
    DataFramePivotFunc             pivot;
    DataFrameMeltFunc              melt;
    DataFrameDropDuplicatesFunc    dropDuplicates;
    DataFrameUniqueFunc            unique;

    /* Aggregations returning double */
    DataFrameSumFunc               sum;
    DataFrameMeanFunc              mean;
    DataFrameMinFunc               min;
    DataFrameMaxFunc               max;

    // Additional aggregator pointers:
    DataFrameCountFunc             count;
    DataFrameMedianFunc            median;
    DataFrameModeFunc              mode;
    DataFrameStdFunc               std;
    DataFrameVarFunc               var;
    DataFrameRangeFunc             range;
    DataFrameQuantileFunc          quantile;
    DataFrameIQRFunc               iqr;
    DataFrameNullCountFunc         nullCount;
    DataFrameUniqueCountFunc       uniqueCount;
    DataFrameProductFunc           product;
    DataFrameNthLargestFunc        nthLargest;
    DataFrameNthSmallestFunc       nthSmallest;
    DataFrameSkewnessFunc          skewness;
    DataFrameKurtosisFunc          kurtosis;
    DataFrameCovarianceFunc        covariance;
    DataFrameCorrelationFunc       correlation;

    // Additional DataFrame-returning transforms
    DataFrameUniqueValuesFunc      uniqueValues;
    DataFrameValueCountsFunc       valueCounts;
    DataFrameCumulativeSumFunc     cumulativeSum;
    DataFrameCumulativeProductFunc cumulativeProduct;
    DataFrameCumulativeMaxFunc     cumulativeMax;
    DataFrameCumulativeMinFunc     cumulativeMin;

    /* Other transforms */
    DataFrameTransposeFunc         transpose;
    DataFrameIndexOfFunc           indexOf;
    DataFrameApplyFunc             apply;
    DataFrameWhereFunc             where;
    DataFrameExplodeFunc           explode;

    /* Indexing-like methods */
    DataFrameAtFunc                at;
    DataFrameIatFunc               iat;
    DataFrameLocFunc               loc;
    DataFrameIlocFunc              iloc;
    DataFrameDropFunc              drop;
    DataFramePopFunc               pop;
    DataFrameInsertFunc            insert;
    DataFrameIndexFunc             index;
    DataFrameColumnsFunc           cols;

    /* Combining */
    DataFrameConcatFunc            concat;
    DataFrameMergeFunc             merge;
    DataFrameJoinFunc              join;

    /* IO / Plotting / Conversion */
    DataFramePrintFunc             print;
    DataFrameReadCsvFunc           readCsv;
    DataFramePlotFunc              plot;

    /* Date/Time */
    DataFrameConvertToDatetimeFunc convertToDatetime;
    DataFrameDatetimeToStringFunc  datetimeToString;
    DataFrameDatetimeFilterFunc    datetimeFilter;
    DataFrameDatetimeTruncateFunc  datetimeTruncate;
    DataFrameDatetimeAddFunc       datetimeAdd;
    DataFrameDatetimeDiffFunc      datetimeDiff;
    DataFrameDatetimeExtractFunc   datetimeExtract;
    DataFrameDatetimeGroupByFunc   datetimeGroupBy;
    DataFrameDatetimeRoundFunc     datetimeRound;
    DataFrameDatetimeRebaseFunc    datetimeRebase;
    DataFrameDatetimeBetweenFunc   datetimeBetween;
    DataFrameDatetimeClampFunc     datetimeClamp;
};

/* -------------------------------------------------------------------------
 * Public API
 * ------------------------------------------------------------------------- */

/**
 * @brief Create a new DataFrame object and set all its function pointers.
 *        After calling this, the DataFrame is ready to use.
 */
void DataFrame_Create(DataFrame* df);

/**
 * @brief Destroy (clean up) a DataFrame object.
 */
void DataFrame_Destroy(DataFrame* df);

#endif // DATAFRAME_H
