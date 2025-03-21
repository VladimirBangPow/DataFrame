#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "dataframe.h"

// If your DataFrame struct references RowPredicate or RowFunction, 
// make sure they are declared in `dataframe.h`, e.g.:
// typedef bool (*RowPredicate)(const struct DataFrame*, size_t);
// typedef void (*RowFunction)(struct DataFrame* outDF, const struct DataFrame* inDF, size_t rowIndex);

/* -------------------------------------------------------------------------
 *  Forward declarations of all *impl functions from other .c files 
 *  (matching return types exactly).
 * ------------------------------------------------------------------------- */
extern void dfInit_impl(DataFrame* df);
extern void dfFree_impl(DataFrame* df);
extern bool dfAddSeries_impl(DataFrame* df, const Series* s);
extern size_t dfNumColumns_impl(const DataFrame* df);
extern size_t dfNumRows_impl(const DataFrame* df);
extern const Series* dfGetSeries_impl(const DataFrame* df, size_t colIndex);
extern bool dfAddRow_impl(DataFrame* df, const void** rowData);
extern bool dfGetRow_impl(const DataFrame* df, size_t rowIndex, void*** outRow);

/* The corrected signatures now return DataFrame: */
extern DataFrame dfHead_impl(const DataFrame* df, size_t n);
extern DataFrame dfTail_impl(const DataFrame* df, size_t n);
extern DataFrame dfDescribe_impl(const DataFrame* df);
extern DataFrame dfSlice_impl(const DataFrame* df, size_t start, size_t end);
extern DataFrame dfSample_impl(const DataFrame* df, size_t count);
extern DataFrame dfSelectColumns_impl(const DataFrame* df, const size_t* colIndices, size_t count);
extern DataFrame dfDropColumns_impl(const DataFrame* df, const size_t* dropIndices, size_t dropCount);
extern DataFrame dfRenameColumns_impl(const DataFrame* df, const char** oldNames, const char** newNames, size_t count);
extern DataFrame dfFilter_impl(const DataFrame* df, RowPredicate);
extern DataFrame dfDropNA_impl(const DataFrame* df);
extern DataFrame dfSort_impl(const DataFrame* df, size_t colIndex, bool ascending);
extern DataFrame dfGroupBy_impl(const DataFrame* df, size_t groupColIndex);
extern DataFrame dfPivot_impl(const DataFrame* df, size_t indexCol, size_t columnsCol, size_t valuesCol);
extern DataFrame dfMelt_impl(const DataFrame* df, const size_t* idCols, size_t idCount);
extern DataFrame dfDropDuplicates_impl(const DataFrame* df, const size_t* subsetCols, size_t subsetCount);
extern DataFrame dfUnique_impl(const DataFrame* df, size_t colIndex);

extern double   dfSum_impl(const DataFrame* df, size_t colIndex);
extern double   dfMean_impl(const DataFrame* df, size_t colIndex);
extern double   dfMin_impl(const DataFrame* df, size_t colIndex);
extern double   dfMax_impl(const DataFrame* df, size_t colIndex);

extern double   dfCount_impl(const DataFrame* df, size_t colIndex);
extern double   dfMedian_impl(const DataFrame* df, size_t colIndex);
extern double   dfMode_impl(const DataFrame* df, size_t colIndex);
extern double   dfStd_impl(const DataFrame* df, size_t colIndex);
extern double   dfVar_impl(const DataFrame* df, size_t colIndex);
extern double   dfRange_impl(const DataFrame* df, size_t colIndex);
extern double   dfQuantile_impl(const DataFrame* df, size_t colIndex, double q);
extern double   dfIQR_impl(const DataFrame* df, size_t colIndex);
extern double   dfNullCount_impl(const DataFrame* df, size_t colIndex);
extern double   dfUniqueCount_impl(const DataFrame* df, size_t colIndex);
extern double   dfProduct_impl(const DataFrame* df, size_t colIndex);
extern double   dfNthLargest_impl(const DataFrame* df, size_t colIndex, size_t n);
extern double   dfNthSmallest_impl(const DataFrame* df, size_t colIndex, size_t n);
extern double   dfSkewness_impl(const DataFrame* df, size_t colIndex);
extern double   dfKurtosis_impl(const DataFrame* df, size_t colIndex);
extern double   dfCovariance_impl(const DataFrame* df, size_t colIndex1, size_t colIndex2);
extern double   dfCorrelation_impl(const DataFrame* df, size_t colIndexX, size_t colIndexY);

extern DataFrame dfUniqueValues_impl(const DataFrame* df, size_t colIndex);
extern DataFrame dfValueCounts_impl(const DataFrame* df, size_t colIndex);
extern DataFrame dfCumulativeSum_impl(const DataFrame* df, size_t colIndex);
extern DataFrame dfCumulativeProduct_impl(const DataFrame* df, size_t colIndex);
extern DataFrame dfCumulativeMax_impl(const DataFrame* df, size_t colIndex);
extern DataFrame dfCumulativeMin_impl(const DataFrame* df, size_t colIndex);

extern DataFrame dfTranspose_impl(const DataFrame* df);
extern size_t    dfIndexOf_impl(const DataFrame* df, size_t colIndex, double value);
extern DataFrame dfApply_impl(const DataFrame* df, RowFunction);
extern DataFrame dfWhere_impl(const DataFrame* df, RowPredicate, double);
extern DataFrame dfExplode_impl(const DataFrame* df, size_t colIndex);

extern DataFrame dfAt_impl(const DataFrame* df, size_t rowIndex, const char* colName);
extern DataFrame dfIat_impl(const DataFrame* df, size_t rowIndex, size_t colIndex);
extern DataFrame dfLoc_impl(const DataFrame* df, const size_t* rowIndices, size_t rowCount, const char* const* colNames, size_t colCount);
extern DataFrame dfIloc_impl(const DataFrame* df, size_t rowStart, size_t rowEnd, const size_t* colIndices, size_t colCount);
extern DataFrame dfDrop_impl(const DataFrame* df, const char* const* colNames, size_t nameCount);
extern DataFrame dfPop_impl(const DataFrame* df, const char* colName, DataFrame* poppedColDF);
extern DataFrame dfInsert_impl(DataFrame* df, size_t colIndex, const Series* s);
extern DataFrame dfIndex_impl(const DataFrame* df);
extern DataFrame dfColumns_impl(const DataFrame* df);
extern DataFrame dfSetValue_impl(const DataFrame* df, size_t rowIndex, size_t colIndex, void* value);
extern DataFrame dfSetRow_impl(const DataFrame* df, size_t rowIndex, const void** rowValues, size_t valueCount);
extern DataFrame dfSetColumn_impl(const DataFrame* df, const char* colName, const Series* newCol);
extern DataFrame dfRenameColumn_impl(const DataFrame* df, const char* oldName, const char* newName);
extern DataFrame dfReindex_impl(const DataFrame* df, const size_t* newIndices, size_t newN);
extern DataFrame dfTake_impl(const DataFrame* df, const size_t* rowIndices, size_t count);
extern DataFrame dfReorderColumns_impl(const DataFrame* df, const size_t* newOrder, size_t colCount);

/* Existing combine: */
extern DataFrame dfConcat_impl(const DataFrame*, const DataFrame*);
extern DataFrame dfMerge_impl(const DataFrame*, const DataFrame*, const char*, const char*);
extern DataFrame dfJoin_impl(const DataFrame*, const DataFrame*, const char*, const char*, JoinType);

/* NEW combine functions: */
extern DataFrame dfUnion_impl(const DataFrame* dfA, const DataFrame* dfB);
extern DataFrame dfIntersection_impl(const DataFrame* dfA, const DataFrame* dfB);
extern DataFrame dfDifference_impl(const DataFrame* dfA, const DataFrame* dfB);
extern DataFrame dfSemiJoin_impl(const DataFrame* left, const DataFrame* right, const char* leftKey, const char* rightKey);
extern DataFrame dfAntiJoin_impl(const DataFrame* left, const DataFrame* right, const char* leftKey, const char* rightKey);
extern DataFrame dfCrossJoin_impl(const DataFrame* left, const DataFrame* right);

/* Other non-query methods: */
extern void dfPrint_impl(const DataFrame* df);
extern bool readCsv_impl(DataFrame* df, const char* filename);
extern void dfPlot_impl(const DataFrame* df,
                        size_t xColIndex,
                        const size_t* yColIndices,
                        size_t yCount,
                        const char* plotType,
                        const char* outputFile);

/* Date/Time */
extern bool dfConvertToDatetime_impl(DataFrame* df,
                                       size_t dateColIndex,
                                       const char* formatType);

extern bool dfDatetimeToString_impl(DataFrame* df, 
                                        size_t dateColIndex, 
                                        const char* outFormat);  

extern DataFrame dfDatetimeFilter_impl(const DataFrame* df,
                                    size_t dateColIndex,
                                    long long startEpoch,
                                    long long endEpoch);  

extern bool dfDatetimeTruncate_impl(DataFrame* df,
                                    size_t colIndex,
                                    const char* unit);

extern bool dfDatetimeAdd_impl(DataFrame* df,
                                size_t dateColIndex,
                                long long msToAdd);

extern DataFrame dfDatetimeDiff_impl(const DataFrame* df,
                                    size_t col1Index,
                                    size_t col2Index,
                                    const char* newColName);

extern DataFrame dfDatetimeExtract_impl(const DataFrame* df,
                                        size_t dateColIndex,
                                        const char* const* fields,
                                        size_t numFields);
extern DataFrame dfDatetimeGroupBy_impl(const DataFrame* df, 
                                        size_t dateColIndex,
                                        const char* truncateUnit);

extern bool dfDatetimeRound_impl(DataFrame* df, size_t colIndex, const char* unit);
extern bool dfDatetimeRebase_impl(DataFrame* df, size_t colIndex, long long anchorMs);
extern DataFrame dfDatetimeBetween_impl(const DataFrame* df,
                                        size_t dateColIndex,
                                        const char* startStr,
                                        const char* endStr,
                                        const char* formatType);
extern bool dfDatetimeClamp_impl(const DataFrame* df,
                                    size_t colIndex,
                                    long long minMs,
                                    long long maxMs);


/* -------------------------------------------------------------------------
 * Core Implementation Functions
 * ------------------------------------------------------------------------- */

void dfInit_impl(DataFrame* df)
{
    if (!df) return;
    // Initialize the 'columns' dynamic array with initial capacity
    daInit(&df->columns, 4);
    df->nrows = 0;
}

void dfFree_impl(DataFrame* df)
{
    if (!df) return;
    // Free each Series
    for (size_t i = 0; i < daSize(&df->columns); i++) {
        Series* s = (Series*)daGetMutable(&df->columns, i);
        seriesFree(s);
    }
    daFree(&df->columns);
    df->nrows = 0;
}

/* -------------------------------------------------------------
 * DataFrame_Create
 * ------------------------------------------------------------- */
void DataFrame_Create(DataFrame* df)
{
    if (!df) return;
    memset(df, 0, sizeof(*df));

    // Hook up the "core" pointers:
    df->init         = dfInit_impl;
    df->free         = dfFree_impl;
    df->addSeries    = dfAddSeries_impl;
    df->numColumns   = dfNumColumns_impl;
    df->numRows      = dfNumRows_impl;
    df->getSeries    = dfGetSeries_impl;
    df->addRow       = dfAddRow_impl;
    df->getRow       = dfGetRow_impl;

    // The "query" pointers that return DataFrame:
    df->head         = dfHead_impl;
    df->tail         = dfTail_impl;
    df->describe     = dfDescribe_impl;
    df->slice        = dfSlice_impl;
    df->sample       = dfSample_impl;
    df->selectColumns= dfSelectColumns_impl;
    df->dropColumns  = dfDropColumns_impl;
    df->renameColumns= dfRenameColumns_impl;
    df->filter       = dfFilter_impl;
    df->dropNA       = dfDropNA_impl;
    df->sort         = dfSort_impl;
    df->dropDuplicates= dfDropDuplicates_impl;
    df->unique       = dfUnique_impl;
    df->transpose    = dfTranspose_impl;
    df->indexOf      = dfIndexOf_impl;
    df->apply        = dfApply_impl;
    df->where        = dfWhere_impl;
    df->explode      = dfExplode_impl;

    // Basic Aggregations returning double:
    df->sum          = dfSum_impl;
    df->mean         = dfMean_impl;
    df->min          = dfMin_impl;
    df->max          = dfMax_impl;
    df->count        = dfCount_impl;
    df->median       = dfMedian_impl;
    df->mode         = dfMode_impl;
    df->std          = dfStd_impl;
    df->var          = dfVar_impl;
    df->range        = dfRange_impl;
    df->quantile     = dfQuantile_impl;
    df->iqr          = dfIQR_impl;
    df->nullCount    = dfNullCount_impl;
    df->uniqueCount  = dfUniqueCount_impl;
    df->product      = dfProduct_impl;
    df->nthLargest   = dfNthLargest_impl;
    df->nthSmallest  = dfNthSmallest_impl;
    df->skewness     = dfSkewness_impl;
    df->kurtosis     = dfKurtosis_impl;
    df->covariance   = dfCovariance_impl;
    df->correlation  = dfCorrelation_impl;

    // Additional DataFrame returning transforms
    df->uniqueValues     = dfUniqueValues_impl;
    df->valueCounts      = dfValueCounts_impl;
    df->cumulativeSum    = dfCumulativeSum_impl;
    df->cumulativeProduct= dfCumulativeProduct_impl;
    df->cumulativeMax    = dfCumulativeMax_impl;
    df->cumulativeMin    = dfCumulativeMin_impl;

    // Others:
    df->groupBy      = dfGroupBy_impl;
    df->pivot        = dfPivot_impl;
    df->melt         = dfMelt_impl;

    // Indexing:
    df->at           = dfAt_impl;
    df->iat          = dfIat_impl;
    df->loc          = dfLoc_impl;
    df->iloc         = dfIloc_impl;
    df->drop         = dfDrop_impl;
    df->pop          = dfPop_impl;
    df->insert       = dfInsert_impl;
    df->index        = dfIndex_impl;
    df->cols         = dfColumns_impl;
    df->setValue     = dfSetValue_impl;
    df->setRow       = dfSetRow_impl;
    df->setColumn    = dfSetColumn_impl;
    df->renameColumn = dfRenameColumn_impl;
    df->reindex      = dfReindex_impl;
    df->take         = dfTake_impl;
    df->reorderColumns = dfReorderColumns_impl;


    // Printing / IO:
    df->print        = dfPrint_impl;
    df->readCsv      = readCsv_impl;
    df->plot         = dfPlot_impl;

    //Date/Time:
    df->convertToDatetime = dfConvertToDatetime_impl;
    df->datetimeToString  = dfDatetimeToString_impl;
    df->datetimeFilter    = dfDatetimeFilter_impl;
    df->datetimeTruncate  = dfDatetimeTruncate_impl;
    df->datetimeAdd       = dfDatetimeAdd_impl;
    df->datetimeDiff      = dfDatetimeDiff_impl;
    df->datetimeExtract   = dfDatetimeExtract_impl;
    df->datetimeGroupBy   = dfDatetimeGroupBy_impl;
    df->datetimeRound     = dfDatetimeRound_impl;
    df->datetimeRebase    = dfDatetimeRebase_impl;
    df->datetimeBetween   = dfDatetimeBetween_impl;
    df->datetimeClamp     = dfDatetimeClamp_impl;

    // Existing combining:
    df->concat       = dfConcat_impl;
    df->merge        = dfMerge_impl;
    df->join         = dfJoin_impl;
    df->unionDF      = dfUnion_impl;          // "union" might be a reserved word, so "unionDF"
    df->intersectionDF = dfIntersection_impl; // intersection
    df->differenceDF   = dfDifference_impl;   // difference
    df->semiJoin     = dfSemiJoin_impl;       
    df->antiJoin     = dfAntiJoin_impl;
    df->crossJoin    = dfCrossJoin_impl;

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

/* -------------------------------------------------------------
 * DataFrame Add Series
 * ------------------------------------------------------------- */
bool dfAddSeries_impl(DataFrame* df, const Series* s)
{
    if (!df || !s) return false;

    if (daSize(&df->columns) == 0) {
        df->nrows = seriesSize(s);
    } else {
        if (seriesSize(s) != df->nrows) {
            fprintf(stderr, 
                "Error: new Series '%s' has %zu rows; DataFrame has %zu rows.\n",
                s->name, seriesSize(s), df->nrows);
            return false;
        }
    }

    // Create a new Series in the DataFrame
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

            case DF_DATETIME: {
                long long dtVal;
                if (seriesGetDateTime(s, i, &dtVal)) {
                    seriesAddDateTime(&newSeries, dtVal);
                }
            } break;
        }
    }

    // Add the new Series to the DataFrame
    daPushBack(&df->columns, &newSeries, sizeof(Series));
    return true;
}

size_t dfNumColumns_impl(const DataFrame* df)
{
    if (!df) return 0;
    return daSize(&df->columns);
}

size_t dfNumRows_impl(const DataFrame* df)
{
    if (!df) return 0;
    return df->nrows;
}

const Series* dfGetSeries_impl(const DataFrame* df, size_t colIndex)
{
    if (!df) return NULL;
    if (colIndex >= daSize(&df->columns)) return NULL;
    return (const Series*)daGet(&df->columns, colIndex);
}

/*
 * Modify dfAddRow_impl to handle DF_DATETIME:
 * When rowData[c] is a pointer to a 64-bit datetime value, we add it
 */
bool dfAddRow_impl(DataFrame* df, const void** rowData)
{
    if (!df || !rowData) return false;

    size_t nCols = daSize(&df->columns);
    if (nCols == 0) {
        fprintf(stderr, "Error: DataFrame has no columns; can't add row.\n");
        return false;
    }

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

            /* ---------------------------------------------------------
             * NEW: DF_DATETIME 
             * --------------------------------------------------------- */
            case DF_DATETIME: {
                const long long* dtPtr = (const long long*)rowData[c];
                if (!dtPtr) return false;
                seriesAddDateTime(s, *dtPtr);
            } break;
        }
    }
    df->nrows += 1;
    return true;
}

bool dfGetRow_impl(const DataFrame* df, size_t rowIndex, void*** outRow)
{
    // 1) Validate inputs
    if (!df || !outRow) {
        return false;
    }
    // If rowIndex is out of range, fail
    size_t nRows = df->numRows(df);
    if (rowIndex >= nRows) {
        fprintf(stderr, "dfGetRow_impl: rowIndex=%zu out of range (max %zu)\n",
                rowIndex, (nRows == 0 ? 0 : nRows - 1));
        return false;
    }

    // 2) Determine the number of columns
    size_t nCols = df->numColumns(df);
    if (nCols == 0) {
        fprintf(stderr, "dfGetRow_impl: DataFrame has no columns.\n");
        return false;
    }

    // 3) Allocate an array of pointers - one pointer per column cell
    // The caller will free each pointer.
    void** rowData = (void**)calloc(nCols, sizeof(void*));
    if (!rowData) {
        fprintf(stderr, "dfGetRow_impl: out of memory.\n");
        return false;
    }

    // 4) For each column, read the cell => allocate => store pointer in rowData[c]
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) {
            // Should not happen, but just in case
            rowData[c] = NULL;
            continue;
        }

        switch (s->type) {
            case DF_INT: {
                int val = 0;
                bool got = seriesGetInt(s, rowIndex, &val);
                if (!got) {
                    rowData[c] = NULL;
                    break;
                }
                int* cellPtr = (int*)malloc(sizeof(int));
                if (!cellPtr) {
                    rowData[c] = NULL;
                    break;
                }
                *cellPtr = val;
                rowData[c] = cellPtr;
            } break;

            case DF_DOUBLE: {
                double val = 0.0;
                bool got = seriesGetDouble(s, rowIndex, &val);
                if (!got) {
                    rowData[c] = NULL;
                    break;
                }
                double* cellPtr = (double*)malloc(sizeof(double));
                if (!cellPtr) {
                    rowData[c] = NULL;
                    break;
                }
                *cellPtr = val;
                rowData[c] = cellPtr;
            } break;

            case DF_STRING: {
                // For DF_STRING, we can read a fresh copy from seriesGetString
                char* strVal = NULL;
                bool got = seriesGetString(s, rowIndex, &strVal);
                // If got = true, strVal is a newly allocated string
                if (!got || !strVal) {
                    rowData[c] = NULL;
                    if (strVal) free(strVal); // free partial
                    break;
                }
                // We can just store strVal pointer directly (already allocated)
                rowData[c] = strVal; 
            } break;

            case DF_DATETIME: {
                long long dtVal = 0;
                bool got = seriesGetDateTime(s, rowIndex, &dtVal);
                if (!got) {
                    rowData[c] = NULL;
                    break;
                }
                long long* cellPtr = (long long*)malloc(sizeof(long long));
                if (!cellPtr) {
                    rowData[c] = NULL;
                    break;
                }
                *cellPtr = dtVal;
                rowData[c] = cellPtr;
            } break;
        }
    }

    // 5) Assign the result array to *outRow
    *outRow = rowData;
    return true;
}
