#ifndef DATAFRAME_H
#define DATAFRAME_H

#include <stddef.h>
#include <stdbool.h>
#include "../DynamicArray/dynamic_array.h"
#include "../Series/series.h"
/* -------------------------------------------------------------------------
 * Enums, Structs, and Type Definitions
 * ------------------------------------------------------------------------- */

// A DataFrame holds an array of Series (columns) plus a row count.
typedef struct {
    DynamicArray columns;  // DynamicArray of Series
    size_t       nrows;    // Number of rows (all series must be the same length)
} DataFrame;


/* -------------------------------------------------------------------------
 * DataFrame Functions
 * ------------------------------------------------------------------------- */

// Initialize a DataFrame (no columns, 0 rows).
void dfInit(DataFrame* df);

// Free internal memory in the DataFrame (including all Series).
void dfFree(DataFrame* df);

// Add a Series to the DataFrame. 
// If the DataFrame is empty, it just adds it and sets df->nrows = seriesSize.
// If not empty, the Series must have the same length as existing columns.
bool dfAddSeries(DataFrame* df, const Series* s);

// Number of columns in the DataFrame.
size_t dfNumColumns(const DataFrame* df);

// Number of rows in the DataFrame.
size_t dfNumRows(const DataFrame* df);

// Get a pointer to the Series at a given column index (NULL if out of range).
const Series* dfGetSeries(const DataFrame* df, size_t colIndex);

// Add a single row to the DataFrame by specifying data for each column.
//
// data[i] should be a pointer to an int/double/char* depending on the column type.
// Example usage for row insertion with 2 columns:
//    const void* rowData[2];
//    int valInt = 10;
//    double valDouble = 3.14;
//    rowData[0] = &valInt; 
//    rowData[1] = &valDouble;
//    dfAddRow(&df, rowData);
//
bool dfAddRow(DataFrame* df, const void** rowData);

// Print the entire DataFrame in a basic table format.
void dfPrint(const DataFrame* df);

// Print the first N rows of the DataFrame.
void dfHead(const DataFrame* df, size_t n);

// Print the last N rows of the DataFrame.
void dfTail(const DataFrame* df, size_t n);

// Example of a simple "describe" function, printing min, max, count, and mean
// for numeric columns.
void dfDescribe(const DataFrame* df);

void dfPlot(const DataFrame* df,
    size_t xColIndex,
    const size_t* yColIndices,
    size_t yCount,
    const char* plotType,
    const char* outputFile);


/**
 * readCsv:
 *  - Creates columns based on the header line in the CSV.
 *  - Guesses types from the first data line (int, double, or fallback to string).
 *  - Loads all rows into the DataFrame.
 *
 * Returns true on success, false on failure.
 */
bool readCsv(DataFrame* df, const char* filename);



/**
 * dfConvertDatesToEpoch:
 *  Takes a numeric column that represents a date/time and converts
 *  each value into a Unix timestamp (in seconds or milliseconds).
 *
 *  For example, if your data is in 'YYYYMMDD' integer format, we parse
 *  it into a time_t. Or if you have some other numeric date, we convert accordingly.
 *
 *  After this, dfPlot can treat that column as a numeric epoch. Then in the
 *  generated Python script, we do:
 *      pd.to_datetime(df_data['time'], unit='ms')
 *  or 'unit='s'' if we used seconds.
 *
 * @param df:          pointer to your DataFrame
 * @param dateColIndex index of the column in the DataFrame that has numeric date/time
 * @param formatType:  how the numeric date/time is encoded (e.g.,  'YYYYMMDD', 'excel', 'unix_seconds', etc.)
 * @param toMillis:    if true, output in milliseconds since epoch; else seconds
 *
 * Returns true on success, false on error.
 */
bool dfConvertDatesToEpoch(DataFrame* df, size_t dateColIndex, const char* formatType, bool toMillis);

#endif // DATAFRAME_H
