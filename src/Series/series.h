#ifndef SERIES_H
#define SERIES_H

#include <stddef.h>   // for size_t
#include <stdbool.h>  // for bool

#include "../DynamicArray/dynamic_array.h"  // or wherever dynamic_array.h is

/*
 * ColumnType enum might live in dataframe.h, but if it’s needed by Series
 * alone, you can keep it here. For consistency, let's keep it here.
 */
typedef enum {
    DF_INT,
    DF_DOUBLE,
    DF_STRING
} ColumnType;

/*
 * Series: a single column of data (with a name, type, and dynamic array).
 */
typedef struct {
    char*        name;
    ColumnType   type;
    DynamicArray data;   // each element is a copy of the item
} Series;

/*
 * Series Functions
 */

/**
 * Initialize a Series with a name and type.
 */
void seriesInit(Series* s, const char* name, ColumnType type);

/**
 * Free internal memory in a Series.
 * (Does not free the Series pointer itself if it’s allocated on the heap;
 *  it only frees the internal data array and the name string.)
 */
void seriesFree(Series* s);

/**
 * Get the size (number of elements) of the Series.
 */
size_t seriesSize(const Series* s);

/**
 * Add an integer value to the Series (type must be DF_INT).
 */
void seriesAddInt(Series* s, int value);

/**
 * Add a double value to the Series (type must be DF_DOUBLE).
 */
void seriesAddDouble(Series* s, double value);

/**
 * Add a string value to the Series (type must be DF_STRING).
 * Internally copies the string.
 */
void seriesAddString(Series* s, const char* str);

/**
 * Retrieve an integer from a Series at a given row index (if DF_INT).
 * Returns false if out of range or wrong type.
 */
bool seriesGetInt(const Series* s, size_t index, int* outValue);

/**
 * Retrieve a double from a Series at a given row index (if DF_DOUBLE).
 * Returns false if out of range or wrong type.
 */
bool seriesGetDouble(const Series* s, size_t index, double* outValue);

/**
 * Retrieve a string from a Series at a given row index (if DF_STRING).
 * The function allocates a copy into *outStr (caller must free).
 * Returns false if out of range or wrong type.
 */
bool seriesGetString(const Series* s, size_t index, char** outStr);

/**
 * Print the contents of the Series (for debugging).
 */
void seriesPrint(const Series* s);

#endif // SERIES_H
