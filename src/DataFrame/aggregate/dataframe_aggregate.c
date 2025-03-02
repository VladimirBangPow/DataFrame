#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <math.h>
#include <string.h>
#include <stdbool.h>

#include "../dataframe.h"  // Make sure this has RowPredicate, RowFunction, etc.
#include "../../Series/series.h"


/* -------------------------------------------------------------------------
 * SUM
 * ------------------------------------------------------------------------- */

double dfSum_impl(const DataFrame* df, size_t colIndex)
{
    if (!df) return 0.0;
    const Series* s = df->getSeries(df, colIndex);
    if (!s) return 0.0;

    double sumVal = 0.0;
    size_t nRows = seriesSize(s);

    switch (s->type) {
        case DF_INT: {
            for (size_t r = 0; r < nRows; r++) {
                int v;
                if (seriesGetInt(s, r, &v)) {
                    sumVal += v;
                }
            }
        } break;
        case DF_DOUBLE: {
            for (size_t r = 0; r < nRows; r++) {
                double d;
                if (seriesGetDouble(s, r, &d)) {
                    sumVal += d;
                }
            }
        } break;
        /* ----------------------------------------
         * NEW: DF_DATETIME => treat epoch as double
         * ----------------------------------------*/
        case DF_DATETIME: {
            for (size_t r = 0; r < nRows; r++) {
                long long dtVal;
                if (seriesGetDateTime(s, r, &dtVal)) {
                    sumVal += (double)dtVal;
                }
            }
        } break;
        default:
            break;
    }
    return sumVal;
}

/* -------------------------------------------------------------------------
 * MEAN
 * ------------------------------------------------------------------------- */

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

/* -------------------------------------------------------------------------
 * MIN
 * ------------------------------------------------------------------------- */
double dfMin_impl(const DataFrame* df, size_t colIndex)
{
    if (!df) return 0.0;
    const Series* s = df->getSeries(df, colIndex);
    if (!s) return 0.0;

    size_t n = seriesSize(s);
    if (n == 0) return 0.0;

    double minVal = 0.0;

    switch (s->type) {
        case DF_INT: {
            int tmp;
            // initialize from row 0
            if (!seriesGetInt(s, 0, &tmp)) {
                return 0.0;
            }
            minVal = (double)tmp;
            for (size_t r = 1; r < n; r++) {
                if (seriesGetInt(s, r, &tmp)) {
                    if (tmp < minVal) {
                        minVal = (double)tmp;
                    }
                }
            }
        } break;
        case DF_DOUBLE: {
            double d;
            // initialize from row 0
            if (!seriesGetDouble(s, 0, &d)) {
                return 0.0;
            }
            minVal = d;
            for (size_t r = 1; r < n; r++) {
                if (seriesGetDouble(s, r, &d)) {
                    if (d < minVal) {
                        minVal = d;
                    }
                }
            }
        } break;
        /* -----------------------------------
         * NEW: DF_DATETIME => treat as double
         * -----------------------------------*/
        case DF_DATETIME: {
            long long dtVal;
            // init from row 0
            if (!seriesGetDateTime(s, 0, &dtVal)) {
                return 0.0;
            }
            double d = (double)dtVal;
            minVal = d;
            for (size_t r = 1; r < n; r++) {
                if (seriesGetDateTime(s, r, &dtVal)) {
                    d = (double)dtVal;
                    if (d < minVal) {
                        minVal = d;
                    }
                }
            }
        } break;
        default:
            break;
    }
    return minVal;
}

/* -------------------------------------------------------------------------
 * MAX
 * ------------------------------------------------------------------------- */
double dfMax_impl(const DataFrame* df, size_t colIndex)
{
    if (!df) return 0.0;
    const Series* s = df->getSeries(df, colIndex);
    if (!s) return 0.0;

    size_t n = seriesSize(s);
    if (n == 0) return 0.0;

    double maxVal = 0.0;

    switch (s->type) {
        case DF_INT: {
            int tmp;
            if (!seriesGetInt(s, 0, &tmp)) {
                return 0.0;
            }
            maxVal = (double)tmp;
            for (size_t r = 1; r < n; r++) {
                if (seriesGetInt(s, r, &tmp)) {
                    if (tmp > maxVal) {
                        maxVal = (double)tmp;
                    }
                }
            }
        } break;
        case DF_DOUBLE: {
            double d;
            if (!seriesGetDouble(s, 0, &d)) {
                return 0.0;
            }
            maxVal = d;
            for (size_t r = 1; r < n; r++) {
                if (seriesGetDouble(s, r, &d)) {
                    if (d > maxVal) {
                        maxVal = d;
                    }
                }
            }
        } break;
        /* --------------------------------
         * NEW: DF_DATETIME => treat numeric
         * --------------------------------*/
        case DF_DATETIME: {
            long long dtVal;
            if (!seriesGetDateTime(s, 0, &dtVal)) {
                return 0.0;
            }
            double d = (double)dtVal;
            maxVal = d;
            for (size_t r = 1; r < n; r++) {
                if (seriesGetDateTime(s, r, &dtVal)) {
                    d = (double)dtVal;
                    if (d > maxVal) {
                        maxVal = d;
                    }
                }
            }
        } break;
        default:
            break;
    }
    return maxVal;
}



/* -------------------------------------------------------------------------
 * COUNT
 * ------------------------------------------------------------------------- */

 double dfCount_impl(const DataFrame* df, size_t colIndex)
 {
     if (!df) return 0.0;
 
     const Series* s = df->getSeries(df, colIndex);
     if (!s) return 0.0;
 
     size_t nRows = seriesSize(s);
     double countVal = 0.0;
 
     switch (s->type) {
         case DF_INT: {
             for (size_t i = 0; i < nRows; i++) {
                 int val;
                 if (seriesGetInt(s, i, &val)) {
                     countVal += 1.0;
                 }
             }
         } break;
         case DF_DOUBLE: {
             for (size_t i = 0; i < nRows; i++) {
                 double val;
                 if (seriesGetDouble(s, i, &val)) {
                     countVal += 1.0;
                 }
             }
         } break;
         case DF_DATETIME: {
             for (size_t i = 0; i < nRows; i++) {
                 long long val;
                 if (seriesGetDateTime(s, i, &val)) {
                     countVal += 1.0;
                 }
             }
         } break;
         case DF_STRING: {
             for (size_t i = 0; i < nRows; i++) {
                 char* tmp = NULL;
                 if (seriesGetString(s, i, &tmp)) {
                     // If you consider empty string as valid, just increment.
                     // If you want to consider it "null", check `tmp[0] != '\0'`.
                     countVal += 1.0;
                     free(tmp);
                 }
             }
         } break;
     }
 
     return countVal;
 }
 

/* -------------------------------------------------------------------------
 * MEDIAN
 * ------------------------------------------------------------------------- */
static int compareDoubles(const void* a, const void* b) {
    double da = *(const double*)a;
    double db = *(const double*)b;
    return (da > db) - (da < db); // simple comparison
}

double dfMedian_impl(const DataFrame* df, size_t colIndex)
{
    if (!df) return 0.0;

    const Series* s = df->getSeries(df, colIndex);
    if (!s) return 0.0;

    size_t n = seriesSize(s);
    if (n == 0) return 0.0;

    // Collect numeric values in a temp array
    double* values = (double*)malloc(n * sizeof(double));
    if (!values) return 0.0;

    size_t count = 0;
    for (size_t i = 0; i < n; i++) {
        double dVal = 0.0;
        bool ok = false;
        switch (s->type) {
            case DF_INT: {
                int tmp;
                ok = seriesGetInt(s, i, &tmp);
                if (ok) dVal = (double)tmp;
            } break;
            case DF_DOUBLE: {
                double tmp;
                ok = seriesGetDouble(s, i, &tmp);
                if (ok) dVal = tmp;
            } break;
            case DF_DATETIME: {
                long long tmp;
                ok = seriesGetDateTime(s, i, &tmp);
                if (ok) dVal = (double)tmp;
            } break;
            case DF_STRING:
            default:
                // no numeric extraction for strings
                ok = false;
                break;
        }
        if (ok) {
            values[count++] = dVal;
        }
    }

    if (count == 0) {
        free(values);
        return 0.0; // no numeric data
    }

    // sort the portion we filled
    qsort(values, count, sizeof(double), compareDoubles);

    double median = 0.0;
    if (count % 2 == 1) {
        // odd
        median = values[count / 2];
    } else {
        // even
        size_t mid = count / 2;
        median = (values[mid - 1] + values[mid]) / 2.0;
    }

    free(values);
    return median;
}


/* -------------------------------------------------------------------------
 * MODE
 * ------------------------------------------------------------------------- */


typedef struct {
    double value;
    size_t count;
} ModeEntry;

double dfMode_impl(const DataFrame* df, size_t colIndex)
{
    if (!df) return 0.0;
    const Series* s = df->getSeries(df, colIndex);
    if (!s) return 0.0;

    size_t n = seriesSize(s);
    if (n == 0) return 0.0;

    // We'll read numeric values into an array first
    double* arr = (double*)malloc(n * sizeof(double));
    if (!arr) return 0.0;

    size_t count = 0;
    for (size_t i = 0; i < n; i++) {
        double dVal;
        bool ok = false;
        switch (s->type) {
            case DF_INT: {
                int tmp;
                ok = seriesGetInt(s, i, &tmp);
                if (ok) dVal = (double)tmp;
            } break;
            case DF_DOUBLE: {
                double tmp;
                ok = seriesGetDouble(s, i, &tmp);
                if (ok) dVal = tmp;
            } break;
            case DF_DATETIME: {
                long long tmp;
                ok = seriesGetDateTime(s, i, &tmp);
                if (ok) dVal = (double)tmp;
            } break;
            default:
                // DF_STRING or unknown => skip
                break;
        }
        if (ok) {
            arr[count++] = dVal;
        }
    }
    if (count == 0) {
        free(arr);
        return 0.0;
    }

    // We'll do a simple frequency calculation
    // In production, you'd use a hash map. Here, let's do a naive approach ( O(n^2) ).
    double modeVal = arr[0];
    size_t modeCount = 1;

    for (size_t i = 0; i < count; i++) {
        double candidate = arr[i];
        size_t freq = 1;
        // count how many times candidate appears
        for (size_t j = i + 1; j < count; j++) {
            if (arr[j] == candidate) {
                freq++;
            }
        }
        if (freq > modeCount) {
            modeVal = candidate;
            modeCount = freq;
        }
    }

    free(arr);
    return modeVal;
}



/* -------------------------------------------------------------------------
 * STANDARD DEVIATION
 * ------------------------------------------------------------------------- */ 




 /* -------------------------------------------------------------------------
 * VARIANCE
 * ------------------------------------------------------------------------- */

 double dfVar_impl(const DataFrame* df, size_t colIndex)
 {
     if (!df) return 0.0;
     const Series* s = df->getSeries(df, colIndex);
     if (!s) return 0.0;
 
     size_t nRows = seriesSize(s);
     if (nRows < 2) {
         // can't compute sample variance with < 2
         return 0.0;
     }
 
     // 1) gather numeric values in a vector
     double* arr = (double*)malloc(nRows * sizeof(double));
     if (!arr) return 0.0;
 
     size_t count = 0;
     for (size_t i = 0; i < nRows; i++) {
         double dVal = 0.0;
         bool ok = false;
         switch (s->type) {
             case DF_INT: {
                 int tmp;
                 ok = seriesGetInt(s, i, &tmp);
                 if (ok) dVal = (double)tmp;
             } break;
             case DF_DOUBLE: {
                 double tmp;
                 ok = seriesGetDouble(s, i, &tmp);
                 if (ok) dVal = tmp;
             } break;
             case DF_DATETIME: {
                 long long tmp;
                 ok = seriesGetDateTime(s, i, &tmp);
                 if (ok) dVal = (double)tmp;
             } break;
             default:
                 break;
         }
         if (ok) {
             arr[count++] = dVal;
         }
     }
     if (count < 2) {
         free(arr);
         return 0.0;
     }
 
     // 2) compute mean
     double sum = 0.0;
     for (size_t i = 0; i < count; i++) {
         sum += arr[i];
     }
     double mean = sum / (double)count;
 
     // 3) sum of squares
     double sqSum = 0.0;
     for (size_t i = 0; i < count; i++) {
         double diff = arr[i] - mean;
         sqSum += diff * diff;
     }
 
     // sample variance => sqSum/(count-1)
     double variance = sqSum / ((double)count - 1.0);
     free(arr);
     return variance;
 }
 


 /* -------------------------------------------------------------------------
 * RANGE
 * ------------------------------------------------------------------------- */
double dfRange_impl(const DataFrame* df, size_t colIndex)
{

    double maxV = df->max(df, colIndex);
    double minV = df->min(df, colIndex);
    return (maxV - minV);
}



/* -------------------------------------------------------------------------
* QUANTILES/PERCENTILES
* ------------------------------------------------------------------------- */
double dfQuantile_impl(const DataFrame* df, size_t colIndex, double q)
{
    if (!df) return 0.0;
    if (q < 0.0) q = 0.0;
    if (q > 1.0) q = 1.0;

    const Series* s = df->getSeries(df, colIndex);
    if (!s) return 0.0;

    size_t n = seriesSize(s);
    if (n == 0) return 0.0;

    double* arr = (double*)malloc(n * sizeof(double));
    if (!arr) return 0.0;

    // gather
    size_t count = 0;
    for (size_t i = 0; i < n; i++) {
        double dVal;
        bool ok = false;
        switch (s->type) {
            case DF_INT: {
                int tmp;
                ok = seriesGetInt(s, i, &tmp);
                if (ok) dVal = (double)tmp;
            } break;
            case DF_DOUBLE: {
                double tmp;
                ok = seriesGetDouble(s, i, &tmp);
                if (ok) dVal = tmp;
            } break;
            case DF_DATETIME: {
                long long tmp;
                ok = seriesGetDateTime(s, i, &tmp);
                if (ok) dVal = (double)tmp;
            } break;
            default:
                break;
        }
        if (ok) {
            arr[count++] = dVal;
        }
    }

    if (count == 0) {
        free(arr);
        return 0.0;
    }

    // sort
    qsort(arr, count, sizeof(double), compareDoubles);

    // position
    double pos = q * (count - 1);
    size_t idxBelow = (size_t)floor(pos);
    size_t idxAbove = (size_t)ceil(pos);

    double result = 0.0;
    if (idxBelow == idxAbove) {
        // no interpolation needed
        result = arr[idxBelow];
    } else {
        // linear interpolation
        double fraction = pos - (double)idxBelow;
        result = arr[idxBelow] + fraction * (arr[idxAbove] - arr[idxBelow]);
    }

    free(arr);
    return result;
}


/* -------------------------------------------------------------------------
* IQR
* ------------------------------------------------------------------------- */
double dfIQR_impl(const DataFrame* df, size_t colIndex)
{
    double q1 = df->quantile(df, colIndex, 0.25);
    double q3 = df->quantile(df, colIndex, 0.75);
    return (q3 - q1);
}



/* -------------------------------------------------------------------------
* NULL COUNT
* ------------------------------------------------------------------------- */

double dfNullCount_impl(const DataFrame* df, size_t colIndex)
{
    if (!df) return 0.0;
    const Series* s = df->getSeries(df, colIndex);
    if (!s) return 0.0;

    size_t nRows = seriesSize(s);
    double nullCount = 0.0;

    switch (s->type) {
        case DF_INT: {
            for (size_t i = 0; i < nRows; i++) {
                int tmp;
                if (!seriesGetInt(s, i, &tmp)) {
                    nullCount += 1.0;
                }
            }
        } break;
        case DF_DOUBLE: {
            for (size_t i = 0; i < nRows; i++) {
                double d;
                if (!seriesGetDouble(s, i, &d)) {
                    nullCount += 1.0;
                }
            }
        } break;
        case DF_DATETIME: {
            for (size_t i = 0; i < nRows; i++) {
                long long dt;
                if (!seriesGetDateTime(s, i, &dt)) {
                    nullCount += 1.0;
                }
            }
        } break;
        case DF_STRING: {
            for (size_t i = 0; i < nRows; i++) {
                char* tmp = NULL;
                if (!seriesGetString(s, i, &tmp)) {
                    nullCount += 1.0;
                } else {
                    free(tmp);
                }
            }
        } break;
    }
    return nullCount;
}



/* -------------------------------------------------------------------------
* UNIQUE COUNT
    Definition: The number of distinct values in the column. For strings, we do a string comparison. For numeric, store them as double.

    In production, you’d likely use a hash set. Below is a simplified approach with a dynamic array and an O(n^2) check.
* ------------------------------------------------------------------------- */



double dfUniqueCount_impl(const DataFrame* df, size_t colIndex)
{
    if (!df) return 0.0;
    const Series* s = df->getSeries(df, colIndex);
    if (!s) return 0.0;

    size_t nRows = seriesSize(s);
    // We'll store values in memory (like we do in median)
    // but for strings, we store them separately.

    // For simplicity, handle numeric vs string separately:
    if (s->type == DF_STRING) {
        // gather all strings in a temporary array
        char** strArr = (char**)malloc(nRows * sizeof(char*));
        size_t count = 0;
        for (size_t i = 0; i < nRows; i++) {
            char* tmp = NULL;
            if (seriesGetString(s, i, &tmp) && tmp) {
                strArr[count++] = tmp; 
            }
        }
        // now find distinct strings
        size_t uniqueCount = 0;
        for (size_t i = 0; i < count; i++) {
            bool duplicate = false;
            for (size_t j = 0; j < i; j++) {
                if (strcmp(strArr[i], strArr[j]) == 0) {
                    duplicate = true;
                    break;
                }
            }
            if (!duplicate) {
                uniqueCount++;
            }
        }
        // free them
        for (size_t i = 0; i < count; i++) {
            free(strArr[i]);
        }
        free(strArr);
        return (double)uniqueCount;
    } else {
        // numeric
        double* arr = (double*)malloc(nRows * sizeof(double));
        size_t count = 0;
        for (size_t i = 0; i < nRows; i++) {
            double dVal;
            bool ok = false;
            switch (s->type) {
                case DF_INT: {
                    int tmp;
                    ok = seriesGetInt(s, i, &tmp);
                    if (ok) dVal = (double)tmp;
                } break;
                case DF_DOUBLE: {
                    double tmp;
                    ok = seriesGetDouble(s, i, &tmp);
                    if (ok) dVal = tmp;
                } break;
                case DF_DATETIME: {
                    long long tmp;
                    ok = seriesGetDateTime(s, i, &tmp);
                    if (ok) dVal = (double)tmp;
                } break;
                default:
                    break;
            }
            if (ok) {
                arr[count++] = dVal;
            }
        }
        // distinct check
        size_t uniqueCount = 0;
        for (size_t i = 0; i < count; i++) {
            bool duplicate = false;
            for (size_t j = 0; j < i; j++) {
                if (arr[i] == arr[j]) {
                    duplicate = true;
                    break;
                }
            }
            if (!duplicate) {
                uniqueCount++;
            }
        }
        free(arr);
        return (double)uniqueCount;
    }
}



/* -------------------------------------------------------------------------
* PRODUCT
* ------------------------------------------------------------------------- */
double dfProduct_impl(const DataFrame* df, size_t colIndex)
{
    if (!df) return 1.0; // if invalid df, return identity (1)
    const Series* s = df->getSeries(df, colIndex);
    if (!s) return 1.0;

    size_t n = seriesSize(s);
    if (n == 0) return 1.0;

    double product = 1.0;

    switch (s->type) {
        case DF_INT: {
            for (size_t i = 0; i < n; i++) {
                int tmp;
                if (seriesGetInt(s, i, &tmp)) {
                    product *= (double)tmp;
                }
            }
        } break;
        case DF_DOUBLE: {
            for (size_t i = 0; i < n; i++) {
                double d;
                if (seriesGetDouble(s, i, &d)) {
                    product *= d;
                }
            }
        } break;
        case DF_DATETIME: {
            for (size_t i = 0; i < n; i++) {
                long long dt;
                if (seriesGetDateTime(s, i, &dt)) {
                    product *= (double)dt;
                }
            }
        } break;
        default:
            // DF_STRING => skip
            break;
    }

    return product;
}

/* -------------------------------------------------------------------------
* Nth LARGEST/SMALLEST
* ------------------------------------------------------------------------- */

static int compareDoubles(const void* a, const void* b)
{
    double da = *(const double*)a;
    double db = *(const double*)b;
    // returns negative if da < db, 0 if equal, positive if da > db
    if (da < db) return -1;
    if (da > db) return 1;
    return 0;
}

// comparator for descending order
static int compareDoublesDescending(const void* a, const void* b)
{
    double da = *(const double*)a;
    double db = *(const double*)b;
    if (da > db) return -1;  // larger first
    if (da < db) return 1;
    return 0;
}

double dfNthLargest_impl(const DataFrame* df, size_t colIndex, size_t n)
{
    // Basic validations
    if (!df || n == 0) {
        // n=0 is invalid if we treat n as 1-based
        return 0.0;
    }

    const Series* s = df->getSeries(df, colIndex);
    if (!s) return 0.0;

    size_t nRows = seriesSize(s);
    if (nRows == 0) return 0.0;

    // Allocate an array for numeric data
    double* arr = (double*)malloc(nRows * sizeof(double));
    if (!arr) {
        // out of memory
        return 0.0;
    }

    // Gather numeric values
    size_t count = 0;
    for (size_t i = 0; i < nRows; i++) {
        bool ok = false;
        double dVal = 0.0;

        switch (s->type) {
            case DF_INT: {
                int tmp;
                ok = seriesGetInt(s, i, &tmp);
                if (ok) dVal = (double)tmp;
            } break;
            case DF_DOUBLE: {
                double tmp;
                ok = seriesGetDouble(s, i, &tmp);
                if (ok) dVal = tmp;
            } break;
            case DF_DATETIME: {
                long long tmp;
                ok = seriesGetDateTime(s, i, &tmp);
                if (ok) dVal = (double)tmp;
            } break;
            default:
                // DF_STRING or unknown => skip
                break;
        }
        if (ok) {
            arr[count++] = dVal;
        }
    }

    if (count == 0) {
        // no numeric data
        free(arr);
        return 0.0;
    }

    // Sort in descending order
    qsort(arr, count, sizeof(double), compareDoublesDescending);

    // If n > count => out of range
    if (n > count) {
        free(arr);
        return 0.0;
    }

    // nth largest => arr[n-1], because array is sorted desc
    double result = arr[n - 1];
    free(arr);
    return result;
}

double dfNthSmallest_impl(const DataFrame* df, size_t colIndex, size_t n)
{
    if (!df || n == 0) {
        return 0.0;
    }

    const Series* s = df->getSeries(df, colIndex);
    if (!s) return 0.0;

    size_t nRows = seriesSize(s);
    if (nRows == 0) return 0.0;

    double* arr = (double*)malloc(nRows * sizeof(double));
    if (!arr) {
        return 0.0;
    }

    size_t count = 0;
    for (size_t i = 0; i < nRows; i++) {
        bool ok = false;
        double dVal = 0.0;
        switch (s->type) {
            case DF_INT: {
                int tmp;
                ok = seriesGetInt(s, i, &tmp);
                if (ok) dVal = (double)tmp;
            } break;
            case DF_DOUBLE: {
                double tmp;
                ok = seriesGetDouble(s, i, &tmp);
                if (ok) dVal = tmp;
            } break;
            case DF_DATETIME: {
                long long tmp;
                ok = seriesGetDateTime(s, i, &tmp);
                if (ok) dVal = (double)tmp;
            } break;
            default:
                // DF_STRING => skip
                break;
        }
        if (ok) {
            arr[count++] = dVal;
        }
    }

    if (count == 0) {
        free(arr);
        return 0.0;
    }

    // Sort ascending
    extern int compareDoubles(const void*, const void*); 
    // (define above or inline)
    qsort(arr, count, sizeof(double), compareDoubles);

    if (n > count) {
        free(arr);
        return 0.0;
    }

    // nth smallest => arr[n-1] in ascending array
    double result = arr[n - 1];
    free(arr);
    return result;
}



/* -------------------------------------------------------------------------
* SKEWNESS
* ------------------------------------------------------------------------- */
double dfSkewness_impl(const DataFrame* df, size_t colIndex)
{
    if (!df) {
        // Invalid DataFrame pointer
        return 0.0;
    }

    // 1) Retrieve the column
    const Series* s = df->getSeries(df, colIndex);
    if (!s) {
        // Invalid colIndex or no series
        return 0.0;
    }

    // 2) Number of rows
    size_t nRows = seriesSize(s);
    if (nRows < 3) {
        // We can't do sample skewness with fewer than 3 points
        return 0.0;
    }

    // 3) Allocate an array to store numeric values
    double* values = (double*)malloc(nRows * sizeof(double));
    if (!values) {
        // Out of memory
        return 0.0;
    }

    // 4) Gather numeric data
    size_t count = 0;
    for (size_t i = 0; i < nRows; i++) {
        bool ok = false;
        double dVal = 0.0;

        switch (s->type) {
            case DF_INT: {
                int tmp;
                ok = seriesGetInt(s, i, &tmp);
                if (ok) dVal = (double)tmp;
            } break;
            case DF_DOUBLE: {
                double tmp;
                ok = seriesGetDouble(s, i, &tmp);
                if (ok) dVal = tmp;
            } break;
            case DF_DATETIME: {
                long long tmp;
                ok = seriesGetDateTime(s, i, &tmp);
                if (ok) dVal = (double)tmp;
            } break;
            default:
                // DF_STRING or unknown => skip
                break;
        }
        if (ok) {
            values[count++] = dVal;
        }
    }

    // If we ended up with fewer than 3 numeric points, can't compute sample skewness
    if (count < 3) {
        free(values);
        return 0.0;
    }

    // 5) Compute mean
    double sum = 0.0;
    for (size_t i = 0; i < count; i++) {
        sum += values[i];
    }
    double mean = sum / (double)count;

    // 6) Compute sample standard deviation
    //    variance = sum((x - mean)^2) / (count - 1)
    double sqSum = 0.0;
    for (size_t i = 0; i < count; i++) {
        double diff = values[i] - mean;
        sqSum += diff * diff;
    }
    double var = sqSum / ((double)count - 1.0);
    if (var == 0.0) {
        // All values are identical => skewness=0
        free(values);
        return 0.0;
    }
    double stdev = sqrt(var);

    // 7) Compute sum of cubes
    double cubeSum = 0.0;
    for (size_t i = 0; i < count; i++) {
        double diff = values[i] - mean;
        // (diff^3)
        cubeSum += diff * diff * diff;
    }

    // 8) Apply the Fisher–Pearson sample skewness formula:
    //    skew = (n / ((n-1)*(n-2))) * ( cubeSum / (stdev^3) )
    double n = (double)count;
    double numerator   = cubeSum;
    double denominator = stdev * stdev * stdev; // stdev^3
    double correction  = n / ((n - 1.0) * (n - 2.0));
    double skew = correction * (numerator / denominator);

    // Cleanup
    free(values);

    // 9) Return result
    return skew;
}

/* -------------------------------------------------------------------------
* KURTOSIS
* ------------------------------------------------------------------------- */
double dfKurtosis_impl(const DataFrame* df, size_t colIndex)
{
    if (!df) {
        // invalid DataFrame pointer
        return 0.0;
    }

    // 1) Retrieve the column
    const Series* s = df->getSeries(df, colIndex);
    if (!s) {
        // invalid colIndex or no series
        return 0.0;
    }

    size_t nRows = seriesSize(s);
    if (nRows < 4) {
        // we need at least 4 points to do sample kurtosis reliably
        return 0.0;
    }

    // 2) Allocate an array for numeric values
    double* values = (double*)malloc(nRows * sizeof(double));
    if (!values) {
        // out of memory
        return 0.0;
    }

    // 3) Gather numeric data (DF_INT, DF_DOUBLE, DF_DATETIME)
    size_t count = 0;
    for (size_t i = 0; i < nRows; i++) {
        bool ok = false;
        double dVal = 0.0;

        switch (s->type) {
            case DF_INT: {
                int tmp;
                ok = seriesGetInt(s, i, &tmp);
                if (ok) dVal = (double)tmp;
            } break;
            case DF_DOUBLE: {
                double tmp;
                ok = seriesGetDouble(s, i, &tmp);
                if (ok) dVal = tmp;
            } break;
            case DF_DATETIME: {
                long long tmp;
                ok = seriesGetDateTime(s, i, &tmp);
                if (ok) dVal = (double)tmp;
            } break;
            default:
                // DF_STRING => skip
                break;
        }
        if (ok) {
            values[count++] = dVal;
        }
    }

    // If fewer than 4 numeric values
    if (count < 4) {
        free(values);
        return 0.0;
    }

    // 4) Compute the mean
    double sum = 0.0;
    for (size_t i = 0; i < count; i++) {
        sum += values[i];
    }
    double mean = sum / (double)count;

    // 5) Compute sample variance => stdev
    double sqSum = 0.0;
    for (size_t i = 0; i < count; i++) {
        double diff = values[i] - mean;
        sqSum += diff * diff;
    }
    double var = sqSum / (double)(count - 1);
    if (var == 0.0) {
        // data is constant => kurtosis=0
        free(values);
        return 0.0;
    }
    double stdev = sqrt(var);

    // 6) Compute the sum of (x_i - mean)^4
    double sumFourth = 0.0;
    for (size_t i = 0; i < count; i++) {
        double diff = (values[i] - mean) / stdev;  // standardize
        sumFourth += diff * diff * diff * diff;    // (diff^4)
    }

    // 7) Apply the Fisher-Pearson sample kurtosis formula (excess kurtosis)
    double n = (double)count;
    double c1 = (n * (n + 1)) / ((n - 1) * (n - 2) * (n - 3));
    double c2 = 3.0 * (n - 1) * (n - 1) / ((n - 2) * (n - 3));

    double kurtosis = c1 * sumFourth - c2;

    // Cleanup
    free(values);

    return kurtosis;
}
/* -------------------------------------------------------------------------
* COVARIANCE
* ------------------------------------------------------------------------- */
double dfCovariance_impl(const DataFrame* df, size_t colIndex1, size_t colIndex2)
{
    // 1) Validate inputs
    if (!df) {
        return 0.0;
    }

    const Series* s1 = df->getSeries(df, colIndex1);
    const Series* s2 = df->getSeries(df, colIndex2);
    if (!s1 || !s2) {
        // Invalid column indices or no series
        return 0.0;
    }

    // 2) Determine row count
    size_t nRows = df->numRows(df);
    if (nRows < 2) {
        // Need at least 2 points to compute sample covariance
        return 0.0;
    }

    // 3) Gather paired numeric data
    //    We'll keep them in parallel arrays xVals[i], yVals[i]
    double* xVals = (double*)malloc(nRows * sizeof(double));
    double* yVals = (double*)malloc(nRows * sizeof(double));
    if (!xVals || !yVals) {
        // Out of memory
        free(xVals);  // if yVals is null or vice versa
        free(yVals);
        return 0.0;
    }

    size_t count = 0;
    // For each row, try to read numeric from both columns
    for (size_t i = 0; i < nRows; i++) {
        bool ok1 = false;
        bool ok2 = false;
        double dx = 0.0;
        double dy = 0.0;

        // Column 1 read
        switch (s1->type) {
            case DF_INT: {
                int tmp;
                ok1 = seriesGetInt(s1, i, &tmp);
                if (ok1) dx = (double)tmp;
            } break;
            case DF_DOUBLE: {
                double tmp;
                ok1 = seriesGetDouble(s1, i, &tmp);
                if (ok1) dx = tmp;
            } break;
            case DF_DATETIME: {
                long long tmp;
                ok1 = seriesGetDateTime(s1, i, &tmp);
                if (ok1) dx = (double)tmp;
            } break;
            default:
                // DF_STRING or unknown => skip
                break;
        }

        // Column 2 read
        switch (s2->type) {
            case DF_INT: {
                int tmp;
                ok2 = seriesGetInt(s2, i, &tmp);
                if (ok2) dy = (double)tmp;
            } break;
            case DF_DOUBLE: {
                double tmp;
                ok2 = seriesGetDouble(s2, i, &tmp);
                if (ok2) dy = tmp;
            } break;
            case DF_DATETIME: {
                long long tmp;
                ok2 = seriesGetDateTime(s2, i, &tmp);
                if (ok2) dy = (double)tmp;
            } break;
            default:
                break;
        }

        // If both reads are successful => store them
        if (ok1 && ok2) {
            xVals[count] = dx;
            yVals[count] = dy;
            count++;
        }
    }

    // If fewer than 2 valid numeric pairs => can't do sample covariance
    if (count < 2) {
        free(xVals);
        free(yVals);
        return 0.0;
    }

    // 4) Compute meanX, meanY
    double sumX = 0.0, sumY = 0.0;
    for (size_t i = 0; i < count; i++) {
        sumX += xVals[i];
        sumY += yVals[i];
    }
    double meanX = sumX / (double)count;
    double meanY = sumY / (double)count;

    // 5) Compute the sum of (x_i - meanX)*(y_i - meanY)
    double covSum = 0.0;
    for (size_t i = 0; i < count; i++) {
        double dx = xVals[i] - meanX;
        double dy = yVals[i] - meanY;
        covSum += (dx * dy);
    }

    // 6) Sample covariance => covSum / (count - 1)
    double covariance = covSum / (double)(count - 1);

    // 7) Cleanup
    free(xVals);
    free(yVals);

    // 8) Return the covariance
    return covariance;
}

/* -------------------------------------------------------------------------
* CORRELATION
* ------------------------------------------------------------------------- */
double dfCorrelation_impl(const DataFrame* df, size_t colIndexX, size_t colIndexY)
{
    // Basic validations
    if (!df) return 0.0;



    double cov = df->covariance(df, colIndexX, colIndexY);
    if (cov == 0.0) {
        // Either no data, or actual covariance=0 => correlation=0
        return 0.0;
    }

    double stdX = df->std(df, colIndexX);
    double stdY = df->std(df, colIndexY);

    // If either column has zero variance => correlation is undefined => return 0.0
    if (stdX == 0.0 || stdY == 0.0) {
        return 0.0;
    }

    // correlation = covariance / (stdX * stdY)
    return cov / (stdX * stdY);
}
/* -------------------------------------------------------------------------
* UNIQUE VALUES
* ------------------------------------------------------------------------- */
DataFrame dfUniqueValues_impl(const DataFrame* df, size_t colIndex)
{
    // Create an empty DataFrame to return on failure or if no data
    DataFrame result;
    DataFrame_Create(&result);

    // 1) Validate input
    if (!df) {
        return result; // empty
    }

    const Series* s = df->getSeries(df, colIndex);
    if (!s) {
        // invalid column index
        return result;
    }

    size_t nRows = df->numRows(df);
    if (nRows == 0) {
        // no data
        return result;
    }

    // 2) We'll store the distinct values in a dynamic array (or arrays).
    //    Implementation differs by type; let's do one approach per type:
    switch (s->type) {
        case DF_INT: {
            // We'll gather distinct ints
            int* values = (int*)malloc(nRows * sizeof(int));
            size_t count = 0;

            for (size_t r = 0; r < nRows; r++) {
                int val;
                if (seriesGetInt(s, r, &val)) {
                    // check for duplicates in [values[0..count-1]]
                    bool found = false;
                    for (size_t i = 0; i < count; i++) {
                        if (values[i] == val) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        values[count++] = val;
                    }
                }
            }
            // Now we have 'count' distinct ints in 'values'.

            // Build a Series of DF_INT
            Series outS;
            seriesInit(&outS, "unique", DF_INT);

            for (size_t i = 0; i < count; i++) {
                seriesAddInt(&outS, values[i]);
            }

            // Add to result DataFrame
            result.addSeries(&result, &outS);
            seriesFree(&outS);
            free(values);
        } break;

        case DF_DOUBLE: {
            double* values = (double*)malloc(nRows * sizeof(double));
            size_t count = 0;

            for (size_t r = 0; r < nRows; r++) {
                double val;
                if (seriesGetDouble(s, r, &val)) {
                    // check duplicates
                    bool found = false;
                    for (size_t i = 0; i < count; i++) {
                        if (values[i] == val) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        values[count++] = val;
                    }
                }
            }

            // Build output Series as DF_DOUBLE
            Series outS;
            seriesInit(&outS, "unique", DF_DOUBLE);

            for (size_t i = 0; i < count; i++) {
                seriesAddDouble(&outS, values[i]);
            }

            result.addSeries(&result, &outS);
            seriesFree(&outS);
            free(values);
        } break;

        case DF_DATETIME: {
            // We'll treat DF_DATETIME as a 64-bit integer (long long) but store it in that type
            long long* values = (long long*)malloc(nRows * sizeof(long long));
            size_t count = 0;

            for (size_t r = 0; r < nRows; r++) {
                long long dt;
                if (seriesGetDateTime(s, r, &dt)) {
                    bool found = false;
                    for (size_t i = 0; i < count; i++) {
                        if (values[i] == dt) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        values[count++] = dt;
                    }
                }
            }

            // We'll create a DF_DATETIME series
            Series outS;
            seriesInit(&outS, "unique", DF_DATETIME);

            for (size_t i = 0; i < count; i++) {
                seriesAddDateTime(&outS, values[i]);
            }

            result.addSeries(&result, &outS);
            seriesFree(&outS);
            free(values);
        } break;

        case DF_STRING: {
            // We'll gather distinct strings
            // We must store them separately to compare content
            char** values = (char**)malloc(nRows * sizeof(char*));
            size_t count = 0;

            for (size_t r = 0; r < nRows; r++) {
                char* strVal = NULL;
                if (seriesGetString(s, r, &strVal) && strVal) {
                    // check duplicates
                    bool found = false;
                    for (size_t i = 0; i < count; i++) {
                        if (strcmp(values[i], strVal) == 0) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        values[count++] = strVal; // keep it
                    } else {
                        free(strVal); // discard
                    }
                }
            }

            // Build output series as DF_STRING
            Series outS;
            seriesInit(&outS, "unique", DF_STRING);

            for (size_t i = 0; i < count; i++) {
                seriesAddString(&outS, values[i]);
                free(values[i]); // seriesAddString copies, so free
            }
            result.addSeries(&result, &outS);
            seriesFree(&outS);

            free(values);
        } break;
    }

    return result;
}
/* -------------------------------------------------------------------------
* VALUE COUNTS/FREQUENCY TABLE
* ------------------------------------------------------------------------- */
DataFrame dfValueCounts_impl(const DataFrame* df, size_t colIndex)
{
    // Create an empty DataFrame to return if anything fails
    DataFrame result;
    DataFrame_Create(&result);

    // Validate input
    if (!df) {
        return result;
    }

    const Series* s = df->getSeries(df, colIndex);
    if (!s) {
        // invalid column
        return result;
    }

    size_t nRows = df->numRows(df);
    if (nRows == 0) {
        // no data
        return result;
    }

    // We'll build arrays of (distinct value, count)
    // Then convert them into 2 Series: "value", "count"

    switch (s->type) {
        case DF_INT: {
            // We'll store distinct integers + frequency
            int*   values = (int*)malloc(nRows * sizeof(int));
            int*   counts = (int*)calloc(nRows, sizeof(int));
            size_t distinctCount = 0;

            for (size_t r = 0; r < nRows; r++) {
                int val;
                if (seriesGetInt(s, r, &val)) {
                    // search if we already have 'val'
                    bool found = false;
                    for (size_t i = 0; i < distinctCount; i++) {
                        if (values[i] == val) {
                            counts[i]++;
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        values[distinctCount] = val;
                        counts[distinctCount] = 1;
                        distinctCount++;
                    }
                }
            }

            // Now build "value" (DF_INT) and "count" (DF_INT)
            Series valSeries;
            Series cntSeries;
            seriesInit(&valSeries,  "value", DF_INT);
            seriesInit(&cntSeries, "count", DF_INT);

            for (size_t i = 0; i < distinctCount; i++) {
                seriesAddInt(&valSeries,  values[i]);
                seriesAddInt(&cntSeries, counts[i]);
            }

            result.addSeries(&result, &valSeries);
            result.addSeries(&result, &cntSeries);

            seriesFree(&valSeries);
            seriesFree(&cntSeries);

            free(values);
            free(counts);
        } break;

        case DF_DOUBLE: {
            double* vals   = (double*)malloc(nRows * sizeof(double));
            int*    counts = (int*)calloc(nRows, sizeof(int));
            size_t distinctCount = 0;

            for (size_t r = 0; r < nRows; r++) {
                double d;
                if (seriesGetDouble(s, r, &d)) {
                    bool found = false;
                    for (size_t i = 0; i < distinctCount; i++) {
                        if (vals[i] == d) {
                            counts[i]++;
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        vals[distinctCount]   = d;
                        counts[distinctCount] = 1;
                        distinctCount++;
                    }
                }
            }

            // Build "value" (DF_DOUBLE), "count" (DF_INT)
            Series valSeries, cntSeries;
            seriesInit(&valSeries, "value", DF_DOUBLE);
            seriesInit(&cntSeries, "count", DF_INT);

            for (size_t i = 0; i < distinctCount; i++) {
                seriesAddDouble(&valSeries, vals[i]);
                seriesAddInt(&cntSeries, counts[i]);
            }

            result.addSeries(&result, &valSeries);
            result.addSeries(&result, &cntSeries);

            seriesFree(&valSeries);
            seriesFree(&cntSeries);

            free(vals);
            free(counts);
        } break;

        case DF_DATETIME: {
            // We'll treat DF_DATETIME as 64-bit integers
            long long* values = (long long*)malloc(nRows * sizeof(long long));
            int*       counts = (int*)calloc(nRows, sizeof(int));
            size_t distinctCount = 0;

            for (size_t r = 0; r < nRows; r++) {
                long long dtVal;
                if (seriesGetDateTime(s, r, &dtVal)) {
                    bool found = false;
                    for (size_t i = 0; i < distinctCount; i++) {
                        if (values[i] == dtVal) {
                            counts[i]++;
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        values[distinctCount] = dtVal;
                        counts[distinctCount] = 1;
                        distinctCount++;
                    }
                }
            }

            // Build "value" (DF_DATETIME), "count" (DF_INT)
            Series valSeries, cntSeries;
            seriesInit(&valSeries, "value", DF_DATETIME);
            seriesInit(&cntSeries, "count", DF_INT);

            for (size_t i = 0; i < distinctCount; i++) {
                seriesAddDateTime(&valSeries, values[i]);
                seriesAddInt(&cntSeries, counts[i]);
            }

            result.addSeries(&result, &valSeries);
            result.addSeries(&result, &cntSeries);

            seriesFree(&valSeries);
            seriesFree(&cntSeries);

            free(values);
            free(counts);
        } break;

        case DF_STRING: {
            // We'll store distinct strings + frequency
            char** strArr = (char**)malloc(nRows * sizeof(char*));
            int*   counts = (int*)calloc(nRows, sizeof(int));
            size_t distinctCount = 0;

            for (size_t r = 0; r < nRows; r++) {
                char* strVal = NULL;
                if (seriesGetString(s, r, &strVal) && strVal) {
                    bool found = false;
                    for (size_t i = 0; i < distinctCount; i++) {
                        if (strcmp(strArr[i], strVal) == 0) {
                            counts[i]++;
                            found = true;
                            free(strVal); // not storing a new copy
                            break;
                        }
                    }
                    if (!found) {
                        strArr[distinctCount] = strVal;
                        counts[distinctCount] = 1;
                        distinctCount++;
                    }
                }
            }

            // Build "value" (DF_STRING), "count" (DF_INT)
            Series valSeries, cntSeries;
            seriesInit(&valSeries, "value", DF_STRING);
            seriesInit(&cntSeries, "count", DF_INT);

            for (size_t i = 0; i < distinctCount; i++) {
                seriesAddString(&valSeries, strArr[i]);
                seriesAddInt(&cntSeries, counts[i]);
                free(strArr[i]); // seriesAddString copies, so safe to free
            }
            result.addSeries(&result, &valSeries);
            result.addSeries(&result, &cntSeries);

            seriesFree(&valSeries);
            seriesFree(&cntSeries);

            free(strArr);
            free(counts);
        } break;
    }

    return result;
}
/* -------------------------------------------------------------------------
* CUMULATIVE SUM
* ------------------------------------------------------------------------- */
DataFrame dfCumulativeSum_impl(const DataFrame* df, size_t colIndex)
{
    // Create an empty DataFrame if something fails
    DataFrame result;
    DataFrame_Create(&result);

    if (!df) {
        return result;
    }

    const Series* s = df->getSeries(df, colIndex);
    if (!s) {
        // invalid column
        return result;
    }

    // We'll create a new Series for the cumsum (type DF_DOUBLE).
    Series cumsumSeries;
    seriesInit(&cumsumSeries, "cumsum", DF_DOUBLE);

    size_t nRows = df->numRows(df);
    double partialSum = 0.0;

    for (size_t r = 0; r < nRows; r++) {
        bool readOk = false;
        double val = 0.0;

        // Attempt to read numeric data
        switch (s->type) {
            case DF_INT: {
                int tmp;
                if (seriesGetInt(s, r, &tmp)) {
                    val = (double)tmp;
                    readOk = true;
                }
            } break;
            case DF_DOUBLE: {
                double tmp;
                if (seriesGetDouble(s, r, &tmp)) {
                    val = tmp;
                    readOk = true;
                }
            } break;
            case DF_DATETIME: {
                long long tmp;
                if (seriesGetDateTime(s, r, &tmp)) {
                    val = (double)tmp;
                    readOk = true;
                }
            } break;
            case DF_STRING:
            default:
                // Not numeric => can't read => readOk=false
                break;
        }

        if (readOk) {
            partialSum += val;
        }
        // Even if read fails, we keep partialSum as is.

        // Store the current partialSum in the new Series for row r
        seriesAddDouble(&cumsumSeries, partialSum);
    }

    // Now add this cumsumSeries to the result DataFrame
    bool ok = result.addSeries(&result, &cumsumSeries);
    (void)ok; // ignore or check if needed

    seriesFree(&cumsumSeries);

    return result;
}
/* -------------------------------------------------------------------------
* CUMULATIVE PRODUCT
* ------------------------------------------------------------------------- */
DataFrame dfCumulativeProduct_impl(const DataFrame* df, size_t colIndex)
{
    // Create an empty DataFrame if something fails
    DataFrame result;
    DataFrame_Create(&result);

    if (!df) {
        return result;
    }

    // Fetch the desired Series
    const Series* s = df->getSeries(df, colIndex);
    if (!s) {
        // invalid column
        return result;
    }

    size_t nRows = df->numRows(df);
    if (nRows == 0) {
        // no data => return empty
        return result;
    }

    // Create a new Series for the cumprod results (DF_DOUBLE)
    Series cumprodSeries;
    seriesInit(&cumprodSeries, "cumprod", DF_DOUBLE);

    double partialProduct = 1.0; // start with 1 for multiplication identity

    for (size_t r = 0; r < nRows; r++) {
        bool readOk = false;
        double val = 0.0;

        // Attempt to read numeric data from row r
        switch (s->type) {
            case DF_INT: {
                int tmp;
                if (seriesGetInt(s, r, &tmp)) {
                    val = (double)tmp;
                    readOk = true;
                }
            } break;
            case DF_DOUBLE: {
                double tmp;
                if (seriesGetDouble(s, r, &tmp)) {
                    val = tmp;
                    readOk = true;
                }
            } break;
            case DF_DATETIME: {
                long long tmp;
                if (seriesGetDateTime(s, r, &tmp)) {
                    val = (double)tmp;
                    readOk = true;
                }
            } break;
            case DF_STRING:
            default:
                // Not numeric => skip
                break;
        }

        // If row was read successfully => multiply partialProduct
        if (readOk) {
            partialProduct *= val;
        }

        // Store the current partialProduct in the new column
        seriesAddDouble(&cumprodSeries, partialProduct);
    }

    // Add this new Series to 'result'
    result.addSeries(&result, &cumprodSeries);

    // Clean up
    seriesFree(&cumprodSeries);

    return result;
}
/* -------------------------------------------------------------------------
* CUMULATIVE MAX/MIN
* ------------------------------------------------------------------------- */
DataFrame dfCumulativeMax_impl(const DataFrame* df, size_t colIndex)
{
    // Create an empty DataFrame if something fails
    DataFrame result;
    DataFrame_Create(&result);

    if (!df) {
        return result;
    }

    // Get the desired Series
    const Series* s = df->getSeries(df, colIndex);
    if (!s) {
        return result;
    }

    size_t nRows = df->numRows(df);
    if (nRows == 0) {
        // No data => return empty
        return result;
    }

    // Create a new Series for the cumulative max
    Series cummaxSeries;
    seriesInit(&cummaxSeries, "cummax", DF_DOUBLE);

    // Initialize partialMax to the smallest possible double
    double partialMax = -DBL_MAX;

    for (size_t r = 0; r < nRows; r++) {
        bool readOk = false;
        double val = 0.0;

        // Attempt to read numeric data from row r
        switch (s->type) {
            case DF_INT: {
                int tmp;
                if (seriesGetInt(s, r, &tmp)) {
                    val = (double)tmp;
                    readOk = true;
                }
            } break;
            case DF_DOUBLE: {
                double tmp;
                if (seriesGetDouble(s, r, &tmp)) {
                    val = tmp;
                    readOk = true;
                }
            } break;
            case DF_DATETIME: {
                long long tmp;
                if (seriesGetDateTime(s, r, &tmp)) {
                    val = (double)tmp;
                    readOk = true;
                }
            } break;
            default: // DF_STRING or unknown
                break;
        }

        if (readOk) {
            if (val > partialMax) {
                partialMax = val;
            }
        }
        // If read fails => partialMax stays same

        seriesAddDouble(&cummaxSeries, partialMax);
    }

    // Add this Series to 'result'
    result.addSeries(&result, &cummaxSeries);
    seriesFree(&cummaxSeries);

    return result;
}


DataFrame dfCumulativeMin_impl(const DataFrame* df, size_t colIndex)
{
    // Create an empty DataFrame if fail
    DataFrame result;
    DataFrame_Create(&result);

    if (!df) {
        return result;
    }

    const Series* s = df->getSeries(df, colIndex);
    if (!s) {
        return result;
    }

    size_t nRows = df->numRows(df);
    if (nRows == 0) {
        return result;
    }

    // Create a Series for the cumulative min
    Series cumminSeries;
    seriesInit(&cumminSeries, "cummin", DF_DOUBLE);

    // Initialize partialMin to the largest possible double
    double partialMin = DBL_MAX;

    for (size_t r = 0; r < nRows; r++) {
        bool readOk = false;
        double val = 0.0;

        switch (s->type) {
            case DF_INT: {
                int tmp;
                if (seriesGetInt(s, r, &tmp)) {
                    val = (double)tmp;
                    readOk = true;
                }
            } break;
            case DF_DOUBLE: {
                double tmp;
                if (seriesGetDouble(s, r, &tmp)) {
                    val = tmp;
                    readOk = true;
                }
            } break;
            case DF_DATETIME: {
                long long tmp;
                if (seriesGetDateTime(s, r, &tmp)) {
                    val = (double)tmp;
                    readOk = true;
                }
            } break;
            default:
                break;
        }

        if (readOk) {
            if (val < partialMin) {
                partialMin = val;
            }
        }
        // If read fails => partialMin stays the same

        seriesAddDouble(&cumminSeries, partialMin);
    }

    result.addSeries(&result, &cumminSeries);
    seriesFree(&cumminSeries);

    return result;
}

/* -------------------------------------------------------------------------
* GROUP BY
* ------------------------------------------------------------------------- */

/**
 * We'll store an array of (key, count).
 */
typedef struct {
    char* key;
    size_t count;
} GroupItem;

/**
 * We'll keep 'items', 'size', 'capacity' in a GroupContext.
 */
typedef struct {
    GroupItem* items;
    size_t size;
    size_t capacity;
} GroupContext;

/**
 * @brief ensureCapacity
 * Expand dynamic array if needed.
 */
static void ensureCapacity(GroupContext* ctx)
{
    if (!ctx) return;
    if (ctx->size >= ctx->capacity) {
        ctx->capacity = (ctx->capacity == 0) ? 8 : ctx->capacity * 2;
        ctx->items = (GroupItem*)realloc(ctx->items, ctx->capacity * sizeof(GroupItem));
    }
}

/**
 * @brief findOrCreateItem
 *  Linear search for 'key', or create a new item with count=0.
 */
static GroupItem* findOrCreateItem(GroupContext* ctx, const char* key)
{
    if (!ctx) return NULL;
    // linear search
    for (size_t i = 0; i < ctx->size; i++) {
        if (strcmp(ctx->items[i].key, key) == 0) {
            return &ctx->items[i];
        }
    }
    // not found -> create new
    ensureCapacity(ctx);
    ctx->items[ctx->size].key   = strdup(key);
    ctx->items[ctx->size].count = 0;
    ctx->size++;
    return &ctx->items[ctx->size - 1];
}

/**
 * @brief dfGroupBy_impl
 * Group the DataFrame by a single column (`groupColIndex`), returning a new
 * DataFrame with columns ["group", "count"] for each unique value.
 */
DataFrame dfGroupBy_impl(const DataFrame* df, size_t groupColIndex)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    size_t nCols = df->numColumns(df);
    if (groupColIndex >= nCols) {
        // invalid column
        return result;
    }

    const Series* groupSeries = df->getSeries(df, groupColIndex);
    if (!groupSeries) {
        // no data
        return result;
    }

    // We'll store our items in a local GroupContext
    GroupContext ctx;
    ctx.items    = NULL;
    ctx.size     = 0;
    ctx.capacity = 0;

    size_t nRows = df->numRows(df);

    for (size_t r = 0; r < nRows; r++) {
        char buffer[128];
        buffer[0] = '\0';

        // Convert the cell to string
        switch (groupSeries->type) {
            case DF_INT: {
                int val;
                if (seriesGetInt(groupSeries, r, &val)) {
                    snprintf(buffer, sizeof(buffer), "%d", val);
                }
            } break;
            case DF_DOUBLE: {
                double d;
                if (seriesGetDouble(groupSeries, r, &d)) {
                    snprintf(buffer, sizeof(buffer), "%g", d);
                }
            } break;
            case DF_STRING: {
                char* str = NULL;
                if (seriesGetString(groupSeries, r, &str)) {
                    strncpy(buffer, str, sizeof(buffer) - 1);
                    buffer[sizeof(buffer) - 1] = '\0';
                    free(str);
                }
            } break;
            /* --------------------------------------------
             * NEW: DF_DATETIME => convert epoch to string
             * -------------------------------------------- */
            case DF_DATETIME: {
                long long dtVal;
                if (seriesGetDateTime(groupSeries, r, &dtVal)) {
                    snprintf(buffer, sizeof(buffer), "%lld", dtVal);
                }
            } break;
        }

        // find or create item
        GroupItem* gi = findOrCreateItem(&ctx, buffer);
        if (gi) {
            gi->count++;
        }
    }

    // Build a result DataFrame with columns: "group" (string) and "count" (int)
    Series groupCol, countCol;
    seriesInit(&groupCol, "group", DF_STRING);
    seriesInit(&countCol, "count", DF_INT);

    // Add each item
    for (size_t i = 0; i < ctx.size; i++) {
        seriesAddString(&groupCol, ctx.items[i].key);
        seriesAddInt(&countCol, (int)ctx.items[i].count);
    }

    // Add them to the result
    result.addSeries(&result, &groupCol);
    result.addSeries(&result, &countCol);

    seriesFree(&groupCol);
    seriesFree(&countCol);

    // free memory
    for (size_t i = 0; i < ctx.size; i++) {
        free(ctx.items[i].key);
    }
    free(ctx.items);

    return result;
}