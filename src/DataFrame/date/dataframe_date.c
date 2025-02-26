#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "../dataframe.h"

/* -------------------------------------------------------------------------
 * Forward-declared static helper
 * ------------------------------------------------------------------------- */
static time_t parseYYYYMMDD(double num);

/* -------------------------------------------------------------------------
 * The "public" function to be assigned in dataframe_core.c
 * ------------------------------------------------------------------------- */
bool dfConvertDatesToEpoch_impl(
    DataFrame* df, 
    size_t dateColIndex, 
    const char* formatType, 
    bool toMillis
)
{
    if (!df) return false;

    // Access the series we want to convert
    Series* s = (Series*)daGetMutable(&df->columns, dateColIndex);
    if (!s) return false;

    if (s->type != DF_INT && s->type != DF_DOUBLE) {
        fprintf(stderr, "dfConvertDatesToEpoch_impl: column %zu not numeric.\n", dateColIndex);
        return false;
    }

    size_t nRows = seriesSize(s);
    for (size_t r = 0; r < nRows; r++) {
        double numericVal = 0.0;
        if (s->type == DF_INT) {
            int tmp = 0;
            seriesGetInt(s, r, &tmp);
            numericVal = (double)tmp;
        } else {
            seriesGetDouble(s, r, &numericVal);
        }

        time_t epochSec = 0;
        if (strcmp(formatType, "YYYYMMDD") == 0) {
            epochSec = parseYYYYMMDD(numericVal);
            if (epochSec == (time_t)-1) {
                epochSec = 0; // fallback if parse fails
            }
        } 
        else if (strcmp(formatType, "unix_seconds") == 0) {
            epochSec = (time_t)numericVal;
        }
        else if (strcmp(formatType, "unix_millis") == 0) {
            epochSec = (time_t)(numericVal / 1000.0);
        }
        else {
            // unrecognized format
            epochSec = 0;
        }

        double finalVal = toMillis ? (epochSec * 1000.0) : (double)epochSec;

        // Overwrite the cell
        if (s->type == DF_INT) {
            // Could overflow if the date is beyond 2038 in 32-bit systems
            int converted = (int)finalVal;
            *(int*)daGetMutable(&s->data, r) = converted;
        } else {
            *(double*)daGetMutable(&s->data, r) = finalVal;
        }
    }
    return true;
}

/* -------------------------------------------------------------------------
 * static helper
 * ------------------------------------------------------------------------- */
static time_t parseYYYYMMDD(double num)
{
    // Expect something like 20230131 => 2023-01-31
    int dateVal = (int)num;
    // Very naive check
    if (dateVal <= 10000101) {
        return -1;
    }
    int year  = dateVal / 10000;
    int month = (dateVal / 100) % 100;
    int day   = dateVal % 100;

    struct tm tinfo;
    memset(&tinfo, 0, sizeof(tinfo));

    tinfo.tm_year = year - 1900;
    tinfo.tm_mon  = month - 1;
    tinfo.tm_mday = day;
    tinfo.tm_hour = 0;
    tinfo.tm_min  = 0;
    tinfo.tm_sec  = 0;
    tinfo.tm_isdst= -1;

    time_t seconds = mktime(&tinfo);
    if (seconds == -1) {
        return -1;
    }
    return seconds;
}
