#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdbool.h>
#include "../dataframe.h"

/**
 * parseEpochSec:
 *  Takes a string "strVal" and a "formatType", returns a long long epoch (in seconds).
 *  If parse fails, returns 0 (or you could choose -1).
 * 
 * Supported formatTypes: "YYYYMMDD", "unix_seconds", "unix_millis",
 *                        "%Y-%m-%d %H:%M:%S" (via strptime)
 * 
 */

 /** parseYYYYMMDD: same as your original code */
static time_t parseYYYYMMDD(double num)
{
    int dateVal = (int)num;
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

static long long parseEpochSec(const char* strVal, const char* formatType)
{
    if (!strVal || !*strVal) {
        // empty => 0
        return 0;
    }

    long long epochSec = 0;

    // 1) "YYYYMMDD" -> parse numeric -> interpret via parseYYYYMMDD
    if (strcmp(formatType, "YYYYMMDD") == 0) {
        double numericVal = atof(strVal);
        // parseYYYYMMDD returns time_t
        extern time_t parseYYYYMMDD(double);  // forward-decl from your code
        time_t tsec = parseYYYYMMDD(numericVal);
        if (tsec == (time_t)-1) {
            epochSec = 0;
        } else {
            epochSec = (long long)tsec;
        }
    }
    // 2) "unix_seconds"
    else if (strcmp(formatType, "unix_seconds") == 0) {
        epochSec = atoll(strVal);
    }
    // 3) "unix_millis"
    else if (strcmp(formatType, "unix_millis") == 0) {
        long long val = atoll(strVal);
        epochSec = (long long)(val / 1000LL);
    }
    // 4) strptime "%Y-%m-%d %H:%M:%S"
    else if (strcmp(formatType, "%Y-%m-%d %H:%M:%S") == 0) {
        struct tm tmVal;
        memset(&tmVal, 0, sizeof(tmVal));
        char* ret = strptime(strVal, "%Y-%m-%d %H:%M:%S", &tmVal);
        if (!ret) {
            epochSec = 0;
        } else {
            // If you want UTC-based => timegm; if local => mktime
            epochSec = (long long)timegm(&tmVal);
        }
    }
    // 5) Unrecognized => 0
    else {
        epochSec = 0;
    }

    return epochSec;
}

/**
 * dfConvertDatesToEpoch_impl (streamlined):
 *   - If DF_INT/DF_DOUBLE => convert numeric to string, parse => store new numeric epoch in-place.
 *   - If DF_STRING => parse each row, build a new DF_DATETIME column (long long).
 *   - If parse fails => store 0. 
 *   - If toMillis => multiply final epoch by 1000.
 */
bool dfConvertToDatetime_impl(
    DataFrame* df,
    size_t dateColIndex,
    const char* formatType,
    bool toMillis
)
{
    if (!df) return false;
    Series* s = (Series*)daGetMutable(&df->columns, dateColIndex);
    if (!s) return false;

    size_t nRows = seriesSize(s);

    // Create a new DF_DATETIME column from scratch
    Series newSeries;
    seriesInit(&newSeries, s->name, DF_DATETIME);

    // For each row => convert to string => parse => store
    for (size_t r = 0; r < nRows; r++) {
        char buffer[64];
        // 1) Convert the cell to a string, no matter if s->type is INT, DOUBLE, or STRING
        if (s->type == DF_INT) {
            int ival = 0;
            seriesGetInt(s, r, &ival);
            snprintf(buffer, sizeof(buffer), "%d", ival);
        }
        else if (s->type == DF_DOUBLE) {
            double dval = 0.0;
            seriesGetDouble(s, r, &dval);
            snprintf(buffer, sizeof(buffer), "%.f", dval);
        }
        else if (s->type == DF_STRING) {
            // read the string
            char* tmp = NULL;
            bool got = seriesGetString(s, r, &tmp);
            if (!got || !tmp) {
                // store 0
                seriesAddDateTime(&newSeries, 0LL);
                if (tmp) free(tmp);
                continue;
            }
            strncpy(buffer, tmp, sizeof(buffer));
            buffer[sizeof(buffer) - 1] = '\0';
            free(tmp);
        }
        else {
            // if DF_DATETIME or something else -> log error or store 0
            seriesAddDateTime(&newSeries, 0LL);
            continue;
        }

        // 2) parse the string => epochSec
        long long epochSec = parseEpochSec(buffer, formatType); 
        // (like the function from earlier that handles "YYYYMMDD", "unix_seconds", etc.)

        // 3) if toMillis => multiply by 1000
        long long finalVal = toMillis ? (epochSec * 1000LL) : epochSec;

        // 4) store in newSeries
        seriesAddDateTime(&newSeries, finalVal);
    }

    // Replace old column with new DF_DATETIME
    seriesFree(s);
    memcpy(s, &newSeries, sizeof(Series));

    return true;
}





// --------------------------------------------------------------------
// 1) dfDatetimeToString_impl
//    Convert DF_DATETIME => DF_STRING, using strftime format "outFormat"
//    If "storedAsMillis" is true, your DF_DATETIME col is in ms => we split out remainder
// --------------------------------------------------------------------
bool dfDatetimeToString_impl(DataFrame* df,
                             size_t dateColIndex,
                             const char* outFormat,    // e.g. "%Y-%m-%d %H:%M:%S"
                             bool storedAsMillis)
{
    if (!df) {
        fprintf(stderr, "dfDatetimeToString_impl: df is NULL.\n");
        return false;
    }
    Series* s = (Series*)daGetMutable(&df->columns, dateColIndex);
    if (!s) {
        fprintf(stderr, "dfDatetimeToString_impl: invalid colIndex.\n");
        return false;
    }
    // Must be DF_DATETIME
    if (s->type != DF_DATETIME) {
        fprintf(stderr, "dfDatetimeToString_impl: not DF_DATETIME.\n");
        return false;
    }

    size_t nRows = seriesSize(s);

    // Create new DF_STRING column
    Series newSeries;
    seriesInit(&newSeries, s->name, DF_STRING);

    for (size_t r = 0; r < nRows; r++) {
        long long epochVal = 0;
        bool got = seriesGetDateTime(s, r, &epochVal);
        if (!got) {
            // no data => store empty
            seriesAddString(&newSeries, "");
            continue;
        }
        // If storedAsMillis => separate out seconds + remainder
        long long seconds = epochVal;
        long long millis = 0;
        if (storedAsMillis) {
            seconds = epochVal / 1000LL;
            millis  = epochVal % 1000LL;
        }

        time_t tSec = (time_t)seconds;
        struct tm* tmPtr = gmtime(&tSec); // or localtime
        if (!tmPtr) {
            seriesAddString(&newSeries, "");
            continue;
        }

        char buffer[64];
        strftime(buffer, sizeof(buffer), outFormat, tmPtr);

        // If we want fractional ms appended, e.g. ".123"
        if (storedAsMillis && millis > 0) {
            char msBuf[8];
            snprintf(msBuf, sizeof(msBuf), ".%03lld", millis);
            strcat(buffer, msBuf);
        }

        seriesAddString(&newSeries, buffer);
    }

    // Replace DF_DATETIME with DF_STRING
    seriesFree(s);
    memcpy(s, &newSeries, sizeof(Series));

    return true;
}

// --------------------------------------------------------------------
// 2) dfDatetimeAdd_impl
//    Add a certain # of days (or hours, minutes) to each row in DF_DATETIME
//    Example: We'll do "daysToAdd" as a parameter
// --------------------------------------------------------------------
bool dfDatetimeAdd_impl(DataFrame* df,
                        size_t dateColIndex,
                        int daysToAdd)
{
    if (!df) return false;
    Series* s = (Series*)daGetMutable(&df->columns, dateColIndex);
    if (!s) return false;

    if (s->type != DF_DATETIME) {
        fprintf(stderr, "dfDatetimeAdd_impl: col not DF_DATETIME.\n");
        return false;
    }

    size_t nRows = seriesSize(s);
    for (size_t r = 0; r < nRows; r++) {
        long long epochVal = 0;
        bool got = seriesGetDateTime(s, r, &epochVal);
        if (!got) continue;

        // Convert to struct tm
        time_t tSec = (time_t)epochVal;
        struct tm tmVal;
        memset(&tmVal, 0, sizeof(tmVal));
        // gmtime_r => if available. We'll do gmtime for example:
        struct tm* retTM = gmtime(&tSec);
        if (!retTM) continue; // skip

        // Copy out retTM => tmVal
        tmVal = *retTM;

        // Add days
        tmVal.tm_mday += daysToAdd;

        // Re-normalize
        time_t newSec = timegm(&tmVal); // or mktime for local time
        if (newSec < 0) newSec = 0;

        *(long long*)daGetMutable(&s->data, r) = (long long)newSec;
    }
    return true;
}

// --------------------------------------------------------------------
// 3) dfDatetimeDiff_impl
//    Return a new DataFrame with 1 column "Diff" = col2 - col1 (in seconds).
//    If you store ms, handle that accordingly
// --------------------------------------------------------------------
DataFrame dfDatetimeDiff_impl(const DataFrame* df,
                              size_t col1Index,
                              size_t col2Index,
                              const char* newColName)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    const Series* s1 = df->getSeries(df, col1Index);
    const Series* s2 = df->getSeries(df, col2Index);
    if (!s1 || !s2) return result;
    if (s1->type != DF_DATETIME || s2->type != DF_DATETIME) {
        fprintf(stderr, "dfDatetimeDiff_impl: both columns must be DF_DATETIME.\n");
        return result;
    }

    // Create a DF_INT or DF_DOUBLE column. We'll pick DF_INT for "seconds" difference
    Series diffSeries;
    seriesInit(&diffSeries, newColName, DF_INT);

    size_t nRows = df->numRows(df);
    for (size_t r = 0; r < nRows; r++) {
        long long val1 = 0, val2 = 0;
        bool g1 = seriesGetDateTime(s1, r, &val1);
        bool g2 = seriesGetDateTime(s2, r, &val2);
        if (!g1 || !g2) {
            seriesAddInt(&diffSeries, 0);
            continue;
        }
        long long diff = (val2 - val1);
        seriesAddInt(&diffSeries, (int)diff);  // watch for overflow
    }

    bool ok = result.addSeries(&result, &diffSeries);
    seriesFree(&diffSeries);

    if (!ok) {
        // If something fails, result might be empty
        DataFrame_Destroy(&result);
    }

    return result;
}

// --------------------------------------------------------------------
// 4) dfDatetimeFilter_impl
//    Keep rows in [startEpoch, endEpoch] in DF_DATETIME col => return new DF
//    We'll define a small RowPredicate approach
// --------------------------------------------------------------------
typedef struct {
    long long start;
    long long end;
    size_t colIndex;
} DTFilterCtx;

// We store the context in a global or static for the predicate
static DTFilterCtx g_dtFilterCtx;

static bool dtRangePredicate(const DataFrame* d, size_t row)
{
    const Series* s = d->getSeries(d, g_dtFilterCtx.colIndex);
    if (!s) return false;
    long long val = 0;
    bool got = seriesGetDateTime(s, row, &val);
    if (!got) return false;
    return (val >= g_dtFilterCtx.start && val <= g_dtFilterCtx.end);
}

DataFrame dfDatetimeFilter_impl(const DataFrame* df,
                                size_t dateColIndex,
                                long long startEpoch,
                                long long endEpoch)
{
    DataFrame empty;
    DataFrame_Create(&empty);
    if (!df) return empty;

    // set up global context
    g_dtFilterCtx.colIndex = dateColIndex;
    g_dtFilterCtx.start = startEpoch;
    g_dtFilterCtx.end   = endEpoch;

    // rely on your existing dfFilter_impl
    // e.g. "extern DataFrame dfFilter_impl(const DataFrame*, RowPredicate);"
    return df->filter(df, dtRangePredicate);
}

// --------------------------------------------------------------------
// 5) dfDatetimeTruncate_impl
//    Truncate times to day/hour => in place
//    We'll do a simple: if "unit"=="day" => zero out hour/min/sec
// --------------------------------------------------------------------
bool dfDatetimeTruncate_impl(DataFrame* df,
                             size_t colIndex,
                             const char* unit)
{
    if (!df) return false;
    Series* s = (Series*)daGetMutable(&df->columns, colIndex);
    if (!s) return false;
    if (s->type != DF_DATETIME) {
        fprintf(stderr, "dfDatetimeTruncate_impl: not DF_DATETIME.\n");
        return false;
    }

    size_t nRows = seriesSize(s);
    for (size_t r=0; r<nRows; r++) {
        long long val=0;
        bool got = seriesGetDateTime(s, r, &val);
        if (!got) continue;

        time_t tSec = (time_t)val;
        struct tm tmVal;
        memset(&tmVal, 0, sizeof(tmVal));
        struct tm* ret = gmtime(&tSec);
        if (!ret) continue;
        tmVal = *ret;

        if (strcmp(unit, "day")==0) {
            tmVal.tm_hour=0; tmVal.tm_min=0; tmVal.tm_sec=0;
        } 
        else if (strcmp(unit, "hour")==0) {
            tmVal.tm_min=0; tmVal.tm_sec=0;
        }
        // You can add "month" => set day=1, hour=0, ...
        // or "year" => set month=0, day=1, hour=0...
        // etc.

        time_t newSec = timegm(&tmVal);
        if (newSec < 0) newSec=0;
        *(long long*)daGetMutable(&s->data, r) = (long long)newSec;
    }

    return true;
}

// --------------------------------------------------------------------
// 6) dfDatetimeExtract_impl
//    Return a new DF with columns for year/month/day/hour...
//    We'll pass a list of "fields", each becomes an int column
// --------------------------------------------------------------------
DataFrame dfDatetimeExtract_impl(const DataFrame* df, 
                                 size_t dateColIndex,
                                 const char* const* fields, // e.g. {"year","month","day",NULL}
                                 size_t numFields)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!df) return result;

    const Series* s = df->getSeries(df, dateColIndex);
    if (!s || s->type != DF_DATETIME) {
        fprintf(stderr, "dfDatetimeExtract_impl: not DF_DATETIME.\n");
        return result;
    }

    size_t nRows = df->numRows(df);

    // For each requested field => build a new DF_INT column
    for (size_t f=0; f<numFields; f++) {
        Series newS;
        seriesInit(&newS, fields[f], DF_INT);

        for (size_t r=0; r<nRows; r++) {
            long long val=0;
            bool got = seriesGetDateTime(s, r, &val);
            if (!got) {
                seriesAddInt(&newS, 0);
                continue;
            }
            time_t tSec = (time_t)val;
            struct tm tmVal;
            memset(&tmVal, 0, sizeof(tmVal));
            struct tm* ret = gmtime(&tSec);
            if (!ret) {
                seriesAddInt(&newS, 0);
                continue;
            }
            tmVal = *ret;

            int outVal=0;
            if (strcmp(fields[f],"year")==0) {
                outVal = tmVal.tm_year + 1900;
            } else if (strcmp(fields[f],"month")==0) {
                outVal = tmVal.tm_mon + 1;
            } else if (strcmp(fields[f],"day")==0) {
                outVal = tmVal.tm_mday;
            } else if (strcmp(fields[f],"hour")==0) {
                outVal = tmVal.tm_hour;
            } else if (strcmp(fields[f],"minute")==0) {
                outVal = tmVal.tm_min;
            } else if (strcmp(fields[f],"second")==0) {
                outVal = tmVal.tm_sec;
            } else {
                // unknown => 0
                outVal = 0;
            }
            seriesAddInt(&newS, outVal);
        }

        bool ok = result.addSeries(&result, &newS);
        seriesFree(&newS);
        if (!ok) {
            // if fail => cleanup
            DataFrame_Destroy(&result);
            DataFrame_Create(&result);
            return result;
        }
    }

    return result; 
}

// --------------------------------------------------------------------
// 7) dfDatetimeGroupBy_impl
//    e.g. group by day or month. We'll do a quick approach:
//    1) copy the DF
//    2) dfDatetimeTruncate_impl on that col
//    3) call dfGroupBy_impl on the truncated col
// --------------------------------------------------------------------
DataFrame dfDatetimeGroupBy_impl(const DataFrame* df,
                                 size_t dateColIndex,
                                 const char* truncateUnit)
{
    DataFrame empty;
    DataFrame_Create(&empty);
    if (!df) return empty;

    // 1) Make a copy of df
    // Suppose we have a dfCopy function or we do df->some approach
    // For simplicity, let's do a naive approach: slice all rows
    DataFrame copyAll = df->slice(df, 0, df->numRows(df));
    if (copyAll.numRows(&copyAll) == 0) {
        return copyAll; 
    }

    // 2) Truncate 
    dfDatetimeTruncate_impl(&copyAll, dateColIndex, truncateUnit);

    // 3) groupBy 
    // e.g. "DataFrame dfGroupBy_impl(const DataFrame*, size_t);"
    DataFrame grouped = copyAll.groupBy(&copyAll, dateColIndex);

    DataFrame_Destroy(&copyAll);
    return grouped;
}

// --------------------------------------------------------------------
// 8) dfDatetimeValidate_impl
//    Filter out rows whose DF_DATETIME col is outside [minVal, maxVal] range
// --------------------------------------------------------------------
typedef struct {
    long long minVal;
    long long maxVal;
    size_t colIndex;
} DTValidCtx;

static DTValidCtx g_validCtx;

static bool dtValidPredicate(const DataFrame* d, size_t row)
{
    const Series* s = d->getSeries(d, g_validCtx.colIndex);
    if (!s) return false;
    long long val=0;
    bool got = seriesGetDateTime(s, row, &val);
    if (!got) return false;
    return (val >= g_validCtx.minVal && val <= g_validCtx.maxVal);
}

DataFrame dfDatetimeValidate_impl(const DataFrame* df,
                                  size_t colIndex,
                                  long long minEpoch,
                                  long long maxEpoch)
{
    DataFrame empty;
    DataFrame_Create(&empty);
    if (!df) return empty;

    // store in a static context
    g_validCtx.colIndex = colIndex;
    g_validCtx.minVal   = minEpoch;
    g_validCtx.maxVal   = maxEpoch;

    return df->filter(df, dtValidPredicate);
}