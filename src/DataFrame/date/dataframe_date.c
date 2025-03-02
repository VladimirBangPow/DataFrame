#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdbool.h>
#include "../dataframe.h"

//------------------------------------------------------------------------------------
// 1) parseYYYYMMDD helper for "YYYYMMDD" -> timegm
//    Produces UTC-based epoch (in seconds).
//------------------------------------------------------------------------------------
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

    tinfo.tm_year = year - 1900;  // e.g. 2023 => 123
    tinfo.tm_mon  = month - 1;    // 1..12 => 0..11
    tinfo.tm_mday = day;
    tinfo.tm_hour = 0;
    tinfo.tm_min  = 0;
    tinfo.tm_sec  = 0;
    tinfo.tm_isdst= -1;

    // Use timegm (GNU extension) to interpret the struct tm as UTC
    time_t seconds = timegm(&tinfo);
    if (seconds == (time_t)-1) {
        return -1;
    }
    return seconds;
}

/**
 * parseEpochSec:
 *  Takes a string "strVal" and a "formatType", returns a long long epoch (in seconds).
 *  If parse fails, returns 0.
 *  
 *  Supported formatTypes: 
 *    - "YYYYMMDD"
 *    - "unix_seconds"
 *    - "unix_millis"
 *    - "%Y-%m-%d %H:%M:%S"
 *  
 *  We always produce seconds here. We'll multiply by 1000 later.
 */
static long long parseEpochSec(const char* strVal, const char* formatType)
{
    if (!strVal || !*strVal) {
        return 0;
    }

    long long epochSec = 0;

    if (strcmp(formatType, "YYYYMMDD") == 0) {
        double numericVal = atof(strVal);
        // parseYYYYMMDD returns time_t
        time_t tsec = parseYYYYMMDD(numericVal);
        if (tsec == (time_t)-1) {
            epochSec = 0;
        } else {
            epochSec = (long long)tsec;
        }
    }
    else if (strcmp(formatType, "unix_seconds") == 0) {
        epochSec = atoll(strVal);
    }
    else if (strcmp(formatType, "unix_millis") == 0) {
        // interpret the string as milliseconds => convert to seconds
        long long val = atoll(strVal);
        epochSec = val / 1000LL;
    }
    else if (strcmp(formatType, "%Y-%m-%d %H:%M:%S") == 0) {
        struct tm tmVal;
        memset(&tmVal, 0, sizeof(tmVal));
        char* ret = strptime(strVal, "%Y-%m-%d %H:%M:%S", &tmVal);
        if (!ret) {
            epochSec = 0;
        } else {
            // UTC => timegm
            epochSec = (long long)timegm(&tmVal);
        }
    }
    else {
        // unrecognized => 0
        epochSec = 0;
    }

    return epochSec;
}

/**
 * A helper function to set the datetime (long long) in a DF_DATETIME Series.
 * This parallels seriesGetDateTime(...) but performs a 'set' operation.
 */
bool seriesSetDateTime(Series* s, size_t index, long long value) {
    if (!s || s->type != DF_DATETIME) return false;
    if (index >= daSize(&s->data)) return false;

    long long* valPtr = (long long*)daGetMutable(&s->data, index);
    if (!valPtr) return false;

    *valPtr = value;
    return true;
}

/**
 * dfConvertToDatetime_impl:
 *   - For DF_INT / DF_DOUBLE => convert numeric -> string -> parse => store final as epoch MS.
 *   - For DF_STRING => parse each row -> store final as epoch MS in DF_DATETIME column.
 *   - If parse fails => store 0 ms.
 * 
 * Everything is stored as epoch *milliseconds* in DF_DATETIME.
 */
bool dfConvertToDatetime_impl(
    DataFrame* df,
    size_t dateColIndex,
    const char* formatType
)
{
    if (!df) return false;
    Series* s = (Series*)daGetMutable(&df->columns, dateColIndex);
    if (!s) return false;

    size_t nRows = seriesSize(s);

    // We'll build a new DF_DATETIME column that always stores MS
    Series newSeries;
    seriesInit(&newSeries, s->name, DF_DATETIME);

    // Convert each cell to string => parse => multiply => store
    for (size_t r = 0; r < nRows; r++) {
        char buffer[64];

        if (s->type == DF_INT) {
            int ival=0;
            seriesGetInt(s, r, &ival);
            snprintf(buffer, sizeof(buffer), "%d", ival);
        }
        else if (s->type == DF_DOUBLE) {
            double dval=0.0;
            seriesGetDouble(s, r, &dval);
            snprintf(buffer, sizeof(buffer), "%.f", dval);
        }
        else if (s->type == DF_STRING) {
            char* tmp=NULL;
            bool got = seriesGetString(s, r, &tmp);
            if (!got || !tmp) {
                // store 0
                seriesAddDateTime(&newSeries, 0LL);
                if (tmp) free(tmp);
                continue;
            }
            strncpy(buffer, tmp, sizeof(buffer));
            buffer[sizeof(buffer)-1] = '\0';
            free(tmp);
        }
        else {
            // DF_DATETIME or something => store 0
            seriesAddDateTime(&newSeries, 0LL);
            continue;
        }

        // parse => epoch in seconds
        long long epochSec = parseEpochSec(buffer, formatType);
        // now multiply => MS
        long long finalMs = epochSec * 1000LL;

        seriesAddDateTime(&newSeries, finalMs);
    }

    seriesFree(s);
    memcpy(s, &newSeries, sizeof(Series));
    return true;
}

/**
 * dfDatetimeToString_impl:
 *   - Reads DF_DATETIME (which is in MS).
 *   - Splits off "seconds = msVal / 1000; remainder = msVal % 1000".
 *   - Calls strftime on the seconds -> e.g. "%Y-%m-%d %H:%M:%S"
 *   - Appends .xxx if remainder>0 to show ms, if you want that behavior.
 * 
 * We have no toggle for "millis" now, always treat them as ms.
 */
bool dfDatetimeToString_impl(
    DataFrame* df,
    size_t dateColIndex,
    const char* outFormat
)
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
        long long msVal=0;
        bool got = seriesGetDateTime(s, r, &msVal);
        if (!got) {
            seriesAddString(&newSeries, "");
            continue;
        }

        // convert msVal => seconds + remainder
        long long seconds = msVal / 1000LL;
        long long millis  = msVal % 1000LL;

        time_t tSec = (time_t)seconds;
        struct tm* tmPtr = gmtime(&tSec);
        if (!tmPtr) {
            seriesAddString(&newSeries, "");
            continue;
        }

        char buffer[64];
        strftime(buffer, sizeof(buffer), outFormat, tmPtr);

        // If we want fractional .xxx
        // e.g. if (millis>0) then append .%03lld
        if (millis > 0) {
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

/**
 * dfDatetimeAdd_impl:
 *   - We assume the column is in MS
 *   - Convert MS => seconds => struct tm => add days => timegm => reconvert to MS
 */
bool dfDatetimeAdd_impl(
    DataFrame* df,
    size_t dateColIndex,
    long long msToAdd
)
{
    if (!df) return false;

    Series* s = (Series*)daGetMutable(&df->columns, dateColIndex);
    if (!s) return false;
    if (s->type != DF_DATETIME) {
        fprintf(stderr, "dfDatetimeAddMs_impl: col not DF_DATETIME.\n");
        return false;
    }

    size_t nRows = seriesSize(s);
    for (size_t r = 0; r < nRows; r++) {
        long long msVal = 0;
        bool got = seriesGetDateTime(s, r, &msVal);
        if (!got) {
            continue;  // skip invalid row
        }

        // Just add msToAdd directly
        long long newMs = msVal + msToAdd;

        // If negative => fallback to 0
        if (newMs < 0) {
            newMs = 0;
        }

        // Store updated value back into the column
        *(long long*)daGetMutable(&s->data, r) = newMs;
    }

    return true;
}


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

    // Both columns must be DF_DATETIME storing ms
    if (s1->type != DF_DATETIME || s2->type != DF_DATETIME) {
        fprintf(stderr, "dfDatetimeDiff_impl: columns not DF_DATETIME.\n");
        return result;
    }

    // We'll store the millisecond difference as a DF_INT (could overflow if huge)
    Series diffS;
    seriesInit(&diffS, newColName, DF_INT);

    size_t nRows = df->numRows(df);

    for (size_t r = 0; r < nRows; r++)
    {
        long long ms1 = 0, ms2 = 0;
        bool g1 = seriesGetDateTime(s1, r, &ms1);
        bool g2 = seriesGetDateTime(s2, r, &ms2);

        if (!g1 || !g2)
        {
            // If either cell is invalid, store 0
            seriesAddInt(&diffS, 0);
            continue;
        }

        // difference in milliseconds
        long long diffMs = (ms2 - ms1);

        // Insert into DF_INT. Potential overflow if diffMs > 2^31-1 or < -2^31
        seriesAddInt(&diffS, (int)diffMs);
    }

    bool ok = result.addSeries(&result, &diffS);
    seriesFree(&diffS);

    if (!ok)
    {
        DataFrame_Destroy(&result);
    }

    return result;
}


/**
 * dfDatetimeFilter_impl:
 *   - We keep rows if DF_DATETIME (ms) is in [startMs, endMs].
 */
typedef struct {
    long long start;
    long long end;
    size_t colIndex;
} DTFilterCtx;

static DTFilterCtx g_filterCtx;

static bool dtRangePredicate(const DataFrame* d, size_t row)
{
    const Series* s = d->getSeries(d, g_filterCtx.colIndex);
    if (!s) return false;
    long long msVal=0;
    bool got= seriesGetDateTime(s, row, &msVal);
    if (!got) return false;
    return (msVal>=g_filterCtx.start && msVal<=g_filterCtx.end);
}

DataFrame dfDatetimeFilter_impl(const DataFrame* df,
                                size_t dateColIndex,
                                long long startMs,
                                long long endMs)
{
    DataFrame empty;
    DataFrame_Create(&empty);
    if (!df) return empty;

    g_filterCtx.colIndex = dateColIndex;
    g_filterCtx.start    = startMs;
    g_filterCtx.end      = endMs;

    return df->filter(df, dtRangePredicate);
}

/**
 * dfDatetimeTruncate_impl:
 *   - Our DF_DATETIME is in ms. We'll convert to seconds => struct tm => zero out partial.
 *     Then convert back => store in ms.
 */
bool dfDatetimeTruncate_impl(DataFrame* df,
                             size_t colIndex,
                             const char* unit)
{
    if (!df) return false;

    // Fetch the Series; must be DF_DATETIME (storing ms).
    Series* s = (Series*)daGetMutable(&df->columns, colIndex);
    if (!s || s->type != DF_DATETIME) return false;

    size_t nRows = seriesSize(s);

    for (size_t r = 0; r < nRows; r++)
    {
        long long msVal = 0;
        bool got = seriesGetDateTime(s, r, &msVal);
        if (!got) continue; // skip invalid row

        // Convert ms -> seconds
        long long sec = msVal / 1000LL;
        // Cast to time_t properly
        time_t tSec = (time_t)sec;

        // Convert to UTC struct tm
        struct tm* retTM = gmtime(&tSec);
        if (!retTM) continue;

        // Make a local copy
        struct tm tmVal = *retTM;

        if (strcmp(unit, "day") == 0)
        {
            // zero out hour, min, sec
            tmVal.tm_hour = 0;
            tmVal.tm_min  = 0;
            tmVal.tm_sec  = 0;
        }
        else if (strcmp(unit, "hour") == 0)
        {
            // zero out min, sec
            tmVal.tm_min  = 0;
            tmVal.tm_sec  = 0;
        }
        else if (strcmp(unit, "month") == 0)
        {
            // zero out day (set to 1) + hour/min/sec
            // => start of the month
            tmVal.tm_mday = 1;
            tmVal.tm_hour = 0;
            tmVal.tm_min  = 0;
            tmVal.tm_sec  = 0;
        }
        else if (strcmp(unit, "year") == 0)
        {
            // zero out month=0 => January, day=1, hour=0, min=0, sec=0
            tmVal.tm_mon  = 0;    // january
            tmVal.tm_mday = 1;    // day=1
            tmVal.tm_hour = 0;
            tmVal.tm_min  = 0;
            tmVal.tm_sec  = 0;
        }
        // else: unrecognized => no changes

        // Rebuild epoch seconds from truncated tmVal
        time_t newSec = timegm(&tmVal);
        if (newSec < (time_t)0) {
            newSec = 0; // fallback if negative
        }

        // Convert back to ms
        long long newMs = (long long)newSec * 1000LL;

        // Store truncated ms back into DF_DATETIME
        *(long long*)daGetMutable(&s->data, r) = newMs;
    }

    return true;
}



/**
 * dfDatetimeExtract_impl:
 *   - We read DF_DATETIME in ms => convert to gmtime => extract year/month/day/etc => store in DF_INT
 */
DataFrame dfDatetimeExtract_impl(const DataFrame* df,
                                 size_t dateColIndex,
                                 const char* const* fields,
                                 size_t numFields)
{
    DataFrame result;
    DataFrame_Create(&result);
    if(!df) return result;

    const Series* s= df->getSeries(df, dateColIndex);
    if(!s || s->type!=DF_DATETIME) return result;

    size_t nRows= df->numRows(df);

    for(size_t f=0;f<numFields;f++){
        Series newS;
        seriesInit(&newS, fields[f], DF_INT);

        for(size_t r=0; r<nRows; r++){
            long long msVal=0;
            bool got= seriesGetDateTime(s,r,&msVal);
            if(!got){
                seriesAddInt(&newS,0);
                continue;
            }
            long long sec= msVal/1000LL;
            struct tm* retTM= gmtime((time_t*)&sec);
            if(!retTM){
                seriesAddInt(&newS,0);
                continue;
            }
            struct tm tmVal= *retTM;

            int outVal=0;
            if(strcmp(fields[f],"year")==0) {
                outVal= tmVal.tm_year+1900;
            } else if(strcmp(fields[f],"month")==0){
                outVal= tmVal.tm_mon+1;
            } else if(strcmp(fields[f],"day")==0){
                outVal= tmVal.tm_mday;
            } else if(strcmp(fields[f],"hour")==0){
                outVal= tmVal.tm_hour;
            } else if(strcmp(fields[f],"minute")==0){
                outVal= tmVal.tm_min;
            } else if(strcmp(fields[f],"second")==0){
                outVal= tmVal.tm_sec;
            }
            seriesAddInt(&newS, outVal);
        }
        bool ok= result.addSeries(&result, &newS);
        seriesFree(&newS);
        if(!ok){
            DataFrame_Destroy(&result);
            DataFrame_Create(&result);
            return result;
        }
    }
    return result;
}

/**
 * dfDatetimeGroupBy_impl:
 *   - Make a copy => truncate => groupBy
 *   - Always storing ms
 */
DataFrame dfDatetimeGroupBy_impl(const DataFrame* df,
                                 size_t dateColIndex,
                                 const char* truncateUnit)
{
    DataFrame empty;
    DataFrame_Create(&empty);
    if(!df) return empty;

    // copy all rows
    DataFrame copyAll= df->slice(df,0, df->numRows(df));
    if(copyAll.numRows(&copyAll)==0){
        return copyAll;
    }

    // truncate in ms
    dfDatetimeTruncate_impl(&copyAll, dateColIndex, truncateUnit);

    // groupBy
    DataFrame grouped= copyAll.groupBy(&copyAll, dateColIndex);
    DataFrame_Destroy(&copyAll);
    return grouped;
}



bool dfDatetimeRound_impl(DataFrame* df, size_t colIndex, const char* unit)
{
    if (!df) return false;

    // Get the Series to round. Must be DF_DATETIME.
    Series* s = (Series*)daGetMutable(&df->columns, colIndex);
    if (!s || s->type != DF_DATETIME) return false;

    size_t nRows = seriesSize(s);
    for (size_t r = 0; r < nRows; r++)
    {
        long long msVal = 0;
        bool got = seriesGetDateTime(s, r, &msVal);
        if (!got) {
            // skip rows we cannot read
            continue;
        }

        // Split into total whole seconds and leftover milliseconds.
        long long sec      = msVal / 1000LL;
        long long remainder= msVal % 1000LL;

        // Handle negative values gracefully: ensure remainder is [0..999].
        // E.g., if msVal is negative, msVal % 1000 could be negative on some platforms.
        if (remainder < 0) {
            remainder += 1000;
            sec       -= 1;
        }

        // Convert sec -> struct tm (UTC).
        time_t tSec   = (time_t)sec;
        struct tm* retTM = gmtime(&tSec);
        if (!retTM) continue;

        struct tm tmVal = *retTM;

        // -----------------------------------------------------------------
        // STEP 1: "Half-second" rounding. If remainder >= 500, increment tm_sec.
        // This effectively rounds to the nearest second.
        // -----------------------------------------------------------------
        if (remainder >= 500) {
            tmVal.tm_sec += 1;
        }

        // -----------------------------------------------------------------
        // STEP 2: Additional rounding based on 'unit'.
        //         (We’ve already handled second-level rounding above.)
        // -----------------------------------------------------------------
        if (strcmp(unit, "second") == 0) {
            // second-level rounding is done; do nothing more
        }
        else if (strcmp(unit, "minute") == 0) {
            // If tm_sec >= 30 => round up minute by 1
            // (You can also include leftover ms check if you want extremely precise logic,
            //  but typically the leftover ms is already accounted for in tm_sec by now.)
            if (tmVal.tm_sec > 30 ||
               (tmVal.tm_sec == 30 && remainder >= 500)) {
                tmVal.tm_min += 1;
            }
            // zero out seconds
            tmVal.tm_sec = 0;
        }
        else if (strcmp(unit, "hour") == 0) {
            // If tm_min >= 30 => round up hour by 1
            // Also consider if tm_min == 30 and tm_sec >= 30, etc.
            if (tmVal.tm_min > 30 ||
               (tmVal.tm_min == 30 && tmVal.tm_sec >= 30)) {
                tmVal.tm_hour += 1;
            }
            tmVal.tm_min = 0;
            tmVal.tm_sec = 0;
        }
        else if (strcmp(unit, "day") == 0) {
            // If tm_hour >= 12 => round up day by 1
            // You might also check if tm_hour == 12 && (tm_min >= 30 || tm_sec >= 30) 
            if (tmVal.tm_hour > 12 ||
               (tmVal.tm_hour == 12 && (tmVal.tm_min >= 30 || tmVal.tm_sec >= 30))) {
                tmVal.tm_mday += 1;
            }
            tmVal.tm_hour = 0;
            tmVal.tm_min  = 0;
            tmVal.tm_sec  = 0;
        }
        else if (strcmp(unit, "month") == 0) {
            // If tm_mday >= 16 => round up month (roughly half-month threshold)
            if (tmVal.tm_mday > 15 ||
               (tmVal.tm_mday == 15 && (tmVal.tm_hour >= 12))) {
                tmVal.tm_mon += 1;
                // handle month overflow
                if (tmVal.tm_mon > 11) {
                    tmVal.tm_mon  = 0;
                    tmVal.tm_year += 1; // next year
                }
            }
            tmVal.tm_mday = 1;
            tmVal.tm_hour = 0;
            tmVal.tm_min  = 0;
            tmVal.tm_sec  = 0;
        }
        else if (strcmp(unit, "year") == 0) {
            // If tm_mon >= 6 => round up year (roughly half-year threshold)
            if (tmVal.tm_mon > 5 ||
               (tmVal.tm_mon == 5 && (tmVal.tm_mday >= 16))) {
                tmVal.tm_year += 1;
            }
            tmVal.tm_mon  = 0;  // January
            tmVal.tm_mday = 1;
            tmVal.tm_hour = 0;
            tmVal.tm_min  = 0;
            tmVal.tm_sec  = 0;
        }
        // else: unrecognized => no further action

        // -----------------------------------------------------------------
        // STEP 3: Convert adjusted struct tm back to epoch time
        // -----------------------------------------------------------------
        time_t newSec = timegm(&tmVal);  // On some platforms, you may need a replacement for timegm()
        if (newSec < 0) {
            newSec = 0; // fallback if negative
        }

        long long newMs = (long long)newSec * 1000LL;
        // Finally, store this updated millisecond value back into the Series
        seriesSetDateTime(s, r, newMs);
    }

    return true;
}

/**
 * @brief Returns a new DataFrame filtered by date values in the specified column
 *        that lie between `startStr` and `endStr` (inclusive).
 *        The strings are parsed according to `formatType` (e.g., "%Y-%m-%d").
 *
 * If the user accidentally passes startMs > endMs, we swap them.
 * We rely on a helper parseEpochSec(startStr, formatType) that returns time_t 
 * (and 0 on failure), then convert to milliseconds.
 */
DataFrame dfDatetimeBetween_impl(const DataFrame* df,
                                 size_t dateColIndex,
                                 const char* startStr,
                                 const char* endStr,
                                 const char* formatType)
{
    // Create an empty DataFrame to return if anything fails
    DataFrame empty;
    DataFrame_Create(&empty);

    if (!df || !startStr || !endStr || !formatType) {
        return empty;
    }

    // 1) parse start/end strings -> epoch seconds
    extern long long parseEpochSec(const char* datetimeStr, const char* fmt); 
    long long startSec = parseEpochSec(startStr, formatType);  // returns 0 if fail
    long long endSec   = parseEpochSec(endStr,   formatType);

    // 2) Convert to milliseconds
    long long startMs = startSec * 1000LL;
    long long endMs   = endSec   * 1000LL;

    // If startMs > endMs => swap them
    if (startMs > endMs) {
        long long tmp = startMs;
        startMs = endMs;
        endMs   = tmp;
    }

    // 3) Call df->datetimeFilter(...) to keep rows in [startMs..endMs]
    return df->datetimeFilter(df, dateColIndex, startMs, endMs);
}

/**
 * @brief Re-base each datetime value in the specified column by subtracting
 *        `anchorMs`. If (value - anchorMs) < 0, clamp to 0.
 */
bool dfDatetimeRebase_impl(DataFrame* df, size_t colIndex, long long anchorMs)
{
    if (!df) {
        fprintf(stderr, "dfDatetimeRebase_impl: invalid df.\n");
        return false;
    }

    // Fetch the column
    Series* s = (Series*)daGetMutable(&df->columns, colIndex);
    if (!s) {
        fprintf(stderr, "dfDatetimeRebase_impl: invalid colIndex.\n");
        return false;
    }
    // Must be DF_DATETIME
    if (s->type != DF_DATETIME) {
        fprintf(stderr, "dfDatetimeRebase_impl: not DF_DATETIME.\n");
        return false;
    }

    size_t nRows = seriesSize(s);

    // For each row => (msVal - anchorMs)
    for (size_t r = 0; r < nRows; r++)
    {
        long long msVal = 0;
        bool got = seriesGetDateTime(s, r, &msVal);
        if (!got) {
            // if reading fails => skip or set to 0
            continue;
        }

        long long newMs = msVal - anchorMs;
        // Optionally clamp negative to 0
        if (newMs < 0) {
            newMs = 0;
        }

        // Store updated value using seriesSetDateTime
        seriesSetDateTime(s, r, newMs);
    }

    return true;
}

/**
 * @brief Clamps each datetime in the given column to be within [minMs, maxMs].
 *        Any value < minMs is set to minMs; any value > maxMs is set to maxMs.
 */
bool dfDatetimeClamp_impl(DataFrame* df,
                          size_t colIndex,
                          long long minMs,
                          long long maxMs)
{
    if (!df) {
        fprintf(stderr, "dfDatetimeClamp_impl: invalid df pointer.\n");
        return false;
    }

    // Fetch the Series; must be DF_DATETIME
    Series* s = (Series*)daGetMutable(&df->columns, colIndex);
    if (!s) {
        fprintf(stderr, "dfDatetimeClamp_impl: invalid colIndex.\n");
        return false;
    }
    if (s->type != DF_DATETIME) {
        fprintf(stderr, "dfDatetimeClamp_impl: not DF_DATETIME.\n");
        return false;
    }

    size_t nRows = seriesSize(s);
    for (size_t r = 0; r < nRows; r++) {
        long long msVal = 0;
        bool got = seriesGetDateTime(s, r, &msVal);
        if (!got) {
            // If reading fails for a row, skip or optionally set to minMs
            continue;
        }

        // Clamp
        if (msVal < minMs) {
            msVal = minMs;
        } else if (msVal > maxMs) {
            msVal = maxMs;
        }

        // Store back
        seriesSetDateTime(s, r, msVal);
    }

    return true;
}