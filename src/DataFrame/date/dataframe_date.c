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
    for (size_t r=0; r<nRows; r++) {
        long long msVal=0;
        bool got = seriesGetDateTime(s, r, &msVal);
        if (!got) continue;

        // convert msVal => seconds
        long long seconds = msVal / 1000LL;
        time_t tSec = (time_t)seconds;
        struct tm* retTM = gmtime(&tSec);
        if (!retTM) continue;

        struct tm tmVal = *retTM;
        tmVal.tm_mday += daysToAdd;

        time_t newSec = timegm(&tmVal);
        if (newSec<0) newSec=0;
        long long newMs = ((long long)newSec)*1000LL;

        *(long long*)daGetMutable(&s->data, r) = newMs;
    }

    return true;
}

/**
 * dfDatetimeDiff_impl:
 *   - We have two DF_DATETIME columns (ms).
 *   - Return the difference in "ms" or "seconds" in a new column? 
 *   Here we'll do "seconds difference" => DF_INT
 */
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
        fprintf(stderr,"dfDatetimeDiff_impl: columns not DF_DATETIME.\n");
        return result;
    }

    Series diffS;
    seriesInit(&diffS, newColName, DF_INT);

    size_t nRows = df->numRows(df);
    for (size_t r=0; r<nRows; r++) {
        long long ms1=0, ms2=0;
        bool g1 = seriesGetDateTime(s1, r, &ms1);
        bool g2 = seriesGetDateTime(s2, r, &ms2);
        if (!g1||!g2) {
            seriesAddInt(&diffS, 0);
            continue;
        }
        // difference in seconds:
        long long diffSec = (ms2 - ms1)/1000LL;
        seriesAddInt(&diffS, (int)diffSec);
    }

    bool ok = result.addSeries(&result, &diffS);
    seriesFree(&diffS);

    if (!ok) {
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

/**
 * dfDatetimeValidate_impl:
 *   - Keep only rows whose DF_DATETIME (ms) is in [minMs..maxMs]
 */
typedef struct {
    long long minVal;
    long long maxVal;
    size_t colIndex;
} DTValidCtx;

static DTValidCtx g_validCtx;

static bool dtValidPredicate(const DataFrame* d,size_t row)
{
    const Series* s= d->getSeries(d, g_validCtx.colIndex);
    if(!s) return false;
    long long msVal=0;
    bool got= seriesGetDateTime(s, row,&msVal);
    if(!got) return false;
    return (msVal>= g_validCtx.minVal && msVal<= g_validCtx.maxVal);
}

DataFrame dfDatetimeValidate_impl(const DataFrame* df,
                                  size_t colIndex,
                                  long long minMs,
                                  long long maxMs)
{
    DataFrame empty;
    DataFrame_Create(&empty);
    if(!df) return empty;

    g_validCtx.colIndex= colIndex;
    g_validCtx.minVal  = minMs;
    g_validCtx.maxVal  = maxMs;

    return df->filter(df, dtValidPredicate);
}
