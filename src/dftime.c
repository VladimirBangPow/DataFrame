#include <time.h>
#include <string.h>   // for memset, strlen, memcpy
#include <stdlib.h>   // for malloc, free
#include <stddef.h>   // for size_t
#include <stdbool.h>  // for bool 
/* 
 * Example: parse a date string (like "2025-02-27 12:34:56") 
 * into microseconds since the Unix epoch (UTC).
 */
long long parseDatetimeToMicroseconds(const char* dateStr, const char* format) {
    // e.g. format = "%Y-%m-%d %H:%M:%S"
    struct tm tmVal;
    memset(&tmVal, 0, sizeof(tmVal));
    // strptime parses dateStr into tmVal
    strptime(dateStr, format, &tmVal);

    // Use timegm if you want UTC-based parsing (GNU extension).
    // Use mktime if you want localtime-based.
    time_t epochSeconds = timegm(&tmVal);
    // Convert to microseconds
    return (long long)epochSeconds * 1000000LL;
}

/*
 * Example: convert microseconds-since-epoch to a YYYY-MM-dd HH:mm:ss string.
 */
bool microsecondsToString(long long micros, char* outBuf, size_t bufSize, const char* format) {
    // Convert microseconds -> time_t
    time_t seconds = (time_t)(micros / 1000000LL);
    struct tm tmVal;
    gmtime_r(&seconds, &tmVal);

    // Now format the time into outBuf
    if (strftime(outBuf, bufSize, format, &tmVal) == 0) {
        return false; // if buffer too small
    }
    // If needed, handle sub-second precision manually.
    return true;
}