/**
 * dfConvertDatesToEpoch_impl.c
 *
 * Example implementation of dfConvertDatesToEpoch_impl
 * that supports numeric columns (DF_INT, DF_DOUBLE) and
 * string columns (DF_STRING) with multiple date/time formats.
 *
 * IMPORTANT: If you're on Windows, 'timegm' is not standard.
 * You may need to define your own or use 'mktime' with local time.
 */

 #include <stdio.h>
 #include <stdlib.h>
 #include <string.h>
 #include <time.h>
 #include "../dataframe.h"  // adjust include path as needed
 
 /**
  * Forward declaration of a naive helper that parses YYYYMMDD
  * numeric values, e.g. 20230301 => March 1, 2023.
  */
 static time_t parseYYYYMMDD(double num);
 
 /**
  * dfConvertDatesToEpoch_impl:
  *  - If the column is DF_INT or DF_DOUBLE, treat values as numeric
  *    and interpret them based on the formatType (e.g. "YYYYMMDD",
  *    "unix_seconds", "unix_millis").
  *  - If the column is DF_STRING, parse each string into epoch time
  *    using the requested format. Then switch the column to DF_DATETIME.
  *  - If toMillis=true, multiply the final epoch by 1000 for ms.
  */
 bool dfConvertDatesToEpoch_impl(
     DataFrame* df,
     size_t dateColIndex,
     const char* formatType,
     bool toMillis
 )
 {
     if (!df) {
         return false;
     }
 
     // Access the Series we want to convert
     Series* s = (Series*)daGetMutable(&df->columns, dateColIndex);
     if (!s) {
         return false;
     }
 
     // Case 1: If column is numeric (DF_INT / DF_DOUBLE)
     if (s->type == DF_INT || s->type == DF_DOUBLE) {
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
                 // Unrecognized format => fallback to 0
                 epochSec = 0;
             }
 
             double finalVal = toMillis
                               ? (epochSec * 1000.0)
                               : (double)epochSec;
 
             // Overwrite the cell
             if (s->type == DF_INT) {
                 int converted = (int)finalVal;
                 *(int*)daGetMutable(&s->data, r) = converted;
             } else {
                 *(double*)daGetMutable(&s->data, r) = finalVal;
             }
         }
 
         return true;
     }
     // Case 2: If column is DF_STRING, parse each date string => epoch
     else if (s->type == DF_STRING) {
         size_t nRows = seriesSize(s);
 
         // We'll build a new DF_DATETIME column
         Series newSeries;
         seriesInit(&newSeries, s->name, DF_DATETIME);
 
         for (size_t r = 0; r < nRows; r++) {
             char* dateStr = NULL;
             bool gotStr = seriesGetString(s, r, &dateStr);
             if (!gotStr || !dateStr) {
                 // treat as empty => store 0
                 seriesAddDateTime(&newSeries, 0LL);
                 if (dateStr) free(dateStr);
                 continue;
             }
 
             long long epochSec = 0;
 
             if (strcmp(formatType, "YYYYMMDD") == 0) {
                 // parse numeric "20230301" style strings
                 double numericVal = atof(dateStr);
                 time_t tsec = parseYYYYMMDD(numericVal);
                 if (tsec == (time_t)-1) {
                     tsec = 0;
                 }
                 epochSec = (long long)tsec;
             }
             else if (strcmp(formatType, "unix_seconds") == 0) {
                 // interpret the string directly as seconds
                 long long val = atoll(dateStr);
                 epochSec = val;
             }
             else if (strcmp(formatType, "unix_millis") == 0) {
                 long long val = atoll(dateStr);
                 epochSec = (long long)(val / 1000LL);
             }
             // NEW BRANCH: parse "YYYY-MM-DD HH:MM:SS" using strptime
             else if (strcmp(formatType, "%Y-%m-%d %H:%M:%S") == 0) {
                 struct tm tmVal;
                 memset(&tmVal, 0, sizeof(tmVal));
 
                 char* ret = strptime(dateStr, "%Y-%m-%d %H:%M:%S", &tmVal);
                 if (!ret) {
                     epochSec = 0;
                 } else {
                     // For UTC-based epoch, use timegm if available
                     // For localtime, use mktime
                     epochSec = (long long)timegm(&tmVal);
                     // or => epochSec = (long long)mktime(&tmVal);
                 }
             }
             else {
                 // unrecognized => fallback
                 epochSec = 0;
             }
 
             // Possibly multiply by 1000 if toMillis is true
             long long finalVal = toMillis
                                  ? (epochSec * 1000LL)
                                  : epochSec;
 
             seriesAddDateTime(&newSeries, finalVal);
 
             free(dateStr);
         }
 
         // Replace old string column with new DF_DATETIME column
         seriesFree(s);  // frees old string data
         memcpy(s, &newSeries, sizeof(Series));
 
         return true;
     }
     else {
         fprintf(stderr, "dfConvertDatesToEpoch_impl: column %zu not numeric or string.\n",
                 dateColIndex);
         return false;
     }
 }
 
 /**
  * parseYYYYMMDD:
  *  A very naive approach that expects a numeric like 20230301
  *  => year=2023, month=03, day=01 => mktime => epoch seconds
  */
 static time_t parseYYYYMMDD(double num)
 {
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
 