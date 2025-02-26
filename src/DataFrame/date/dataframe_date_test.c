#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "dataframe_date_test.h"
#include "../dataframe.h"   // for DataFrame, df->convertDatesToEpoch, etc.
#include "../../Series/series.h"           // for creating Series

/**
 * Helper: create a numeric column (DF_INT or DF_DOUBLE) with 'count' entries.
 * We can then interpret these values as 'YYYYMMDD' or 'unix_millis' in tests.
 */
static Series buildNumericSeries(const char* name, ColumnType type, const double* values, size_t count)
{
    Series s;
    seriesInit(&s, name, type);
    for (size_t i = 0; i < count; i++) {
        if (type == DF_INT) {
            seriesAddInt(&s, (int)values[i]);
        } else {
            seriesAddDouble(&s, values[i]);
        }
    }
    return s;
}

/**
 * Convert an epoch (seconds) to a struct tm for verifying year, month, day, etc.
 */
static void epochToYMD(time_t epochSec, int* outYear, int* outMonth, int* outDay)
{
    // We'll do localtime or gmtime. 
    // If you want strictly UTC, use gmtime().
    // If you want local time, use localtime(). 
    // We'll do gmtime for consistency.
    struct tm* tm_info = gmtime(&epochSec);
    *outYear = tm_info->tm_year + 1900;
    *outMonth = tm_info->tm_mon + 1;
    *outDay = tm_info->tm_mday;
}

/**
 * Test converting a DF_INT column that uses YYYYMMDD to epoch seconds.
 */
static void testConvertDatesYYYYMMDD(void)
{
    // We'll create 3 rows with date values: 20230101, 20231231, 19800101
    // We'll store them in an int column, then call convertDatesToEpoch with "YYYYMMDD".
    double dateVals[] = { 20230101, 20231231, 19800101 };
    Series s = buildNumericSeries("DateCol", DF_INT, dateVals, 3);

    DataFrame df;
    DataFrame_Create(&df);

    bool ok = df.addSeries(&df, &s);
    assert(ok == true);
    seriesFree(&s);

    // So we have 1 column, 3 rows
    assert(df.numColumns(&df) == 1);
    assert(df.numRows(&df) == 3);

    // Now convert with format "YYYYMMDD", toMillis=false => store epoch seconds
    ok = df.convertDatesToEpoch(&df, 0, "YYYYMMDD", false);
    assert(ok == true);

    // Now the column should still be DF_INT (since we started with DF_INT).
    // The values are now epoch seconds. Let's verify each row:
    const Series* col = df.getSeries(&df, 0);
    assert(col && col->type == DF_INT);
    int epochInt = 0;

    // Row 0: 2023-01-01
    seriesGetInt(col, 0, &epochInt);
    {
        time_t t = (time_t)epochInt;
        int year, mon, day;
        epochToYMD(t, &year, &mon, &day);
        // 2023-01-01 in UTC might be 2023-01-01 indeed. 
        // We'll check year=2023, mon=1, day=1
        assert(year == 2023 && mon == 1 && day == 1);
    }

    // Row 1: 2023-12-31
    seriesGetInt(col, 1, &epochInt);
    {
        time_t t = (time_t)epochInt;
        int year, mon, day;
        epochToYMD(t, &year, &mon, &day);
        assert(year == 2023 && mon == 12 && day == 31);
    }

    // Row 2: 1980-01-01
    seriesGetInt(col, 2, &epochInt);
    {
        time_t t = (time_t)epochInt;
        int year, mon, day;
        epochToYMD(t, &year, &mon, &day);
        assert(year == 1980 && mon == 1 && day == 1);
    }

    DataFrame_Destroy(&df);
}

/**
 * Test converting a DF_DOUBLE column that uses YYYYMMDD, to milliseconds since epoch.
 */
static void testConvertDatesToMillis(void)
{
    double dateVals[] = { 20230615, 19991231 };
    // We'll store them in DF_DOUBLE so that after conversion toMillis,
    // we remain double. 
    Series s = buildNumericSeries("DateCol", DF_DOUBLE, dateVals, 2);

    DataFrame df;
    DataFrame_Create(&df);

    bool ok = df.addSeries(&df, &s);
    assert(ok == true);
    seriesFree(&s);

    // Convert with "YYYYMMDD", toMillis=true
    ok = df.convertDatesToEpoch(&df, 0, "YYYYMMDD", true);
    assert(ok == true);

    // Now the column should still be DF_DOUBLE, containing epoch millis
    const Series* col = df.getSeries(&df, 0);
    assert(col && col->type == DF_DOUBLE);

    double epochMillis = 0.0;
    // Row 0: 2023-06-15
    bool gotVal = seriesGetDouble(col, 0, &epochMillis);
    assert(gotVal);
    {
        // Convert to epochSec for verification
        time_t epochSec = (time_t)(epochMillis / 1000.0);
        int year, mon, day;
        epochToYMD(epochSec, &year, &mon, &day);
        // We expect 2023-06-15
        assert(year == 2023 && mon == 6 && day == 15);
    }

    // Row 1: 1999-12-31
    gotVal = seriesGetDouble(col, 1, &epochMillis);
    assert(gotVal);
    {
        time_t epochSec = (time_t)(epochMillis / 1000.0);
        int year, mon, day;
        epochToYMD(epochSec, &year, &mon, &day);
        assert(year == 1999 && mon == 12 && day == 31);
    }

    DataFrame_Destroy(&df);
}

/**
 * Test converting numeric columns that are already unix_seconds or unix_millis.
 * E.g. "unix_seconds", "unix_millis" format strings.
 */
static void testConvertUnixValues(void)
{
    // Let's pick an arbitrary time: 2023-08-01. 
    // We'll get its epochSec, epochMillis, then store in DF_INT/DF_DOUBLE.
    struct tm tinfo;
    tinfo.tm_year = 2023 - 1900;
    tinfo.tm_mon = 7;  // August is 7 in 0-based
    tinfo.tm_mday = 1;
    tinfo.tm_hour = 0; 
    tinfo.tm_min = 0; 
    tinfo.tm_sec = 0; 
    tinfo.tm_isdst = -1;
    time_t epochSecRef = mktime(&tinfo);
    double epochMsRef = (double)epochSecRef * 1000.0;

    // We'll create one column with epochSecRef as int,
    // another column with epochMsRef as double.
    // Then call convertDatesToEpoch with "unix_seconds" and "unix_millis".
    double colSecData[] = { (double)epochSecRef };
    double colMsData[]  = { epochMsRef };

    Series sSec = buildNumericSeries("SecCol", DF_INT, colSecData, 1);
    Series sMs  = buildNumericSeries("MsCol",  DF_DOUBLE, colMsData, 1);

    DataFrame df;
    DataFrame_Create(&df);

    bool ok = df.addSeries(&df, &sSec);
    assert(ok == true);
    ok = df.addSeries(&df, &sMs);
    assert(ok == true);

    seriesFree(&sSec);
    seriesFree(&sMs);

    // Now we have 2 columns, each with 1 row. 
    // Column0 => int secs, Column1 => double millis
    // We'll convert col0 with "unix_seconds" => toMillis=false => no real change
    // We'll convert col1 with "unix_millis" => toMillis=false => should become pure seconds as double
    // Then we verify the results.

    // Convert col0 (sec)
    ok = df.convertDatesToEpoch(&df, 0, "unix_seconds", false);
    assert(ok == true);

    // Convert col1 (ms)
    ok = df.convertDatesToEpoch(&df, 1, "unix_millis", false);
    assert(ok == true);

    // col0 remains DF_INT, col1 remains DF_DOUBLE
    const Series* c0 = df.getSeries(&df, 0);
    const Series* c1 = df.getSeries(&df, 1);
    assert(c0 && c0->type == DF_INT);
    assert(c1 && c1->type == DF_DOUBLE);

    // Check c0: should be epochSecRef
    int iVal = 0;
    seriesGetInt(c0, 0, &iVal);
    assert((time_t)iVal == epochSecRef);

    // Check c1: should now store epochSecRef as a double
    double dVal = 0.0;
    seriesGetDouble(c1, 0, &dVal);
    // We'll allow a small float tolerance
    assert(dVal > (epochSecRef - 0.0001) && dVal < (epochSecRef + 0.0001));

    DataFrame_Destroy(&df);
}

/**
 * @brief testDate
 * Main test driver for dataframe_date (dfConvertDatesToEpoch).
 * Calls sub-tests for various formats and usage.
 */
void testDate(void)
{
    printf("Running DataFrame date tests...\n");

    testConvertDatesYYYYMMDD();
    printf(" - YYYYMMDD -> epochSec test passed.\n");

    testConvertDatesToMillis();
    printf(" - YYYYMMDD -> epochMillis test passed.\n");

    testConvertUnixValues();
    printf(" - unix_seconds / unix_millis conversion test passed.\n");

    printf("All dataframe_date tests passed successfully!\n");
}
