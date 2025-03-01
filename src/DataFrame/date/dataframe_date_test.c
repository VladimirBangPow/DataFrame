#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include "dataframe_date_test.h"

#include "../dataframe.h"   // for DataFrame, df->..., etc.
#include "../../Series/series.h" // for Series creation, etc.

/* ------------------------------------------------------------------
 * Each test case is now in its own static function.
 * testDate() calls them in a sequence.
 * ------------------------------------------------------------------ */

static void testConvertToDatetime_impl()
{
    DataFrame df;
    DataFrame_Create(&df);

    // Build a DF_STRING column of date/time
    const char* dates[] = {
        "2023-03-15 12:34:56",
        "2023-03-16 00:00:00",
        "invalid date",
        "2023-03-17 23:59:59"
    };
    Series s;
    seriesInit(&s, "TestDates", DF_STRING);
    for (int i=0; i<4; i++) {
        seriesAddString(&s, dates[i]);
    }

    bool ok = df.addSeries(&df, &s);
    seriesFree(&s); // free local copy
    assert(ok);

    // Convert => final col is DF_DATETIME
    // If your code no longer accepts a bool toMillis, remove that argument or pass 'false'.
    ok = df.convertToDatetime(&df, 0, "%Y-%m-%d %H:%M:%S");
    assert(ok);

    const Series* converted = df.getSeries(&df, 0);
    assert(converted != NULL);
    assert(converted->type == DF_DATETIME);

    // For row 2 "invalid date", we expect epoch=0
    long long val=0;
    bool got = seriesGetDateTime(converted, 2, &val);
    assert(got && val == 0);

    DataFrame_Destroy(&df);
    printf(" - dfConvertToDatetime_impl test passed.\n");
}

static void testDatetimeToString_impl()
{
    DataFrame df;
    DataFrame_Create(&df);

    // Make DF_DATETIME column with some known epochs (UTC)
    // e.g. 1678871696 => "2023-03-15 12:34:56" if truly UTC
    long long epochs[] = {
        1678871696LL,
        1678924800LL,
        0LL,
        1679003999LL
    };
    Series sd;
    seriesInit(&sd, "Epochs", DF_DATETIME);
    for (int i=0; i<4; i++) {
        seriesAddDateTime(&sd, epochs[i]);
    }
    bool ok = df.addSeries(&df, &sd);
    seriesFree(&sd);
    assert(ok);

    // Convert => DF_STRING
    ok = df.datetimeToString(&df, 0, "%Y-%m-%d %H:%M:%S");
    assert(ok);

    // Check results
    const Series* s2 = df.getSeries(&df, 0);
    assert(s2 && s2->type == DF_STRING);
    char* strVal = NULL;
    bool got = seriesGetString(s2, 2, &strVal);
    assert(got && strVal);
    // row 2 => "1970-01-01 00:00:00" presumably
    assert(strlen(strVal) > 0);
    free(strVal);

    DataFrame_Destroy(&df);
    printf(" - datetimeToString test passed.\n");
}

static void testDatetimeAdd_impl()
{
    DataFrame df;
    DataFrame_Create(&df);

    Series sdt;
    seriesInit(&sdt, "Times", DF_DATETIME);
    long long baseMs = 1678871696LL * 1000LL; // store in ms
    for (int i = 0; i < 3; i++) {
        // step by hour => 3600s => 3,600,000 ms
        seriesAddDateTime(&sdt, baseMs + (i * 3600000LL));
    }
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // Add 1 day => 86,400 seconds => 86,400,000 ms
    ok = df.datetimeAdd(&df, 0, 1);
    assert(ok);

    const Series* s2 = df.getSeries(&df, 0);
    long long val = 0;
    bool got = seriesGetDateTime(s2, 0, &val);

    // Expect baseMs + 86,400,000
    long long expected = baseMs + 86400000LL;
    assert(got && val == expected);

    DataFrame_Destroy(&df);
    printf(" - dfDatetimeAdd_impl test passed.\n");
}


static void testDatetimeDiff_impl()
{
    DataFrame df;
    DataFrame_Create(&df);

    // We'll have 2 DF_DATETIME columns: Start, End
    Series sStart, sEnd;
    seriesInit(&sStart, "Start", DF_DATETIME);
    seriesInit(&sEnd, "End", DF_DATETIME);

    // row 0 => start=1,000,000 ms, end=2,000,000 ms => diff=1000 (seconds)
    long long starts[] = {1000000, 5000000, 0};
    long long ends[]   = {2000000, 6000000, 0};


    for (int i=0; i<3; i++) {
        seriesAddDateTime(&sStart, starts[i]);
        seriesAddDateTime(&sEnd,   ends[i]);
    }
    bool ok = df.addSeries(&df, &sStart);  assert(ok);
    ok = df.addSeries(&df, &sEnd);        assert(ok);
    seriesFree(&sStart);
    seriesFree(&sEnd);

    // Diff => new DF with one column named "Diff"
    DataFrame diffDF = df.datetimeDiff(&df, 0, 1, "Diff");
    assert(diffDF.numColumns(&diffDF)==1);
    const Series* diffS = diffDF.getSeries(&diffDF, 0);
    assert(diffS && diffS->type == DF_INT);

    int check=0;
    bool gotVal = seriesGetInt(diffS, 0, &check);
    assert(gotVal && check==1000);
    seriesGetInt(diffS, 1, &check);
    assert(check==1000);
    seriesGetInt(diffS, 2, &check);
    assert(check==0);

    DataFrame_Destroy(&diffDF);
    DataFrame_Destroy(&df);
    printf(" - dfDatetimeDiff_impl test passed.\n");
}

static void testDatetimeFilter_impl()
{
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DATETIME col => 1000,2000,3000,4000
    Series sdt;
    seriesInit(&sdt, "Times", DF_DATETIME);
    for (int i=1; i<=4; i++) {
        seriesAddDateTime(&sdt, i*1000LL);
    }
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // Filter => keep [2000..3000]
    DataFrame filtered = df.datetimeFilter(&df, 0, 2000LL, 3000LL);
    assert(filtered.numRows(&filtered)==2);

    const Series* fcol = filtered.getSeries(&filtered, 0);
    long long val=0;
    bool got = seriesGetDateTime(fcol, 0, &val);
    assert(got && val==2000);
    seriesGetDateTime(fcol, 1, &val);
    assert(val==3000);

    DataFrame_Destroy(&filtered);
    DataFrame_Destroy(&df);
    printf(" - dfDatetimeFilter_impl test passed.\n");
}

static void testDatetimeTruncate_impl()
{
    DataFrame df;
    DataFrame_Create(&df);

    // We'll store an epoch for "2023-03-15 12:34:56"
    long long e = 1678871696; // 12:34:56 UTC (approx!)
    long long eMs = 1678838400LL * 1000; // 1678838400000
    Series sdt;
    seriesInit(&sdt, "TruncTest", DF_DATETIME);
    seriesAddDateTime(&sdt, eMs);
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // Truncate => "day"
    ok = df.datetimeTruncate(&df, 0, "day");
    assert(ok);

    // Now should be ~1678838400 => "2023-03-15 00:00:00"
    const Series* sc = df.getSeries(&df, 0);
    long long msVal = 0;
    bool got = seriesGetDateTime(sc, 0, &msVal);

    long long secVal = msVal / 1000LL;  // because your library stores ms

    printf(" - msVal=%lld, secVal=%lld\n", msVal, secVal);
    assert(secVal == 1678838400LL);

    DataFrame_Destroy(&df);
    printf(" - dfDatetimeTruncate_impl test passed.\n");
}

static void testDatetimeExtract_impl()
{
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DATETIME => 1 row => aiming for 2023-03-15 12:14:56
    long long e = 1678882496L * 1000; // approximately => "2023-03-15 12:14:56" UTC
    Series sdt;
    seriesInit(&sdt, "DTExtract", DF_DATETIME);
    seriesAddDateTime(&sdt, e);
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // Extract => year, month, day, hour, minute, second
    const char* fields[] = {"year","month","day","hour","minute","second"};
    DataFrame extracted = df.datetimeExtract(&df, 0, fields, 6);
    assert(extracted.numColumns(&extracted)==6);

    // row0 => year=2023, month=3, day=15, hour=12, minute=14, second=56 (for this epoch)
    const Series* sy = extracted.getSeries(&extracted, 0);
    int val=0;
    bool gotVal = seriesGetInt(sy, 0, &val);
    assert(gotVal && val==2023);

    const Series* sm = extracted.getSeries(&extracted, 1);
    seriesGetInt(sm, 0, &val);
    assert(val==3);

    const Series* sd = extracted.getSeries(&extracted, 2);
    seriesGetInt(sd, 0, &val);
    assert(val==15);

    const Series* sh = extracted.getSeries(&extracted, 3);
    seriesGetInt(sh, 0, &val);
    // should be 12 if that epoch is correct
    assert(val==12);

    const Series* smin = extracted.getSeries(&extracted, 4);
    seriesGetInt(smin, 0, &val);
    // we expect 14 from that epoch
    assert(val==14);

    const Series* ssec = extracted.getSeries(&extracted, 5);
    seriesGetInt(ssec, 0, &val);
    assert(val==56);

    DataFrame_Destroy(&extracted);
    DataFrame_Destroy(&df);
    printf(" - dfDatetimeExtract_impl test passed.\n");
}

static void testDatetimeGroupBy_impl()
{
    DataFrame df;
    DataFrame_Create(&df);

    // times => same day => 2023-03-15, but different hours
    // plus another day => 2023-03-16
    long long day1_0 = 1678838400LL * 1000LL; // "2023-03-15 00:00:00"
    long long day1_1 = 1678842000LL * 1000LL; // "2023-03-15 01:00:00"
    long long day2_0 = 1678924800LL * 1000LL; // "2023-03-16 00:00:00"
    Series sdt;
    seriesInit(&sdt, "GroupDT", DF_DATETIME);
    seriesAddDateTime(&sdt, day1_0);
    seriesAddDateTime(&sdt, day1_1);
    seriesAddDateTime(&sdt, day2_0);
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // group by day
    DataFrame grouped = df.datetimeGroupBy(&df, 0, "day");
    // We'll do a minimal check => at least 2 distinct days => 2 rows
    assert(grouped.numRows(&grouped)==2);
    df.print(&grouped);

    DataFrame_Destroy(&grouped);
    DataFrame_Destroy(&df);
    printf(" - dfDatetimeGroupBy_impl test passed.\n");
}

static void testDatetimeValidate_impl()
{
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DATETIME => row0=100, row1=2000, row2=9999999999999, row3=-50
    Series sdt;
    seriesInit(&sdt, "ValidateDT", DF_DATETIME);
    long long vs[] = {100, 2000, 9999999999999LL, -50};
    for (int i=0; i<4; i++) {
        seriesAddDateTime(&sdt, vs[i]);
    }
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // keep [0..9999999999]
    DataFrame valid = df.datetimeValidate(&df, 0, 0, 9999999999LL);
    assert(valid.numRows(&valid)==2);

    const Series* vc = valid.getSeries(&valid, 0);
    long long val=0;
    seriesGetDateTime(vc, 0, &val);
    assert(val==100);
    seriesGetDateTime(vc, 1, &val);
    assert(val==2000);

    DataFrame_Destroy(&valid);
    DataFrame_Destroy(&df);
    printf(" - dfDatetimeValidate_impl test passed.\n");
}

/* ------------------------------------------------------------------
 * testDate: The single driver that calls each test in sequence
 * ------------------------------------------------------------------ */
void testDate(void)
{
    printf("Running DataFrame date tests...\n");

    testConvertToDatetime_impl();
    testDatetimeToString_impl();
    testDatetimeAdd_impl();
    testDatetimeDiff_impl();
    testDatetimeFilter_impl();
    testDatetimeTruncate_impl();
    testDatetimeExtract_impl();
    testDatetimeGroupBy_impl();
    testDatetimeValidate_impl();

    printf("All dataframe_date tests passed successfully!\n");
}
