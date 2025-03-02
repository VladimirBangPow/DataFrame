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

    // Build a DF_DATETIME column in milliseconds
    Series sdt;
    seriesInit(&sdt, "Times", DF_DATETIME);

    // For example, base epoch ~ 2023-03-15 12:34:56 UTC
    // Convert to ms by multiplying by 1000
    long long baseMs = 1678871696LL * 1000LL;

    // Add three rows, each 1 hour apart => 3600000 ms
    for (int i = 0; i < 3; i++) {
        seriesAddDateTime(&sdt, baseMs + (i * 3600000LL));
    }
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // We want to add 1 day => 86,400 seconds => 86,400,000 ms
    // (Assuming your "df.datetimeAdd" now expects ms to add)
    long long oneDayMs = 86400000LL;
    ok = df.datetimeAdd(&df, 0, oneDayMs);
    assert(ok);

    // Check the first row's new value
    const Series* s2 = df.getSeries(&df, 0);
    long long val = 0;
    bool got = seriesGetDateTime(s2, 0, &val);

    // Expect baseMs + 86,400,000
    long long expected = baseMs + oneDayMs;
    assert(got && val == expected);

    DataFrame_Destroy(&df);
    printf(" - dfDatetimeAdd_impl test passed.\n");
}



static void testDatetimeDiff_impl()
{
    DataFrame df;
    DataFrame_Create(&df);

    // We'll have 2 DF_DATETIME columns: Start, End
    // Each storing epoch in milliseconds.
    Series sStart, sEnd;
    seriesInit(&sStart, "Start", DF_DATETIME);
    seriesInit(&sEnd, "End", DF_DATETIME);

    // row 0 => start=1,000,000 ms, end=2,000,000 ms => diff=1,000,000 ms
    // row 1 => start=5,000,000 ms, end=6,000,000 ms => diff=1,000,000 ms
    // row 2 => start=0, end=0 => diff=0 ms
    long long starts[] = {1000000LL, 5000000LL, 0LL};
    long long ends[]   = {2000000LL, 6000000LL, 0LL};

    for (int i = 0; i < 3; i++) {
        seriesAddDateTime(&sStart, starts[i]);
        seriesAddDateTime(&sEnd,   ends[i]);
    }
    bool ok = df.addSeries(&df, &sStart);
    assert(ok);
    ok = df.addSeries(&df, &sEnd);
    assert(ok);

    seriesFree(&sStart);
    seriesFree(&sEnd);

    // Diff => new DF with one column named "Diff"
    // Now returns difference in ms
    DataFrame diffDF = df.datetimeDiff(&df, 0, 1, "Diff");
    assert(diffDF.numColumns(&diffDF) == 1);

    const Series* diffS = diffDF.getSeries(&diffDF, 0);
    assert(diffS && diffS->type == DF_INT);

    // Check the results
    int check = 0;
    bool gotVal = seriesGetInt(diffS, 0, &check);
    // row0 => 2,000,000 - 1,000,000 => 1,000,000 ms
    assert(gotVal && check == 1000000);

    seriesGetInt(diffS, 1, &check);
    // row1 => 6,000,000 - 5,000,000 => 1,000,000 ms
    assert(check == 1000000);

    seriesGetInt(diffS, 2, &check);
    // row2 => 0 - 0 => 0
    assert(check == 0);

    DataFrame_Destroy(&diffDF);
    DataFrame_Destroy(&df);
    printf(" - dfDatetimeDiff_impl test (ms) passed.\n");
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
    // df.print(&grouped);

    DataFrame_Destroy(&grouped);
    DataFrame_Destroy(&df);
    printf(" - dfDatetimeGroupBy_impl test passed.\n");
}


/**
 * @brief Test dfDatetimeRound_impl
 * We’ll create a DataFrame with a DF_DATETIME column that includes
 * various times (with milliseconds), then round to different units and
 * assert the results.
 */
static void testDatetimeRound_impl()
{
    printf("Running testDatetimeRound_impl...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // Create a DF_DATETIME column with some known epoch-millis:
    // Let's pick a base time: 2023-03-15 12:34:56.789 => epoch = 1678871696, leftover .789 ms
    // Multiply by 1000 for ms => 1678871696789
    long long baseMs = 1678871696789LL;
    long long times[] = {
        baseMs,                // ~ 12:34:56.789
        baseMs + 501,         // ~ 12:34:57.290 (should round up to 12:34:58 if rounding second)
        baseMs + 45*1000,     // ~ 12:35:41.789 (should test rounding minute)
        baseMs + 3600*1000,   // ~ 13:34:56.789 (test hour rounding)
        baseMs - 200LL        // negative remainder check near the boundary
    };

    Series sdt;
    seriesInit(&sdt, "RoundTimes", DF_DATETIME);
    for (int i = 0; i < 5; i++) {
        seriesAddDateTime(&sdt, times[i]);
    }
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // We'll test a single rounding unit first: "second"
    ok = df.datetimeRound(&df, 0, "second");
    assert(ok);

    // Validate row 0 => original remainder .789 => >= 500 => +1 sec
    // row0 was 1678871696789 => break that into (seconds=1678871696, remainder=789).
    // => final => 1678871697 in seconds => *1000 => 1678871697000
    const Series* col = df.getSeries(&df, 0);
    long long val = 0;
    bool gotVal = seriesGetDateTime(col, 0, &val);
    assert(gotVal);
    assert(val == 1678871697000LL);

    // Validate row 1 => was baseMs+501 => remainder ~ 501 => round up => +1 sec from base
    // So we expect second = baseSec+1 => 1678871697 in seconds => 1678871697000 ms
    seriesGetDateTime(col, 1, &val);
    assert(val == 1678871697000LL);

    // We won’t check all rows in detail here, but you can. Let’s at least confirm row 4 works.
    seriesGetDateTime(col, 4, &val);
    // row4 was baseMs - 200 => 1678871696589 => remainder=589 => round up => second=1678871697
    assert(val == 1678871697000LL);

    // Now let’s do "minute" rounding on row0 to see if it changes to 12:35:00
    // We can re-round the entire column or re-add times. For simplicity, re-insert them:
    DataFrame_Destroy(&df);
    DataFrame_Create(&df);
    seriesInit(&sdt, "RoundTimes", DF_DATETIME);
    for (int i = 0; i < 5; i++) {
        seriesAddDateTime(&sdt, times[i]);
    }
    df.addSeries(&df, &sdt);
    seriesFree(&sdt);

    // Round to minute
    df.datetimeRound(&df, 0, "minute");

    col = df.getSeries(&df, 0);
    seriesGetDateTime(col, 0, &val);
    // base => "12:34:56.789" => second=56 => >=30 => round up => minute=35 => new time=12:35:00
    // Let's check the resulting epoch in UTC
    // 12:35:00 on 2023-03-15 => epoch=1678871700 => in ms => 1678871700000
    assert(val == 1678871700000LL);

    DataFrame_Destroy(&df);
    printf(" - dfDatetimeRound_impl test passed.\n");
}

/**
 * @brief Test dfDatetimeBetween_impl
 * We add a DF_DATETIME column, call dfDatetimeBetween_impl with start/end
 * strings, and verify the filtered DataFrame matches the expected rows.
 */
static void testDatetimeBetween_impl()
{
    printf("Running testDatetimeBetween_impl...\n");
    DataFrame df;
    DataFrame_Create(&df);

    
    long long times[] = {
        1678838400LL * 1000, // "2023-03-15 00:00:00" in MILLISECONDS
        1678871696LL * 1000, // "2023-03-15 9:14:56"
        1678924800LL * 1000, // "2023-03-16 00:00:00"
        1679000000LL * 1000  // "2023-03-16 20:53:20"
    };
    

    Series sdt;
    seriesInit(&sdt, "BetweenTest", DF_DATETIME);
    for (int i = 0; i < 4; i++) {
        // Storing raw seconds. If your code expects ms in DF_DATETIME,
        // multiply by 1000. But we'll store seconds for clarity here.
        seriesAddDateTime(&sdt, times[i]);
    }
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // We'll keep rows between "2023-03-15 12:00:00" and "2023-03-16 00:00:00" inclusive
    // => start=1678862400, end=1678924800
    DataFrame result = df.datetimeBetween(
        &df,              // inDF
        0,                // dateColIndex
        "2023-03-15 9:13:00",  // start
        "2023-03-16 00:00:00",  // end
        "%Y-%m-%d %H:%M:%S"     // format
    );

    // The only rows in that range:
    //   times[1] = 1678871696000 => ~ 2023-03-15 12:34:56
    //   times[2] = 1678924800000 => 2023-03-16 00:00:00 (inclusive)
    assert(result.numRows(&result) == 2);
    result.print(&result);
    const Series* sres = result.getSeries(&result, 0);
    long long val=0;
    // row0 => 1678871696
    bool gotVal = seriesGetDateTime(sres, 0, &val);
    assert(gotVal && val == 1678871696000LL);
    // row1 => 1678924800
    seriesGetDateTime(sres, 1, &val);
    assert(val == 1678924800000LL);

    DataFrame_Destroy(&result);
    DataFrame_Destroy(&df);
    printf(" - dfDatetimeBetween_impl test passed.\n");
}

/**
 * @brief Test dfDatetimeRebase_impl
 * We create a DF_DATETIME column, pick an anchor, and then do rebase => val - anchor.
 * If negative, clamp to 0. Then we check the results.
 */
static void testDatetimeRebase_impl()
{
    printf("Running testDatetimeRebase_impl...\n");
    DataFrame df;
    DataFrame_Create(&df);

    Series sdt;
    seriesInit(&sdt, "RebaseTest", DF_DATETIME);
    // We'll store times: 1000, 2000, 3000, 500
    long long times[] = {1000LL, 2000LL, 3000LL, 500LL};
    for (int i=0; i<4; i++) {
        seriesAddDateTime(&sdt, times[i]);
    }
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // rebase with anchor=1500 => newVal = oldVal -1500, clamp >=0
    // so => row0=1000 => -500 => clamp=0
    //        row1=2000 => 500
    //        row2=3000 => 1500
    //        row3=500  => -1000 => clamp=0
    ok = df.datetimeRebase(&df, 0, 1500LL);
    assert(ok);

    const Series* col = df.getSeries(&df, 0);
    long long val=0;
    seriesGetDateTime(col, 0, &val);
    assert(val == 0LL);
    seriesGetDateTime(col, 1, &val);
    assert(val == 500LL);
    seriesGetDateTime(col, 2, &val);
    assert(val == 1500LL);
    seriesGetDateTime(col, 3, &val);
    assert(val == 0LL);

    DataFrame_Destroy(&df);
    printf(" - dfDatetimeRebase_impl test passed.\n");
}

/**
 * @brief Test dfDatetimeClamp_impl
 * We store times in DF_DATETIME, specify a minMs and maxMs, and check results.
 */
static void testDatetimeClamp_impl()
{
    printf("Running testDatetimeClamp_impl...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // Create a DF_DATETIME col => 10, 50, 100, 9999
    Series sdt;
    seriesInit(&sdt, "ClampTest", DF_DATETIME);
    long long vals[] = {10LL, 50LL, 100LL, 9999LL};
    for (int i=0; i<4; i++) {
        seriesAddDateTime(&sdt, vals[i]);
    }
    bool ok = df.addSeries(&df, &sdt);
    seriesFree(&sdt);
    assert(ok);

    // clamp => min=20, max=9000
    // => 10 => 20
    // => 50 => 50
    // => 100 => 100
    // => 9999 => 9000
    ok = df.datetimeClamp(&df, 0, 20LL, 9000LL);
    assert(ok);

    const Series* col = df.getSeries(&df, 0);
    long long val=0;

    seriesGetDateTime(col, 0, &val);
    assert(val == 20LL);
    seriesGetDateTime(col, 1, &val);
    assert(val == 50LL);
    seriesGetDateTime(col, 2, &val);
    assert(val == 100LL);
    seriesGetDateTime(col, 3, &val);
    assert(val == 9000LL);

    DataFrame_Destroy(&df);
    printf(" - dfDatetimeClamp_impl test passed.\n");
}

/* ------------------------------------------------------------------
 * testDate: The single driver that calls each test in sequence
 * ------------------------------------------------------------------ */
void testDate(void)
{
    printf("Running DataFrame date tests...\n");

    // Existing tests
    testConvertToDatetime_impl();
    testDatetimeToString_impl();
    testDatetimeAdd_impl();
    testDatetimeDiff_impl();
    testDatetimeFilter_impl();
    testDatetimeTruncate_impl();
    testDatetimeExtract_impl();
    testDatetimeGroupBy_impl();
    testDatetimeRound_impl();
    testDatetimeBetween_impl();
    testDatetimeRebase_impl();
    testDatetimeClamp_impl();

    printf("All dataframe_date tests passed successfully!\n");
}

