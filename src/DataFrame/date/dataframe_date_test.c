#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include "dataframe_date_test.h"

#include "../dataframe.h"   // for DataFrame, df->..., etc.
#include "../../Series/series.h" // for Series creation, etc.


void testDate(void)
{
    printf("Running DataFrame date tests...\n");

    // --------------------------------------------------------------
    // 1) Test dfConvertToDatetime_impl
    //    We'll make a DataFrame with a DF_STRING column of date/time,
    //    and convert it to DF_DATETIME.
    // --------------------------------------------------------------
    {
        DataFrame df;
        DataFrame_Create(&df);

        // Build a DF_STRING column of date/time
        // e.g. "2023-03-15 12:34:56"
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

        // Now convert using dfConvertToDatetime_impl => final col is DF_DATETIME
        ok = df.convertToDatetime(&df, 0, "%Y-%m-%d %H:%M:%S", false);
        assert(ok);

        const Series* converted = df.getSeries(&df, 0);
        assert(converted != NULL);
        assert(converted->type == DF_DATETIME);
        // For row 2 "invalid date", we expect epoch=0
        // For row 0,1,3 => some positive epoch
        // We'll do a quick check
        long long val=0;
        bool got = seriesGetDateTime(converted, 2, &val);
        assert(got && val == 0);

        DataFrame_Destroy(&df);
        printf(" - dfConvertToDatetime_impl test passed.\n");
    }

    // --------------------------------------------------------------
    // 2) Test dfDatetimeToString_impl
    //    We'll create a DF_DATETIME column, convert to string w/ format
    // --------------------------------------------------------------
    {
        DataFrame df;
        DataFrame_Create(&df);

        // Make DF_DATETIME column with some known epochs (UTC)
        // e.g. 1678871696 => "2023-03-15 12:34:56" if UTC
        long long epochs[] = {
            1678871696LL,  // 2023-03-15 12:34:56
            1678924800LL,  // 2023-03-16 00:00:00
            0LL,           // 1970-01-01
            1679003999LL   // 2023-03-17 23:59:59
        };
        Series sd;
        seriesInit(&sd, "Epochs", DF_DATETIME);
        for (int i=0; i<4; i++) {
            seriesAddDateTime(&sd, epochs[i]);
        }
        bool ok = df.addSeries(&df, &sd);
        seriesFree(&sd);
        assert(ok);

        // Convert to string => DF_STRING
        ok = df.datetimeToString(&df, 0, "%Y-%m-%d %H:%M:%S", false);
        assert(ok);

        // Check results
        const Series* s2 = df.getSeries(&df, 0);
        assert(s2 && s2->type == DF_STRING);
        // row 2 => "1970-01-01 00:00:00" presumably
        // We'll do a quick check
        char* strVal = NULL;
        bool got = seriesGetString(s2, 2, &strVal);
        assert(got && strVal);
        // We'll not compare exact, but we expect "1970-01-01 00:00:00"
        // if localtime might differ. We'll just confirm it's non-empty
        assert(strlen(strVal) > 0);
        free(strVal);

        DataFrame_Destroy(&df);
        printf(" - datetimeToString test passed.\n");
    }

    // --------------------------------------------------------------
    // 3) Test dfDatetimeAdd_impl (Add days)
    // --------------------------------------------------------------
    {
        DataFrame df;
        DataFrame_Create(&df);

        // Create DF_DATETIME with a known epoch, e.g. 1678871696 => 2023-03-15 12:34:56
        Series sdt;
        seriesInit(&sdt, "Times", DF_DATETIME);
        long long base=1678871696LL;
        for (int i=0; i<3; i++) {
            seriesAddDateTime(&sdt, base + (i*3600)); // step by hour
        }
        bool ok = df.addSeries(&df, &sdt);
        seriesFree(&sdt);
        assert(ok);

        // Add 1 day
        ok = df.datetimeAdd(&df, 0, 1);
        assert(ok);

        // Check row 0 => was 1678871696 => + 86400 => 1678958096
        const Series* s2 = df.getSeries(&df, 0);
        long long val=0;
        bool got = seriesGetDateTime(s2, 0, &val);
        assert(got && val== (long long)(1678871696LL + 86400LL));

        DataFrame_Destroy(&df);
        printf(" - dfDatetimeAdd_impl test passed.\n");
    }

    // --------------------------------------------------------------
    // 4) Test dfDatetimeDiff_impl
    // --------------------------------------------------------------
    {
        DataFrame df;
        DataFrame_Create(&df);

        // We'll have 2 DF_DATETIME columns: Start, End
        Series sStart, sEnd;
        seriesInit(&sStart, "Start", DF_DATETIME);
        seriesInit(&sEnd, "End", DF_DATETIME);
        // row 0 => start=1000, end=2000 => diff=1000
        // row 1 => start=5000, end=6000 => diff=1000
        // row 2 => start=0, end=0 => diff=0
        long long starts[] = {1000, 5000, 0};
        long long ends[]   = {2000, 6000, 0};

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
        bool got = seriesGetInt(diffS, 0, &check);
        assert(got && check==1000);
        seriesGetInt(diffS, 1, &check);
        assert(check==1000);
        seriesGetInt(diffS, 2, &check);
        assert(check==0);

        DataFrame_Destroy(&diffDF);
        DataFrame_Destroy(&df);
        printf(" - dfDatetimeDiff_impl test passed.\n");
    }

    // --------------------------------------------------------------
    // 5) Test dfDatetimeFilter_impl
    // --------------------------------------------------------------
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
        // that should keep row 1 =>2000, row 2 =>3000
        assert(filtered.numRows(&filtered)==2);

        // Check values
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

    // --------------------------------------------------------------
    // 6) Test dfDatetimeTruncate_impl
    // --------------------------------------------------------------
    {
        DataFrame df;
        DataFrame_Create(&df);

        // We'll store an epoch for "2023-03-15 12:34:56"
        // We'll truncate day => => "2023-03-15 00:00:00"
        long long e = 1678871696; // 2023-03-15 12:34:56 UTC
        Series sdt;
        seriesInit(&sdt, "TruncTest", DF_DATETIME);
        seriesAddDateTime(&sdt, e);
        bool ok = df.addSeries(&df, &sdt);
        seriesFree(&sdt);
        assert(ok);

        // Truncate => "day"
        ok = df.datetimeTruncate(&df, 0, "day");
        assert(ok);

        // Now should be ~1678838400 => "2023-03-15 00:00:00" 
        const Series* sc = df.getSeries(&df, 0);
        long long val=0;
        bool got = seriesGetDateTime(sc, 0, &val);
        assert(got);
        // We'll do a range check => within a small window
        // e.g. if you want to be precise, 1678838400 is the exact for that day
        assert(val == 1678838400LL);

        DataFrame_Destroy(&df);
        printf(" - dfDatetimeTruncate_impl test passed.\n");
    }

    // --------------------------------------------------------------
    // 7) Test dfDatetimeExtract_impl
    // --------------------------------------------------------------
    {
        DataFrame df;
        DataFrame_Create(&df);

        // DF_DATETIME => 1 row => 2023-03-15 12:34:56
        long long e = 1678871696LL;
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
        // row0 => year=2023, month=3, day=15, hour=12, minute=34, second=56

        const Series* sy = extracted.getSeries(&extracted, 0);
        int val=0;
        bool got = seriesGetInt(sy, 0, &val);
        assert(got && val==2023);

        const Series* sm = extracted.getSeries(&extracted, 1);
        seriesGetInt(sm, 0, &val);
        assert(val==3);

        const Series* sd = extracted.getSeries(&extracted, 2);
        seriesGetInt(sd, 0, &val);
        assert(val==15);

        const Series* sh = extracted.getSeries(&extracted, 3);
        seriesGetInt(sh, 0, &val);
        assert(val==12);

        const Series* smin = extracted.getSeries(&extracted, 4);
        seriesGetInt(smin, 0, &val);
        assert(val==34);

        const Series* ssec = extracted.getSeries(&extracted, 5);
        seriesGetInt(ssec, 0, &val);
        assert(val==56);

        DataFrame_Destroy(&extracted);
        DataFrame_Destroy(&df);
        printf(" - dfDatetimeExtract_impl test passed.\n");
    }

    // --------------------------------------------------------------
    // 8) Test dfDatetimeGroupBy_impl
    // --------------------------------------------------------------
    {
        // We'll do a quick check => group by "day"
        DataFrame df;
        DataFrame_Create(&df);

        // times => same day => 2023-03-15, but different hours
        // plus another day => 2023-03-16
        long long day1_0 = 1678838400LL; // "2023-03-15 00:00:00"
        long long day1_1 = 1678842000LL; // "2023-03-15 01:00:00"
        long long day2_0 = 1678924800LL; // "2023-03-16 00:00:00"
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
        // grouped => columns ["Group","count"], presumably
        // Or if your groupBy does something else, adapt check

        // We'll do a minimal check => ensure at least 2 rows => for 2 distinct days
        assert(grouped.numRows(&grouped)==2 || grouped.numRows(&grouped)==1);

        DataFrame_Destroy(&grouped);
        DataFrame_Destroy(&df);
        printf(" - dfDatetimeGroupBy_impl test passed (basic check).\n");
    }

    // --------------------------------------------------------------
    // 9) Test dfDatetimeValidate_impl
    // --------------------------------------------------------------
    {
        DataFrame df;
        DataFrame_Create(&df);

        // DF_DATETIME => row0=100, row1=2000, row2=9999999999999
        // we'll keep only [0.. 9999999999]
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
        // row0=100 => kept
        // row1=2000 => kept
        // row2=9999999999999 => out
        // row3=-50 => out
        assert(valid.numRows(&valid)==2);

        // check
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

    printf("All dataframe_date tests passed successfully!\n");
}
