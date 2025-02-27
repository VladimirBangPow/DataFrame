#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dataframe_aggregate_test.h"
#include "../dataframe.h"   // The DataFrame struct & methods
#include "../../Series/series.h"  // For creating Series

// ------------------------------------------------------------------
// Helper routines for building test Series
// ------------------------------------------------------------------

/** Build a small integer Series with given name and values. */
static Series buildIntSeries(const char* name, const int* values, size_t count)
{
    Series s;
    seriesInit(&s, name, DF_INT);
    for (size_t i = 0; i < count; i++) {
        seriesAddInt(&s, values[i]);
    }
    return s;
}

/** Build a string Series for testing. */
static Series buildStringSeries(const char* name, const char* const* strings, size_t count)
{
    Series s;
    seriesInit(&s, name, DF_STRING);
    for (size_t i = 0; i < count; i++) {
        seriesAddString(&s, strings[i]);
    }
    return s;
}

// ------------------------------------------------------------------
// GROUPBY
// ------------------------------------------------------------------
static void testGroupBy(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // We'll add 1 column of int with repeated values => e.g. [10,10,20,20,10]
    int v[] = {10,10,20,20,10};
    Series s = buildIntSeries("Key", v, 5);
    bool ok = df.addSeries(&df, &s);
    assert(ok);
    seriesFree(&s);

    // groupBy => produce [group, count]
    DataFrame grp = df.groupBy(&df, 0);
    // We expect 2 distinct groups => "10" => count=3, "20"=>count=2
    // The result has 2 rows => row0 => group=10, count=3
    //                         row1 => group=20, count=2
    assert(grp.numRows(&grp) == 2);
    assert(grp.numColumns(&grp) == 2);
    {
        const Series* groupSer = grp.getSeries(&grp, 0);
        const Series* countSer = grp.getSeries(&grp, 1);
        // row0 => group="10", count=3
        char* g0 = NULL;
        seriesGetString(groupSer, 0, &g0);
        assert(strcmp(g0, "10")==0);
        free(g0);
        int c0=0;
        seriesGetInt(countSer, 0, &c0);
        assert(c0==3);

        // row1 => group="20", count=2
        char* g1 = NULL;
        seriesGetString(groupSer, 1, &g1);
        assert(strcmp(g1, "20")==0);
        free(g1);
        int c1=0;
        seriesGetInt(countSer, 1, &c1);
        assert(c1==2);
    }
    DataFrame_Destroy(&grp);

    DataFrame_Destroy(&df);
}

// ------------------------------------------------------------------
// sum,mean,min,max
// ------------------------------------------------------------------
static void testAggregations(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // 1 col => DF_INT => values [10,20,30]
    int v[] = {10,20,30};
    Series s = buildIntSeries("Vals", v, 3);
    bool ok = df.addSeries(&df, &s);
    assert(ok);
    seriesFree(&s);

    double sum = df.sum(&df, 0);
    assert(sum == 60.0);

    double mean = df.mean(&df, 0);
    assert(mean == 20.0);

    double mn = df.min(&df, 0);
    assert(mn == 10.0);

    double mx = df.max(&df, 0);
    assert(mx == 30.0);

    DataFrame_Destroy(&df);
}




void testAggregate(void){
    printf("Testing aggregate...\n");
    // GroupBy
    testGroupBy();
    printf(" - dfGroupBy tests passed.\n");

    //sum,mean,min,max
    testAggregations();
    printf(" - dfSum, dfMean, dfMin, dfMax tests passed.\n");
    printf("aggregate tests passed.\n");

}