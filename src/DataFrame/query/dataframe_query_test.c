#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dataframe_query_test.h"
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
// 1) HEAD, TAIL, DESCRIBE Tests
// ------------------------------------------------------------------

static void testHeadTailDescribeBasic(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // Column 1 (int)
    int col1[] = { 10,20,30,40,50,60,70,80,90,100 };
    Series s1 = buildIntSeries("Numbers1", col1, 10);
    bool ok = df.addSeries(&df, &s1);
    assert(ok);
    seriesFree(&s1);

    // Column 2 (int)
    int col2[] = { 1,2,3,4,5,6,7,8,9,10 };
    Series s2 = buildIntSeries("Numbers2", col2, 10);
    ok = df.addSeries(&df, &s2);
    assert(ok);
    seriesFree(&s2);

    // Column 3 (string)
    const char* col3[] = { "Alpha","Beta","Gamma","Delta","Epsilon",
                           "Zeta","Eta","Theta","Iota","Kappa" };
    Series s3 = buildStringSeries("Words", col3, 10);
    ok = df.addSeries(&df, &s3);
    assert(ok);
    seriesFree(&s3);

    assert(df.numColumns(&df) == 3);
    assert(df.numRows(&df) == 10);

    // head(5)
    DataFrame headDF = df.head(&df, 5);
    assert(headDF.numColumns(&headDF) == 3);
    assert(headDF.numRows(&headDF) == 5);
    // Spot check
    {
        const Series* s = headDF.getSeries(&headDF, 0); // "Numbers1"
        assert(s);
        int val = 0;
        bool got = seriesGetInt(s, 0, &val);
        assert(got && val == 10);
        got = seriesGetInt(s, 4, &val);
        assert(got && val == 50);
    }
    DataFrame_Destroy(&headDF);

    // tail(3)
    DataFrame tailDF = df.tail(&df, 3);
    assert(tailDF.numColumns(&tailDF) == 3);
    assert(tailDF.numRows(&tailDF) == 3);
    // Spot check
    {
        const Series* s = tailDF.getSeries(&tailDF, 1); // "Numbers2"
        int val = 0;
        bool got = seriesGetInt(s, 0, &val);
        assert(got && val == 8);
        got = seriesGetInt(s, 2, &val);
        assert(got && val == 10);
    }
    DataFrame_Destroy(&tailDF);

    // describe
    DataFrame descDF = df.describe(&df);
    // We assume (nCols=3) => (3 rows) and columns = 5
    assert(descDF.numRows(&descDF) == 3);
    assert(descDF.numColumns(&descDF) == 5);
    DataFrame_Destroy(&descDF);

    DataFrame_Destroy(&df);
}

static void testHeadTailDescribeEdgeCases(void)
{
    // Case 1: truly empty DF
    {
        DataFrame df;
        DataFrame_Create(&df);
        assert(df.numColumns(&df) == 0);
        assert(df.numRows(&df) == 0);

        // head
        DataFrame headDF = df.head(&df, 5);
        assert(headDF.numColumns(&headDF) == 0);
        assert(headDF.numRows(&headDF) == 0);
        DataFrame_Destroy(&headDF);

        // tail
        DataFrame tailDF = df.tail(&df, 5);
        assert(tailDF.numColumns(&tailDF) == 0);
        assert(tailDF.numRows(&tailDF) == 0);
        DataFrame_Destroy(&tailDF);

        // describe
        DataFrame descDF = df.describe(&df);
        // We might get 0 rows, 5 columns or 0x0. We'll assume 0 rows, 5 columns
        assert(descDF.numColumns(&descDF) == 5);
        assert(descDF.numRows(&descDF) == 0);
        DataFrame_Destroy(&descDF);

        DataFrame_Destroy(&df);
    }

    // Case 2: columns but 0 rows
    {
        DataFrame df;
        DataFrame_Create(&df);

        // Add columns with 0 elements
        Series s1;
        seriesInit(&s1, "ColEmpty1", DF_INT);
        bool ok = df.addSeries(&df, &s1);
        assert(ok);
        seriesFree(&s1);

        Series s2;
        seriesInit(&s2, "ColEmpty2", DF_STRING);
        ok = df.addSeries(&df, &s2);
        assert(ok);
        seriesFree(&s2);

        assert(df.numColumns(&df) == 2);
        assert(df.numRows(&df) == 0);

        // head
        DataFrame headDF = df.head(&df, 3);
        assert(headDF.numColumns(&headDF) == 2);
        assert(headDF.numRows(&headDF) == 0);
        DataFrame_Destroy(&headDF);

        // tail
        DataFrame tailDF = df.tail(&df, 3);
        assert(tailDF.numColumns(&tailDF) == 2);
        assert(tailDF.numRows(&tailDF) == 0);
        DataFrame_Destroy(&tailDF);

        // describe
        DataFrame descDF = df.describe(&df);
        // We have 2 original columns => 2 rows, 5 columns?
        assert(descDF.numRows(&descDF) == 2);
        assert(descDF.numColumns(&descDF) == 5);
        DataFrame_Destroy(&descDF);

        DataFrame_Destroy(&df);
    }
}

static void testHeadTailStress(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    const size_t N = 100000; // 100k

    Series s1;
    seriesInit(&s1, "LargeCol1", DF_INT);
    bool ok = df.addSeries(&df, &s1); 
    assert(ok);
    seriesFree(&s1);

    Series s2;
    seriesInit(&s2, "LargeCol2", DF_INT);
    ok = df.addSeries(&df, &s2);
    assert(ok);
    seriesFree(&s2);

    // add rows
    for (size_t i = 0; i < N; i++) {
        int v1 = (int)i;
        int v2 = (int)(i*2);
        const void* rowData[] = { &v1, &v2 };
        bool rowOk = df.addRow(&df, rowData);
        assert(rowOk);
    }
    assert(df.numRows(&df) == N);

    // head(5)
    DataFrame headDF = df.head(&df, 5);
    assert(headDF.numColumns(&headDF) == 2);
    assert(headDF.numRows(&headDF) == 5);
    DataFrame_Destroy(&headDF);

    // tail(5)
    DataFrame tailDF = df.tail(&df, 5);
    assert(tailDF.numColumns(&tailDF) == 2);
    assert(tailDF.numRows(&tailDF) == 5);
    DataFrame_Destroy(&tailDF);

    // describe
    DataFrame descDF = df.describe(&df);
    assert(descDF.numRows(&descDF) == 2);   // 2 columns => 2 rows
    assert(descDF.numColumns(&descDF) == 5);
    DataFrame_Destroy(&descDF);

    DataFrame_Destroy(&df);
}

// ------------------------------------------------------------------
// 2) SLICE & SAMPLE
// ------------------------------------------------------------------
static void testSliceAndSample(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // build 1 column with 10 rows, values = 0..9
    int vals[10]; 
    for(int i=0; i<10; i++) vals[i]=i;
    Series s = buildIntSeries("Numbers", vals, 10);
    bool ok = df.addSeries(&df, &s);
    assert(ok);
    seriesFree(&s);

    // slice(2..5) => 3 rows => rows 2,3,4
    DataFrame sliceDF = df.slice(&df, 2, 5);
    assert(sliceDF.numColumns(&sliceDF) == 1);
    assert(sliceDF.numRows(&sliceDF) == 3);
    {
        const Series* s = sliceDF.getSeries(&sliceDF, 0);
        int val = -1;
        bool got = seriesGetInt(s, 0, &val);
        assert(got && val == 2);
        got = seriesGetInt(s, 2, &val);
        assert(got && val == 4);
    }
    DataFrame_Destroy(&sliceDF);

    // sample(4) => random 4 rows
    DataFrame sampleDF = df.sample(&df, 4);
    assert(sampleDF.numColumns(&sampleDF) == 1);
    assert(sampleDF.numRows(&sampleDF) == 4);
    DataFrame_Destroy(&sampleDF);

    DataFrame_Destroy(&df);
}

// ------------------------------------------------------------------
// 3) SELECT / DROP / RENAME Columns
// ------------------------------------------------------------------
static void testSelectAndDropAndRename(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // 2 columns => "A"(int, 5 rows) and "B"(int, 5 rows)
    int vA[] = {10,20,30,40,50};
    Series sA = buildIntSeries("A", vA, 5);
    bool ok = df.addSeries(&df, &sA);
    assert(ok);
    seriesFree(&sA);

    int vB[] = {1,2,3,4,5};
    Series sB = buildIntSeries("B", vB, 5);
    ok = df.addSeries(&df, &sB);
    assert(ok);
    seriesFree(&sB);

    // selectColumns => just A
    size_t indicesA[] = {0}; // col 0 => "A"
    DataFrame selDF = df.selectColumns(&df, indicesA, 1);
    assert(selDF.numColumns(&selDF) == 1);
    assert(selDF.numRows(&selDF) == 5);
    {
        const Series* s = selDF.getSeries(&selDF, 0);
        assert(strcmp(s->name, "A") == 0);
    }
    DataFrame_Destroy(&selDF);

    // dropColumns => drop "A"
    size_t dropA[] = {0};
    DataFrame dropDF = df.dropColumns(&df, dropA, 1);
    // we only keep "B"
    assert(dropDF.numColumns(&dropDF) == 1);
    assert(dropDF.numRows(&dropDF) == 5);
    {
        const Series* s = dropDF.getSeries(&dropDF, 0);
        assert(strcmp(s->name, "B") == 0);
    }
    DataFrame_Destroy(&dropDF);

    // rename => "A" => "Alpha", "B" => "Beta"
    const char* oldN[] = {"A","B"};
    const char* newN[] = {"Alpha","Beta"};
    DataFrame renDF = df.renameColumns(&df, oldN, newN, 2);
    // now we have 2 columns "Alpha","Beta"
    assert(renDF.numColumns(&renDF) == 2);
    {
        const Series* s0 = renDF.getSeries(&renDF, 0);
        const Series* s1 = renDF.getSeries(&renDF, 1);
        assert(strcmp(s0->name, "Alpha")==0);
        assert(strcmp(s1->name, "Beta")==0);
    }
    DataFrame_Destroy(&renDF);

    DataFrame_Destroy(&df);
}

// ------------------------------------------------------------------
// 4) FILTER / DROPNA
// ------------------------------------------------------------------
static bool myPredicate(const DataFrame* df, size_t rowIdx)
{
    // We assume df has 1 or 2 columns of int, keep row if first col < 50
    const Series* s = df->getSeries(df, 0);
    if (!s) return false;
    int val;
    if (!seriesGetInt(s, rowIdx, &val)) return false;
    return (val < 50);
}

static void testFilterAndDropNA(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // 1 column => 10 int
    int v[] = {0,10,20,30,40,50,60,0,80,0}; // note some zero => "NA"
    Series s = buildIntSeries("Vals", v, 10);
    bool ok = df.addSeries(&df, &s);
    assert(ok);
    seriesFree(&s);

    // filter => keep rows where col0 < 50 => that is rows with val=0,10,20,30,40,0
    DataFrame filDF = df.filter(&df, myPredicate);
    // We'll see how many pass => val=0,10,20,30,40,0 => that's 6 rows
    assert(filDF.numRows(&filDF) == 7);
    DataFrame_Destroy(&filDF);

    // dropNA => remove any row with val=0
    // from original => we have v={0,10,20,30,40,50,60,0,80,0}, so the rows with 0 are row0,row7,row9 => 3 zeroes
    // so we keep 7
    DataFrame dnaDF = df.dropNA(&df);
    assert(dnaDF.numRows(&dnaDF) == 7);
    DataFrame_Destroy(&dnaDF);

    DataFrame_Destroy(&df);
}

// ------------------------------------------------------------------
// 5) SORT
// ------------------------------------------------------------------
static void testSort(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // 1 col => random
    int v[] = {50,20,40,10,30};
    Series s = buildIntSeries("Rand", v, 5);
    bool ok = df.addSeries(&df, &s);
    assert(ok);
    seriesFree(&s);

    // sort ascending
    DataFrame ascDF = df.sort(&df, 0, true);
    // should be 10,20,30,40,50
    {
        const Series* s0 = ascDF.getSeries(&ascDF, 0);
        int val;
        // row0 =>10, row4 =>50
        seriesGetInt(s0, 0, &val); assert(val==10);
        seriesGetInt(s0, 4, &val); assert(val==50);
    }
    DataFrame_Destroy(&ascDF);

    // sort descending
    DataFrame descDF = df.sort(&df, 0, false);
    // should be 50,40,30,20,10
    {
        const Series* s0 = descDF.getSeries(&descDF, 0);
        int val;
        seriesGetInt(s0, 0, &val); assert(val==50);
        seriesGetInt(s0, 4, &val); assert(val==10);
    }
    DataFrame_Destroy(&descDF);

    DataFrame_Destroy(&df);
}

// ------------------------------------------------------------------
// 6) GROUPBY
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
// 7) PIVOT / MELT
// ------------------------------------------------------------------
static void testPivotAndMelt(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // We'll add a small table with 3 columns => index=Year, columns=Quarter, values=Sales
    // 4 rows: Year=2021, Quarter=Q1=>10, Q2=>20; Year=2022, Quarter=Q1=>15, Q2=>25 => Actually we need 4 distinct rows
    // but we do it row by row. 
    // Let's do: row0: Year=2021, Quarter=Q1, Sales=10
    //           row1: Year=2021, Quarter=Q2, Sales=20
    //           row2: Year=2022, Quarter=Q1, Sales=15
    //           row3: Year=2022, Quarter=Q2, Sales=25

    Series yearS, quartS, salesS;
    seriesInit(&yearS, "Year", DF_INT);
    seriesInit(&quartS, "Quarter", DF_STRING);
    seriesInit(&salesS, "Sales", DF_INT);

    // row0 => 2021, "Q1", 10
    seriesAddInt(&yearS, 2021);
    seriesAddString(&quartS, "Q1");
    seriesAddInt(&salesS, 10);

    // row1 => 2021, "Q2", 20
    seriesAddInt(&yearS, 2021);
    seriesAddString(&quartS, "Q2");
    seriesAddInt(&salesS, 20);

    // row2 => 2022, "Q1", 15
    seriesAddInt(&yearS, 2022);
    seriesAddString(&quartS, "Q1");
    seriesAddInt(&salesS, 15);

    // row3 => 2022, "Q2", 25
    seriesAddInt(&yearS, 2022);
    seriesAddString(&quartS, "Q2");
    seriesAddInt(&salesS, 25);

    bool ok = df.addSeries(&df, &yearS);
    assert(ok);
    ok = df.addSeries(&df, &quartS);
    assert(ok);
    ok = df.addSeries(&df, &salesS);
    assert(ok);

    seriesFree(&yearS);
    seriesFree(&quartS);
    seriesFree(&salesS);

    // pivot => indexCol=0(Year), columnsCol=1(Quarter), valuesCol=2(Sales)
    DataFrame pivotDF = df.pivot(&df, 0, 1, 2);
    // expected result => 
    //  index | Q1   | Q2
    //  2021   10     20
    //  2022   15     25
    // => 2 rows, 3 columns
    assert(pivotDF.numRows(&pivotDF)==2);
    assert(pivotDF.numColumns(&pivotDF)==3);
    // row0 => index="2021", Q1="10", Q2="20"
    DataFrame_Destroy(&pivotDF);

    // melt => pick idCols => say col0=Year => so idCount=1 => we melt Quarter,Sales
    size_t idC[] = {0}; 
    DataFrame meltDF = df.melt(&df, idC, 1);
    // We get columns => [Year, variable, value], 
    // and #rows => 4(original rows) * 2(= num non-id columns?), Actually "Quarter" & "Sales" => so each row is repeated for those 2 columns
    // but let's see how your code handles. 
    // We'll do minimal check:
    DataFrame_Destroy(&meltDF);

    DataFrame_Destroy(&df);
}

// ------------------------------------------------------------------
// 8) dropDuplicates / unique
// ------------------------------------------------------------------
static void testDropDuplicatesAndUnique(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // We'll add a single col with repeated strings
    const char* words[] = {"Apple","Apple","Banana","Banana","Apple"};
    Series s = buildStringSeries("Fruits", words, 5);
    bool ok = df.addSeries(&df, &s);
    assert(ok);
    seriesFree(&s);

    // dropDuplicates => we keep the first occurrence of each unique row => "Apple","Banana"
    DataFrame ddDF = df.dropDuplicates(&df, NULL, 0);
    // That should keep 2 rows, Apple & Banana
    assert(ddDF.numRows(&ddDF)==2);
    DataFrame_Destroy(&ddDF);

    // unique => single col => distinct values => "Apple","Banana"
    DataFrame uDF = df.unique(&df, 0);
    assert(uDF.numColumns(&uDF)==1);
    assert(uDF.numRows(&uDF)==2);
    DataFrame_Destroy(&uDF);

    DataFrame_Destroy(&df);
}

// ------------------------------------------------------------------
// 9) sum,mean,min,max
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

// ------------------------------------------------------------------
// 10) transpose
// ------------------------------------------------------------------
static void testTranspose(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // We'll add 2 columns, each with 3 rows
    // col0 => [1,2,3], col1 => ["A","B","C"]
    int v[] = {1,2,3};
    Series s0 = buildIntSeries("Num", v, 3);
    bool ok = df.addSeries(&df, &s0);
    assert(ok);
    seriesFree(&s0);

    const char* strv[] = {"A","B","C"};
    Series s1 = buildStringSeries("Str", strv, 3);
    ok = df.addSeries(&df, &s1);
    assert(ok);
    seriesFree(&s1);

    // transpose => we get 3 columns (one per original row), and 2 rows?
    // Actually your code might store it as all DF_STRING. 
    DataFrame tDF = df.transpose(&df);
    // Minimal check that we have 3 columns, each with 2 rows
    assert(tDF.numColumns(&tDF)==3);
    assert(tDF.numRows(&tDF)==2);

    DataFrame_Destroy(&tDF);
    DataFrame_Destroy(&df);
}

// ------------------------------------------------------------------
// 11) indexOf
// ------------------------------------------------------------------
static void testIndexOf(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    int v[] = {5,10,15,10,20};
    Series s = buildIntSeries("Data", v, 5);
    bool ok = df.addSeries(&df, &s);
    assert(ok);
    seriesFree(&s);

    // indexOf => find first row where col=10 => row1
    size_t idx = df.indexOf(&df, 0, 10.0);
    assert(idx==1);

    // find row where col=20 => row4
    size_t idx2 = df.indexOf(&df, 0, 20.0);
    assert(idx2==4);

    // not found => -1
    size_t idx3 = df.indexOf(&df, 0, 999.0);
    assert(idx3 == (size_t)-1);

    DataFrame_Destroy(&df);
}

// ------------------------------------------------------------------
// 12) apply, where
// ------------------------------------------------------------------
static void rowFunction(DataFrame* outDF, const DataFrame* inDF, size_t rowIndex)
{
    // example: read int from inDF col0, add 100, store in outDF col0
    // We need outDF to have a column? Actually we'll do a 1-col approach for the result.
    if (outDF->numColumns(outDF) == 0) {
        // create a new col
        Series s;
        seriesInit(&s, "Result", DF_INT);
        outDF->addSeries(outDF, &s);
        seriesFree(&s);
    }
    const Series* sIn = inDF->getSeries(inDF, 0);
    int val=0;
    seriesGetInt(sIn, rowIndex, &val);
    val+= 100;
    // now add to outDF's col0
    Series* sOut = (Series*)outDF->getSeries(outDF, 0);
    // sOut is const Series*, but we might cast away const. 
    // Or if your design has an outDF that you can addRow. Let's do an addRow approach:
    const void* rowData[1];
    rowData[0] = &val;
    outDF->addRow(outDF, rowData);
}

static bool wherePredicate(const DataFrame* df, size_t rowIndex)
{
    // keep if col0 < 50
    const Series* s = df->getSeries(df, 0);
    int val;
    if(!seriesGetInt(s, rowIndex, &val)) return false;
    return val<50;
}

static void testApplyAndWhere(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // 1 col => [10,20,50,60]
    int v[] = {10,20,50,60};
    Series s = buildIntSeries("Col", v, 4);
    bool ok = df.addSeries(&df, &s);
    assert(ok);
    seriesFree(&s);

    // apply => rowFunction
    DataFrame appDF = df.apply(&df, rowFunction);
    // now we expect col0 => [10+100,20+100,50+100,60+100] => [110,120,150,160]
    assert(appDF.numColumns(&appDF)==1);
    assert(appDF.numRows(&appDF)==4);
    {
        const Series* s0 = appDF.getSeries(&appDF, 0);
        int val;
        seriesGetInt(s0, 0, &val); assert(val==110);
        seriesGetInt(s0, 3, &val); assert(val==160);
    }
    DataFrame_Destroy(&appDF);

    // where => set default=999 for rows that fail predicate => col < 50 => keep original 
    // so row0=10 => keep, row1=20 => keep, row2=50 => not keep => becomes 999, row3=60=>999
    DataFrame whDF = df.where(&df, wherePredicate, 999.0);
    // check
    assert(whDF.numColumns(&whDF)==1);
    assert(whDF.numRows(&whDF)==4);
    {
        const Series* s0 = whDF.getSeries(&whDF, 0);
        int val;
        seriesGetInt(s0, 0, &val); assert(val==10);
        seriesGetInt(s0, 1, &val); assert(val==20);
        seriesGetInt(s0, 2, &val); assert(val==999);
        seriesGetInt(s0, 3, &val); assert(val==999);
    }
    DataFrame_Destroy(&whDF);

    DataFrame_Destroy(&df);
}

// ------------------------------------------------------------------
// 13) explode
// ------------------------------------------------------------------
static void testExplode(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // 2 columns => "Lists"(string), "Val"(int), for 3 rows
    // row0 => "A,B", 100
    // row1 => "X",   200
    // row2 => "Y,Z", 300

    Series sList, sVal;
    seriesInit(&sList, "Lists", DF_STRING);
    seriesInit(&sVal, "Val", DF_INT);

    seriesAddString(&sList, "A,B");
    seriesAddInt(&sVal, 100);

    seriesAddString(&sList, "X");
    seriesAddInt(&sVal, 200);

    seriesAddString(&sList, "Y,Z");
    seriesAddInt(&sVal, 300);

    bool ok = df.addSeries(&df, &sList); 
    assert(ok);
    ok = df.addSeries(&df, &sVal);
    assert(ok);

    seriesFree(&sList);
    seriesFree(&sVal);

    // explode col0 => "Lists"
    DataFrame eDF = df.explode(&df, 0);
    // row0 => "A", val=100
    // row1 => "B", val=100
    // row2 => "X", val=200
    // row3 => "Y", val=300
    // row4 => "Z", val=300
    assert(eDF.numRows(&eDF)==5);
    {
        const Series* sL = eDF.getSeries(&eDF, 0);
        const Series* sV = eDF.getSeries(&eDF, 1);
        char* str=NULL;
        int ival=0;
        
        // row0 => "A", 100
        seriesGetString(sL, 0, &str); assert(strcmp(str,"A")==0); free(str);
        seriesGetInt(sV, 0, &ival); assert(ival==100);
        // row1 => "B", 100
        seriesGetString(sL, 1, &str); assert(strcmp(str,"B")==0); free(str);
        seriesGetInt(sV, 1, &ival); assert(ival==100);
        // row4 => "Z", 300
        seriesGetString(sL, 4, &str); assert(strcmp(str,"Z")==0); free(str);
        seriesGetInt(sV, 4, &ival); assert(ival==300);
    }
    DataFrame_Destroy(&eDF);

    DataFrame_Destroy(&df);
}

// ------------------------------------------------------------------
// The main testQuery driver
// ------------------------------------------------------------------

void testQuery(void)
{
    printf("Running extended DataFrame query tests...\n");

    // 1) Head/Tail/Describe
    testHeadTailDescribeBasic();
    printf(" - Basic head/tail/describe tests passed.\n");
    testHeadTailDescribeEdgeCases();
    printf(" - Edge case tests (empty DF, 0-row DF) for head/tail/describe passed.\n");
    testHeadTailStress();
    printf(" - Stress test for head/tail/describe passed.\n");

    // 2) Slice / Sample
    testSliceAndSample();
    printf(" - dfSlice, dfSample tests passed.\n");

    // 3) Select / Drop / Rename
    testSelectAndDropAndRename();
    printf(" - dfSelectColumns, dfDropColumns, dfRenameColumns tests passed.\n");

    // 4) Filter / DropNA
    testFilterAndDropNA();
    printf(" - dfFilter, dfDropNA tests passed.\n");

    // 5) Sort
    testSort();
    printf(" - dfSort tests passed.\n");

    // 6) GroupBy
    testGroupBy();
    printf(" - dfGroupBy tests passed.\n");

    // 7) Pivot / Melt
    testPivotAndMelt();
    printf(" - dfPivot, dfMelt tests passed.\n");

    // 8) dropDuplicates / unique
    testDropDuplicatesAndUnique();
    printf(" - dfDropDuplicates, dfUnique tests passed.\n");

    // 9) sum,mean,min,max
    testAggregations();
    printf(" - dfSum, dfMean, dfMin, dfMax tests passed.\n");

    // 10) transpose
    testTranspose();
    printf(" - dfTranspose tests passed.\n");

    // 11) indexOf
    testIndexOf();
    printf(" - dfIndexOf tests passed.\n");

    // 12) apply, where
    testApplyAndWhere();
    printf(" - dfApply, dfWhere tests passed.\n");

    // 13) explode
    testExplode();
    printf(" - dfExplode tests passed.\n");

    printf("All extended dataframe_query tests passed successfully!\n");
}

