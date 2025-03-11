#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "query_test.h"  // the header for this test suite
#include "dataframe.h"
#include "series.h"
/***************************************************************
 * HELPER ROUTINES
 ***************************************************************/
static Series buildIntSeries(const char* name, const int* values, size_t count)
{
    Series s;
    seriesInit(&s, name, DF_INT);
    for (size_t i = 0; i < count; i++) {
        seriesAddInt(&s, values[i]);
    }
    return s;
}

static Series buildStringSeries(const char* name, const char* const* strings, size_t count)
{
    Series s;
    seriesInit(&s, name, DF_STRING);
    for (size_t i = 0; i < count; i++) {
        seriesAddString(&s, strings[i]);
    }
    return s;
}

/***************************************************************
 *  TEST HEAD
 ***************************************************************/
static void testHead(void)
{
    // Create a DataFrame with 1 column & 6 rows for demonstration
    DataFrame df;
    DataFrame_Create(&df);

    int col1[] = {10,20,30,40,50,60};
    Series s1 = buildIntSeries("Nums", col1, 6);
    bool ok = df.addSeries(&df, &s1);
    assert(ok);
    seriesFree(&s1);

    // HEAD(3) => expect 3 rows
    DataFrame headDF = df.head(&df, 3);
    assert(headDF.numColumns(&headDF) == 1);
    assert(headDF.numRows(&headDF) == 3);

    // Spot check values
    const Series* s = headDF.getSeries(&headDF, 0);
    int val=0;
    bool got = seriesGetInt(s, 0, &val);
    assert(got && val==10);
    got = seriesGetInt(s, 2, &val);
    assert(got && val==30);

    DataFrame_Destroy(&headDF);
    DataFrame_Destroy(&df);
    printf("testHead passed.\n");
}

/***************************************************************
 *  TEST TAIL
 ***************************************************************/
static void testTail(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    int col1[] = {100,200,300,400,500};
    Series s1 = buildIntSeries("Data", col1, 5);
    bool ok = df.addSeries(&df, &s1);
    assert(ok);
    seriesFree(&s1);

    // TAIL(2) => last 2 rows => [400, 500]
    DataFrame tailDF = df.tail(&df, 2);
    assert(tailDF.numColumns(&tailDF) == 1);
    assert(tailDF.numRows(&tailDF) == 2);

    const Series* s = tailDF.getSeries(&tailDF, 0);
    int val=0;
    // row0 => 400, row1 => 500
    bool got = seriesGetInt(s, 0, &val);
    assert(got && val==400);
    seriesGetInt(s, 1, &val);
    assert(val==500);

    DataFrame_Destroy(&tailDF);
    DataFrame_Destroy(&df);
    printf("testTail passed.\n");
}

/***************************************************************
 *  TEST DESCRIBE
 ***************************************************************/
static void testDescribe(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // We'll add 2 numeric columns
    int col1[] = {10,20,30,40};
    Series s1 = buildIntSeries("C1", col1, 4);
    df.addSeries(&df, &s1);
    seriesFree(&s1);

    int col2[] = {5,5,10,20};
    Series s2 = buildIntSeries("C2", col2, 4);
    df.addSeries(&df, &s2);
    seriesFree(&s2);

    // describe => should produce 2 rows (one per col), each with 5 columns: 
    // colName, count, min, max, mean
    DataFrame descDF = df.describe(&df);
    // expect 2 rows, 5 columns
    assert(descDF.numRows(&descDF)==2);
    assert(descDF.numColumns(&descDF)==5);

    DataFrame_Destroy(&descDF);
    DataFrame_Destroy(&df);
    printf("testDescribe passed.\n");
}

/***************************************************************
 *  TEST SLICE
 ***************************************************************/
static void testSlice(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // Single column => 6 values => 0..5
    int arr[] = {0,1,2,3,4,5};
    Series s = buildIntSeries("Vals", arr, 6);
    df.addSeries(&df, &s);
    seriesFree(&s);

    // SLICE(2..5) => rows 2,3,4 => total 3
    DataFrame sliceDF = df.slice(&df, 2, 5);
    assert(sliceDF.numRows(&sliceDF)==3);
    {
        const Series* c = sliceDF.getSeries(&sliceDF, 0);
        int val;
        seriesGetInt(c, 0, &val); // originally row2 => 2
        assert(val==2);
        seriesGetInt(c, 2, &val); // originally row4 => 4
        assert(val==4);
    }

    DataFrame_Destroy(&sliceDF);
    DataFrame_Destroy(&df);
    printf("testSlice passed.\n");
}

/***************************************************************
 *  TEST SAMPLE
 ***************************************************************/
static void testSample(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    int arr[] = {10,11,12,13,14,15};
    Series s = buildIntSeries("Rand", arr, 6);
    df.addSeries(&df, &s);
    seriesFree(&s);

    // sample(3) => random subset of 3
    DataFrame samp = df.sample(&df, 3);
    assert(samp.numRows(&samp)==3);
    assert(samp.numColumns(&samp)==1);

    DataFrame_Destroy(&samp);
    DataFrame_Destroy(&df);
    printf("testSample passed.\n");
}

/***************************************************************
 *  TEST SELECT COLUMNS
 ***************************************************************/
static void testSelectColumns(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // We'll have 3 columns, want to select the second & third
    int colA[] = {1,2,3};
    int colB[] = {10,20,30};
    const char* colC[] = {"X","Y","Z"};

    Series sA = buildIntSeries("A", colA, 3);
    Series sB = buildIntSeries("B", colB, 3);
    Series sC = buildStringSeries("C", colC, 3);

    df.addSeries(&df, &sA);
    df.addSeries(&df, &sB);
    df.addSeries(&df, &sC);

    seriesFree(&sA);
    seriesFree(&sB);
    seriesFree(&sC);

    // We want to select columns #1 and #2 => "B","C"
    size_t indices[] = {1,2};
    DataFrame sel = df.selectColumns(&df, indices, 2);
    assert(sel.numColumns(&sel)==2);
    assert(sel.numRows(&sel)==3);

    // check col0 => "B"
    const Series* s0 = sel.getSeries(&sel, 0);
    assert(strcmp(s0->name,"B")==0);

    DataFrame_Destroy(&sel);
    DataFrame_Destroy(&df);
    printf("testSelectColumns passed.\n");
}

/***************************************************************
 *  TEST DROP COLUMNS
 ***************************************************************/
static void testDropColumns(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // 3 columns => "A","B","C"
    int colA[] = {5,6,7};
    int colB[] = {10,20,30};
    int colC[] = {1,2,3};

    Series sA = buildIntSeries("A", colA, 3);
    Series sB = buildIntSeries("B", colB, 3);
    Series sC;
    seriesInit(&sC, "C", DF_INT);
    for (int i=0; i<3; i++){
        seriesAddInt(&sC, colC[i]);
    }

    df.addSeries(&df, &sA);
    df.addSeries(&df, &sB);
    df.addSeries(&df, &sC);

    seriesFree(&sA);
    seriesFree(&sB);
    seriesFree(&sC);

    // drop columns #1 => that is "B"
    size_t dropIdx[] = {1};
    DataFrame dropped = df.dropColumns(&df, dropIdx, 1);
    // we keep "A","C"
    assert(dropped.numColumns(&dropped)==2);
    assert(dropped.numRows(&dropped)==3);

    // check first col => "A"
    const Series* c0 = dropped.getSeries(&dropped, 0);
    assert(strcmp(c0->name, "A")==0);

    DataFrame_Destroy(&dropped);
    DataFrame_Destroy(&df);
    printf("testDropColumns passed.\n");
}

/***************************************************************
 *  TEST RENAME COLUMNS
 ***************************************************************/
static void testRenameColumns(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    int arr[] = {9,8,7};
    Series s = buildIntSeries("OldName", arr, 3);
    df.addSeries(&df, &s);
    seriesFree(&s);

    // rename OldName => NewName
    const char* oldN[] = {"OldName"};
    const char* newN[] = {"NewName"};
    DataFrame ren = df.renameColumns(&df, oldN, newN, 1);

    // check col0 => "NewName"
    const Series* c0 = ren.getSeries(&ren, 0);
    assert(strcmp(c0->name,"NewName")==0);

    DataFrame_Destroy(&ren);
    DataFrame_Destroy(&df);
    printf("testRenameColumns passed.\n");
}

/***************************************************************
 *  TEST FILTER
 ***************************************************************/
static bool filterPredicateExample(const DataFrame* df, size_t rowIdx)
{
    // keep if col0 < 50
    const Series* s = df->getSeries(df, 0);
    int val=0;
    if(!seriesGetInt(s, rowIdx, &val)) return false;
    return val<50;
}

static void testFilter(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    int arr[] = {10,20,50,60};
    Series sCol = buildIntSeries("Col", arr, 4);
    df.addSeries(&df, &sCol);
    seriesFree(&sCol);

    DataFrame filtered = df.filter(&df, filterPredicateExample);
    // keep rows where col<50 => that is row0=10, row1=20 => total 2
    assert(filtered.numRows(&filtered)==2);

    DataFrame_Destroy(&filtered);
    DataFrame_Destroy(&df);
    printf("testFilter passed.\n");
}

/***************************************************************
 *  TEST DROPNA
 ***************************************************************/
static void testDropNA(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // 1 column => [0,10,0,20]
    int arr[] = {0,10,0,20};
    Series s = buildIntSeries("Values", arr, 4);
    df.addSeries(&df, &s);
    seriesFree(&s);

    // dropNA => remove row if col=0
    // => we keep row1=10, row3=20 => total 2
    DataFrame noNA = df.dropNA(&df);
    assert(noNA.numRows(&noNA)==2);

    DataFrame_Destroy(&noNA);
    DataFrame_Destroy(&df);
    printf("testDropNA passed.\n");
}

/***************************************************************
 *  TEST SORT
 ***************************************************************/
static void testSort(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    int arr[] = {30,10,20};
    Series s = buildIntSeries("Data", arr, 3);
    df.addSeries(&df, &s);
    seriesFree(&s);

    // sort ascending => [10,20,30]
    DataFrame asc = df.sort(&df, 0, true);
    {
        const Series* c0 = asc.getSeries(&asc, 0);
        int val;
        seriesGetInt(c0, 0, &val); assert(val==10);
        seriesGetInt(c0, 2, &val); assert(val==30);
    }
    DataFrame_Destroy(&asc);

    // sort descending => [30,20,10]
    DataFrame desc = df.sort(&df, 0, false);
    {
        const Series* c0 = desc.getSeries(&desc, 0);
        int val;
        seriesGetInt(c0, 0, &val); assert(val==30);
        seriesGetInt(c0, 2, &val); assert(val==10);
    }
    DataFrame_Destroy(&desc);

    DataFrame_Destroy(&df);
    printf("testSort passed.\n");
}

/***************************************************************
 *  TEST DROP DUPLICATES
 ***************************************************************/
static void testDropDuplicates(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    const char* arr[] = {"Apple","Apple","Banana","Apple"};
    Series s = buildStringSeries("Fruits", arr, 4);
    df.addSeries(&df, &s);
    seriesFree(&s);

    // dropDuplicates => keep first occurrence => Apple, Banana
    DataFrame dd = df.dropDuplicates(&df, NULL, 0); // entire row
    assert(dd.numRows(&dd)==2);
    DataFrame_Destroy(&dd);

    DataFrame_Destroy(&df);
    printf("testDropDuplicates passed.\n");
}

/***************************************************************
 *  TEST UNIQUE
 ***************************************************************/
static void testUnique(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // 1 col => repeated strings
    const char* arr[] = {"A","A","B","C","C","C"};
    Series s = buildStringSeries("Letters", arr, 6);
    df.addSeries(&df, &s);
    seriesFree(&s);

    DataFrame un = df.unique(&df, 0);
    // distinct => "A","B","C"
    assert(un.numRows(&un)==3);
    DataFrame_Destroy(&un);

    DataFrame_Destroy(&df);
    printf("testUnique passed.\n");
}

/***************************************************************
 *  TEST TRANSPOSE
 ***************************************************************/
static void testTranspose(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // 2 columns => "X"(int), "Y"(string), each 2 rows
    int colX[] = {1,2};
    const char* colY[] = {"Alpha","Beta"};

    Series sX = buildIntSeries("X", colX, 2);
    Series sY = buildStringSeries("Y", colY, 2);

    df.addSeries(&df, &sX);
    df.addSeries(&df, &sY);

    seriesFree(&sX);
    seriesFree(&sY);

    DataFrame t = df.transpose(&df);
    // now we get 2 original rows => so 2 columns in new DF. 
    // each col has 2 strings (since we do a textual transpose).

    assert(t.numColumns(&t)==2);
    // spot check row count => 2
    assert(t.numRows(&t)==2);

    DataFrame_Destroy(&t);
    DataFrame_Destroy(&df);
    printf("testTranspose passed.\n");
}

/***************************************************************
 *  TEST INDEXOF
 ***************************************************************/
static void testIndexOf(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    int arr[] = {10,20,30,20};
    Series s = buildIntSeries("Vals", arr, 4);
    df.addSeries(&df, &s);
    seriesFree(&s);

    // indexOf => find first row where col=20 => row1
    size_t idx = df.indexOf(&df, 0, 20.0);
    assert(idx==1);

    // not found => -1
    size_t idx2 = df.indexOf(&df, 0, 999.0);
    assert(idx2 == (size_t)-1);

    DataFrame_Destroy(&df);
    printf("testIndexOf passed.\n");
}

/***************************************************************
 *  TEST APPLY
 ***************************************************************/
static void rowFunc(DataFrame* outDF, const DataFrame* inDF, size_t rowIndex)
{
    // read col0, add 5, store
    if (outDF->numColumns(outDF)==0) {
        // create 1 col in outDF
        Series s;
        seriesInit(&s, "OutCol", DF_INT);
        outDF->addSeries(outDF, &s);
        seriesFree(&s);
    }
    const Series* sIn = inDF->getSeries(inDF, 0);
    int val;
    seriesGetInt(sIn, rowIndex, &val);
    val+=5;
    // addRow => { &val }
    const void* rowData[1];
    rowData[0] = &val;
    outDF->addRow(outDF, rowData);
}

static void testApply(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    int arr[] = {1,2,3};
    Series s = buildIntSeries("Base", arr, 3);
    df.addSeries(&df, &s);
    seriesFree(&s);

    DataFrame result = df.apply(&df, rowFunc);
    // => col0 => [1+5, 2+5, 3+5] => [6,7,8]
    assert(result.numRows(&result)==3);
    const Series* c0 = result.getSeries(&result, 0);
    int val;
    seriesGetInt(c0, 2, &val);
    assert(val==8);

    DataFrame_Destroy(&result);
    DataFrame_Destroy(&df);
    printf("testApply passed.\n");
}

/***************************************************************
 *  TEST WHERE
 ***************************************************************/
static bool wherePred(const DataFrame* df, size_t rowIndex)
{
    // keep if < 50
    const Series* s = df->getSeries(df, 0);
    int val;
    seriesGetInt(s, rowIndex, &val);
    return (val<50);
}

static void testWhere(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    int arr[] = {10,20,50};
    Series s = buildIntSeries("Vals", arr, 3);
    df.addSeries(&df, &s);
    seriesFree(&s);

    // where => if predicate fails => set default=999
    // row0=10 => keep 10, row1=20 => keep 20, row2=50 => 999
    DataFrame wh = df.where(&df, wherePred, 999.0);
    assert(wh.numRows(&wh)==3);
    {
        const Series* c0 = wh.getSeries(&wh, 0);
        int val;
        seriesGetInt(c0, 2, &val);
        assert(val==999);
    }
    DataFrame_Destroy(&wh);
    DataFrame_Destroy(&df);
    printf("testWhere passed.\n");
}

/***************************************************************
 *  TEST EXPLODE
 ***************************************************************/
static void testExplode(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // "List" => string, "Code" => int
    Series sList, sCode;
    seriesInit(&sList, "List", DF_STRING);
    seriesInit(&sCode, "Code", DF_INT);

    seriesAddString(&sList, "A,B");
    seriesAddInt(&sCode, 100);

    seriesAddString(&sList, "X");
    seriesAddInt(&sCode, 200);

    df.addSeries(&df, &sList);
    df.addSeries(&df, &sCode);

    seriesFree(&sList);
    seriesFree(&sCode);

    // explode col0 => "List"
    DataFrame ex = df.explode(&df, 0);
    // row0 => "A", code=100
    // row1 => "B", code=100
    // row2 => "X", code=200
    assert(ex.numRows(&ex)==3);
    {
        const Series* cList = ex.getSeries(&ex, 0);
        char* st=NULL;
        seriesGetString(cList, 1, &st);
        assert(strcmp(st,"B")==0);
        free(st);
    }

    DataFrame_Destroy(&ex);
    DataFrame_Destroy(&df);
    printf("testExplode passed.\n");
}

/***************************************************************
 * MASTER TEST QUERY
 ***************************************************************/
void testQuery(void)
{
    printf("Running DataFrame Query Tests (split) ...\n");

    // 1) head/tail/describe
    testHead();
    testTail();
    testDescribe();

    // 2) slice, sample
    testSlice();
    testSample();

    // 3) select, drop, rename
    testSelectColumns();
    testDropColumns();
    testRenameColumns();

    // 4) filter, dropNA
    testFilter();
    testDropNA();

    // 5) sort
    testSort();

    // 6) dropDuplicates
    testDropDuplicates();

    // 7) unique
    testUnique();

    // 8) transpose
    testTranspose();

    // 9) indexOf
    testIndexOf();

    // 10) apply
    testApply();

    // 11) where
    testWhere();

    // 12) explode
    testExplode();

    printf("All DataFrame query tests passed successfully!\n");
}
