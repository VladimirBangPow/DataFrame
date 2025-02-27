#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dataframe_indexing_test.h"  // the header for this test suite
#include "../dataframe.h"            // your DataFrame struct and function pointers
#include "../../Series/series.h"     // for creating Series

// ------------------------------------------------------------------
// Helper routines for building test Series
// ------------------------------------------------------------------

/** 
 * Build a small integer Series with given name and values. 
 */
static Series buildIntSeries(const char* name, const int* values, size_t count)
{
    Series s;
    seriesInit(&s, name, DF_INT);
    for (size_t i = 0; i < count; i++) {
        seriesAddInt(&s, values[i]);
    }
    return s;
}

/** 
 * Build a string Series for testing. 
 */
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
// 1) Test dfAt_impl and dfIat_impl
// ------------------------------------------------------------------

static void testAtIatBasic(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // We'll build 2 columns: "Nums"(int), "Words"(string), with 5 rows
    int nums[] = {10, 20, 30, 40, 50};
    const char* words[] = {"Alpha","Beta","Gamma","Delta","Epsilon"};

    Series s1 = buildIntSeries("Nums", nums, 5);
    bool ok = df.addSeries(&df, &s1);
    assert(ok);
    seriesFree(&s1);

    Series s2 = buildStringSeries("Words", words, 5);
    ok = df.addSeries(&df, &s2);
    assert(ok);
    seriesFree(&s2);

    assert(df.numColumns(&df) == 2);
    assert(df.numRows(&df) == 5);

    // Test dfAt_impl for (row=2, colName="Nums") => that should be 30
    DataFrame cellDF = df.at(&df, 2, "Nums");
    // We expect a 1×1 DataFrame with column name="Nums", row=0 => 30
    assert(cellDF.numColumns(&cellDF)==1);
    assert(cellDF.numRows(&cellDF)==1);
    {
        const Series* c = cellDF.getSeries(&cellDF, 0);
        assert(strcmp(c->name, "Nums")==0);
        int val=0;
        bool got = seriesGetInt(c, 0, &val);
        assert(got && val==30);
    }
    DataFrame_Destroy(&cellDF);

    // Test dfAt_impl for out-of-range => row=10 => empty
    DataFrame emptyDF = df.at(&df, 10, "Nums");
    assert(emptyDF.numColumns(&emptyDF)==0);
    assert(emptyDF.numRows(&emptyDF)==0);
    DataFrame_Destroy(&emptyDF);

    // Test dfAt_impl for colName not found => "Bogus" => empty
    DataFrame noColDF = df.at(&df, 1, "Bogus");
    assert(noColDF.numColumns(&noColDF)==0);
    assert(noColDF.numRows(&noColDF)==0);
    DataFrame_Destroy(&noColDF);

    // Test dfIat_impl for (row=3, col=1) => that is row3 of "Words", which is "Delta"
    DataFrame iatDF = df.iat(&df, 3, 1);
    assert(iatDF.numColumns(&iatDF)==1);
    assert(iatDF.numRows(&iatDF)==1);
    {
        const Series* c = iatDF.getSeries(&iatDF, 0);
        assert(strcmp(c->name, "Words")==0);
        char* st=NULL;
        bool got = seriesGetString(c, 0, &st);
        assert(got && strcmp(st,"Delta")==0);
        free(st);
    }
    DataFrame_Destroy(&iatDF);

    // iat out-of-range => row=7 => empty
    DataFrame iatEmpty = df.iat(&df, 7, 1);
    assert(iatEmpty.numColumns(&iatEmpty)==0);
    assert(iatEmpty.numRows(&iatEmpty)==0);
    DataFrame_Destroy(&iatEmpty);

    // iat col out-of-range => col=5 => empty
    DataFrame iatEmptyCol = df.iat(&df, 1, 5);
    assert(iatEmptyCol.numColumns(&iatEmptyCol)==0);
    assert(iatEmptyCol.numRows(&iatEmptyCol)==0);
    DataFrame_Destroy(&iatEmptyCol);

    DataFrame_Destroy(&df);
    printf(" - dfAt_impl and dfIat_impl tests passed.\n");
}

// ------------------------------------------------------------------
// 2) Test dfLoc_impl
// ------------------------------------------------------------------
static void testLocSubsets(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // 3 columns: "A"(int), "B"(int), "C"(string)
    int va[] = {10,20,30,40,50};
    Series sA = buildIntSeries("A", va, 5);
    bool ok = df.addSeries(&df, &sA);
    assert(ok);
    seriesFree(&sA);

    int vb[] = {100,200,300,400,500};
    Series sB = buildIntSeries("B", vb, 5);
    ok = df.addSeries(&df, &sB);
    assert(ok);
    seriesFree(&sB);

    const char* vc[] = {"X","Y","Z","P","Q"};
    Series sC = buildStringSeries("C", vc, 5);
    ok = df.addSeries(&df, &sC);
    assert(ok);
    seriesFree(&sC);

    assert(df.numColumns(&df)==3);
    assert(df.numRows(&df)==5);

    // We want rowIndices => {0,2,4}, colNames => {"A","C"}
    size_t rowIdx[] = {0,2,4};
    const char* wantedCols[] = {"A","C"};

    DataFrame subDF = df.loc(&df, rowIdx, 3, wantedCols, 2);
    // We expect subDF => 2 columns => "A","C", and 3 rows => from original rows 0,2,4
    assert(subDF.numColumns(&subDF)==2);
    assert(subDF.numRows(&subDF)==3);

    // Check col0 => "A" => row0 =>10, row1 =>30, row2 =>50
    {
        const Series* col0 = subDF.getSeries(&subDF, 0);
        assert(strcmp(col0->name,"A")==0);
        int val=0;
        bool got = seriesGetInt(col0, 0, &val);
        assert(got && val==10);
        got = seriesGetInt(col0, 2, &val);
        assert(got && val==50);
    }
    // Check col1 => "C" => row0 => "X", row1 => "Z", row2 => "Q"
    {
        const Series* col1 = subDF.getSeries(&subDF, 1);
        assert(strcmp(col1->name,"C")==0);
        char* st=NULL;
        bool got = seriesGetString(col1, 1, &st); // row1 => "Z"
        assert(got && strcmp(st,"Z")==0);
        free(st);
    }
    DataFrame_Destroy(&subDF);

    // If we specify a colName not found => skip. e.g. wantedCols2 => {"A","Bogus","C"}
    const char* wantedCols2[] = {"A","Bogus","C"};
    DataFrame skipDF = df.loc(&df, rowIdx, 3, wantedCols2, 3);
    // We still get only "A","C" because "Bogus" is not found
    assert(skipDF.numColumns(&skipDF)==2);
    assert(skipDF.numRows(&skipDF)==3);
    DataFrame_Destroy(&skipDF);

    // If row index is out-of-range => skip that row
    size_t rowIdx2[] = {1,9}; // row9 doesn't exist
    const char* wantedCols3[] = {"B"};
    DataFrame partialDF = df.loc(&df, rowIdx2, 2, wantedCols3, 1);
    // That means row1 => 1 row only, since row9 is OOR => skip
    assert(partialDF.numColumns(&partialDF)==1);
    assert(partialDF.numRows(&partialDF)==1);
    {
        const Series* cB = partialDF.getSeries(&partialDF, 0);
        int val=0;
        bool got = seriesGetInt(cB, 0, &val);
        assert(got && val==200); // from original row1 => B=200
    }
    DataFrame_Destroy(&partialDF);

    DataFrame_Destroy(&df);
    printf(" - dfLoc_impl tests passed.\n");
}

// ------------------------------------------------------------------
// 3) Test dfIloc_impl
// ------------------------------------------------------------------

static void testIlocSubsets(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // 3 columns: "X"(string), "Y"(int), "Z"(int)
    const char* vx[] = {"cat","dog","bird","fish","lion"};
    Series sX = buildStringSeries("X", vx, 5);
    bool ok = df.addSeries(&df, &sX);
    assert(ok);
    seriesFree(&sX);

    int vy[] = {1,2,3,4,5};
    Series sY = buildIntSeries("Y", vy, 5);
    ok = df.addSeries(&df, &sY);
    assert(ok);
    seriesFree(&sY);

    int vz[] = {10,20,30,40,50};
    Series sZ;
    seriesInit(&sZ, "Z", DF_INT);
    for(size_t i=0;i<5;i++){
        seriesAddInt(&sZ, vz[i]);
    }
    ok = df.addSeries(&df, &sZ);
    assert(ok);
    seriesFree(&sZ);

    assert(df.numColumns(&df)==3);
    assert(df.numRows(&df)==5);

    // df.iloc(&df, rowStart=1, rowEnd=4, colIndices={0,2}, colCount=2)
    // That means rows [1..4) => row1, row2, row3 => total 3 rows
    // columns => col0="X", col2="Z"
    size_t wantedCols[] = {0,2};
    DataFrame sliceDF = df.iloc(&df, 1, 4, wantedCols, 2);
    // => 2 columns, 3 rows
    assert(sliceDF.numColumns(&sliceDF)==2);
    assert(sliceDF.numRows(&sliceDF)==3);

    // check the string col => row0 => originally row1 => "dog", row2 => originally row3 => "fish"
    {
        const Series* cX = sliceDF.getSeries(&sliceDF, 0);
        assert(strcmp(cX->name,"X")==0);
        char* st=NULL;
        bool got = seriesGetString(cX, 2, &st); // row2 => originally row3 => "fish"
        assert(got && strcmp(st,"fish")==0);
        free(st);
    }
    // check the int col => row0 => originally row1 => Z=20, row2 => originally row3 => Z=40
    {
        const Series* cZ = sliceDF.getSeries(&sliceDF, 1);
        assert(strcmp(cZ->name,"Z")==0);
        int val=0;
        bool got = seriesGetInt(cZ, 0, &val); 
        assert(got && val==20); // row1 => 20
        got = seriesGetInt(cZ, 2, &val);
        assert(got && val==40);
    }
    DataFrame_Destroy(&sliceDF);

    // If rowStart >= nRows => empty
    DataFrame emptyDF = df.iloc(&df, 10, 12, wantedCols, 2);
    assert(emptyDF.numColumns(&emptyDF)==0);
    assert(emptyDF.numRows(&emptyDF)==0);
    DataFrame_Destroy(&emptyDF);

    // If colIndices has out-of-range => skip
    size_t bigCols[] = { 1, 5 }; // col5 doesn't exist, so we skip it
    DataFrame skipColDF = df.iloc(&df, 0, 2, bigCols, 2);
    // => only col1 => "Y", for rows 0..2 => row0, row1 => 2 rows, 1 column
    assert(skipColDF.numColumns(&skipColDF)==1);
    assert(skipColDF.numRows(&skipColDF)==2);
    {
        const Series* cY = skipColDF.getSeries(&skipColDF, 0);
        assert(strcmp(cY->name,"Y")==0);
        int val=0;
        bool got = seriesGetInt(cY, 1, &val); // row1 => originally row1 => 2
        assert(got && val==2);
    }
    DataFrame_Destroy(&skipColDF);

    DataFrame_Destroy(&df);
    printf(" - dfIloc_impl tests passed.\n");
}

// ------------------------------------------------------------------
// 4) Test the new functions: dfDrop, dfPop, dfInsert, dfIndex, dfColumns
// ------------------------------------------------------------------

static void testDropPopInsert(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // 3 columns: "A","B","C" => each 3 rows
    int colA[] = {1,2,3};
    int colB[] = {10,20,30};
    const char* colC[] = {"apple","banana","cherry"};

    Series sA = buildIntSeries("A", colA, 3);
    Series sB = buildIntSeries("B", colB, 3);
    Series sC = buildStringSeries("C", colC, 3);

    bool ok = df.addSeries(&df, &sA); assert(ok);
    ok = df.addSeries(&df, &sB);     assert(ok);
    ok = df.addSeries(&df, &sC);     assert(ok);

    seriesFree(&sA);
    seriesFree(&sB);
    seriesFree(&sC);

    // test drop => drop "B"
    const char* dropNames[] = {"B"};
    DataFrame droppedDF = df.drop(&df, dropNames, 1); // new DF missing "B"
    assert(droppedDF.numColumns(&droppedDF)==2);
    assert(droppedDF.numRows(&droppedDF)==3);
    {
        // We expect columns "A", "C"
        const Series* c0 = droppedDF.getSeries(&droppedDF, 0);
        assert(strcmp(c0->name,"A")==0);
        const Series* c1 = droppedDF.getSeries(&droppedDF, 1);
        assert(strcmp(c1->name,"C")==0);
    }
    DataFrame_Destroy(&droppedDF);

    // test pop => pop "C"
    DataFrame popped;
    DataFrame_Create(&popped);
    DataFrame popDF = df.pop(&df, "C", &popped);
    // popDF => 2 columns => "A","B"; popped => 1 column => "C"
    assert(popDF.numColumns(&popDF)==2);
    assert(popDF.numRows(&popDF)==3);
    {
        const Series* c0 = popDF.getSeries(&popDF, 0);
        assert(strcmp(c0->name,"A")==0);
        const Series* c1 = popDF.getSeries(&popDF, 1);
        assert(strcmp(c1->name,"B")==0);
    }
    assert(popped.numColumns(&popped)==1);
    assert(popped.numRows(&popped)==3);
    {
        const Series* pc = popped.getSeries(&popped, 0);
        assert(strcmp(pc->name,"C")==0);
        char* st=NULL;
        bool got = seriesGetString(pc, 2, &st);
        assert(got && strcmp(st,"cherry")==0);
        free(st);
    }
    DataFrame_Destroy(&popped);
    DataFrame_Destroy(&popDF);

    // test insert => insert a new column "Z" after col0 => position=1
    Series sZ;
    seriesInit(&sZ, "Z", DF_INT);
    // must match row count=3
    seriesAddInt(&sZ, 100);
    seriesAddInt(&sZ, 200);
    seriesAddInt(&sZ, 300);

    DataFrame insertedDF = df.insert(&df, 1, &sZ);
    // now columns => [ "A"(col0), "Z"(col1), "B"(col2), "C"(col3) ]
    assert(insertedDF.numColumns(&insertedDF)==4);
    // check col1 => "Z"
    {
        const Series* zcol = insertedDF.getSeries(&insertedDF, 1);
        assert(strcmp(zcol->name, "Z")==0);
        int val;
        bool got = seriesGetInt(zcol, 2, &val);
        assert(got && val==300);
    }
    DataFrame_Destroy(&insertedDF);

    seriesFree(&sZ);

    // test insert mismatch => let's create a col with 2 rows, while DF has 3 => 
    // see if your code just copies original or errors out
    Series sBad;
    seriesInit(&sBad, "Bad", DF_INT);
    seriesAddInt(&sBad, 999);
    seriesAddInt(&sBad, 111);
    // Insert => expecting to skip or warn
    DataFrame mismatchDF = df.insert(&df, 1, &sBad);
    // if your code does skip or fallback to original => we see 3 columns only
    assert(mismatchDF.numColumns(&mismatchDF)==3);
    // i.e. "A","B","C"
    DataFrame_Destroy(&mismatchDF);

    seriesFree(&sBad);

    DataFrame_Destroy(&df);
    printf(" - dfDrop_impl, dfPop_impl, dfInsert_impl tests passed.\n");
}

// ------------------------------------------------------------------
// 5) Test dfIndex_impl and dfColumns_impl
// ------------------------------------------------------------------

static void testIndexAndColumns(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // 2 columns, 5 rows
    int col1[] = {1,2,3,4,5};
    const char* col2[] = {"alpha","beta","gamma","delta","epsilon"};

    Series s1 = buildIntSeries("Nums", col1, 5);
    Series s2 = buildStringSeries("Words", col2, 5);
    bool ok = df.addSeries(&df, &s1);
    assert(ok);
    ok = df.addSeries(&df, &s2);
    assert(ok);

    seriesFree(&s1);
    seriesFree(&s2);

    // test df.index => single col => "index" => [0,1,2,3,4]
    DataFrame idxDF = df.index(&df);
    // => 1 col, 5 rows
    assert(idxDF.numColumns(&idxDF)==1);
    assert(idxDF.numRows(&idxDF)==5);
    {
        const Series* iSer = idxDF.getSeries(&idxDF, 0);
        assert(strcmp(iSer->name,"index")==0);
        int val;
        bool got = seriesGetInt(iSer, 4, &val);
        assert(got && val==4);
    }
    DataFrame_Destroy(&idxDF);

    // test df.cols => single col => "columns" => ["Nums","Words"]
    DataFrame colsDF = df.cols(&df);
    // => 1 col, 2 rows
    assert(colsDF.numColumns(&colsDF)==1);
    assert(colsDF.numRows(&colsDF)==2);
    {
        const Series* colSer = colsDF.getSeries(&colsDF, 0);
        assert(strcmp(colSer->name,"columns")==0);
        // row0 => "Nums", row1 => "Words"
        char* st=NULL;
        bool got = seriesGetString(colSer, 0, &st);
        assert(got && strcmp(st,"Nums")==0);
        free(st);
        got = seriesGetString(colSer, 1, &st);
        assert(got && strcmp(st,"Words")==0);
        free(st);
    }
    DataFrame_Destroy(&colsDF);

    DataFrame_Destroy(&df);
    printf(" - dfIndex_impl, dfColumns_impl tests passed.\n");
}

// ------------------------------------------------------------------
// The main test driver for indexing + new pandas-like methods
// ------------------------------------------------------------------

void testIndexing(void)
{
    printf("Running DataFrame indexing tests (at, iat, loc, iloc, drop, pop, insert, index, columns)...\n");

    // Basic indexing
    testAtIatBasic();
    testLocSubsets();
    testIlocSubsets();

    // Extended methods
    testDropPopInsert();
    testIndexAndColumns();

    printf("All DataFrame indexing tests (including new functions) passed successfully!\n");
}
