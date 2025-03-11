#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "indexing_test.h"  // The header for this test suite
#include "dataframe.h"
#include "series.h"
// ------------------------------------------------------------------
// Helper routines for building test Series
// ------------------------------------------------------------------

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

// ------------------------------------------------------------------
// 1) Test dfAt_impl
// ------------------------------------------------------------------
static void testDfAt(void)
{
    printf("Running testDfAt...\n");

    DataFrame df;
    DataFrame_Create(&df);

    // Build 2 columns: "Nums"(int), "Words"(string), 5 rows
    int nums[] = {10,20,30,40,50};
    const char* words[] = {"Alpha","Beta","Gamma","Delta","Epsilon"};

    Series sNums = buildIntSeries("Nums", nums, 5);
    df.addSeries(&df, &sNums);
    seriesFree(&sNums);

    Series sWords = buildStringSeries("Words", words, 5);
    df.addSeries(&df, &sWords);
    seriesFree(&sWords);

    // 1) Normal usage: at(row=2, colName="Nums") => should produce a 1×1 DF with "Nums"[0] = 30
    {
        DataFrame cellDF = df.at(&df, 2, "Nums");
        assert(cellDF.numColumns(&cellDF)==1);
        assert(cellDF.numRows(&cellDF)==1);

        const Series* c = cellDF.getSeries(&cellDF, 0);
        assert(strcmp(c->name, "Nums")==0);
        int val=0;
        bool got = seriesGetInt(c, 0, &val);
        assert(got && val==30);

        DataFrame_Destroy(&cellDF);
    }

    // 2) Out-of-range row => empty DF
    {
        DataFrame emptyDF = df.at(&df, 10, "Nums");
        assert(emptyDF.numColumns(&emptyDF)==0);
        assert(emptyDF.numRows(&emptyDF)==0);
        DataFrame_Destroy(&emptyDF);
    }

    // 3) colName not found => empty DF
    {
        DataFrame noCol = df.at(&df, 1, "Bogus");
        assert(noCol.numColumns(&noCol)==0);
        assert(noCol.numRows(&noCol)==0);
        DataFrame_Destroy(&noCol);
    }

    DataFrame_Destroy(&df);
    printf("testDfAt passed.\n");
}

// ------------------------------------------------------------------
// 2) Test dfIat_impl
// ------------------------------------------------------------------
static void testDfIat(void)
{
    printf("Running testDfIat...\n");

    DataFrame df;
    DataFrame_Create(&df);

    int nums[] = {10,20,30,40,50};
    const char* words[] = {"Alpha","Beta","Gamma","Delta","Epsilon"};

    Series sNums = buildIntSeries("Nums", nums, 5);
    df.addSeries(&df, &sNums);
    seriesFree(&sNums);

    Series sWords = buildStringSeries("Words", words, 5);
    df.addSeries(&df, &sWords);
    seriesFree(&sWords);

    // 1) dfIat(row=3, col=1) => should produce "Words" row => "Delta"
    {
        DataFrame cDF = df.iat(&df, 3, 1);
        assert(cDF.numColumns(&cDF)==1);
        assert(cDF.numRows(&cDF)==1);

        const Series* col = cDF.getSeries(&cDF, 0);
        assert(strcmp(col->name,"Words")==0);
        char* st=NULL;
        bool got = seriesGetString(col, 0, &st);
        assert(got && strcmp(st,"Delta")==0);
        free(st);

        DataFrame_Destroy(&cDF);
    }

    // 2) row out-of-range => empty
    {
        DataFrame eDF = df.iat(&df, 10, 1);
        assert(eDF.numColumns(&eDF)==0);
        assert(eDF.numRows(&eDF)==0);
        DataFrame_Destroy(&eDF);
    }

    // 3) col out-of-range => empty
    {
        DataFrame e2 = df.iat(&df, 1, 5);
        assert(e2.numColumns(&e2)==0);
        assert(e2.numRows(&e2)==0);
        DataFrame_Destroy(&e2);
    }

    DataFrame_Destroy(&df);
    printf("testDfIat passed.\n");
}

// ------------------------------------------------------------------
// 3) Test dfLoc_impl
// ------------------------------------------------------------------
static void testDfLoc(void)
{
    printf("Running testDfLoc...\n");

    DataFrame df;
    DataFrame_Create(&df);

    // 3 columns => "A","B","C"
    int arrA[] = {10,20,30,40,50};
    int arrB[] = {100,200,300,400,500};
    const char* arrC[] = {"X","Y","Z","P","Q"};

    Series sA = buildIntSeries("A", arrA, 5);
    df.addSeries(&df, &sA);
    seriesFree(&sA);

    Series sB = buildIntSeries("B", arrB, 5);
    df.addSeries(&df, &sB);
    seriesFree(&sB);

    Series sC = buildStringSeries("C", arrC, 5);
    df.addSeries(&df, &sC);
    seriesFree(&sC);

    // 1) rowIndices => {0,2,4}, colNames => {"A","C"}
    {
        size_t rowIdx[] = {0,2,4};
        const char* colNames[] = {"A","C"};
        DataFrame subDF = df.loc(&df, rowIdx, 3, colNames, 2);
        assert(subDF.numColumns(&subDF)==2);
        assert(subDF.numRows(&subDF)==3);

        // col0 => "A" => row0 =>10, row1 =>30, row2 =>50
        const Series* c0 = subDF.getSeries(&subDF, 0);
        assert(strcmp(c0->name,"A")==0);
        int val=0;
        bool got = seriesGetInt(c0, 2, &val);
        assert(got && val==50);

        // col1 => "C" => row1 => "Z"
        const Series* c1 = subDF.getSeries(&subDF, 1);
        char* st=NULL;
        got = seriesGetString(c1, 1, &st);
        assert(got && strcmp(st,"Z")==0);
        free(st);

        DataFrame_Destroy(&subDF);
    }

    // 2) unknown col => skip
    {
        size_t rowIdx2[] = {0,1,2};
        const char* colNames2[] = {"A","Bogus","C"};
        DataFrame skipDF = df.loc(&df, rowIdx2, 3, colNames2, 3);
        // => col "A","C" only
        assert(skipDF.numColumns(&skipDF)==2);
        DataFrame_Destroy(&skipDF);
    }

    // 3) out-of-range row => skip
    {
        size_t rowIdx3[] = {1,9}; 
        const char* coln[] = {"B"};
        DataFrame part = df.loc(&df, rowIdx3, 2, coln, 1);
        // => only row1 is valid => 1 row
        assert(part.numColumns(&part)==1);
        assert(part.numRows(&part)==1);
        DataFrame_Destroy(&part);
    }

    DataFrame_Destroy(&df);
    printf("testDfLoc passed.\n");
}

// ------------------------------------------------------------------
// 4) Test dfIloc_impl
// ------------------------------------------------------------------
static void testDfIloc(void)
{
    printf("Running testDfIloc...\n");

    DataFrame df;
    DataFrame_Create(&df);

    // 3 columns => "X"(string), "Y"(int), "Z"(int)
    const char* vx[] = {"cat","dog","bird","fish","lion"};
    Series sX = buildStringSeries("X", vx, 5);
    df.addSeries(&df, &sX);
    seriesFree(&sX);

    int vy[] = {1,2,3,4,5};
    Series sY = buildIntSeries("Y", vy, 5);
    df.addSeries(&df, &sY);
    seriesFree(&sY);

    int vz[] = {10,20,30,40,50};
    Series sZ = buildIntSeries("Z", vz, 5);
    df.addSeries(&df, &sZ);
    seriesFree(&sZ);

    // 1) rows => [1..4) => row1,row2,row3 => columns => col0("X"), col2("Z")
    {
        size_t wantedCols[] = {0,2};
        DataFrame slice = df.iloc(&df, 1, 4, wantedCols, 2);
        assert(slice.numColumns(&slice)==2);
        assert(slice.numRows(&slice)==3);

        // col0 => "X", row2 => originally row3 => "fish"
        const Series* cX = slice.getSeries(&slice, 0);
        char* st=NULL;
        bool got = seriesGetString(cX, 2, &st);
        assert(got && strcmp(st,"fish")==0);
        free(st);

        // col1 => "Z", row0 => originally row1 => 20
        const Series* cZ = slice.getSeries(&slice, 1);
        int val=0;
        got = seriesGetInt(cZ, 0, &val);
        assert(got && val==20);

        DataFrame_Destroy(&slice);
    }

    // 2) rowStart >= nRows => empty
    {
        size_t wantedCols2[] = {0,1};
        DataFrame eDF = df.iloc(&df, 10, 12, wantedCols2, 2);
        assert(eDF.numColumns(&eDF)==0);
        assert(eDF.numRows(&eDF)==0);
        DataFrame_Destroy(&eDF);
    }

    // 3) colIndices out-of-range => skip
    {
        size_t bigCols[] = {1,5};
        DataFrame skipCols = df.iloc(&df, 0, 2, bigCols, 2);
        // => only col1 => "Y"
        assert(skipCols.numColumns(&skipCols)==1);
        assert(skipCols.numRows(&skipCols)==2);
        DataFrame_Destroy(&skipCols);
    }

    DataFrame_Destroy(&df);
    printf("testDfIloc passed.\n");
}

// ------------------------------------------------------------------
// 5) Test dfDrop_impl
// ------------------------------------------------------------------
static void testDfDrop(void)
{
    printf("Running testDfDrop...\n");

    DataFrame df;
    DataFrame_Create(&df);

    int colA[] = {1,2,3};
    int colB[] = {10,20,30};
    const char* colC[] = {"apple","banana","cherry"};

    Series sA = buildIntSeries("A", colA, 3);
    Series sB = buildIntSeries("B", colB, 3);
    Series sC = buildStringSeries("C", colC, 3);

    df.addSeries(&df, &sA);
    df.addSeries(&df, &sB);
    df.addSeries(&df, &sC);

    seriesFree(&sA);
    seriesFree(&sB);
    seriesFree(&sC);

    // drop "B"
    {
        const char* dropNames[] = {"B"};
        DataFrame dropped = df.drop(&df, dropNames, 1);
        assert(dropped.numColumns(&dropped)==2);
        assert(dropped.numRows(&dropped)==3);

        const Series* c0 = dropped.getSeries(&dropped, 0);
        assert(strcmp(c0->name,"A")==0);
        const Series* c1 = dropped.getSeries(&dropped, 1);
        assert(strcmp(c1->name,"C")==0);

        DataFrame_Destroy(&dropped);
    }

    // drop multiple => e.g. "A","C"
    {
        const char* dropMulti[] = {"A","C"};
        DataFrame d2 = df.drop(&df, dropMulti, 2);
        // => only "B" remains
        assert(d2.numColumns(&d2)==1);
        assert(d2.numRows(&d2)==3);

        const Series* onlyCol = d2.getSeries(&d2, 0);
        assert(strcmp(onlyCol->name,"B")==0);

        DataFrame_Destroy(&d2);
    }

    DataFrame_Destroy(&df);
    printf("testDfDrop passed.\n");
}

// ------------------------------------------------------------------
// 6) Test dfPop_impl
// ------------------------------------------------------------------
static void testDfPop(void)
{
    printf("Running testDfPop...\n");

    DataFrame df;
    DataFrame_Create(&df);

    int colA[] = {1,2,3};
    int colB[] = {10,20,30};
    const char* colC[] = {"apple","banana","cherry"};

    Series sA = buildIntSeries("A", colA, 3);
    df.addSeries(&df, &sA);
    seriesFree(&sA);

    Series sB = buildIntSeries("B", colB, 3);
    df.addSeries(&df, &sB);
    seriesFree(&sB);

    Series sC = buildStringSeries("C", colC, 3);
    df.addSeries(&df, &sC);
    seriesFree(&sC);

    // pop "B"
    {
        DataFrame poppedCol;
        DataFrame_Create(&poppedCol);

        DataFrame afterPop = df.pop(&df, "B", &poppedCol);
        // afterPop => "A","C" => 2 cols, 3 rows
        // poppedCol => "B" => 1 col, 3 rows
        assert(afterPop.numColumns(&afterPop)==2);
        assert(afterPop.numRows(&afterPop)==3);
        assert(poppedCol.numColumns(&poppedCol)==1);
        assert(poppedCol.numRows(&poppedCol)==3);

        const Series* poppedSeries = poppedCol.getSeries(&poppedCol, 0);
        assert(strcmp(poppedSeries->name,"B")==0);
        int val=0;
        bool got = seriesGetInt(poppedSeries,2,&val);
        assert(got && val==30);

        DataFrame_Destroy(&poppedCol);
        DataFrame_Destroy(&afterPop);
    }

    DataFrame_Destroy(&df);
    printf("testDfPop passed.\n");
}

// ------------------------------------------------------------------
// 7) Test dfInsert_impl
// ------------------------------------------------------------------
static void testDfInsert(void)
{
    printf("Running testDfInsert...\n");

    DataFrame df;
    DataFrame_Create(&df);

    int colA[] = {1,2,3};
    int colB[] = {10,20,30};
    Series sA = buildIntSeries("A", colA, 3);
    Series sB = buildIntSeries("B", colB, 3);

    df.addSeries(&df, &sA);
    df.addSeries(&df, &sB);

    seriesFree(&sA);
    seriesFree(&sB);

    // Insert "Z"(3 rows) at position=1
    Series sZ;
    seriesInit(&sZ, "Z", DF_INT);
    seriesAddInt(&sZ,100);
    seriesAddInt(&sZ,200);
    seriesAddInt(&sZ,300);

    DataFrame insDF = df.insert(&df,1,&sZ);
    // => columns => ["A"(0), "Z"(1), "B"(2)] => total 3 columns
    assert(insDF.numColumns(&insDF)==3);
    const Series* zCol = insDF.getSeries(&insDF, 1);
    assert(strcmp(zCol->name,"Z")==0);
    int val=0;
    bool got = seriesGetInt(zCol, 2, &val);
    assert(got && val==300);

    DataFrame_Destroy(&insDF);
    seriesFree(&sZ);

    // Insert mismatch => 2 rows vs. DF has 3 => skip
    Series sBad;
    seriesInit(&sBad,"Bad",DF_INT);
    seriesAddInt(&sBad,999);
    seriesAddInt(&sBad,111);

    DataFrame mismatch = df.insert(&df,1,&sBad);
    // => should remain 2 columns => "A","B"
    assert(mismatch.numColumns(&mismatch)==2);

    DataFrame_Destroy(&mismatch);
    seriesFree(&sBad);

    DataFrame_Destroy(&df);
    printf("testDfInsert passed.\n");
}

// ------------------------------------------------------------------
// 8) Test dfIndex_impl
// ------------------------------------------------------------------
static void testDfIndex(void)
{
    printf("Running testDfIndex...\n");

    DataFrame df;
    DataFrame_Create(&df);

    // 2 columns => 4 rows
    int colX[] = {11,22,33,44};
    int colY[] = {100,200,300,400};
    Series sX = buildIntSeries("X", colX, 4);
    Series sY = buildIntSeries("Y", colY, 4);
    df.addSeries(&df, &sX);
    df.addSeries(&df, &sY);

    seriesFree(&sX);
    seriesFree(&sY);

    // df.index => single col => "index" => [0,1,2,3]
    DataFrame idxDF = df.index(&df);
    assert(idxDF.numColumns(&idxDF)==1);
    assert(idxDF.numRows(&idxDF)==4);

    const Series* idxS = idxDF.getSeries(&idxDF, 0);
    assert(strcmp(idxS->name,"index")==0);
    int val=0;
    bool got = seriesGetInt(idxS,3,&val);
    assert(got && val==3);

    DataFrame_Destroy(&idxDF);
    DataFrame_Destroy(&df);

    printf("testDfIndex passed.\n");
}

// ------------------------------------------------------------------
// 9) Test dfColumns_impl
// ------------------------------------------------------------------
static void testDfColumns(void)
{
    printf("Running testDfColumns...\n");

    DataFrame df;
    DataFrame_Create(&df);

    // 3 columns => "One","Two","Three"
    int col1[] = {10,20};
    Series s1 = buildIntSeries("One", col1, 2);
    df.addSeries(&df, &s1);
    seriesFree(&s1);

    int col2[] = {100,200};
    Series s2 = buildIntSeries("Two", col2, 2);
    df.addSeries(&df, &s2);
    seriesFree(&s2);

    const char* arr3[] = {"Hello","World"};
    Series s3 = buildStringSeries("Three", arr3, 2);
    df.addSeries(&df,&s3);
    seriesFree(&s3);

    // df.columns => single col => "columns" => rows => "One","Two","Three"
    DataFrame colsDF = df.cols(&df);
    assert(colsDF.numColumns(&colsDF)==1);
    assert(colsDF.numRows(&colsDF)==3);

    const Series* colSer = colsDF.getSeries(&colsDF, 0);
    assert(strcmp(colSer->name,"columns")==0);

    char* st=NULL;
    // row0 => "One", row1=>"Two", row2=>"Three"
    bool got = seriesGetString(colSer, 2, &st);
    assert(got && strcmp(st,"Three")==0);
    free(st);

    DataFrame_Destroy(&colsDF);
    DataFrame_Destroy(&df);

    printf("testDfColumns passed.\n");
}

// If you need a quick copyDataFrame to help in tests:
static void copyDataFrame(const DataFrame* src, DataFrame* dst)
{
    DataFrame_Create(dst);
    size_t nCols = src->numColumns(src);
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = src->getSeries(src, c);
        if (!s) continue;
        // build a copy series
        Series copyS;
        seriesInit(&copyS, s->name, s->type);

        size_t nRows = seriesSize(s);
        for (size_t r = 0; r < nRows; r++) {
            switch (s->type) {
                case DF_INT: {
                    int val;
                    if (seriesGetInt(s, r, &val)) {
                        seriesAddInt(&copyS, val);
                    }
                } break;
                case DF_DOUBLE: {
                    double dv;
                    if (seriesGetDouble(s, r, &dv)) {
                        seriesAddDouble(&copyS, dv);
                    }
                } break;
                case DF_STRING: {
                    char* strVal = NULL;
                    if (seriesGetString(s, r, &strVal)) {
                        seriesAddString(&copyS, strVal);
                        free(strVal);
                    }
                } break;
                case DF_DATETIME: {
                    long long dtVal;
                    if (seriesGetDateTime(s, r, &dtVal)) {
                        seriesAddDateTime(&copyS, dtVal);
                    }
                } break;
            }
        }
        dst->addSeries(dst, &copyS);
        seriesFree(&copyS);
    }
}

// ------------- 1) test dfSetValue_impl -----------------

static void testSetValue(void)
{
    printf("Running testSetValue...\n");

    DataFrame df;
    DataFrame_Create(&df);

    int colA[] = {10,20,30};
    Series sA = buildIntSeries("A", colA, 3);
    df.addSeries(&df, &sA);
    seriesFree(&sA);

    // set cell => row=1,col=0 => from 20 => let's set it to 999
    int newVal = 999;
    DataFrame updated = df.setValue(&df, 1, 0, &newVal);
    // check if updated => col0 => row1 => 999
    const Series* updCol = updated.getSeries(&updated, 0);
    int val=0;
    bool got = seriesGetInt(updCol, 1, &val);
    assert(got && val==999);

    // check row0 => remains 10
    seriesGetInt(updCol, 0, &val);
    assert(val==10);

    DataFrame_Destroy(&updated);
    DataFrame_Destroy(&df);

    printf("testSetValue passed.\n");
}

// ------------- 2) test dfSetRow_impl -----------------

static void testSetRow(void)
{
    printf("Running testSetRow...\n");

    DataFrame df;
    DataFrame_Create(&df);

    int colA[] = {1,2,3,4};
    int colB[] = {10,20,30,40};
    Series sA = buildIntSeries("A", colA, 4);
    Series sB = buildIntSeries("B", colB, 4);
    df.addSeries(&df, &sA);
    df.addSeries(&df, &sB);

    seriesFree(&sA);
    seriesFree(&sB);

    // We'll set row=2 => new values => for 2 columns => {777,888}
    int newValA = 777;
    int newValB = 888;
    const void* rowVals[2];
    rowVals[0] = &newValA; // for col0 => "A"
    rowVals[1] = &newValB; // for col1 => "B"

    DataFrame updated = df.setRow(&df, 2, rowVals, 2);
    // check => row2 => col"A"=777, col"B"=888
    {
        const Series* cA = updated.getSeries(&updated, 0);
        const Series* cB = updated.getSeries(&updated, 1);
        int vA=0, vB=0;
        bool gotA = seriesGetInt(cA, 2, &vA);
        bool gotB = seriesGetInt(cB, 2, &vB);
        assert(gotA && gotB);
        assert(vA==777);
        assert(vB==888);
    }
    // check row1 => still 2,20
    {
        const Series* cA = updated.getSeries(&updated, 0);
        int valA=0;
        seriesGetInt(cA,1,&valA);
        assert(valA==2);
    }

    DataFrame_Destroy(&updated);
    DataFrame_Destroy(&df);

    printf("testSetRow passed.\n");
}

// ------------- 3) test dfSetColumn_impl --------------

static void testSetColumn(void)
{
    printf("Running testSetColumn...\n");

    DataFrame df;
    DataFrame_Create(&df);

    int cX[] = {11,22,33};
    Series sX = buildIntSeries("X", cX, 3);
    int cY[] = {100,200,300};
    Series sY = buildIntSeries("Y", cY, 3);

    df.addSeries(&df, &sX);
    df.addSeries(&df, &sY);

    seriesFree(&sX);
    seriesFree(&sY);

    // We'll build a new column "NewY" with values => 999,999,999 => but same rowcount
    int newVals[] = {999,999,999};
    Series sNew;
    seriesInit(&sNew, "NewY", DF_INT);
    for (int i=0;i<3;i++){
        seriesAddInt(&sNew, newVals[i]);
    }

    // setColumn => oldName="Y" => newCol => sNew
    DataFrame updated = df.setColumn(&df, "Y", &sNew);
    // check => col0 => "X" unchanged => row0 => 11, col1 => "Y" data => now [999,999,999], but name => "NewY"? 
    // Actually we keep the new name => "NewY" or you can keep old name. Up to your implementation.
    // We'll assume we replaced with exactly newCol => name => "NewY".
    const Series* c0 = updated.getSeries(&updated, 0);
    const Series* c1 = updated.getSeries(&updated, 1);
    assert(strcmp(c0->name,"X")==0);
    assert(strcmp(c1->name,"NewY")==0);

    int val=0;
    bool got = seriesGetInt(c1,2,&val);
    assert(got && val==999);

    DataFrame_Destroy(&updated);
    seriesFree(&sNew);
    DataFrame_Destroy(&df);

    printf("testSetColumn passed.\n");
}

// ------------- 4) test dfRenameColumn_impl --------------

static void testRenameColumn(void)
{
    printf("Running testRenameColumn...\n");

    DataFrame df;
    DataFrame_Create(&df);

    int colA[] = {1,2,3};
    Series sA = buildIntSeries("A", colA, 3);
    df.addSeries(&df,&sA);
    seriesFree(&sA);

    int colB[] = {10,20,30};
    Series sB = buildIntSeries("B", colB, 3);
    df.addSeries(&df,&sB);
    seriesFree(&sB);

    // rename "A" => "Alpha"
    DataFrame renamed = df.renameColumn(&df, "A","Alpha");
    // check => col0 => name="Alpha", col1 => name="B"
    const Series* c0 = renamed.getSeries(&renamed, 0);
    const Series* c1 = renamed.getSeries(&renamed, 1);
    assert(strcmp(c0->name,"Alpha")==0);
    assert(strcmp(c1->name,"B")==0);

    // rename non-existing => "Bogus" => "Nope" => skip
    DataFrame skip = df.renameColumn(&df, "Bogus","Nope");
    // col0 => "A", col1=>"B"
    const Series* sc0 = skip.getSeries(&skip, 0);
    assert(strcmp(sc0->name,"A")==0);

    DataFrame_Destroy(&renamed);
    DataFrame_Destroy(&skip);
    DataFrame_Destroy(&df);

    printf("testRenameColumn passed.\n");
}

// ------------- 5) test dfReindex_impl -----------------


static void testReindex(void)
{
    printf("Running testReindex...\n");

    DataFrame df;
    DataFrame_Create(&df);

    int arr1[] = {10,20,30,40};
    Series s1 = buildIntSeries("One", arr1, 4);
    df.addSeries(&df,&s1);
    seriesFree(&s1);

    // reindex => e.g. newIndices => {0,2,5}, size=3 => row0=>0, row1=>2, row2=>5 => out-of-range => NA
    size_t newIdx[] = {0,2,5};
    DataFrame rdx = df.reindex(&df, newIdx, 3);

    // col0 => "One", row0 => 10, row1 =>30, row2 => NA(0?)
    const Series* col0 = rdx.getSeries(&rdx, 0);
    int val=0;
    bool got = seriesGetInt(col0, 1, &val);
    assert(got && val==30);
    got = seriesGetInt(col0, 2, &val);
    // row2 => old row5 => out-of-range => NA => 0 if int
    assert(got && val==0);

    DataFrame_Destroy(&rdx);
    DataFrame_Destroy(&df);

    printf("testReindex passed.\n");
}

// ------------- 6) test dfTake_impl -----------------

static void testTake(void)
{
    printf("Running testTake...\n");

    DataFrame df;
    DataFrame_Create(&df);

    int arrA[] = {10,20,30};
    Series sA = buildIntSeries("A", arrA, 3);
    df.addSeries(&df,&sA);
    seriesFree(&sA);

    // e.g. take => {2,2,0} => duplicates => row2, row2, row0
    size_t tIdx[] = {2,2,0};
    DataFrame took = df.take(&df, tIdx, 3);
    // => 1 column => "A", 3 rows => row0 => old row2 => 30, row1 => old row2 => 30, row2 => old row0 => 10
    assert(took.numColumns(&took)==1);
    assert(took.numRows(&took)==3);

    const Series* col = took.getSeries(&took,0);
    int val=0;
    bool got = seriesGetInt(col,0,&val);
    assert(got && val==30);
    seriesGetInt(col,2,&val);
    assert(val==10);

    DataFrame_Destroy(&took);
    DataFrame_Destroy(&df);

    printf("testTake passed.\n");
}

// ------------- 7) test dfReorderColumns_impl -----------


static void testReorderColumns(void)
{
    printf("Running testReorderColumns...\n");

    DataFrame df;
    DataFrame_Create(&df);

    int colA[] = {1,2,3};
    int colB[] = {4,5,6};
    int colC[] = {7,8,9};
    Series sA = buildIntSeries("A", colA, 3);
    Series sB = buildIntSeries("B", colB, 3);
    Series sC = buildIntSeries("C", colC, 3);

    df.addSeries(&df,&sA);
    df.addSeries(&df,&sB);
    df.addSeries(&df,&sC);

    seriesFree(&sA);
    seriesFree(&sB);
    seriesFree(&sC);

    // reorder => e.g. newOrder => {2,0,1} => means col2->A, col0->B, col1->C
    size_t newOrd[] = {2,0,1};
    DataFrame reordered = df.reorderColumns(&df, newOrd, 3);
    // => columns => 3 => col0 => old2 => "C", col1 => old0 => "A", col2 => old1 => "B"

    const Series* c0 = reordered.getSeries(&reordered,0);
    const Series* c1 = reordered.getSeries(&reordered,1);
    const Series* c2 = reordered.getSeries(&reordered,2);
    assert(strcmp(c0->name,"C")==0);
    assert(strcmp(c1->name,"A")==0);
    assert(strcmp(c2->name,"B")==0);

    // check row2 => c0 => old row2 => colC => 9
    int val=0;
    bool got = seriesGetInt(c0,2,&val);
    assert(got && val==9);

    DataFrame_Destroy(&reordered);
    DataFrame_Destroy(&df);

    printf("testReorderColumns passed.\n");
}

// ------------------------------------------------------------------
// Master test driver: calls each function's test
// ------------------------------------------------------------------
void testIndexing(void)
{
    printf("Running DataFrame indexing tests...\n");

    testDfAt();
    testDfIat();
    testDfLoc();
    testDfIloc();
    testDfDrop();
    testDfPop();
    testDfInsert();
    testDfIndex();
    testDfColumns();
    testSetValue();
    testSetRow();
    testSetColumn();
    testRenameColumn();
    testReindex();
    testTake();
    testReorderColumns();
    printf("All DataFrame indexing tests passed successfully!\n");
}
