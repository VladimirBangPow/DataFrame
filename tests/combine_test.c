#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "combine_test.h" // the header for this test suite
#include "dataframe.h"
#include "series.h"
// ------------------------------------------------------------------
// Helpers: buildIntSeries, buildStringSeries
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
// 1) Test dfConcat_impl
// ------------------------------------------------------------------
static void testConcatBasic(void)
{
    printf("Testing dfConcat_impl...\n");

    // Build top DataFrame: 2 columns => col1(int), col2(string), 3 rows
    DataFrame top;
    DataFrame_Create(&top);

    int col1_top[] = { 10, 20, 30 };
    const char* col2_top[] = { "Alpha", "Beta", "Gamma" };

    Series s1 = buildIntSeries("Numbers", col1_top, 3);
    bool ok = top.addSeries(&top, &s1);
    assert(ok);
    seriesFree(&s1);

    Series s2 = buildStringSeries("Words", col2_top, 3);
    ok = top.addSeries(&top, &s2);
    assert(ok);
    seriesFree(&s2);

    // Build bottom DataFrame: same 2 columns => same names & types, 2 rows
    DataFrame bottom;
    DataFrame_Create(&bottom);

    int col1_bot[] = { 40, 50 };
    const char* col2_bot[] = { "Delta", "Epsilon" };

    Series s3 = buildIntSeries("Numbers", col1_bot, 2);
    bottom.addSeries(&bottom, &s3);
    seriesFree(&s3);

    Series s4 = buildStringSeries("Words", col2_bot, 2);
    bottom.addSeries(&bottom, &s4);
    seriesFree(&s4);

    // Now concat
    DataFrame concatDF = top.concat(&top, &bottom);

    // Expect 5 rows, 2 columns
    assert(concatDF.numColumns(&concatDF)==2);
    assert(concatDF.numRows(&concatDF)==5);

    // Check data in "Numbers"
    const Series* numbers = concatDF.getSeries(&concatDF, 0);
    assert(strcmp(numbers->name, "Numbers")==0);
    // row0 =>10, row1 =>20, row2=>30, row3=>40, row4=>50
    for (size_t r=0; r<5; r++) {
        int val=0;
        bool g = seriesGetInt(numbers, r, &val);
        assert(g);
        int expected = (int)((r+1)*10);
        assert(val==expected); 
    }

    // Check data in "Words"
    const Series* words = concatDF.getSeries(&concatDF,1);
    assert(strcmp(words->name,"Words")==0);
    // row0=>"Alpha", row1=>"Beta", row2=>"Gamma", row3=>"Delta", row4=>"Epsilon"
    const char* expectedWords[5] = {"Alpha","Beta","Gamma","Delta","Epsilon"};
    for (size_t r=0; r<5; r++) {
        char* st=NULL;
        bool g = seriesGetString(words, r, &st);
        assert(g);
        assert(strcmp(st, expectedWords[r])==0);
        free(st);
    }

    DataFrame_Destroy(&concatDF);
    DataFrame_Destroy(&top);
    DataFrame_Destroy(&bottom);

    printf(" - dfConcat_impl test passed.\n");
}

// ------------------------------------------------------------------
// 2) Test dfMerge_impl
// ------------------------------------------------------------------
static void testMergeBasic(void)
{
    printf("Testing dfMerge_impl (inner only)...\n");

    // We'll create left DF with columns: "Key"(int), "A"(int)
    // 4 rows => Key=1,2,3,4; A=100,200,300,400
    DataFrame left;
    DataFrame_Create(&left);

    int keysLeft[] = {1,2,3,4};
    int colA[]     = {100,200,300,400};

    Series sKeyLeft = buildIntSeries("Key", keysLeft, 4);
    left.addSeries(&left, &sKeyLeft);
    seriesFree(&sKeyLeft);

    Series sA = buildIntSeries("A", colA, 4);
    left.addSeries(&left, &sA);
    seriesFree(&sA);

    // Right DF => columns: "kid"(int), "B"(string)
    // 3 rows => kid=2,3,5; B= "two", "three", "five"
    DataFrame right;
    DataFrame_Create(&right);

    int keysRight[] = {2,3,5};
    const char* colB[] = {"two","three","five"};

    Series sKid = buildIntSeries("kid", keysRight, 3);
    right.addSeries(&right, &sKid);
    seriesFree(&sKid);

    Series sB = buildStringSeries("B", colB, 3);
    right.addSeries(&right, &sB);
    seriesFree(&sB);

    // Merge => leftKey="Key", rightKey="kid"
    DataFrame merged = left.merge(&left, &right, "Key","kid");
    // We expect an inner join => matches on key=2,3 => so 2 rows
    // columns => [ Key, A, B ]
    assert(merged.numColumns(&merged)==3);
    assert(merged.numRows(&merged)==2);

    // check row0 => Key=2 => A=200 => B="two"
    // check row1 => Key=3 => A=300 => B="three"
    const Series* keyMerged = merged.getSeries(&merged,0);
    const Series* aMerged   = merged.getSeries(&merged,1);
    const Series* bMerged   = merged.getSeries(&merged,2);

    assert(strcmp(keyMerged->name,"Key")==0);
    assert(strcmp(aMerged->name,"A")==0);
    assert(strcmp(bMerged->name,"B")==0);

    // row0 => Key=2
    {
        int kv; bool g = seriesGetInt(keyMerged, 0, &kv);
        assert(g && kv==2);
        int av; g= seriesGetInt(aMerged, 0, &av);
        assert(g && av==200);
        char* st=NULL;
        g= seriesGetString(bMerged, 0, &st);
        assert(g && strcmp(st,"two")==0);
        free(st);
    }
    // row1 => Key=3
    {
        int kv; bool g = seriesGetInt(keyMerged, 1, &kv);
        assert(g && kv==3);
        int av; g= seriesGetInt(aMerged, 1, &av);
        assert(g && av==300);
        char* st=NULL;
        g= seriesGetString(bMerged, 1, &st);
        assert(g && strcmp(st,"three")==0);
        free(st);
    }

    DataFrame_Destroy(&merged);
    DataFrame_Destroy(&left);
    DataFrame_Destroy(&right);

    printf(" - dfMerge_impl test (inner) passed.\n");
}

// ------------------------------------------------------------------
// 3) Test dfJoin_impl => JOIN_INNER, JOIN_LEFT, JOIN_RIGHT
// ------------------------------------------------------------------
static void testJoinBasic(void)
{
    printf("Testing dfJoin_impl...\n");

    // We'll reuse a scenario similar to testMerge, but add a twist
    // Left => Key=1,2,3,4 ; A=100,200,300,400
    // Right => Key2=2,4,5 ; C="two","four","five" 
    // We'll do leftKeyName="Key", rightKeyName="Key2"

    DataFrame left;
    DataFrame_Create(&left);

    int keysLeft[] = {1,2,3,4};
    int colA[]     = {100,200,300,400};

    Series sKeyLeft = buildIntSeries("Key", keysLeft, 4);
    left.addSeries(&left, &sKeyLeft);
    seriesFree(&sKeyLeft);

    Series sA = buildIntSeries("A", colA, 4);
    left.addSeries(&left, &sA);
    seriesFree(&sA);

    DataFrame right;
    DataFrame_Create(&right);

    int keysRight[] = {2,4,5};
    const char* colC[] = {"two","four","five"};

    Series sKeyRight = buildIntSeries("Key2", keysRight, 3);
    right.addSeries(&right, &sKeyRight);
    seriesFree(&sKeyRight);

    Series sC = buildStringSeries("C", colC, 3);
    right.addSeries(&right, &sC);
    seriesFree(&sC);

    // a) JOIN_INNER => matches are Key=2,4 => expect 2 rows => columns => [Key, A, C]
    {
        DataFrame joined = left.join(&left, &right, "Key","Key2", JOIN_INNER);
        assert(joined.numColumns(&joined)==3);
        assert(joined.numRows(&joined)==2);

        // row0 => Key=2 => A=200 => C="two"
        // row1 => Key=4 => A=400 => C="four"
        const Series* k = joined.getSeries(&joined, 0);
        const Series* a = joined.getSeries(&joined, 1);
        const Series* c = joined.getSeries(&joined, 2);

        int kv; seriesGetInt(k, 0, &kv); assert(kv==2);
        int av; seriesGetInt(a, 0, &av); assert(av==200);
        char* st=NULL; seriesGetString(c, 0, &st); assert(strcmp(st,"two")==0); free(st);

        seriesGetInt(k,1,&kv); assert(kv==4);
        seriesGetInt(a,1,&av); assert(av==400);
        seriesGetString(c,1,&st); assert(strcmp(st,"four")==0); free(st);

        DataFrame_Destroy(&joined);
    }

    // b) JOIN_LEFT => keep unmatched left => Key=1,3 => those rows => right columns => "NA"
    {
        DataFrame joined = left.join(&left, &right, "Key","Key2", JOIN_LEFT);
        // matched => Key=2,4 => 2 rows
        // unmatched => Key=1,3 => 2 rows => total 4 rows
        // columns => [Key,A,C]
        assert(joined.numColumns(&joined)==3);
        assert(joined.numRows(&joined)==4);

        const Series* k = joined.getSeries(&joined,0);
        const Series* a = joined.getSeries(&joined,1);
        const Series* c = joined.getSeries(&joined,2);

        // row0 => key=1 => A=100 => c="NA"
        {
            int kv; bool g= seriesGetInt(k, 0, &kv);
            assert(g && kv==1);
            int av; g= seriesGetInt(a,0,&av);
            assert(g && av==100);
            char* st=NULL;
            g= seriesGetString(c,0,&st);
            assert(g && strcmp(st,"NA")==0);
            free(st);
        }
        // row1 => key=2 => c="two"
        {
            int kv; seriesGetInt(k,1,&kv);
            assert(kv==2);
            int av; seriesGetInt(a,1,&av);
            assert(av==200);
            char* st=NULL; seriesGetString(c,1,&st);
            assert(strcmp(st,"two")==0);
            free(st);
        }
        // row2 => key=3 => c="NA"
        {
            int kv; seriesGetInt(k,2,&kv);
            assert(kv==3);
            int av; seriesGetInt(a,2,&av);
            assert(av==300);
            char* st=NULL; seriesGetString(c,2,&st);
            assert(strcmp(st,"NA")==0);
            free(st);
        }
        // row3 => key=4 => c="four"
        {
            int kv; seriesGetInt(k,3,&kv);
            assert(kv==4);
            int av; seriesGetInt(a,3,&av);
            assert(av==400);
            char* st=NULL; seriesGetString(c,3,&st);
            assert(strcmp(st,"four")==0);
            free(st);
        }

        DataFrame_Destroy(&joined);
    }

    // c) JOIN_RIGHT => keep unmatched right => Key2=5 => that row => left columns => "NA"
    {
        DataFrame joined = left.join(&left, &right, "Key","Key2", JOIN_RIGHT);
        // matched => Key=2,4 => 2 rows
        // unmatched => Key2=5 => 1 row => total 3 rows
        // columns => [Key,A,C]
        assert(joined.numColumns(&joined)==3);
        assert(joined.numRows(&joined)==3);

        const Series* k = joined.getSeries(&joined, 0);
        const Series* a = joined.getSeries(&joined, 1);
        const Series* c = joined.getSeries(&joined, 2);

        // row0 => key=2 => a=200 => c="two"
        {
            int kv; bool g= seriesGetInt(k,0,&kv);
            assert(g && kv==2);
            int av; g= seriesGetInt(a,0,&av);
            assert(g && av==200);
            char* st=NULL; g= seriesGetString(c,0,&st);
            assert(g && strcmp(st,"two")==0);
            free(st);
        }
        // row1 => key=4 => a=400 => c="four"
        {
            int kv; bool g= seriesGetInt(k,1,&kv);
            assert(g && kv==4);
            int av; g= seriesGetInt(a,1,&av);
            assert(g && av==400);
            char* st=NULL; g= seriesGetString(c,1,&st);
            assert(g && strcmp(st,"four")==0);
            free(st);
        }
        // row2 => Key=0 => A=0 => c="five" 
        {
            int kv; bool g= seriesGetInt(k,2,&kv);
            assert(g && kv==0);  // we store int "NA" as 0
            int av; g= seriesGetInt(a,2,&av);
            assert(g && av==0);
            char* st=NULL; g= seriesGetString(c,2,&st);
            assert(g && strcmp(st,"five")==0);
            free(st);
        }

        DataFrame_Destroy(&joined);
    }

    DataFrame_Destroy(&left);
    DataFrame_Destroy(&right);

    printf(" - dfJoin_impl tests (INNER,LEFT,RIGHT) passed.\n");
}


static void testUnion(void)
{
    printf("Testing dfUnion_impl...\n");

    // We'll create 2 DataFrames with 1 column => "Val" (int).
    // dfA => [1,2,2], dfB => [2,3]
    // Union => distinct => [1,2,3]
    DataFrame dfA;
    DataFrame_Create(&dfA);
    int arrA[] = {1,2,2};
    Series sA = buildIntSeries("Val", arrA, 3);
    dfA.addSeries(&dfA, &sA);
    seriesFree(&sA);

    DataFrame dfB;
    DataFrame_Create(&dfB);
    int arrB[] = {2,3};
    Series sB = buildIntSeries("Val", arrB, 2);
    dfB.addSeries(&dfB, &sB);
    seriesFree(&sB);

    // union => [1,2,3]
    DataFrame un = dfA.unionDF(&dfA, &dfB);
    // expect 1 col, 3 rows => distinct => 1,2,3
    assert(un.numColumns(&un)==1);
    assert(un.numRows(&un)==3);

    // check that the set is {1,2,3}
    // we won't check order strictly, but let's read them:
    bool found1=false, found2=false, found3=false;
    const Series* sU = un.getSeries(&un,0);
    size_t nr = un.numRows(&un);
    for (size_t r=0; r< nr; r++){
        int v; seriesGetInt(sU, r, &v);
        if (v==1) found1=true;
        if (v==2) found2=true;
        if (v==3) found3=true;
    }
    assert(found1 && found2 && found3);

    DataFrame_Destroy(&un);
    DataFrame_Destroy(&dfA);
    DataFrame_Destroy(&dfB);

    printf(" - dfUnion_impl test passed.\n");
}

static void testIntersection(void)
{
    printf("Testing dfIntersection_impl...\n");

    // dfA => [2,2,3,4]
    // dfB => [2,4,4,5]
    // intersection => {2,4} (unique rows wise)
    DataFrame dfA;
    DataFrame_Create(&dfA);
    int arrA[] = {2,2,3,4};
    Series sA = buildIntSeries("Num", arrA, 4);
    dfA.addSeries(&dfA, &sA);
    seriesFree(&sA);

    DataFrame dfB;
    DataFrame_Create(&dfB);
    int arrB[] = {2,4,4,5};
    Series sB = buildIntSeries("Num", arrB, 4);
    dfB.addSeries(&dfB, &sB);
    seriesFree(&sB);

    DataFrame inter = dfA.intersectionDF(&dfA, &dfB);
    // expect {2,4} => 2 distinct rows
    assert(inter.numColumns(&inter)==1);
    size_t nr= inter.numRows(&inter);
    // might have duplicates if implemented literally. If you do a "drop duplicates" approach, expect 2. 
    // We'll assume your code does set-like intersection => 2 unique rows.

    assert(nr==2);

    const Series* sI = inter.getSeries(&inter,0);
    bool found2=false, found4=false;
    for (size_t r=0; r< nr; r++){
        int v=0;
        seriesGetInt(sI, r, &v);
        if (v==2) found2=true;
        if (v==4) found4=true;
    }
    assert(found2 && found4);

    DataFrame_Destroy(&inter);
    DataFrame_Destroy(&dfA);
    DataFrame_Destroy(&dfB);

    printf(" - dfIntersection_impl test passed.\n");
}

static void testDifference(void)
{
    printf("Testing dfDifference_impl...\n");

    // dfA => [1,2,3]
    // dfB => [2,4]
    // difference => {1,3}  ( i.e. A\B )
    DataFrame dfA;
    DataFrame_Create(&dfA);
    int arrA[] = {1,2,3};
    Series sA = buildIntSeries("Val", arrA, 3);
    dfA.addSeries(&dfA, &sA);
    seriesFree(&sA);

    DataFrame dfB;
    DataFrame_Create(&dfB);
    int arrB[] = {2,4};
    Series sB = buildIntSeries("Val", arrB, 2);
    dfB.addSeries(&dfB, &sB);
    seriesFree(&sB);

    DataFrame diff = dfA.differenceDF(&dfA, &dfB);
    // expect [1,3]
    assert(diff.numColumns(&diff)==1);
    size_t nr= diff.numRows(&diff);
    // might be 2 rows => val=1, val=3
    assert(nr==2);

    const Series* sD = diff.getSeries(&diff,0);
    bool found1=false, found3=false;
    for (size_t r=0; r<nr; r++){
        int v=0;
        seriesGetInt(sD, r, &v);
        if (v==1) found1=true;
        if (v==3) found3=true;
    }
    assert(found1 && found3);

    DataFrame_Destroy(&diff);
    DataFrame_Destroy(&dfA);
    DataFrame_Destroy(&dfB);

    printf(" - dfDifference_impl test passed.\n");
}

static void testSemiJoin(void)
{
    printf("Testing dfSemiJoin_impl...\n");
    // left => Key=[1,2,3], left => colX=[10,20,30]
    // right => Key2=[2,4], colY= "two","four"
    // semiJoin(leftKey="Key", rightKey="Key2") => keep left rows that match
    // => matches only Key=2 => row => Key=2 => colX=20
    DataFrame left;
    DataFrame_Create(&left);

    int keyA[] = {1,2,3};
    int colX[] = {10,20,30};
    Series sKeyA = buildIntSeries("Key", keyA, 3);
    left.addSeries(&left, &sKeyA);
    seriesFree(&sKeyA);
    Series sXA = buildIntSeries("X", colX, 3);
    left.addSeries(&left, &sXA);
    seriesFree(&sXA);

    DataFrame right;
    DataFrame_Create(&right);

    int keyB[] = {2,4};
    const char* colY[] = {"two","four"};
    Series sKeyB = buildIntSeries("Key2", keyB, 2);
    right.addSeries(&right, &sKeyB);
    seriesFree(&sKeyB);
    Series sYB = buildStringSeries("Y", colY, 2);
    right.addSeries(&right, &sYB);
    seriesFree(&sYB);

    // semiJoin => left->semiJoin(leftKey="Key", rightKey="Key2")
    DataFrame semi = left.semiJoin(&left, &right, "Key","Key2");
    // expect 1 row => Key=2, X=20
    assert(semi.numColumns(&semi)==2);
    assert(semi.numRows(&semi)==1);

    const Series* k = semi.getSeries(&semi,0);
    const Series* x = semi.getSeries(&semi,1);

    int kv=0; seriesGetInt(k,0,&kv);
    assert(kv==2);
    int xv=0; seriesGetInt(x,0,&xv);
    assert(xv==20);

    DataFrame_Destroy(&semi);
    DataFrame_Destroy(&right);
    DataFrame_Destroy(&left);

    printf(" - dfSemiJoin_impl test passed.\n");
}

static void testAntiJoin(void)
{
    printf("Testing dfAntiJoin_impl...\n");
    // left => Key=[1,2,3], colX=[10,20,30]
    // right => Key2=[2,4], colY= ...
    // antiJoin => keep left rows that DO NOT match => Key=1,3 => 2 rows
    DataFrame left;
    DataFrame_Create(&left);

    int keyA[] = {1,2,3};
    int colX[] = {10,20,30};
    Series sKeyA = buildIntSeries("Key", keyA, 3);
    left.addSeries(&left, &sKeyA);
    seriesFree(&sKeyA);
    Series sXA = buildIntSeries("X", colX, 3);
    left.addSeries(&left, &sXA);
    seriesFree(&sXA);

    DataFrame right;
    DataFrame_Create(&right);
    int keyB[] = {2,4};
    Series sKeyB = buildIntSeries("Key2", keyB, 2);
    right.addSeries(&right, &sKeyB);
    seriesFree(&sKeyB);

    // do the antiJoin
    DataFrame anti = left.antiJoin(&left, &right, "Key","Key2");
    // expected => 2 rows => Key=1 => colX=10, Key=3 => colX=30
    assert(anti.numColumns(&anti)==2);
    assert(anti.numRows(&anti)==2);

    const Series* k = anti.getSeries(&anti,0);
    const Series* x = anti.getSeries(&anti,1);

    // row0 => Key=1 => X=10
    {
        int kv; bool g= seriesGetInt(k,0,&kv);
        assert(g && kv==1);
        int xv; g= seriesGetInt(x,0,&xv);
        assert(g && xv==10);
    }
    // row1 => Key=3 => X=30
    {
        int kv; bool g= seriesGetInt(k,1,&kv);
        assert(g && kv==3);
        int xv; g= seriesGetInt(x,1,&xv);
        assert(g && xv==30);
    }

    DataFrame_Destroy(&anti);
    DataFrame_Destroy(&right);
    DataFrame_Destroy(&left);

    printf(" - dfAntiJoin_impl test passed.\n");
}

static void testCrossJoin(void)
{
    printf("Testing dfCrossJoin_impl...\n");

    // 1) Create a "left" DataFrame with 1 column => "L" = [1,2]
    DataFrame left;
    DataFrame_Create(&left);

    int leftVals[] = {1,2};
    Series sLeft;
    seriesInit(&sLeft, "L", DF_INT);
    seriesAddInt(&sLeft, leftVals[0]);
    seriesAddInt(&sLeft, leftVals[1]);
    left.addSeries(&left, &sLeft);
    seriesFree(&sLeft);

    // 2) Create a "right" DataFrame with 1 column => "R" = [10,20,30]
    DataFrame right;
    DataFrame_Create(&right);

    int rightVals[] = {10,20,30};
    Series sRight;
    seriesInit(&sRight, "R", DF_INT);
    for (int i = 0; i < 3; i++) {
        seriesAddInt(&sRight, rightVals[i]);
    }
    right.addSeries(&right, &sRight);
    seriesFree(&sRight);

    // 3) Call crossJoin => expect (2 * 3) = 6 rows
    DataFrame cross = left.crossJoin(&left, &right);
    // We expect 2 columns => "L" and "R"
    assert(cross.numColumns(&cross) == 2);

    // Should produce 6 rows
    size_t nRows = cross.numRows(&cross);
    assert(nRows == 6);

    // 4) Retrieve the Series => "L" is col0, "R" is col1
    const Series* colL = cross.getSeries(&cross, 0);
    const Series* colR = cross.getSeries(&cross, 1);
    assert(strcmp(colL->name, "L") == 0);
    assert(strcmp(colR->name, "R") == 0);

    // 5) Check the values row-by-row
    // Typically, cross join goes in row-major order:
    //   left row0 => 1 joined with right row0 => 10
    //   left row0 => 1 joined with right row1 => 20
    //   left row0 => 1 joined with right row2 => 30
    //   left row1 => 2 joined with right row0 => 10
    //   left row1 => 2 joined with right row1 => 20
    //   left row1 => 2 joined with right row2 => 30
    // So we expect (L,R): 
    //   (1,10), (1,20), (1,30), (2,10), (2,20), (2,30)

    int expectL[6] = {1,1,1,2,2,2};
    int expectR[6] = {10,20,30,10,20,30};

    for (size_t i = 0; i < 6; i++) {
        int lv, rv;
        bool gotL = seriesGetInt(colL, i, &lv);
        bool gotR = seriesGetInt(colR, i, &rv);
        assert(gotL && gotR);
        assert(lv == expectL[i]);
        assert(rv == expectR[i]);
    }

    DataFrame_Destroy(&cross);
    DataFrame_Destroy(&left);
    DataFrame_Destroy(&right);

    printf(" - dfCrossJoin_impl test passed.\n");
}

// ------------------------------------------------------------------
// Main test driver for combine: concat, merge, join, + new functions
// ------------------------------------------------------------------
void testCombine(void)
{
    printf("Running DataFrame combine tests...\n");

    // existing
    testConcatBasic();
    testMergeBasic();
    testJoinBasic();
    testUnion();
    testIntersection();
    testDifference();
    testSemiJoin();
    testAntiJoin();
    testCrossJoin();
    printf("All DataFrame combine tests passed successfully!\n");
}
