#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dataframe_combine_test.h" // the header for this test suite
#include "../dataframe.h"           // your DataFrame struct, function pointers, JoinType, etc.
#include "../../Series/series.h"    // for creating Series

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

        // We'll check them in order: 
        //   typically the matched rows appear in the same order as left => so we get Key=1,2,3,4 in final
        // row0 => Key=1 => A=100 => C="NA"  (unmatched)
        // row1 => Key=2 => A=200 => C="two" (matched)
        // row2 => Key=3 => A=300 => C="NA"  (unmatched)
        // row3 => Key=4 => A=400 => C="four" (matched)

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
        // But we have to store "NA" for left columns if not matched. 
        // The matched rows appear in order of right? Actually depends on your implementation. 
        // We'll guess => Key2=2 => Key=2 => row0, Key2=4=>Key=4 => row1, Key2=5 => Key= "NA" => row2
        // But you might put them in the order left->right or the opposite. We'll check carefully.

        assert(joined.numColumns(&joined)==3);
        // we expect 3 rows
        assert(joined.numRows(&joined)==3);

        const Series* k = joined.getSeries(&joined, 0);
        const Series* a = joined.getSeries(&joined, 1);
        const Series* c = joined.getSeries(&joined, 2);

        // Typically row0 => key=2 => A=200 => C="two"
        // row1 => key=4 => A=400 => C="four"
        // row2 => key=NA => A=NA => C="five" (unmatched right row)
        {
            // row0 => key=2 => a=200 => c="two"
            int kv; bool g= seriesGetInt(k,0,&kv);
            assert(g && kv==2);
            int av; g= seriesGetInt(a,0,&av);
            assert(g && av==200);
            char* st=NULL; g= seriesGetString(c,0,&st);
            assert(g && strcmp(st,"two")==0);
            free(st);
        }
        {
            // row1 => key=4 => a=400 => c="four"
            int kv; bool g= seriesGetInt(k,1,&kv);
            assert(g && kv==4);
            int av; g= seriesGetInt(a,1,&av);
            assert(g && av==400);
            char* st=NULL; g= seriesGetString(c,1,&st);
            assert(g && strcmp(st,"four")==0);
            free(st);
        }
        {
            // row2 => key=0 => a=0 => c="five"
            // or if your code sets the string to "NA" for key col
            // we used "NA" for strings, 0 for ints => let's see:
            int kv; bool g= seriesGetInt(k,2,&kv);
            // it's "NA" in a string column => but key is int => we do 0 => so kv=0
            assert(g && kv==0);
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

// ------------------------------------------------------------------
// Main test driver for combine: concat, merge, join
// ------------------------------------------------------------------
void testCombine(void)
{
    printf("Running DataFrame combine tests...\n");

    testConcatBasic();
    testMergeBasic();
    testJoinBasic();

    printf("All DataFrame combine tests passed successfully!\n");
}
