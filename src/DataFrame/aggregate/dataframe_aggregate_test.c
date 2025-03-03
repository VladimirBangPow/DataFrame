#include <assert.h>
#include <stdio.h>   // for printf (minimal usage)
#include <math.h>    // for fabs, sqrt
#include <float.h>   // for DBL_MAX
#include <string.h>  // for strcmp
#include "../dataframe.h"   // DataFrame, aggregator prototypes
#include "../../Series/series.h" // for creating test Series, etc.

// A small helper to compare floating results with some tolerance
static void assertAlmostEqual(double val, double expected, double tol) {
    double diff = fabs(val - expected);
    assert(diff <= tol);
}

/* --------------------------------------------------------------------------
 * testDfSum
 * -------------------------------------------------------------------------- */
static void testDfSum(void)
{
    printf("Running testDfSum...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // Build a DF_INT column => [1, 2, 3, 4]
    Series s;
    seriesInit(&s, "Numbers", DF_INT);
    for (int i = 1; i <= 4; i++) {
        seriesAddInt(&s, i);
    }
    bool ok = df.addSeries(&df, &s);
    assert(ok);

    double sumRes = df.sum(&df, 0);
    assertAlmostEqual(sumRes, 1+2+3+4, 1e-9);

    seriesFree(&s);
    DataFrame_Destroy(&df);
    printf("testDfSum passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfMean
 * -------------------------------------------------------------------------- */
static void testDfMean(void)
{
    printf("Running testDfMean...\n");
    DataFrame df;
    DataFrame_Create(&df);

    Series s;
    seriesInit(&s, "MeanTest", DF_DOUBLE);

    // [1.0, 2.0, 3.0, 4.0]
    double arr[] = {1.0, 2.0, 3.0, 4.0};
    for (int i=0; i<4; i++){
        seriesAddDouble(&s, arr[i]);
    }
    bool ok = df.addSeries(&df, &s);
    assert(ok);

    double m = df.mean(&df, 0);
    // average = (1+2+3+4)/4 = 2.5
    assertAlmostEqual(m, 2.5, 1e-9);

    seriesFree(&s);
    DataFrame_Destroy(&df);
    printf("testDfMean passed.\n"); 
}

/* --------------------------------------------------------------------------
 * testDfMin
 * -------------------------------------------------------------------------- */
static void testDfMin(void)
{
    printf("Running testDfMin...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DOUBLE col => [10.5, 2.2, 7.7]
    Series s;
    seriesInit(&s, "MinTest", DF_DOUBLE);
    seriesAddDouble(&s, 10.5);
    seriesAddDouble(&s, 2.2);
    seriesAddDouble(&s, 7.7);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double mn = df.min(&df, 0);
    assertAlmostEqual(mn, 2.2, 1e-9);

    DataFrame_Destroy(&df);
    printf("testDfMin passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfMax
 * -------------------------------------------------------------------------- */
static void testDfMax(void)
{
    printf("Running testDfMax...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT col => [3,9,1]
    Series s;
    seriesInit(&s, "MaxTest", DF_INT);
    seriesAddInt(&s, 3);
    seriesAddInt(&s, 9);
    seriesAddInt(&s, 1);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double mx = df.max(&df, 0);
    assertAlmostEqual(mx, 9, 1e-9);

    DataFrame_Destroy(&df);
    printf("testDfMax passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfCount
 * -------------------------------------------------------------------------- */
static void testDfCount(void)
{
    printf("Running testDfCount...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // We'll do DF_STRING with 4 valid rows
    Series s;
    seriesInit(&s, "CountTest", DF_STRING);
    seriesAddString(&s, "apple");
    seriesAddString(&s, "banana");
    seriesAddString(&s, "orange");
    seriesAddString(&s, "kiwi");
    df.addSeries(&df, &s);
    seriesFree(&s);

    double c = df.count(&df, 0);
    // 4 non-null strings => count=4
    assertAlmostEqual(c, 4.0, 1e-9);

    DataFrame_Destroy(&df);
    printf("testDfCount passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfMedian
 * -------------------------------------------------------------------------- */
static void testDfMedian(void)
{
    printf("Running testDfMedian...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DOUBLE => [2, 4, 6, 8] => median = (4+6)/2=5
    Series s;
    seriesInit(&s, "MedianTest", DF_DOUBLE);
    seriesAddDouble(&s, 2.0);
    seriesAddDouble(&s, 4.0);
    seriesAddDouble(&s, 6.0);
    seriesAddDouble(&s, 8.0);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double med = df.median(&df, 0);
    assertAlmostEqual(med, 5.0, 1e-9);

    DataFrame_Destroy(&df);
    printf("testDfMedian passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfMode
 * -------------------------------------------------------------------------- */
static void testDfMode(void)
{
    printf("Running testDfMode...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [2,2,5,2,5] => mode=2 since freq(2)=3 freq(5)=2
    Series s;
    seriesInit(&s, "ModeTest", DF_INT);
    seriesAddInt(&s, 2);
    seriesAddInt(&s, 2);
    seriesAddInt(&s, 5);
    seriesAddInt(&s, 2);
    seriesAddInt(&s, 5);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double modeVal = df.mode(&df, 0);
    assertAlmostEqual(modeVal, 2.0, 1e-9);

    DataFrame_Destroy(&df);
    printf("testDfMode passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfVar
 * -------------------------------------------------------------------------- */
static void testDfVar(void)
{
    printf("Running testDfVar...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DOUBLE => [1, 2, 3, 4]
    // sample variance => 1.66666666...
    Series s;
    seriesInit(&s, "VarTest", DF_DOUBLE);
    for (int i=1; i<=4; i++){
        seriesAddDouble(&s, i);
    }
    df.addSeries(&df, &s);
    seriesFree(&s);

    double v = df.var(&df, 0);
    // population var=1.25, sample var= 1.6666667 (2 decimal=1.67)
    // 1->1,2->4,3->9,4->16 => mean=2.5 => squares ~ (1.5^2 +0.5^2+0.5^2+1.5^2)=1.5^2=2.25 => sum=5 => /3=1.6667
    assert(fabs(v - 1.6666667) < 1e-5);

    DataFrame_Destroy(&df);
    printf("testDfVar passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfStd (NEW TEST)
 * -------------------------------------------------------------------------- */
static void testDfStd(void)
{
    printf("Running testDfStd...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DOUBLE => [1,2,3,4]
    // sample standard deviation => sqrt(1.6666667) ~ 1.290994
    Series s;
    seriesInit(&s, "StdTest", DF_DOUBLE);
    seriesAddDouble(&s, 1.0);
    seriesAddDouble(&s, 2.0);
    seriesAddDouble(&s, 3.0);
    seriesAddDouble(&s, 4.0);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double stdev = df.std(&df, 0);
    // Expect ~1.290994 (since sample var=1.6667)
    assert(fabs(stdev - 1.290994) < 1e-5);

    DataFrame_Destroy(&df);
    printf("testDfStd passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfRange
 * -------------------------------------------------------------------------- */
static void testDfRange(void)
{
    printf("Running testDfRange...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [3,7,1,9] => min=1, max=9 => range=8
    Series s;
    seriesInit(&s, "RangeTest", DF_INT);
    seriesAddInt(&s, 3);
    seriesAddInt(&s, 7);
    seriesAddInt(&s, 1);
    seriesAddInt(&s, 9);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double r = df.range(&df, 0);
    assertAlmostEqual(r, 8.0, 1e-9);

    DataFrame_Destroy(&df);
    printf("testDfRange passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfQuantile
 * -------------------------------------------------------------------------- */
static void testDfQuantile(void)
{
    printf("Running testDfQuantile...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DOUBLE => [10,20,30,40]
    Series s;
    seriesInit(&s, "QuantTest", DF_DOUBLE);
    seriesAddDouble(&s, 10);
    seriesAddDouble(&s, 20);
    seriesAddDouble(&s, 30);
    seriesAddDouble(&s, 40);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double q25 = df.quantile(&df, 0, 0.25); 
    // sorted => [10,20,30,40], 0.25*(4-1)=0.75 => idxBelow=0, idxAbove=1 => interpol => 17.5
    assertAlmostEqual(q25, 17.5, 1e-9);

    double q75 = df.quantile(&df, 0, 0.75); 
    // pos=0.75*(3)=2.25 => idxBelow=2 => 30 => fraction=0.25 => next=40 => 30+0.25*(10)=32.5
    assertAlmostEqual(q75, 32.5, 1e-9);

    DataFrame_Destroy(&df);
    printf("testDfQuantile passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfIQR
 * -------------------------------------------------------------------------- */
static void testDfIQR(void)
{
    printf("Running testDfIQR...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [2,4,6,8]
    Series s;
    seriesInit(&s, "IQRTest", DF_INT);
    seriesAddInt(&s, 2);
    seriesAddInt(&s, 4);
    seriesAddInt(&s, 6);
    seriesAddInt(&s, 8);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double iqrVal = df.iqr(&df, 0);
    // Based on interpolation => q1=3.5, q3=6.5 => iqr=3
    // We'll allow small tolerance check
    assertAlmostEqual(iqrVal, 3.0, 0.1);

    DataFrame_Destroy(&df);
    printf("testDfIQR passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfNullCount
 * -------------------------------------------------------------------------- */
static void testDfNullCount(void)
{
    printf("Running testDfNullCount...\n");

    // 1) Create a new DataFrame
    DataFrame df;
    DataFrame_Create(&df);

    // 2) Build a DF_STRING column with a single valid entry ("hi")
    Series s;
    seriesInit(&s, "NullTest", DF_STRING);
    seriesAddString(&s, "hi");   // 1 row => "hi"

    bool ok = df.addSeries(&df, &s);
    seriesFree(&s);
    assert(ok);

    // 3) Prepare a "null" pointer for the second row => row2
    const char* row2 = NULL;
    const void* rowData[1];
    rowData[0] = (const void*)row2;

    // Attempt to add a second row. This should fail (strPtr == NULL => no new row).
    if (df.numColumns(&df) == 1) {
        bool added = df.addRow(&df, rowData);
        assert(!added);
    }

    // So only 1 valid row => "hi" => nullCount=0
    double nCount = df.nullCount(&df, 0);
    assert(nCount == 0.0);

    DataFrame_Destroy(&df);
    printf("testDfNullCount passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfUniqueCount
 * -------------------------------------------------------------------------- */
static void testDfUniqueCount(void)
{
    printf("Running testDfUniqueCount...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [1,2,2,3]
    Series s;
    seriesInit(&s, "UniqueCountTest", DF_INT);
    seriesAddInt(&s,1);
    seriesAddInt(&s,2);
    seriesAddInt(&s,2);
    seriesAddInt(&s,3);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double uniq = df.uniqueCount(&df, 0);
    // distinct= {1,2,3} => 3
    assert(uniq==3.0);

    DataFrame_Destroy(&df);
    printf("testDfUniqueCount passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfProduct
 * -------------------------------------------------------------------------- */
static void testDfProduct(void)
{
    printf("Running testDfProduct...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [2,3,4] => product=24
    Series s;
    seriesInit(&s, "ProdTest", DF_INT);
    seriesAddInt(&s,2);
    seriesAddInt(&s,3);
    seriesAddInt(&s,4);
    df.addSeries(&df, &s);
    seriesFree(&s);

    double prod = df.product(&df, 0);
    assert(prod==24.0);

    DataFrame_Destroy(&df);
    printf("testDfProduct passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfNthLargest
 * -------------------------------------------------------------------------- */
static void testDfNthLargest(void)
{
    printf("Running testDfNthLargest...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DOUBLE => [5, 10, 1, 9, 20]
    Series s;
    seriesInit(&s, "NthLarge", DF_DOUBLE);
    seriesAddDouble(&s,5);
    seriesAddDouble(&s,10);
    seriesAddDouble(&s,1);
    seriesAddDouble(&s,9);
    seriesAddDouble(&s,20);
    df.addSeries(&df, &s);
    seriesFree(&s);

    // sorted desc => [20,10,9,5,1]
    // nth largest(1) => 20, nth largest(3) => 9
    double l1 = df.nthLargest(&df,0,1);
    double l3 = df.nthLargest(&df,0,3);
    assert(l1==20.0);
    assert(l3==9.0);

    DataFrame_Destroy(&df);
    printf("testDfNthLargest passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfNthSmallest
 * -------------------------------------------------------------------------- */
static void testDfNthSmallest(void)
{
    printf("Running testDfNthSmallest...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [10,3,5,7]
    Series s;
    seriesInit(&s, "NthSmall", DF_INT);
    seriesAddInt(&s,10);
    seriesAddInt(&s,3);
    seriesAddInt(&s,5);
    seriesAddInt(&s,7);
    df.addSeries(&df, &s);
    seriesFree(&s);

    // sorted ascending => [3,5,7,10], so 1st => 3, 2nd => 5
    double s1 = df.nthSmallest(&df,0,1);
    double s2 = df.nthSmallest(&df,0,2);
    assert(s1==3.0);
    assert(s2==5.0);

    DataFrame_Destroy(&df);
    printf("testDfNthSmallest passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfSkewness
 * -------------------------------------------------------------------------- */
static void testDfSkewness(void)
{
    printf("Running testDfSkewness...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DOUBLE => [1,2,3,4,100] => known to have positive skew
    Series s;
    seriesInit(&s, "SkewTest", DF_DOUBLE);
    double arr[] = {1,2,3,4,100};
    for (int i=0; i<5; i++){
        seriesAddDouble(&s, arr[i]);
    }
    df.addSeries(&df, &s);
    seriesFree(&s);

    double sk = df.skewness(&df, 0);
    // We'll just check it's >0
    assert(sk>0.0);

    DataFrame_Destroy(&df);
    printf("testDfSkewness passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfKurtosis
 * -------------------------------------------------------------------------- */
static void testDfKurtosis(void)
{
    printf("Running testDfKurtosis...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DOUBLE => [1,2,3,4,100] => typically has high kurtosis
    Series s;
    seriesInit(&s, "KurtTest", DF_DOUBLE);
    double arr[] = {1,2,3,4,100};
    for (int i=0; i<5; i++){
        seriesAddDouble(&s, arr[i]);
    }
    df.addSeries(&df, &s);
    seriesFree(&s);

    double kurt = df.kurtosis(&df, 0);
    // Check it's > 0. Typically big outlier => large positive kurt
    assert(kurt>0.0);

    DataFrame_Destroy(&df);
    printf("testDfKurtosis passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfCovariance
 * -------------------------------------------------------------------------- */
static void testDfCovariance(void)
{
    printf("Running testDfCovariance...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // 2 columns => X=[1,2,3], Y=[2,4,6] => correlation=1 => covariance>0
    Series sx, sy;
    seriesInit(&sx, "CovX", DF_INT);
    seriesInit(&sy, "CovY", DF_INT);
    seriesAddInt(&sx,1);
    seriesAddInt(&sx,2);
    seriesAddInt(&sx,3);
    seriesAddInt(&sy,2);
    seriesAddInt(&sy,4);
    seriesAddInt(&sy,6);

    df.addSeries(&df, &sx);
    df.addSeries(&df, &sy);

    seriesFree(&sx);
    seriesFree(&sy);

    double cov = df.covariance(&df, 0,1);
    // Y=2X => perfect correlation => sample cov>0
    assert(cov>0.0);

    DataFrame_Destroy(&df);
    printf("testDfCovariance passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfCorrelation
 * -------------------------------------------------------------------------- */
static void testDfCorrelation(void)
{
    printf("Running testDfCorrelation...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // X=[10,20,30], Y=[20,40,60] => perfect correlation => correlation ~1
    Series sx, sy;
    seriesInit(&sx, "CorrX", DF_INT);
    seriesInit(&sy, "CorrY", DF_INT);

    seriesAddInt(&sx,10);
    seriesAddInt(&sx,20);
    seriesAddInt(&sx,30);

    seriesAddInt(&sy,20);
    seriesAddInt(&sy,40);
    seriesAddInt(&sy,60);

    df.addSeries(&df, &sx);
    df.addSeries(&df, &sy);
    seriesFree(&sx);
    seriesFree(&sy);

    double corr = df.correlation(&df, 0,1);
    // should be near 1
    assertAlmostEqual(corr,1.0,1e-5);

    DataFrame_Destroy(&df);
    printf("testDfCorrelation passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfUniqueValues
 * -------------------------------------------------------------------------- */
static void testDfUniqueValues(void)
{
    printf("Running testDfUniqueValues...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [2,2,5,7,5]
    Series s;
    seriesInit(&s, "UniqueValTest", DF_INT);
    seriesAddInt(&s,2);
    seriesAddInt(&s,2);
    seriesAddInt(&s,5);
    seriesAddInt(&s,7);
    seriesAddInt(&s,5);
    df.addSeries(&df,&s);
    seriesFree(&s);

    DataFrame uniqueDF = df.uniqueValues(&df, 0);
    // distinct => {2,5,7} => expect 3 rows
    size_t rowCount = uniqueDF.numRows(&uniqueDF);
    assert(rowCount==3);

    DataFrame_Destroy(&uniqueDF);
    DataFrame_Destroy(&df);
    printf("testDfUniqueValues passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfValueCounts
 * -------------------------------------------------------------------------- */
static void testDfValueCounts(void)
{
    printf("Running testDfValueCounts...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_STRING => ["apple", "apple", "banana"]
    Series s;
    seriesInit(&s, "VCtest", DF_STRING);
    seriesAddString(&s, "apple");
    seriesAddString(&s, "apple");
    seriesAddString(&s, "banana");
    df.addSeries(&df, &s);
    seriesFree(&s);

    DataFrame vc = df.valueCounts(&df, 0);
    // Expect 2 distinct => ("apple"(2), "banana"(1))
    size_t rowCount = vc.numRows(&vc);
    assert(rowCount==2);

    DataFrame_Destroy(&vc);
    DataFrame_Destroy(&df);
    printf("testDfValueCounts passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfCumulativeSum
 * -------------------------------------------------------------------------- */
static void testDfCumulativeSum(void)
{
    printf("Running testDfCumulativeSum...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_DOUBLE => [1.0, 2.0, 3.0]
    Series s;
    seriesInit(&s, "CumSumTest", DF_DOUBLE);
    seriesAddDouble(&s,1.0);
    seriesAddDouble(&s,2.0);
    seriesAddDouble(&s,3.0);
    df.addSeries(&df,&s);
    seriesFree(&s);

    DataFrame cs = df.cumulativeSum(&df, 0);
    // col "cumsum" => [1.0, 3.0, 6.0]
    const Series* csumCol = cs.getSeries(&cs, 0);
    assert(csumCol && csumCol->type==DF_DOUBLE);

    double v0,v1,v2;
    seriesGetDouble(csumCol, 0, &v0);
    seriesGetDouble(csumCol, 1, &v1);
    seriesGetDouble(csumCol, 2, &v2);
    assertAlmostEqual(v0,1.0,1e-9);
    assertAlmostEqual(v1,3.0,1e-9);
    assertAlmostEqual(v2,6.0,1e-9);

    DataFrame_Destroy(&cs);
    DataFrame_Destroy(&df);
    printf("testDfCumulativeSum passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfCumulativeProduct
 * -------------------------------------------------------------------------- */
static void testDfCumulativeProduct(void)
{
    printf("Running testDfCumulativeProduct...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [2,2,3]
    Series s;
    seriesInit(&s, "CumProdTest", DF_INT);
    seriesAddInt(&s,2);
    seriesAddInt(&s,2);
    seriesAddInt(&s,3);
    df.addSeries(&df,&s);
    seriesFree(&s);

    DataFrame cp = df.cumulativeProduct(&df,0);
    // expect => [2,4,12]
    const Series* cprodCol = cp.getSeries(&cp, 0);
    double v0,v1,v2;
    seriesGetDouble(cprodCol,0,&v0);
    seriesGetDouble(cprodCol,1,&v1);
    seriesGetDouble(cprodCol,2,&v2);
    assertAlmostEqual(v0,2.0,1e-9);
    assertAlmostEqual(v1,4.0,1e-9);
    assertAlmostEqual(v2,12.0,1e-9);

    DataFrame_Destroy(&cp);
    DataFrame_Destroy(&df);
    printf("testDfCumulativeProduct passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfCumulativeMax
 * -------------------------------------------------------------------------- */
static void testDfCumulativeMax(void)
{
    printf("Running testDfCumulativeMax...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [1,3,2,5]
    Series s;
    seriesInit(&s, "CumMaxTest", DF_INT);
    seriesAddInt(&s,1);
    seriesAddInt(&s,3);
    seriesAddInt(&s,2);
    seriesAddInt(&s,5);
    df.addSeries(&df,&s);
    seriesFree(&s);

    DataFrame cm = df.cumulativeMax(&df,0);
    // row0 =>1, row1=>3, row2=>3, row3=>5
    const Series* cmaxCol = cm.getSeries(&cm,0);
    double v0,v1,v2,v3;
    seriesGetDouble(cmaxCol,0,&v0);
    seriesGetDouble(cmaxCol,1,&v1);
    seriesGetDouble(cmaxCol,2,&v2);
    seriesGetDouble(cmaxCol,3,&v3);
    assertAlmostEqual(v0,1.0,1e-9);
    assertAlmostEqual(v1,3.0,1e-9);
    assertAlmostEqual(v2,3.0,1e-9);
    assertAlmostEqual(v3,5.0,1e-9);

    DataFrame_Destroy(&cm);
    DataFrame_Destroy(&df);
    printf("testDfCumulativeMax passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfCumulativeMin
 * -------------------------------------------------------------------------- */
static void testDfCumulativeMin(void)
{
    printf("Running testDfCumulativeMin...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // DF_INT => [3,2,5,1]
    Series s;
    seriesInit(&s, "CumMinTest", DF_INT);
    seriesAddInt(&s,3);
    seriesAddInt(&s,2);
    seriesAddInt(&s,5);
    seriesAddInt(&s,1);
    df.addSeries(&df,&s);
    seriesFree(&s);

    DataFrame cmi = df.cumulativeMin(&df,0);
    // row0=>3, row1=>2, row2=>2, row3=>1
    const Series* cminCol = cmi.getSeries(&cmi,0);
    double v0,v1,v2,v3;
    seriesGetDouble(cminCol,0,&v0);
    seriesGetDouble(cminCol,1,&v1);
    seriesGetDouble(cminCol,2,&v2);
    seriesGetDouble(cminCol,3,&v3);
    assertAlmostEqual(v0,3.0,1e-9);
    assertAlmostEqual(v1,2.0,1e-9);
    assertAlmostEqual(v2,2.0,1e-9);
    assertAlmostEqual(v3,1.0,1e-9);

    DataFrame_Destroy(&cmi);
    DataFrame_Destroy(&df);
    printf("testDfCumulativeMin passed.\n");
}

/* --------------------------------------------------------------------------
 * testDfGroupBy
 * -------------------------------------------------------------------------- */
static void testDfGroupBy(void)
{
    printf("Running testDfGroupBy...\n");
    DataFrame df;
    DataFrame_Create(&df);

    // We'll store DF_STRING => ["apple","banana","banana","apple"]
    // so group => "apple"(2), "banana"(2)
    Series s;
    seriesInit(&s, "Fruit", DF_STRING);
    seriesAddString(&s,"apple");
    seriesAddString(&s,"banana");
    seriesAddString(&s,"banana");
    seriesAddString(&s,"apple");
    df.addSeries(&df,&s);
    seriesFree(&s);

    DataFrame g = df.groupBy(&df,0);
    // We expect 2 rows => group => "apple", "banana"
    size_t r = g.numRows(&g);
    assert(r==2);

    DataFrame_Destroy(&g);
    DataFrame_Destroy(&df);
    printf("testDfGroupBy passed.\n");
}

/* --------------------------------------------------------------------------
 * Master aggregator test function
 * -------------------------------------------------------------------------- */
void testAggregate(void)
{
    // Call each aggregator's test in sequence:
    testDfSum();
    testDfMean();
    testDfMin();
    testDfMax();
    testDfCount();
    testDfMedian();
    testDfMode();
    testDfVar();
    testDfStd();       // <-- NEW test for standard deviation
    testDfRange();
    testDfQuantile();
    testDfIQR();
    testDfNullCount();
    testDfUniqueCount();
    testDfProduct();
    testDfNthLargest();
    testDfNthSmallest();
    testDfSkewness();
    testDfKurtosis();
    testDfCovariance();
    testDfCorrelation();
    testDfUniqueValues();
    testDfValueCounts();
    testDfCumulativeSum();
    testDfCumulativeProduct();
    testDfCumulativeMax();
    testDfCumulativeMin();
    testDfGroupBy();

    printf("All aggregator tests passed successfully!\n");
}
