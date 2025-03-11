#include <stdio.h>
#include "core_test.h"
#include "query_test.h"
#include "print_test.h"
#include "io_test.h"
#include "plot_test.h"
#include "date_test.h"
#include "indexing_test.h"
#include "combine_test.h"
#include "reshape_test.h"
#include "aggregate_test.h"

int main(void)
{
    testCore();
    testDate();
    testAggregate();
    testCombine();

    testQuery();
    testPrint();
    testPlot();
    testIndexing();
    testReshape();
    testIO();

    return 0;
}