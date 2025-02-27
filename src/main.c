#include <stdio.h>
#include "DataFrame/dataframe.h"
#include "./DataFrame/core/dataframe_core_test.h"
#include "./DataFrame/query/dataframe_query_test.h"
#include "./DataFrame/print/dataframe_print_test.h"
#include "./DataFrame/csv/dataframe_csv_test.h"
#include "./DataFrame/plot/dataframe_plot_test.h"
#include "./DataFrame/date/dataframe_date_test.h"
#include "./DataFrame/indexing/dataframe_indexing_test.h"
#include "./DataFrame/combine/dataframe_combine_test.h"
int main(void)
{
    testCore();
    testQuery();
    testPrint();
    testCsv();
    testDate();
    testPlot();
    testIndexing();
    testCombine();
    return 0;
}