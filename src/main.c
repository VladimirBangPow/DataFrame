#include <stdio.h>
#include "DataFrame/dataframe.h"
#include "./DataFrame/core/dataframe_core_test.h"
#include "./DataFrame/query/dataframe_query_test.h"
#include "./DataFrame/print/dataframe_print_test.h"
int main(void)
{
    testCore();
    testQuery();
    testPrint();
    return 0;
}