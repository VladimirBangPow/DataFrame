#include <stdio.h>
#include "DataFrame/dataframe.h"

int main(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // Now all methods are set. For example:
    df.readCsv(&df, "./DataFrame/data/btcusd.csv");
    df.print(&df);
    df.head(&df, 5);
    df.describe(&df);

    // Clean up
    DataFrame_Destroy(&df);
    return 0;
}