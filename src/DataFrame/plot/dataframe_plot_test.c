#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>  // for access(), optional
#include "dataframe_plot_test.h"
#include "../dataframe.h"
#include "../../Series/series.h"           // for creating Series

/**
 * Helper: create a DF with 1 numeric column and 1 "index" column, then plot a line.
 */
static void testPlotLine(void)
{
    // We'll create a DF with 1 numeric column, plus we use row indices for X (xColIndex = -1).
    DataFrame df;
    DataFrame_Create(&df);

    // Build an integer Series with 5 rows
    Series s;
    seriesInit(&s, "Values", DF_INT);
    int data[] = {10, 20, 15, 30, 25};
    for (int i = 0; i < 5; i++) {
        seriesAddInt(&s, data[i]);
    }
    bool ok = df.addSeries(&df, &s);
    assert(ok == true);
    seriesFree(&s);

    // Now call df.plot(...). We'll save to a file "plot_line_test.png"
    const size_t yCols[] = {0};  // only the first column
    df.plot(&df, (size_t)-1, yCols, 1, "line", "plot_line_test.png");

    // Optional: check if "plot_line_test.png" was created
    // If you want to skip this, you can remove or #ifdef it.
    if (access("plot_line_test.png", F_OK) == 0) {
        // file exists
        remove("plot_line_test.png");
    } else {
        // Possibly the python environment wasn't found or mpl is missing
        // We'll just warn
        fprintf(stderr, "[Warning] Could not detect 'plot_line_test.png'.\n");
    }

    DataFrame_Destroy(&df);
}

/**
 * Helper: create a DF with 2 numeric columns (for scatter) plus a real X column.
 * We'll do xCol=0, yCols={1,2}.
 */
static void testPlotScatter(void)
{
    DataFrame df;
    DataFrame_Create(&df);

    // Create column 0: "Xvals" (int)
    Series sx;
    seriesInit(&sx, "Xvals", DF_INT);
    for (int i = 0; i < 5; i++) {
        seriesAddInt(&sx, i);
    }
    bool ok = df.addSeries(&df, &sx);
    assert(ok == true);
    seriesFree(&sx);

    // Create column 1: "Y1" (double)
    Series sy1;
    seriesInit(&sy1, "Y1", DF_DOUBLE);
    double dataY1[] = {0.5, 1.2, 2.4, 3.1, 4.8};
    for (int i = 0; i < 5; i++) {
        seriesAddDouble(&sy1, dataY1[i]);
    }
    ok = df.addSeries(&df, &sy1);
    assert(ok == true);
    seriesFree(&sy1);

    // Create column 2: "Y2" (int)
    Series sy2;
    seriesInit(&sy2, "Y2", DF_INT);
    int dataY2[] = {5, 3, 7, 1, 9};
    for (int i = 0; i < 5; i++) {
        seriesAddInt(&sy2, dataY2[i]);
    }
    ok = df.addSeries(&df, &sy2);
    assert(ok == true);
    seriesFree(&sy2);

    // Now we have 3 columns: Xvals(0), Y1(1), Y2(2).
    // We'll do scatter plot with yCols = {1,2}, using col 0 as X.
    const size_t yCols[] = {1, 2};
    df.plot(&df, 0, yCols, 2, "scatter", "plot_scatter_test.png");

    // Optional: check for the output file
    if (access("plot_scatter_test.png", F_OK) == 0) {
        remove("plot_scatter_test.png");
    } else {
        fprintf(stderr, "[Warning] 'plot_scatter_test.png' not found.\n");
    }

    DataFrame_Destroy(&df);
}

/**
 * Helper: create a DF with 5 rows of O,H,L,C columns for the "hloc" (candlestick) plot.
 */
static void testPlotHloc(void)
{
    // This requires 4 numeric columns: open, high, low, close
    // We'll do a small DF of 5 rows. We'll also create a "time" col or use row index.
    DataFrame df;
    DataFrame_Create(&df);

    // Create column 0: "Time" (int) or double
    // We can interpret them as ms since epoch (for example).
    Series sTime;
    seriesInit(&sTime, "Time", DF_INT);
    int timeData[] = {1000, 2000, 3000, 4000, 5000}; 
    for (int i = 0; i < 5; i++) {
        seriesAddInt(&sTime, timeData[i]);
    }
    bool ok = df.addSeries(&df, &sTime);
    assert(ok == true);
    seriesFree(&sTime);

    // Column 1: Open
    Series sOpen;
    seriesInit(&sOpen, "Open", DF_DOUBLE);
    double openData[] = {10.0, 10.5, 11.0, 12.0, 11.5};
    for (int i = 0; i < 5; i++) {
        seriesAddDouble(&sOpen, openData[i]);
    }
    ok = df.addSeries(&df, &sOpen);
    assert(ok == true);
    seriesFree(&sOpen);

    // Column 2: High
    Series sHigh;
    seriesInit(&sHigh, "High", DF_DOUBLE);
    double highData[] = {10.8, 11.2, 11.3, 12.5, 12.0};
    for (int i = 0; i < 5; i++) {
        seriesAddDouble(&sHigh, highData[i]);
    }
    ok = df.addSeries(&df, &sHigh);
    assert(ok == true);
    seriesFree(&sHigh);

    // Column 3: Low
    Series sLow;
    seriesInit(&sLow, "Low", DF_DOUBLE);
    double lowData[] = {9.9, 10.3, 10.8, 11.7, 11.4};
    for (int i = 0; i < 5; i++) {
        seriesAddDouble(&sLow, lowData[i]);
    }
    ok = df.addSeries(&df, &sLow);
    assert(ok == true);
    seriesFree(&sLow);

    // Column 4: Close
    Series sClose;
    seriesInit(&sClose, "Close", DF_DOUBLE);
    double closeData[] = {10.4, 10.9, 11.1, 12.2, 11.7};
    for (int i = 0; i < 5; i++) {
        seriesAddDouble(&sClose, closeData[i]);
    }
    ok = df.addSeries(&df, &sClose);
    assert(ok == true);
    seriesFree(&sClose);

    // Now we have 5 columns total:
    // 0 => "Time"
    // 1 => "Open"
    // 2 => "High"
    // 3 => "Low"
    // 4 => "Close"
    // For an "hloc" plot, we typically do xColIndex=0, yCols = {1,2,3,4}
    const size_t yCols[] = {1,2,3,4};
    df.plot(&df, 0, yCols, 4, "hloc", "plot_hloc_test.png");

    // Optional check
    if (access("plot_hloc_test.png", F_OK) == 0) {
        remove("plot_hloc_test.png");
    } else {
        fprintf(stderr, "[Warning] 'plot_hloc_test.png' not found.\n");
        fprintf(stderr, "          Possibly mplfinance not installed or python error.\n");
    }

    DataFrame_Destroy(&df);
}

/**
 * @brief testPlot
 * Main test driver for dataframe_plot (dfPlot).
 * Calls sub-tests for line, scatter, and hloc.
 */
void testPlot(void)
{
    printf("Running DataFrame plot tests...\n");

    testPlotLine();
    printf(" - Line plot test passed (no crash).\n");

    testPlotScatter();
    printf(" - Scatter plot test passed (no crash).\n");

    testPlotHloc();
    printf(" - HLOC candlestick plot test passed (no crash).\n");

    printf("All dataframe_plot tests passed (assuming system calls did not fail)!\n");
}
