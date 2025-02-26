#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "dataframe.h"

/* -------------------------------------------------------------------------
 * Forward-declared static helper
 * ------------------------------------------------------------------------- */
static bool getNumericValue(const Series* s, size_t index, double* outVal);

/* -------------------------------------------------------------------------
 * The "public" function to be assigned in dataframe_core.c
 * ------------------------------------------------------------------------- */
void dfPlot_impl(
    const DataFrame* df,
    size_t xColIndex,
    const size_t* yColIndices,
    size_t yCount,
    const char* plotType,
    const char* outputFile
)
{
    if (!df) {
        fprintf(stderr, "dfPlot_impl Error: DataFrame is NULL.\n");
        return;
    }
    size_t nRows = df->numRows(df);
    size_t nCols = df->numColumns(df);
    if (nRows == 0 || nCols == 0) {
        fprintf(stderr, "dfPlot_impl Error: DataFrame is empty.\n");
        return;
    }
    if (!yColIndices || yCount == 0) {
        fprintf(stderr, "dfPlot_impl Error: Must provide at least one y column.\n");
        return;
    }
    if (!plotType) {
        plotType = "line"; // default
    }

    // xColIndex == (size_t)-1 => use row index for X
    bool useIndexAsX = (xColIndex == (size_t)-1);

    // Validate Y columns => must be numeric
    for (size_t i = 0; i < yCount; i++) {
        const Series* sy = df->getSeries(df, yColIndices[i]);
        if (!sy) {
            fprintf(stderr, "dfPlot_impl Error: Invalid yCol index %zu.\n", yColIndices[i]);
            return;
        }
        if (sy->type != DF_INT && sy->type != DF_DOUBLE) {
            fprintf(stderr, 
                "dfPlot_impl Error: yCol '%s' is not numeric.\n",
                sy->name);
            return;
        }
    }

    // If using a real column as X, check numeric
    const Series* sx = NULL;
    if (!useIndexAsX) {
        sx = df->getSeries(df, xColIndex);
        if (!sx) {
            fprintf(stderr, "dfPlot_impl Error: Invalid xCol index %zu.\n", xColIndex);
            return;
        }
        if (sx->type != DF_INT && sx->type != DF_DOUBLE) {
            fprintf(stderr, 
                "dfPlot_impl Error: xCol '%s' is not numeric.\n",
                sx->name);
            return;
        }
    }

    // Create a temp Python script
    const char* pyFilename = "temp_plot.py";
    FILE* pyFile = fopen(pyFilename, "w");
    if (!pyFile) {
        fprintf(stderr, "dfPlot_impl Error: Unable to open '%s'.\n", pyFilename);
        return;
    }

    fprintf(pyFile, "import matplotlib.pyplot as plt\n");
    fprintf(pyFile, "import sys\n\n");

    // Build X array
    if (useIndexAsX) {
        fprintf(pyFile, "x = [");
        for (size_t r = 0; r < nRows; r++) {
            fprintf(pyFile, "%zu", r);
            if (r < nRows - 1) fprintf(pyFile, ", ");
        }
        fprintf(pyFile, "]\n");
    } else {
        fprintf(pyFile, "x = [");
        for (size_t r = 0; r < nRows; r++) {
            double val = 0.0;
            getNumericValue(sx, r, &val);
            fprintf(pyFile, "%g", val);
            if (r < nRows - 1) fprintf(pyFile, ", ");
        }
        fprintf(pyFile, "]\n");
    }

    // Build each Y array
    for (size_t i = 0; i < yCount; i++) {
        const Series* s = df->getSeries(df, yColIndices[i]);
        fprintf(pyFile, "y%zu = [", i);
        for (size_t r = 0; r < nRows; r++) {
            double val = 0.0;
            getNumericValue(s, r, &val);
            fprintf(pyFile, "%g", val);
            if (r < nRows - 1) fprintf(pyFile, ", ");
        }
        fprintf(pyFile, "]\n");
    }

    // Decide plot type
    if (strcmp(plotType, "scatter") == 0) {
        // scatter
        for (size_t i = 0; i < yCount; i++) {
            const Series* s = df->getSeries(df, yColIndices[i]);
            fprintf(pyFile, 
                "plt.scatter(x, y%zu, label=\"%s\")\n",
                i, s->name);
        }
        fprintf(pyFile, 
            "plt.xlabel(\"%s\")\n",
            useIndexAsX ? "Index" : sx->name);
        fprintf(pyFile, "plt.ylabel(\"Value\")\n");
        fprintf(pyFile, "plt.title(\"DataFrame Scatter Plot\")\n");
        fprintf(pyFile, "plt.legend()\n");
    }
    else if (strcmp(plotType, "hloc") == 0) {
        // hloc => must have 4 columns: O, H, L, C
        if (yCount != 4) {
            fprintf(stderr, 
                "dfPlot_impl Error: 'hloc' requires exactly 4 y columns.\n");
            fclose(pyFile);
            remove(pyFilename);
            return;
        }
        // We use mplfinance
        fprintf(pyFile, "import mplfinance as mpf\n");
        fprintf(pyFile, "import pandas as pd\n\n");
        fprintf(pyFile, "candleData = []\n");
        fprintf(pyFile, "for i in range(len(x)):\n");
        fprintf(pyFile, "    candleData.append((x[i], y0[i], y1[i], y2[i], y3[i]))\n\n");
        fprintf(pyFile, 
            "df_data = pd.DataFrame(candleData, "
            "columns=['time','Open','High','Low','Close'])\n");
        fprintf(pyFile, 
            "# If you want to interpret 'x' as epoch ms, uncomment next line:\n");
        fprintf(pyFile, 
            "df_data['time'] = pd.to_datetime(df_data['time'], unit='ms')\n");
        fprintf(pyFile, 
            "df_data.set_index('time', inplace=True)\n");
        fprintf(pyFile, 
            "mpf.plot(df_data, type='candle', style='charles', title='HLOC Candlestick')\n");
    }
    else {
        // line plot
        for (size_t i = 0; i < yCount; i++) {
            const Series* s = df->getSeries(df, yColIndices[i]);
            fprintf(pyFile, 
                "plt.plot(x, y%zu, label=\"%s\")\n",
                i, s->name);
        }
        fprintf(pyFile, 
            "plt.xlabel(\"%s\")\n",
            useIndexAsX ? "Index" : sx->name);
        fprintf(pyFile, "plt.ylabel(\"Value\")\n");
        fprintf(pyFile, "plt.title(\"DataFrame Line Plot\")\n");
        fprintf(pyFile, "plt.legend()\n");
    }

    // Save or show
    if (outputFile && strlen(outputFile) > 0) {
        fprintf(pyFile, "plt.savefig(\"%s\")\n", outputFile);
        fprintf(pyFile, "print(\"Plot saved to %s\")\n", outputFile);
    } else {
        fprintf(pyFile, "plt.show()\n");
    }
    fclose(pyFile);

    // Run the script
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "python3 \"%s\"", pyFilename);
    int ret = system(cmd);
    if (ret != 0) {
        fprintf(stderr, 
            "dfPlot_impl Warning: system(\"%s\") returned %d.\n", 
            cmd, ret);
    }

    // Remove temp file
    remove(pyFilename);
}

/* -------------------------------------------------------------------------
 * static helper to read numeric column values
 * ------------------------------------------------------------------------- */
static bool getNumericValue(const Series* s, size_t index, double* outVal)
{
    if (!s || !outVal) return false;
    if (index >= seriesSize(s)) return false;

    if (s->type == DF_INT) {
        int temp = 0;
        if (!seriesGetInt(s, index, &temp)) return false;
        *outVal = (double)temp;
        return true;
    }
    if (s->type == DF_DOUBLE) {
        double temp = 0.0;
        if (!seriesGetDouble(s, index, &temp)) return false;
        *outVal = temp;
        return true;
    }
    // not numeric
    return false;
}
