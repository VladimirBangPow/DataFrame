#include <stdio.h>
#include <stdlib.h>
#include "dataframe.h"

void dfHead_impl(const DataFrame* df, size_t n)
{
    if (!df) return;
    printf("==== dfHead(%zu) ====\n", n);
    size_t numRows = df->numRows(df);
    size_t limit = (n < numRows) ? n : numRows;

    printf("(Showing first %zu of %zu rows)\n", limit, numRows);
    for (size_t r = 0; r < limit; r++) {
        printf("Row %zu: ", r);
        size_t nCols = df->numColumns(df);
        for (size_t c = 0; c < nCols; c++) {
            const Series* s = df->getSeries(df, c);
            if (!s) continue;
            switch (s->type) {
                case DF_INT: {
                    int val;
                    seriesGetInt(s, r, &val);
                    printf("%s=%d ", s->name, val);
                } break;
                case DF_DOUBLE: {
                    double val;
                    seriesGetDouble(s, r, &val);
                    printf("%s=%.3f ", s->name, val);
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(s, r, &str)) {
                        printf("%s=\"%s\" ", s->name, str);
                        free(str);
                    }
                } break;
            }
        }
        printf("\n");
    }
}

void dfTail_impl(const DataFrame* df, size_t n)
{
    if (!df) return;
    printf("==== dfTail(%zu) ====\n", n);
    size_t numRows = df->numRows(df);
    if (n > numRows) n = numRows;
    size_t start = (numRows > n) ? (numRows - n) : 0;

    printf("(Showing last %zu of %zu rows)\n", n, numRows);
    for (size_t r = start; r < numRows; r++) {
        printf("Row %zu: ", r);
        size_t nCols = df->numColumns(df);
        for (size_t c = 0; c < nCols; c++) {
            const Series* s = df->getSeries(df, c);
            if (!s) continue;
            switch (s->type) {
                case DF_INT: {
                    int val;
                    seriesGetInt(s, r, &val);
                    printf("%s=%d ", s->name, val);
                } break;
                case DF_DOUBLE: {
                    double val;
                    seriesGetDouble(s, r, &val);
                    printf("%s=%.3f ", s->name, val);
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(s, r, &str)) {
                        printf("%s=\"%s\" ", s->name, str);
                        free(str);
                    }
                } break;
            }
        }
        printf("\n");
    }
}

void dfDescribe_impl(const DataFrame* df)
{
    if (!df) return;
    printf("==== dfDescribe() ====\n");

    size_t nCols = df->numColumns(df);
    size_t nRows = df->numRows(df);

    // For each numeric column, compute min, max, etc.
    for (size_t c = 0; c < nCols; c++) {
        const Series* s = df->getSeries(df, c);
        if (!s) continue;

        if (s->type == DF_INT) {
            if (nRows == 0) {
                printf("Column '%s': no data.\n", s->name);
                continue;
            }
            int minVal, maxVal, tmp;
            seriesGetInt(s, 0, &minVal);
            maxVal = minVal;
            double sumVal = minVal;

            for (size_t r = 1; r < nRows; r++) {
                seriesGetInt(s, r, &tmp);
                if (tmp < minVal) minVal = tmp;
                if (tmp > maxVal) maxVal = tmp;
                sumVal += tmp;
            }
            double meanVal = sumVal / nRows;
            printf("Column '%s' (int): count=%zu, min=%d, max=%d, mean=%.3f\n",
                   s->name, nRows, minVal, maxVal, meanVal);
        }
        else if (s->type == DF_DOUBLE) {
            if (nRows == 0) {
                printf("Column '%s': no data.\n", s->name);
                continue;
            }
            double minVal, maxVal, tmp;
            seriesGetDouble(s, 0, &minVal);
            maxVal = minVal;
            double sumVal = minVal;

            for (size_t r = 1; r < nRows; r++) {
                seriesGetDouble(s, r, &tmp);
                if (tmp < minVal) minVal = tmp;
                if (tmp > maxVal) maxVal = tmp;
                sumVal += tmp;
            }
            double meanVal = sumVal / nRows;
            printf("Column '%s' (double): count=%zu, min=%f, max=%f, mean=%.3f\n",
                   s->name, nRows, minVal, maxVal, meanVal);
        }
        else if (s->type == DF_STRING) {
            printf("Column '%s' (string): count=%zu\n", s->name, nRows);
        }
    }
}
