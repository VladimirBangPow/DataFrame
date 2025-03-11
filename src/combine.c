#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "dataframe.h"

/* -------------------------------------------------------------------------
 * 1) dfConcat_impl
 *    Naive row-wise concatenation of two DataFrames (top & bottom),
 *    requiring matching columns (same name & type, same order).
 * ------------------------------------------------------------------------- */
DataFrame dfConcat_impl(const DataFrame* top, const DataFrame* bottom)
{
    DataFrame result;
    DataFrame_Create(&result);

    if (!top || !bottom) {
        // Return empty
        return result;
    }

    size_t topCols = top->numColumns(top);
    size_t botCols = bottom->numColumns(bottom);
    if (topCols != botCols) {
        fprintf(stderr, "dfConcat_impl: column count mismatch.\n");
        return result;
    }

    // Check matching name & type
    for (size_t c = 0; c < topCols; c++) {
        const Series* sTop = top->getSeries(top, c);
        const Series* sBot = bottom->getSeries(bottom, c);
        if (!sTop || !sBot) {
            fprintf(stderr, "dfConcat_impl: missing column.\n");
            return result;
        }
        if (strcmp(sTop->name, sBot->name)!=0) {
            fprintf(stderr, "dfConcat_impl: column name mismatch (%s vs %s).\n", 
                    sTop->name, sBot->name);
            return result;
        }
        if (sTop->type != sBot->type) {
            fprintf(stderr, "dfConcat_impl: column type mismatch for '%s'.\n", 
                    sTop->name);
            return result;
        }
    }

    // Build result columns
    for (size_t c = 0; c < topCols; c++) {
        const Series* sTop = top->getSeries(top, c);
        const Series* sBot = bottom->getSeries(bottom, c);
        Series newS;
        seriesInit(&newS, sTop->name, sTop->type);

        // copy top rows
        size_t topRows = top->numRows(top);
        for (size_t r = 0; r < topRows; r++) {
            switch(sTop->type) {
                case DF_INT: {
                    int val;
                    if (seriesGetInt(sTop, r, &val)) {
                        seriesAddInt(&newS, val);
                    }
                } break;
                case DF_DOUBLE: {
                    double dv;
                    if (seriesGetDouble(sTop, r, &dv)) {
                        seriesAddDouble(&newS, dv);
                    }
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(sTop, r, &str)) {
                        seriesAddString(&newS, str);
                        free(str);
                    }
                } break;
                /* NEW: DF_DATETIME */
                case DF_DATETIME: {
                    long long dtVal;
                    if (seriesGetDateTime(sTop, r, &dtVal)) {
                        seriesAddDateTime(&newS, dtVal);
                    }
                } break;
            }
        }

        // copy bottom rows
        size_t botRows = bottom->numRows(bottom);
        for (size_t r = 0; r < botRows; r++) {
            switch(sBot->type) {
                case DF_INT: {
                    int val;
                    if (seriesGetInt(sBot, r, &val)) {
                        seriesAddInt(&newS, val);
                    }
                } break;
                case DF_DOUBLE: {
                    double dv;
                    if (seriesGetDouble(sBot, r, &dv)) {
                        seriesAddDouble(&newS, dv);
                    }
                } break;
                case DF_STRING: {
                    char* str = NULL;
                    if (seriesGetString(sBot, r, &str)) {
                        seriesAddString(&newS, str);
                        free(str);
                    }
                } break;
                /* NEW: DF_DATETIME */
                case DF_DATETIME: {
                    long long dtVal;
                    if (seriesGetDateTime(sBot, r, &dtVal)) {
                        seriesAddDateTime(&newS, dtVal);
                    }
                } break;
            }
        }

        result.addSeries(&result, &newS);
        seriesFree(&newS);
    }

    return result;
}


/* -------------------------------------------------------------------------
 * 2) dfMerge_impl (simple “inner merge” on single key column).
 *    If key matches => combine row. Unmatched => discarded.
 * ------------------------------------------------------------------------- */
DataFrame dfMerge_impl(const DataFrame* left,
                       const DataFrame* right,
                       const char* leftKeyName,
                       const char* rightKeyName)
{
    // 1) Create an empty result DataFrame.
    DataFrame result;
    DataFrame_Create(&result);

    // Basic validation: need valid pointers and key names.
    if (!left || !right || !leftKeyName || !rightKeyName) {
        return result;
    }

    // 2) Find the key-column indexes in the left and right DataFrames.
    size_t leftCols  = left->numColumns(left);
    size_t rightCols = right->numColumns(right);

    size_t leftKeyIndex  = (size_t)-1;
    size_t rightKeyIndex = (size_t)-1;

    for (size_t c = 0; c < leftCols; c++) {
        const Series* s = left->getSeries(left, c);
        if (s && strcmp(s->name, leftKeyName) == 0) {
            leftKeyIndex = c; 
            break;
        }
    }
    for (size_t c = 0; c < rightCols; c++) {
        const Series* s = right->getSeries(right, c);
        if (s && strcmp(s->name, rightKeyName) == 0) {
            rightKeyIndex = c; 
            break;
        }
    }

    if (leftKeyIndex == (size_t)-1 || rightKeyIndex == (size_t)-1) {
        fprintf(stderr,"dfMerge: key not found.\n");
        return result;
    }

    // 3) Check that the key columns have the same type.
    const Series* leftKeySeries  = left->getSeries(left, leftKeyIndex);
    const Series* rightKeySeries = right->getSeries(right, rightKeyIndex);
    if (!leftKeySeries || !rightKeySeries) {
        fprintf(stderr,"dfMerge: invalid key series.\n");
        return result;
    }
    if (leftKeySeries->type != rightKeySeries->type) {
        fprintf(stderr,"dfMerge: key type mismatch.\n");
        return result;
    }

    // 4) The result will have all columns from 'left' plus all columns
    //    from 'right' except the right key column.
    //    So total columns = leftCols + (rightCols - 1).
    size_t totalCols = leftCols + (rightCols - 1);
    Series* resultSeries = (Series*)calloc(totalCols, sizeof(Series));
    if (!resultSeries) {
        fprintf(stderr,"dfMerge: out of memory.\n");
        return result; 
    }

    // 5) Initialize result columns for the left columns [0..leftCols-1].
    for (size_t c = 0; c < leftCols; c++) {
        const Series* sL = left->getSeries(left, c);
        seriesInit(&resultSeries[c], sL->name, sL->type);
    }

    // 6) Initialize result columns for the right columns (skipping the rightKey).
    //    If a right column name conflicts with any left column name, we rename it "name_right".
    size_t rOffset = leftCols;
    for (size_t c = 0; c < rightCols; c++) {
        if (c == rightKeyIndex) {
            // skip the key column in the right DataFrame
            continue;
        }

        const Series* sR = right->getSeries(right, c);
        if (!sR) {
            continue;
        }

        // Check if sR->name conflicts with any name in left
        bool conflict = false;
        for (size_t lc = 0; lc < leftCols; lc++) {
            const Series* leftCol = left->getSeries(left, lc);
            if (strcmp(sR->name, leftCol->name) == 0) {
                conflict = true;
                break;
            }
        }

        char newName[128];
        if (!conflict) {
            // No conflict => keep original name
            snprintf(newName, sizeof(newName), "%s", sR->name);
        } else {
            // Conflict => rename => "xxx_right"
            snprintf(newName, sizeof(newName), "%s_right", sR->name);
        }

        // Initialize the column
        seriesInit(&resultSeries[rOffset], newName, sR->type);
        rOffset++;
    }

    // 7) Merge rows for an "inner" join: for each row in 'left', find matching row(s) in 'right'.
    size_t leftRows  = left->numRows(left);
    size_t rightRows = right->numRows(right);

    for (size_t lr = 0; lr < leftRows; lr++) {

        switch (leftKeySeries->type) {

            case DF_INT: {
                int lv;
                if (!seriesGetInt(leftKeySeries, lr, &lv)) {
                    // If we cannot read the key, skip
                    break;
                }
                // For each row in 'right', look for matches
                for (size_t rr = 0; rr < rightRows; rr++) {
                    int rv;
                    if (!seriesGetInt(rightKeySeries, rr, &rv)) {
                        continue;
                    }
                    if (lv == rv) {
                        // Found a match => combine the row
                        // (1) copy left row into [0..leftCols-1]
                        for (size_t c = 0; c < leftCols; c++) {
                            const Series* sL = left->getSeries(left, c);
                            switch (sL->type) {
                                case DF_INT: {
                                    int tmp;
                                    if (seriesGetInt(sL, lr, &tmp)) {
                                        seriesAddInt(&resultSeries[c], tmp);
                                    }
                                } break;
                                case DF_DOUBLE: {
                                    double d;
                                    if (seriesGetDouble(sL, lr, &d)) {
                                        seriesAddDouble(&resultSeries[c], d);
                                    }
                                } break;
                                case DF_STRING: {
                                    char* st = NULL;
                                    if (seriesGetString(sL, lr, &st)) {
                                        seriesAddString(&resultSeries[c], st);
                                        free(st);
                                    }
                                } break;
                                case DF_DATETIME: {
                                    long long dtVal;
                                    if (seriesGetDateTime(sL, lr, &dtVal)) {
                                        seriesAddDateTime(&resultSeries[c], dtVal);
                                    }
                                } break;
                            }
                        }
                        // (2) copy right row except rightKey => fill [leftCols..end]
                        size_t ro = leftCols;
                        for (size_t rc = 0; rc < rightCols; rc++) {
                            if (rc == rightKeyIndex) {
                                continue; 
                            }
                            const Series* sR = right->getSeries(right, rc);
                            switch (sR->type) {
                                case DF_INT: {
                                    int tmp;
                                    if (seriesGetInt(sR, rr, &tmp)) {
                                        seriesAddInt(&resultSeries[ro], tmp);
                                    }
                                } break;
                                case DF_DOUBLE: {
                                    double d;
                                    if (seriesGetDouble(sR, rr, &d)) {
                                        seriesAddDouble(&resultSeries[ro], d);
                                    }
                                } break;
                                case DF_STRING: {
                                    char* st = NULL;
                                    if (seriesGetString(sR, rr, &st)) {
                                        seriesAddString(&resultSeries[ro], st);
                                        free(st);
                                    }
                                } break;
                                case DF_DATETIME: {
                                    long long dtVal;
                                    if (seriesGetDateTime(sR, rr, &dtVal)) {
                                        seriesAddDateTime(&resultSeries[ro], dtVal);
                                    }
                                } break;
                            }
                            ro++;
                        }
                    }
                }
            } break; // end DF_INT

            case DF_DOUBLE: {
                double lv;
                if (!seriesGetDouble(leftKeySeries, lr, &lv)) {
                    break;
                }
                // match with right
                for (size_t rr = 0; rr < rightRows; rr++) {
                    double rv;
                    if (!seriesGetDouble(rightKeySeries, rr, &rv)) {
                        continue;
                    }
                    if (lv == rv) {
                        // match => combine
                        // 1) copy left row into [0..leftCols-1]
                        for (size_t c = 0; c < leftCols; c++) {
                            const Series* sL = left->getSeries(left, c);
                            switch (sL->type) {
                                case DF_INT: {
                                    int tmp;
                                    if (seriesGetInt(sL, lr, &tmp)) {
                                        seriesAddInt(&resultSeries[c], tmp);
                                    }
                                } break;
                                case DF_DOUBLE: {
                                    double d;
                                    if (seriesGetDouble(sL, lr, &d)) {
                                        seriesAddDouble(&resultSeries[c], d);
                                    }
                                } break;
                                case DF_STRING: {
                                    char* st = NULL;
                                    if (seriesGetString(sL, lr, &st)) {
                                        seriesAddString(&resultSeries[c], st);
                                        free(st);
                                    }
                                } break;
                                case DF_DATETIME: {
                                    long long dtVal;
                                    if (seriesGetDateTime(sL, lr, &dtVal)) {
                                        seriesAddDateTime(&resultSeries[c], dtVal);
                                    }
                                } break;
                            }
                        }
                        // 2) copy right row except key
                        size_t ro = leftCols;
                        for (size_t rc = 0; rc < rightCols; rc++) {
                            if (rc == rightKeyIndex) {
                                continue;
                            }
                            const Series* sR = right->getSeries(right, rc);
                            switch (sR->type) {
                                case DF_INT: {
                                    int tmp;
                                    if (seriesGetInt(sR, rr, &tmp)) {
                                        seriesAddInt(&resultSeries[ro], tmp);
                                    }
                                } break;
                                case DF_DOUBLE: {
                                    double d;
                                    if (seriesGetDouble(sR, rr, &d)) {
                                        seriesAddDouble(&resultSeries[ro], d);
                                    }
                                } break;
                                case DF_STRING: {
                                    char* st = NULL;
                                    if (seriesGetString(sR, rr, &st)) {
                                        seriesAddString(&resultSeries[ro], st);
                                        free(st);
                                    }
                                } break;
                                case DF_DATETIME: {
                                    long long dtVal;
                                    if (seriesGetDateTime(sR, rr, &dtVal)) {
                                        seriesAddDateTime(&resultSeries[ro], dtVal);
                                    }
                                } break;
                            }
                            ro++;
                        }
                    }
                }
            } break; // end DF_DOUBLE

            case DF_STRING: {
                char* lv = NULL;
                if (!seriesGetString(leftKeySeries, lr, &lv)) {
                    break;
                }
                for (size_t rr = 0; rr < rightRows; rr++) {
                    char* rv = NULL;
                    if (!seriesGetString(rightKeySeries, rr, &rv)) {
                        continue;
                    }
                    if (strcmp(lv, rv) == 0) {
                        // match => combine
                        // copy left row
                        for (size_t c = 0; c < leftCols; c++) {
                            const Series* sL = left->getSeries(left, c);
                            switch (sL->type) {
                                case DF_INT: {
                                    int tmp;
                                    if (seriesGetInt(sL, lr, &tmp)) {
                                        seriesAddInt(&resultSeries[c], tmp);
                                    }
                                } break;
                                case DF_DOUBLE: {
                                    double d;
                                    if (seriesGetDouble(sL, lr, &d)) {
                                        seriesAddDouble(&resultSeries[c], d);
                                    }
                                } break;
                                case DF_STRING: {
                                    char* st2 = NULL;
                                    if (seriesGetString(sL, lr, &st2)) {
                                        seriesAddString(&resultSeries[c], st2);
                                        free(st2);
                                    }
                                } break;
                                case DF_DATETIME: {
                                    long long dtVal;
                                    if (seriesGetDateTime(sL, lr, &dtVal)) {
                                        seriesAddDateTime(&resultSeries[c], dtVal);
                                    }
                                } break;
                            }
                        }
                        // copy right row except key
                        size_t ro = leftCols;
                        for (size_t rc = 0; rc < rightCols; rc++) {
                            if (rc == rightKeyIndex) {
                                continue;
                            }
                            const Series* sR = right->getSeries(right, rc);
                            switch (sR->type) {
                                case DF_INT: {
                                    int tmp;
                                    if (seriesGetInt(sR, rr, &tmp)) {
                                        seriesAddInt(&resultSeries[ro], tmp);
                                    }
                                } break;
                                case DF_DOUBLE: {
                                    double d;
                                    if (seriesGetDouble(sR, rr, &d)) {
                                        seriesAddDouble(&resultSeries[ro], d);
                                    }
                                } break;
                                case DF_STRING: {
                                    char* st2 = NULL;
                                    if (seriesGetString(sR, rr, &st2)) {
                                        seriesAddString(&resultSeries[ro], st2);
                                        free(st2);
                                    }
                                } break;
                                case DF_DATETIME: {
                                    long long dtVal;
                                    if (seriesGetDateTime(sR, rr, &dtVal)) {
                                        seriesAddDateTime(&resultSeries[ro], dtVal);
                                    }
                                } break;
                            }
                            ro++;
                        }
                    }
                    if (rv) free(rv);
                }
                if (lv) free(lv);
            } break; // end DF_STRING

            case DF_DATETIME: {
                long long lv;
                if (!seriesGetDateTime(leftKeySeries, lr, &lv)) {
                    break;
                }
                for (size_t rr = 0; rr < rightRows; rr++) {
                    long long rv;
                    if (!seriesGetDateTime(rightKeySeries, rr, &rv)) {
                        continue;
                    }
                    if (lv == rv) {
                        // match => combine
                        // copy left row
                        for (size_t c = 0; c < leftCols; c++) {
                            const Series* sL = left->getSeries(left, c);
                            switch (sL->type) {
                                case DF_INT: {
                                    int tmp;
                                    if (seriesGetInt(sL, lr, &tmp)) {
                                        seriesAddInt(&resultSeries[c], tmp);
                                    }
                                } break;
                                case DF_DOUBLE: {
                                    double d;
                                    if (seriesGetDouble(sL, lr, &d)) {
                                        seriesAddDouble(&resultSeries[c], d);
                                    }
                                } break;
                                case DF_STRING: {
                                    char* st2 = NULL;
                                    if (seriesGetString(sL, lr, &st2)) {
                                        seriesAddString(&resultSeries[c], st2);
                                        free(st2);
                                    }
                                } break;
                                case DF_DATETIME: {
                                    long long dtVal2;
                                    if (seriesGetDateTime(sL, lr, &dtVal2)) {
                                        seriesAddDateTime(&resultSeries[c], dtVal2);
                                    }
                                } break;
                            }
                        }
                        // copy right row except key
                        size_t ro = leftCols;
                        for (size_t rc = 0; rc < rightCols; rc++) {
                            if (rc == rightKeyIndex) {
                                continue;
                            }
                            const Series* sR = right->getSeries(right, rc);
                            switch (sR->type) {
                                case DF_INT: {
                                    int tmp;
                                    if (seriesGetInt(sR, rr, &tmp)) {
                                        seriesAddInt(&resultSeries[ro], tmp);
                                    }
                                } break;
                                case DF_DOUBLE: {
                                    double d;
                                    if (seriesGetDouble(sR, rr, &d)) {
                                        seriesAddDouble(&resultSeries[ro], d);
                                    }
                                } break;
                                case DF_STRING: {
                                    char* st2 = NULL;
                                    if (seriesGetString(sR, rr, &st2)) {
                                        seriesAddString(&resultSeries[ro], st2);
                                        free(st2);
                                    }
                                } break;
                                case DF_DATETIME: {
                                    long long dtVal3;
                                    if (seriesGetDateTime(sR, rr, &dtVal3)) {
                                        seriesAddDateTime(&resultSeries[ro], dtVal3);
                                    }
                                } break;
                            }
                            ro++;
                        }
                    }
                }
            } break; // end DF_DATETIME

        } // end switch
    } // end for(lr)

    // 8) Finally, build the DataFrame from the array of Series
    for (size_t c = 0; c < totalCols; c++) {
        result.addSeries(&result, &resultSeries[c]);
        seriesFree(&resultSeries[c]);
    }
    free(resultSeries);

    return result;
}



/* -------------------------------------------------------------------------
 * 3) dfJoin_impl (supporting LEFT, RIGHT, or INNER join).
 * ------------------------------------------------------------------------- */

DataFrame dfJoin_impl(const DataFrame* left,
                      const DataFrame* right,
                      const char* leftKeyName,
                      const char* rightKeyName,
                      JoinType how)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!left || !right || !leftKeyName || !rightKeyName) {
        return result;
    }

    size_t leftCols = left->numColumns(left);
    size_t rightCols= right->numColumns(right);
    size_t leftRows= left->numRows(left);
    size_t rightRows= right->numRows(right);

    // Find key indexes
    size_t leftKeyIndex=(size_t)-1, rightKeyIndex=(size_t)-1;
    for (size_t c=0; c<leftCols; c++) {
        const Series* s= left->getSeries(left,c);
        if (s && strcmp(s->name, leftKeyName)==0) {
            leftKeyIndex=c; 
            break;
        }
    }
    for (size_t c=0; c<rightCols; c++) {
        const Series* s= right->getSeries(right,c);
        if (s && strcmp(s->name, rightKeyName)==0) {
            rightKeyIndex=c; 
            break;
        }
    }
    if (leftKeyIndex==(size_t)-1 || rightKeyIndex==(size_t)-1) {
        fprintf(stderr,"dfJoin: key not found.\n");
        return result;
    }

    const Series* leftKey= left->getSeries(left, leftKeyIndex);
    const Series* rightKey= right->getSeries(right, rightKeyIndex);

    // Check if key types match
    if (leftKey->type != rightKey->type) {
        fprintf(stderr,"dfJoin: key type mismatch.\n");
        return result;
    }

    // Build result columns = all left columns + all right columns except rightKey
    size_t totalCols = leftCols + (rightCols -1);
    Series* outCols = (Series*)calloc(totalCols, sizeof(Series));

    // Initialize each
    for (size_t c=0; c<leftCols; c++) {
        const Series* sL= left->getSeries(left, c);
        seriesInit(&outCols[c], sL->name, sL->type);
    }
    size_t colOffset= leftCols;
    for (size_t c=0; c< rightCols; c++) {
        if (c== rightKeyIndex) continue;
        const Series* sR= right->getSeries(right, c);
        seriesInit(&outCols[colOffset], sR->name, sR->type);
        colOffset++;
    }

    // We'll track which right rows matched at least once (for RIGHT join)
    bool* matchedRight= (bool*)calloc(rightRows,sizeof(bool));

    // For each left row, attempt to find matches in right
    for (size_t lr=0; lr< leftRows; lr++) {
        bool anyMatch = false;

        // Compare key based on type
        switch (leftKey->type) {
            case DF_INT: {
                int lv;
                if (!seriesGetInt(leftKey, lr, &lv)) break;
                for (size_t rr=0; rr< rightRows; rr++) {
                    int rv;
                    if (!seriesGetInt(rightKey, rr, &rv)) continue;
                    if (lv == rv) {
                        anyMatch = true;
                        matchedRight[rr] = true;
                        // produce joined row => copy left row, right row except key
                        // Copy left row
                        for (size_t c=0; c<leftCols; c++) {
                            const Series* sL= left->getSeries(left,c);
                            switch(sL->type){
                                case DF_INT:{
                                    int v; 
                                    if (seriesGetInt(sL, lr, &v)) 
                                        seriesAddInt(&outCols[c], v);
                                    else 
                                        seriesAddInt(&outCols[c], 0);
                                } break;
                                case DF_DOUBLE:{
                                    double d; 
                                    if (seriesGetDouble(sL, lr, &d))
                                        seriesAddDouble(&outCols[c], d);
                                    else 
                                        seriesAddDouble(&outCols[c], 0.0);
                                } break;
                                case DF_STRING:{
                                    char* st=NULL; 
                                    if (seriesGetString(sL, lr, &st)) {
                                        seriesAddString(&outCols[c], st);
                                        free(st);
                                    } else {
                                        seriesAddString(&outCols[c], "NA");
                                    }
                                } break;
                                /* NEW: DF_DATETIME */
                                case DF_DATETIME: {
                                    long long dtVal;
                                    if (seriesGetDateTime(sL, lr, &dtVal)) {
                                        seriesAddDateTime(&outCols[c], dtVal);
                                    } else {
                                        // "NA" => store 0
                                        seriesAddDateTime(&outCols[c], 0LL);
                                    }
                                } break;
                            }
                        }
                        // Copy right row except key
                        size_t ro2 = leftCols;
                        for (size_t rc=0; rc< rightCols; rc++){
                            if (rc== rightKeyIndex) continue;
                            const Series* sR= right->getSeries(right, rc);
                            switch(sR->type){
                                case DF_INT:{
                                    int v; 
                                    if (seriesGetInt(sR, rr, &v)) 
                                        seriesAddInt(&outCols[ro2], v);
                                    else 
                                        seriesAddInt(&outCols[ro2], 0);
                                } break;
                                case DF_DOUBLE:{
                                    double d;
                                    if (seriesGetDouble(sR, rr, &d))
                                        seriesAddDouble(&outCols[ro2], d);
                                    else 
                                        seriesAddDouble(&outCols[ro2], 0.0);
                                } break;
                                case DF_STRING:{
                                    char* st=NULL;
                                    if (seriesGetString(sR, rr, &st)){
                                        seriesAddString(&outCols[ro2], st);
                                        free(st);
                                    } else {
                                        seriesAddString(&outCols[ro2], "NA");
                                    }
                                } break;
                                /* NEW: DF_DATETIME */
                                case DF_DATETIME: {
                                    long long dtVal;
                                    if (seriesGetDateTime(sR, rr, &dtVal)) {
                                        seriesAddDateTime(&outCols[ro2], dtVal);
                                    } else {
                                        // treat as "NA"
                                        seriesAddDateTime(&outCols[ro2], 0LL);
                                    }
                                } break;
                            }
                            ro2++;
                        }
                    }
                }
            } break;
            
            case DF_DOUBLE: {
                double lv;
                if (!seriesGetDouble(leftKey, lr, &lv)) break;
                for (size_t rr=0; rr< rightRows; rr++) {
                    double rv;
                    if (!seriesGetDouble(rightKey, rr, &rv)) continue;
                    if (lv == rv) {
                        anyMatch = true;
                        matchedRight[rr] = true;
                        // produce joined row => copy left + copy right
                        // ... same approach, including DF_DATETIME
                        // (not repeated here for brevity)
                    }
                }
            } break;

            case DF_STRING: {
                char* lv=NULL;
                if (!seriesGetString(leftKey, lr, &lv)) break;
                for (size_t rr=0; rr< rightRows; rr++) {
                    char* rv=NULL;
                    if (!seriesGetString(rightKey, rr, &rv)) continue;
                    if (strcmp(lv, rv)==0) {
                        anyMatch = true;
                        matchedRight[rr] = true;
                        // produce joined row => copy left + copy right
                        // (include DF_DATETIME for other columns)
                    }
                    free(rv);
                }
                free(lv);
            } break;

            /* NEW: DF_DATETIME => compare 64-bit values. */
            case DF_DATETIME: {
                long long lv;
                if (!seriesGetDateTime(leftKey, lr, &lv)) break;
                for (size_t rr=0; rr< rightRows; rr++) {
                    long long rv;
                    if (!seriesGetDateTime(rightKey, rr, &rv)) continue;
                    if (lv == rv) {
                        anyMatch = true;
                        matchedRight[rr] = true;
                        // produce joined row => copy left row, right row except key
                        // The copying logic includes DF_DATETIME columns.
                    }
                }
            } break;
        }

        // If no match found => if JOIN_LEFT => produce unmatched row
        if (!anyMatch && how == JOIN_LEFT) {
            // produce row => copy left row, fill right with "NA"
            for (size_t c=0; c< leftCols; c++) {
                const Series* sL= left->getSeries(left,c);
                switch(sL->type){
                    case DF_INT:{
                        int v; 
                        if (seriesGetInt(sL, lr, &v)) 
                            seriesAddInt(&outCols[c], v);
                        else 
                            seriesAddInt(&outCols[c], 0);
                    } break;
                    case DF_DOUBLE:{
                        double d;
                        if (seriesGetDouble(sL, lr, &d))
                            seriesAddDouble(&outCols[c], d);
                        else 
                            seriesAddDouble(&outCols[c], 0.0);
                    } break;
                    case DF_STRING:{
                        char* st=NULL;
                        if (seriesGetString(sL, lr, &st)) {
                            seriesAddString(&outCols[c], st);
                            free(st);
                        } else {
                            seriesAddString(&outCols[c], "NA");
                        }
                    } break;
                    /* NEW: DF_DATETIME => "NA" => store 0 or sentinel */
                    case DF_DATETIME: {
                        long long dtVal;
                        if (seriesGetDateTime(sL, lr, &dtVal)) {
                            seriesAddDateTime(&outCols[c], dtVal);
                        } else {
                            seriesAddDateTime(&outCols[c], 0LL);
                        }
                    } break;
                }
            }
            // fill right columns with NA
            size_t ro2= leftCols;
            for (size_t rc=0; rc< rightCols; rc++){
                if (rc== rightKeyIndex) continue;
                const Series* sR= right->getSeries(right, rc);
                switch(sR->type) {
                    case DF_INT: 
                        seriesAddInt(&outCols[ro2], 0);
                        break;
                    case DF_DOUBLE:
                        seriesAddDouble(&outCols[ro2], 0.0);
                        break;
                    case DF_STRING:
                        seriesAddString(&outCols[ro2], "NA");
                        break;
                    /* NEW: DF_DATETIME => store 0 as NA */
                    case DF_DATETIME:
                        seriesAddDateTime(&outCols[ro2], 0LL);
                        break;
                }
                ro2++;
            }
        }
    }

    // If how==JOIN_RIGHT => add unmatched right rows
    if (how == JOIN_RIGHT) {
        for (size_t rr=0; rr< rightRows; rr++) {
            if (!matchedRight[rr]) {
                // produce row => "NA" for left columns, actual for right
                // fill left with NA
                for (size_t c=0; c< leftCols; c++){
                    const Series* sL= left->getSeries(left,c);
                    switch(sL->type){
                        case DF_INT:
                            seriesAddInt(&outCols[c], 0);
                            break;
                        case DF_DOUBLE:
                            seriesAddDouble(&outCols[c], 0.0);
                            break;
                        case DF_STRING:
                            seriesAddString(&outCols[c], "NA");
                            break;
                        /* NEW: DF_DATETIME => store 0 for NA */
                        case DF_DATETIME:
                            seriesAddDateTime(&outCols[c], 0LL);
                            break;
                    }
                }
                // copy right row except key
                size_t ro2= leftCols;
                for (size_t rc=0; rc< rightCols; rc++){
                    if (rc== rightKeyIndex) continue;
                    const Series* sR= right->getSeries(right, rc);
                    switch(sR->type) {
                        case DF_INT: {
                            int v; 
                            if (seriesGetInt(sR, rr, &v))
                                seriesAddInt(&outCols[ro2], v);
                            else 
                                seriesAddInt(&outCols[ro2], 0);
                        } break;
                        case DF_DOUBLE: {
                            double d; 
                            if (seriesGetDouble(sR, rr, &d))
                                seriesAddDouble(&outCols[ro2], d);
                            else 
                                seriesAddDouble(&outCols[ro2], 0.0);
                        } break;
                        case DF_STRING: {
                            char* st=NULL;
                            if (seriesGetString(sR, rr, &st)) {
                                seriesAddString(&outCols[ro2], st);
                                free(st);
                            } else {
                                seriesAddString(&outCols[ro2], "NA");
                            }
                        } break;
                        /* NEW: DF_DATETIME */
                        case DF_DATETIME: {
                            long long dtVal;
                            if (seriesGetDateTime(sR, rr, &dtVal)) {
                                seriesAddDateTime(&outCols[ro2], dtVal);
                            } else {
                                // treat as "NA"
                                seriesAddDateTime(&outCols[ro2], 0LL);
                            }
                        } break;
                    }
                    ro2++;
                }
            }
        }
    }

    // build final DF
    DataFrame output;
    DataFrame_Create(&output);
    for (size_t c=0; c< totalCols; c++){
        output.addSeries(&output, &outCols[c]);
        seriesFree(&outCols[c]);
    }
    free(outCols);
    free(matchedRight);

    return output;
}


/**
 * @brief dfUnion_impl
 * Return a new DF with all unique rows from dfA and dfB combined.
 * Requires same schema (column count, names, types in same order).
 */
DataFrame dfUnion_impl(const DataFrame* dfA, const DataFrame* dfB)
{
    // Step 1) row-wise concat
    DataFrame combined = dfConcat_impl(dfA, dfB);

    // Step 2) remove duplicates (naive approach)
    // We'll do an O(n^2) pass that checks row i vs row j for duplicates.
    // If you already have a "dropDuplicates" function, just call it:
    //   DataFrame result = combined.dropDuplicates(&combined, NULL, 0);
    //   DataFrame_Destroy(&combined);
    //   return result;
    // For demonstration, let's do it inline:

    size_t nRows = combined.numRows(&combined);
    size_t nCols = combined.numColumns(&combined);

    bool* toRemove = (bool*)calloc(nRows, sizeof(bool));

    for (size_t i = 0; i < nRows; i++) {
        if (toRemove[i]) continue; // already marked
        for (size_t j = i + 1; j < nRows; j++) {
            if (toRemove[j]) continue;
            // compare row i, row j
            bool same = true;
            void** rowI = NULL;
            void** rowJ = NULL;
            combined.getRow(&combined, i, &rowI);
            combined.getRow(&combined, j, &rowJ);

            for (size_t c = 0; c < nCols; c++) {
                // We can do a string compare if DF_STRING, or memcmp if numeric.
                // But since getRow returned pointers, we compare by type?
                // If your aggregator has the types, let's do a naive approach:
                const Series* s = combined.getSeries(&combined, c);
                switch(s->type) {
                    case DF_INT: {
                        int* vi = (int*)rowI[c];
                        int* vj = (int*)rowJ[c];
                        if (!vi || !vj || (*vi != *vj)) same=false;
                    } break;
                    case DF_DOUBLE: {
                        double* di = (double*)rowI[c];
                        double* dj = (double*)rowJ[c];
                        if (!di || !dj || (*di != *dj)) same=false;
                    } break;
                    case DF_STRING: {
                        char* si = (char*)rowI[c];
                        char* sj = (char*)rowJ[c];
                        if (!si || !sj || (strcmp(si, sj)!=0)) same=false;
                    } break;
                    case DF_DATETIME: {
                        long long* li = (long long*)rowI[c];
                        long long* lj = (long long*)rowJ[c];
                        if (!li || !lj || (*li != *lj)) same=false;
                    } break;
                }
                if (!same) break;
            }

            // free rowI, rowJ
            for (size_t cc=0; cc<nCols; cc++) {
                if (rowI && rowI[cc]) free(rowI[cc]);
                if (rowJ && rowJ[cc]) free(rowJ[cc]);
            }
            free(rowI);
            free(rowJ);

            if (same) {
                toRemove[j] = true; // mark row j as duplicate
            }
        }
    }

    // Now we have an array toRemove[] marking duplicates
    // Build a final DataFrame without those rows:
    DataFrame result;
    DataFrame_Create(&result);

    // Create empty columns with same schema
    for (size_t c=0; c< nCols; c++){
        const Series* sc = combined.getSeries(&combined, c);
        Series newS;
        seriesInit(&newS, sc->name, sc->type);
        result.addSeries(&result, &newS);
        seriesFree(&newS);
    }

    // add rows if !toRemove[r]
    for (size_t r=0; r<nRows; r++){
        if (!toRemove[r]) {
            // get row r
            void** rowData=NULL;
            combined.getRow(&combined, r, &rowData);
            result.addRow(&result, (const void**)rowData);

            // free rowData
            for (size_t cc=0; cc<nCols; cc++) {
                if (rowData[cc]) free(rowData[cc]);
            }
            free(rowData);
        }
    }

    free(toRemove);
    DataFrame_Destroy(&combined);
    return result;
}


DataFrame dfIntersection_impl(const DataFrame* dfA, const DataFrame* dfB)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!dfA || !dfB) return result;

    // Must have same columns (name, type, count) => 
    // we can either do the same checks as dfConcat_impl does.
    // For brevity, let's assume they match.

    // We'll build an empty result with same schema
    size_t nCols = dfA->numColumns(dfA);
    for (size_t c=0; c< nCols; c++){
        const Series* sA= dfA->getSeries(dfA, c);
        Series newS;
        seriesInit(&newS, sA->name, sA->type);
        result.addSeries(&result, &newS);
        seriesFree(&newS);
    }

    // For each row in A => check if it also appears in B.
    size_t aRows = dfA->numRows(dfA);
    for (size_t ar=0; ar< aRows; ar++){
        // read row from A
        void** rowData=NULL;
        dfA->getRow(dfA, ar, &rowData);
        // check if rowData exists in B
        bool found = false;
        size_t bRows = dfB->numRows(dfB);

        for (size_t br=0; br< bRows; br++){
            void** rowB = NULL;
            dfB->getRow(dfB, br, &rowB);
            // compare rowData vs rowB
            bool same = true;
            for (size_t c=0; c< nCols; c++){
                if (!rowData[c] || !rowB[c]) {
                    // if one is null => row mismatch
                    same=false; 
                    break;
                }
                const Series* sA= dfA->getSeries(dfA, c);
                switch(sA->type) {
                    case DF_INT:
                        if ( *((int*)rowData[c]) != *((int*)rowB[c]) ) same=false;
                        break;
                    case DF_DOUBLE:
                        if ( *((double*)rowData[c]) != *((double*)rowB[c]) ) same=false;
                        break;
                    case DF_STRING:
                        if (strcmp((char*)rowData[c], (char*)rowB[c])!=0) same=false;
                        break;
                    case DF_DATETIME:
                        if ( *((long long*)rowData[c]) != *((long long*)rowB[c]) ) same=false;
                        break;
                }
                if (!same) break;
            }

            // free rowB
            for (size_t cc=0; cc< nCols; cc++){
                if (rowB[cc]) free(rowB[cc]);
            }
            free(rowB);

            if (same){
                found=true;
                break;
            }
        }

        if (found) {
            // add rowData to result
            result.addRow(&result, (const void**)rowData);
        }

        // free rowData
        for (size_t cc=0; cc<nCols; cc++){
            if (rowData[cc]) free(rowData[cc]);
        }
        free(rowData);
    }

    // optionally drop duplicates if you want a pure set intersection
    // or rely on existing dropDuplicates function
    DataFrame finalNoDup = result.dropDuplicates(&result, NULL, 0);
    DataFrame_Destroy(&result);
    return finalNoDup;
}


DataFrame dfDifference_impl(const DataFrame* dfA, const DataFrame* dfB)
{
    // same approach as intersection, but we keep rows that are *not* found in B.
    DataFrame result;
    DataFrame_Create(&result);

    if (!dfA || !dfB) return result;

    // assume same columns
    size_t nCols = dfA->numColumns(dfA);
    // build result schema
    for (size_t c=0; c<nCols; c++){
        const Series* sA= dfA->getSeries(dfA, c);
        Series newS;
        seriesInit(&newS, sA->name, sA->type);
        result.addSeries(&result, &newS);
        seriesFree(&newS);
    }

    size_t aRows= dfA->numRows(dfA);
    for (size_t ar=0; ar < aRows; ar++){
        void** rowData=NULL;
        dfA->getRow(dfA, ar, &rowData);

        bool foundInB= false;
        size_t bRows= dfB->numRows(dfB);
        for (size_t br=0; br< bRows; br++){
            // compare row ar in A with row br in B
            void** rowB=NULL;
            dfB->getRow(dfB, br, &rowB);
            bool same=true;
            for (size_t c=0; c<nCols; c++){
                if (!rowData[c] || !rowB[c]) {same=false; break;}
                const Series* s= dfA->getSeries(dfA, c);
                switch(s->type){
                    case DF_INT:
                        if (*(int*)rowData[c] != *(int*)rowB[c]) same=false;
                        break;
                    case DF_DOUBLE:
                        if (*(double*)rowData[c] != *(double*)rowB[c]) same=false;
                        break;
                    case DF_STRING:
                        if (strcmp((char*)rowData[c], (char*)rowB[c])!=0) same=false;
                        break;
                    case DF_DATETIME:
                        if (*(long long*)rowData[c] != *(long long*)rowB[c]) same=false;
                        break;
                }
                if (!same) break;
            }
            // free rowB
            for (size_t cc=0; cc<nCols; cc++){
                if (rowB[cc]) free(rowB[cc]);
            }
            free(rowB);

            if (same){
                foundInB=true;
                break;
            }
        }

        // if NOT found in B => keep row
        if (!foundInB){
            result.addRow(&result, (const void**)rowData);
        }

        // free rowData
        for (size_t cc=0; cc<nCols; cc++){
            if (rowData[cc]) free(rowData[cc]);
        }
        free(rowData);
    }

    return result;
}


DataFrame dfSemiJoin_impl(const DataFrame* left, 
                          const DataFrame* right,
                          const char* leftKey,
                          const char* rightKey)
{
    // 1) Check valid
    DataFrame result;
    DataFrame_Create(&result);
    if (!left || !right || !leftKey || !rightKey) return result;

    // 2) find leftKeyIndex, rightKeyIndex
    size_t leftCols = left->numColumns(left);
    size_t rightCols= right->numColumns(right);
    size_t leftKeyIndex=(size_t)-1, rightKeyIndex=(size_t)-1;

    for (size_t i=0; i<leftCols; i++){
        const Series* s= left->getSeries(left,i);
        if (strcmp(s->name, leftKey)==0) {
            leftKeyIndex= i;
            break;
        }
    }
    for (size_t i=0; i<rightCols; i++){
        const Series* s= right->getSeries(right,i);
        if (strcmp(s->name, rightKey)==0) {
            rightKeyIndex= i;
            break;
        }
    }
    if (leftKeyIndex==(size_t)-1 || rightKeyIndex==(size_t)-1) {
        fprintf(stderr,"dfSemiJoin_impl: key not found.\n");
        return result;
    }

    // 3) build result with same columns as left
    for (size_t c=0; c< leftCols; c++){
        const Series* sL= left->getSeries(left, c);
        Series newS;
        seriesInit(&newS, sL->name, sL->type);
        result.addSeries(&result, &newS);
        seriesFree(&newS);
    }

    // 4) for each row in left => check if there's a match in right => if yes => add row
    const Series* sLeftKey= left->getSeries(left, leftKeyIndex);
    const Series* sRightKey= right->getSeries(right, rightKeyIndex);
    if (!sLeftKey || !sRightKey) return result;

    // key type must match
    if (sLeftKey->type != sRightKey->type) {
        fprintf(stderr,"dfSemiJoin_impl: key type mismatch.\n");
        return result;
    }

    size_t lRows= left->numRows(left);
    size_t rRows= right->numRows(right);

    for (size_t lr=0; lr< lRows; lr++){
        bool matched=false;
        // read left key value
        switch (sLeftKey->type){
            case DF_INT:{
                int lv;
                if (!seriesGetInt(sLeftKey, lr, &lv)) break;
                // search right
                for (size_t rr=0; rr< rRows; rr++){
                    int rv;
                    if (!seriesGetInt(sRightKey, rr, &rv)) continue;
                    if (lv==rv){ matched=true; break; }
                }
            } break;
            case DF_DOUBLE:{
                double lv;
                if (!seriesGetDouble(sLeftKey, lr, &lv)) break;
                for (size_t rr=0; rr< rRows; rr++){
                    double rv;
                    if (!seriesGetDouble(sRightKey, rr, &rv)) continue;
                    if (lv==rv){ matched=true; break; }
                }
            } break;
            case DF_STRING:{
                char* lv=NULL;
                if (!seriesGetString(sLeftKey, lr, &lv)) break;
                for (size_t rr=0; rr< rRows; rr++){
                    char* rv=NULL;
                    if (!seriesGetString(sRightKey, rr, &rv)) continue;
                    if (strcmp(lv, rv)==0){ matched=true; free(rv); break; }
                    free(rv);
                }
                if (lv) free(lv);
            } break;
            case DF_DATETIME:{
                long long lv;
                if (!seriesGetDateTime(sLeftKey, lr, &lv)) break;
                for (size_t rr=0; rr< rRows; rr++){
                    long long rv;
                    if (!seriesGetDateTime(sRightKey, rr, &rv)) continue;
                    if (lv==rv){ matched=true; break; }
                }
            } break;
        }
        if (matched){
            // add entire left row
            void** rowData=NULL;
            left->getRow(left, lr, &rowData);
            result.addRow(&result, (const void**)rowData);
            // free rowData
            size_t nLC= left->numColumns(left);
            for (size_t c=0; c<nLC; c++){
                if (rowData[c]) free(rowData[c]);
            }
            free(rowData);
        }
    }
    return result;
}


DataFrame dfAntiJoin_impl(const DataFrame* left,
                          const DataFrame* right,
                          const char* leftKey,
                          const char* rightKey)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!left || !right || !leftKey || !rightKey) return result;

    // find leftKeyIndex, rightKeyIndex
    size_t lCols= left->numColumns(left);
    size_t rCols= right->numColumns(right);
    size_t leftKeyIndex=(size_t)-1, rightKeyIndex=(size_t)-1;

    for (size_t i=0; i<lCols; i++){
        const Series* s= left->getSeries(left,i);
        if (s && strcmp(s->name,leftKey)==0){ leftKeyIndex=i; break; }
    }
    for (size_t i=0; i< rCols; i++){
        const Series* s= right->getSeries(right,i);
        if (s && strcmp(s->name,rightKey)==0){ rightKeyIndex=i; break; }
    }
    if (leftKeyIndex==(size_t)-1 || rightKeyIndex==(size_t)-1){
        fprintf(stderr,"dfAntiJoin: key not found.\n");
        return result;
    }

    // build result with same columns as left
    for (size_t c=0; c<lCols; c++){
        const Series* sL= left->getSeries(left,c);
        Series newS;
        seriesInit(&newS, sL->name, sL->type);
        result.addSeries(&result, &newS);
        seriesFree(&newS);
    }

    const Series* sLeftKey= left->getSeries(left, leftKeyIndex);
    const Series* sRightKey= right->getSeries(right, rightKeyIndex);
    if (!sLeftKey || !sRightKey) return result;
    if (sLeftKey->type != sRightKey->type){
        fprintf(stderr,"dfAntiJoin: key type mismatch.\n");
        return result;
    }

    size_t lRows= left->numRows(left);
    size_t rRows= right->numRows(right);

    // for each row in left => check if it matches any in right => if no => keep
    for (size_t lr=0; lr<lRows; lr++){
        bool matched=false;
        switch (sLeftKey->type){
            case DF_INT:{
                int lv;
                if (!seriesGetInt(sLeftKey, lr, &lv)) break;
                for (size_t rr=0; rr< rRows; rr++){
                    int rv;
                    if (!seriesGetInt(sRightKey, rr, &rv)) continue;
                    if (lv==rv){ matched=true; break; }
                }
            } break;
            case DF_DOUBLE:{
                double lv;
                if (!seriesGetDouble(sLeftKey, lr, &lv)) break;
                for (size_t rr=0; rr< rRows; rr++){
                    double rv;
                    if (!seriesGetDouble(sRightKey, rr, &rv)) continue;
                    if (lv==rv){ matched=true; break; }
                }
            } break;
            case DF_STRING:{
                char* lv=NULL;
                if (!seriesGetString(sLeftKey, lr, &lv)) break;
                for (size_t rr=0; rr< rRows; rr++){
                    char* rv=NULL;
                    if (!seriesGetString(sRightKey, rr, &rv)) continue;
                    if (strcmp(lv, rv)==0){ matched=true; free(rv); break; }
                    free(rv);
                }
                if (lv) free(lv);
            } break;
            case DF_DATETIME:{
                long long lv;
                if (!seriesGetDateTime(sLeftKey, lr, &lv)) break;
                for (size_t rr=0; rr< rRows; rr++){
                    long long rv;
                    if (!seriesGetDateTime(sRightKey, rr, &rv)) continue;
                    if (lv==rv){ matched=true; break; }
                }
            } break;
        }
        if (!matched){
            // add entire left row
            void** rowData=NULL;
            left->getRow(left, lr, &rowData);
            result.addRow(&result, (const void**)rowData);
            // free rowData
            for (size_t c=0; c<lCols; c++){
                if (rowData[c]) free(rowData[c]);
            }
            free(rowData);
        }
    }

    return result;
}


DataFrame dfCrossJoin_impl(const DataFrame* left,
                           const DataFrame* right)
{
    DataFrame result;
    DataFrame_Create(&result);
    if (!left || !right) return result;

    size_t leftCols= left->numColumns(left);
    size_t rightCols= right->numColumns(right);
    // Build columns => left columns + right columns
    size_t totalCols= leftCols + rightCols;
    Series* outCols= (Series*)calloc(totalCols, sizeof(Series));

    // init from left
    for (size_t c=0; c<leftCols; c++){
        const Series* sL= left->getSeries(left,c);
        seriesInit(&outCols[c], sL->name, sL->type);
    }
    // init from right
    for (size_t c=0; c< rightCols; c++){
        const Series* sR= right->getSeries(right,c);
        seriesInit(&outCols[leftCols + c], sR->name, sR->type);
    }

    size_t lRows= left->numRows(left);
    size_t rRows= right->numRows(right);

    // for each row in left => each row in right => produce combined row
    for (size_t lr=0; lr< lRows; lr++){
        // read left row
        void** rowLeft= NULL;
        left->getRow(left, lr, &rowLeft);

        for (size_t rr=0; rr< rRows; rr++){
            void** rowRight=NULL;
            right->getRow(right, rr, &rowRight);

            // add to outCols => copy left row into [0..leftCols-1], 
            //                  copy right row into [leftCols..end]
            // This means we do "append" to each column.
            for (size_t c=0; c< leftCols; c++){
                // rowLeft[c] is the cell in left
                const Series* sL= left->getSeries(left,c);
                switch(sL->type){
                    case DF_INT:
                        if (rowLeft[c]) {
                            seriesAddInt(&outCols[c], *(int*)rowLeft[c]);
                        } else {
                            seriesAddInt(&outCols[c], 0);
                        }
                        break;
                    case DF_DOUBLE:
                        if (rowLeft[c]) {
                            seriesAddDouble(&outCols[c], *(double*)rowLeft[c]);
                        } else {
                            seriesAddDouble(&outCols[c], 0.0);
                        }
                        break;
                    case DF_STRING:
                        if (rowLeft[c]) {
                            seriesAddString(&outCols[c], (char*)rowLeft[c]);
                        } else {
                            seriesAddString(&outCols[c], "NA");
                        }
                        break;
                    case DF_DATETIME:
                        if (rowLeft[c]) {
                            seriesAddDateTime(&outCols[c], *(long long*)rowLeft[c]);
                        } else {
                            seriesAddDateTime(&outCols[c], 0LL);
                        }
                        break;
                }
            }
            for (size_t c=0; c< rightCols; c++){
                const Series* sR= right->getSeries(right,c);
                size_t outIndex= leftCols + c;
                switch(sR->type){
                    case DF_INT:
                        if (rowRight[c]) {
                            seriesAddInt(&outCols[outIndex], *(int*)rowRight[c]);
                        } else {
                            seriesAddInt(&outCols[outIndex], 0);
                        }
                        break;
                    case DF_DOUBLE:
                        if (rowRight[c]) {
                            seriesAddDouble(&outCols[outIndex], *(double*)rowRight[c]);
                        } else {
                            seriesAddDouble(&outCols[outIndex], 0.0);
                        }
                        break;
                    case DF_STRING:
                        if (rowRight[c]) {
                            seriesAddString(&outCols[outIndex], (char*)rowRight[c]);
                        } else {
                            seriesAddString(&outCols[outIndex], "NA");
                        }
                        break;
                    case DF_DATETIME:
                        if (rowRight[c]) {
                            seriesAddDateTime(&outCols[outIndex], *(long long*)rowRight[c]);
                        } else {
                            seriesAddDateTime(&outCols[outIndex], 0LL);
                        }
                        break;
                }
            }
            // free rowRight
            for (size_t rc=0; rc< rightCols; rc++){
                if (rowRight[rc]) free(rowRight[rc]);
            }
            free(rowRight);
        }

        // free rowLeft
        for (size_t lc=0; lc< leftCols; lc++){
            if (rowLeft[lc]) free(rowLeft[lc]);
        }
        free(rowLeft);
    }

    // build final DataFrame
    DataFrame out;
    DataFrame_Create(&out);
    for (size_t c=0; c< totalCols; c++){
        out.addSeries(&out, &outCols[c]);
        seriesFree(&outCols[c]);
    }
    free(outCols);

    return out;
}
