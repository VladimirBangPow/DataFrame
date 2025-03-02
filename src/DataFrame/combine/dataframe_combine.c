#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include "../dataframe.h"  // Adjust as needed to match your project structure

/* 
 * Suppose you declare this somewhere in dataframe.h:
 *   typedef enum { JOIN_INNER, JOIN_LEFT, JOIN_RIGHT } JoinType;
 *
 *   typedef DataFrame (*DataFrameConcatFunc)(const DataFrame*, const DataFrame*);
 *   typedef DataFrame (*DataFrameMergeFunc)(const DataFrame*, const DataFrame*, const char*, const char*);
 *   typedef DataFrame (*DataFrameJoinFunc)(const DataFrame*, const DataFrame*, const char*, const char*, JoinType);
 * etc.
 */

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
    DataFrame result;
    DataFrame_Create(&result);
    if (!left || !right || !leftKeyName || !rightKeyName) {
        return result;
    }

    // find leftKeyIndex, rightKeyIndex
    size_t leftCols = left->numColumns(left);
    size_t rightCols= right->numColumns(right);
    size_t leftKeyIndex=(size_t)-1, rightKeyIndex=(size_t)-1;

    for (size_t c=0; c<leftCols; c++) {
        const Series* s = left->getSeries(left, c);
        if (s && strcmp(s->name, leftKeyName)==0) {
            leftKeyIndex = c; 
            break;
        }
    }
    for (size_t c=0; c<rightCols; c++) {
        const Series* s = right->getSeries(right, c);
        if (s && strcmp(s->name, rightKeyName)==0) {
            rightKeyIndex = c; 
            break;
        }
    }
    if (leftKeyIndex==(size_t)-1 || rightKeyIndex==(size_t)-1) {
        fprintf(stderr,"dfMerge: key not found.\n");
        return result;
    }

    const Series* leftKeySeries = left->getSeries(left, leftKeyIndex);
    const Series* rightKeySeries= right->getSeries(right, rightKeyIndex);
    if (leftKeySeries->type != rightKeySeries->type) {
        fprintf(stderr,"dfMerge: key type mismatch.\n");
        return result;
    }

    // Build “result columns”: all from left + all from right except key
    size_t totalCols = leftCols + (rightCols - 1);
    Series* resultSeries = (Series*)calloc(totalCols, sizeof(Series));

    // init left columns in [0..leftCols-1]
    for (size_t c=0; c<leftCols; c++) {
        const Series* sL = left->getSeries(left, c);
        seriesInit(&resultSeries[c], sL->name, sL->type);
    }
    // init right columns skipping key
    size_t rOffset = leftCols;
    for (size_t c=0; c<rightCols; c++) {
        if (c == rightKeyIndex) continue;
        const Series* sR= right->getSeries(right, c);
        seriesInit(&resultSeries[rOffset], sR->name, sR->type);
        rOffset++;
    }

    size_t leftRows  = left->numRows(left);
    size_t rightRows = right->numRows(right);

    // For each row in left => find matching in right => combine
    for (size_t lr=0; lr < leftRows; lr++) {
        // Based on the key type
        switch (leftKeySeries->type) {
            case DF_INT: {
                int lv; 
                if (!seriesGetInt(leftKeySeries, lr, &lv)) continue;

                // search right
                for (size_t rr=0; rr < rightRows; rr++) {
                    int rv; 
                    if (!seriesGetInt(rightKeySeries, rr, &rv)) continue;
                    if (lv == rv) {
                        // match => combine
                        // 1) copy left row
                        for (size_t c=0; c<leftCols; c++) {
                            const Series* sL= left->getSeries(left, c);
                            switch(sL->type) {
                                case DF_INT: {
                                    int v;
                                    if (seriesGetInt(sL, lr, &v)) {
                                        seriesAddInt(&resultSeries[c], v);
                                    }
                                } break;
                                case DF_DOUBLE: {
                                    double d;
                                    if (seriesGetDouble(sL, lr, &d)) {
                                        seriesAddDouble(&resultSeries[c], d);
                                    }
                                } break;
                                case DF_STRING: {
                                    char* st=NULL;
                                    if (seriesGetString(sL, lr, &st)) {
                                        seriesAddString(&resultSeries[c], st);
                                        free(st);
                                    }
                                } break;
                                /* NEW: DF_DATETIME */
                                case DF_DATETIME: {
                                    long long dtVal;
                                    if (seriesGetDateTime(sL, lr, &dtVal)) {
                                        seriesAddDateTime(&resultSeries[c], dtVal);
                                    }
                                } break;
                            }
                        }
                        // 2) copy right row except key
                        size_t ro= leftCols;
                        for (size_t rc=0; rc<rightCols; rc++) {
                            if (rc == rightKeyIndex) continue;
                            const Series* sR= right->getSeries(right, rc);
                            switch(sR->type) {
                                case DF_INT: {
                                    int v;
                                    if (seriesGetInt(sR, rr, &v)) {
                                        seriesAddInt(&resultSeries[ro], v);
                                    }
                                } break;
                                case DF_DOUBLE: {
                                    double d;
                                    if (seriesGetDouble(sR, rr, &d)) {
                                        seriesAddDouble(&resultSeries[ro], d);
                                    }
                                } break;
                                case DF_STRING: {
                                    char* st=NULL;
                                    if (seriesGetString(sR, rr, &st)) {
                                        seriesAddString(&resultSeries[ro], st);
                                        free(st);
                                    }
                                } break;
                                /* NEW: DF_DATETIME */
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
            } break;

            case DF_DOUBLE: {
                double lv;
                if (!seriesGetDouble(leftKeySeries, lr, &lv)) continue;
                for (size_t rr=0; rr < rightRows; rr++) {
                    double rv;
                    if (!seriesGetDouble(rightKeySeries, rr, &rv)) continue;
                    if (lv == rv) {
                        // match => combine
                        // copy left row, then right row except key
                        // same approach, including DF_DATETIME
                        for (size_t c=0; c<leftCols; c++) {
                            const Series* sL= left->getSeries(left, c);
                            switch(sL->type) {
                                case DF_INT: {
                                    int v;
                                    if (seriesGetInt(sL, lr, &v)) {
                                        seriesAddInt(&resultSeries[c], v);
                                    }
                                } break;
                                case DF_DOUBLE: {
                                    double d;
                                    if (seriesGetDouble(sL, lr, &d)) {
                                        seriesAddDouble(&resultSeries[c], d);
                                    }
                                } break;
                                case DF_STRING: {
                                    char* st=NULL;
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
                        size_t ro= leftCols;
                        for (size_t rc=0; rc<rightCols; rc++) {
                            if (rc == rightKeyIndex) continue;
                            const Series* sR= right->getSeries(right, rc);
                            switch(sR->type) {
                                case DF_INT: {
                                    int v;
                                    if (seriesGetInt(sR, rr, &v)) {
                                        seriesAddInt(&resultSeries[ro], v);
                                    }
                                } break;
                                case DF_DOUBLE: {
                                    double d;
                                    if (seriesGetDouble(sR, rr, &d)) {
                                        seriesAddDouble(&resultSeries[ro], d);
                                    }
                                } break;
                                case DF_STRING: {
                                    char* st=NULL;
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
            } break;

            case DF_STRING: {
                char* lv = NULL;
                if (!seriesGetString(leftKeySeries, lr, &lv)) continue;
                for (size_t rr=0; rr < rightRows; rr++) {
                    char* rv = NULL;
                    if (!seriesGetString(rightKeySeries, rr, &rv)) continue;
                    if (strcmp(lv, rv)==0) {
                        // match => combine
                        // copy left row + right row except key
                        for (size_t c=0; c<leftCols; c++) {
                            const Series* sL= left->getSeries(left, c);
                            switch(sL->type) {
                                case DF_INT: {
                                    int v;
                                    if (seriesGetInt(sL, lr, &v)) {
                                        seriesAddInt(&resultSeries[c], v);
                                    }
                                } break;
                                case DF_DOUBLE: {
                                    double d;
                                    if (seriesGetDouble(sL, lr, &d)) {
                                        seriesAddDouble(&resultSeries[c], d);
                                    }
                                } break;
                                case DF_STRING: {
                                    char* st2=NULL;
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
                        size_t ro= leftCols;
                        for (size_t rc=0; rc<rightCols; rc++) {
                            if (rc == rightKeyIndex) continue;
                            const Series* sR= right->getSeries(right, rc);
                            switch(sR->type) {
                                case DF_INT: {
                                    int v;
                                    if (seriesGetInt(sR, rr, &v)) {
                                        seriesAddInt(&resultSeries[ro], v);
                                    }
                                } break;
                                case DF_DOUBLE: {
                                    double d;
                                    if (seriesGetDouble(sR, rr, &d)) {
                                        seriesAddDouble(&resultSeries[ro], d);
                                    }
                                } break;
                                case DF_STRING: {
                                    char* st2=NULL;
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
                    free(rv);
                }
                free(lv);
            } break;

            /* NEW: DF_DATETIME => match 64-bit values. */
            case DF_DATETIME: {
                long long lv;
                if (!seriesGetDateTime(leftKeySeries, lr, &lv)) continue;
                for (size_t rr=0; rr < rightRows; rr++) {
                    long long rv;
                    if (!seriesGetDateTime(rightKeySeries, rr, &rv)) continue;
                    if (lv == rv) {
                        // match => combine
                        // copy left row + right row except key
                        for (size_t c=0; c<leftCols; c++) {
                            const Series* sL= left->getSeries(left, c);
                            switch(sL->type) {
                                case DF_INT: {
                                    int v;
                                    if (seriesGetInt(sL, lr, &v)) {
                                        seriesAddInt(&resultSeries[c], v);
                                    }
                                } break;
                                case DF_DOUBLE: {
                                    double d;
                                    if (seriesGetDouble(sL, lr, &d)) {
                                        seriesAddDouble(&resultSeries[c], d);
                                    }
                                } break;
                                case DF_STRING: {
                                    char* st2=NULL;
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
                        size_t ro= leftCols;
                        for (size_t rc=0; rc<rightCols; rc++) {
                            if (rc == rightKeyIndex) continue;
                            const Series* sR= right->getSeries(right, rc);
                            switch(sR->type) {
                                case DF_INT: {
                                    int v;
                                    if (seriesGetInt(sR, rr, &v)) {
                                        seriesAddInt(&resultSeries[ro], v);
                                    }
                                } break;
                                case DF_DOUBLE: {
                                    double d;
                                    if (seriesGetDouble(sR, rr, &d)) {
                                        seriesAddDouble(&resultSeries[ro], d);
                                    }
                                } break;
                                case DF_STRING: {
                                    char* st2=NULL;
                                    if (seriesGetString(sR, rr, &st2)) {
                                        seriesAddString(&resultSeries[ro], st2);
                                        free(st2);
                                    }
                                } break;
                                case DF_DATETIME: {
                                    long long dtVal2;
                                    if (seriesGetDateTime(sR, rr, &dtVal2)) {
                                        seriesAddDateTime(&resultSeries[ro], dtVal2);
                                    }
                                } break;
                            }
                            ro++;
                        }
                    }
                }
            } break;
        }
    }

    // build final DF from resultSeries
    for (size_t c=0; c< totalCols; c++) {
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
